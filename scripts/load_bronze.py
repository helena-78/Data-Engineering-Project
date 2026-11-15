#!/usr/bin/env python3
"""Load RIK open-data CSV files into the ClickHouse bronze tables.

Usage examples (inside the Airflow container paths; adjust on the host):
    python scripts/load_bronze.py --data-dir /opt/airflow/data/sample
    python scripts/load_bronze.py --bundle /opt/airflow/data/sample/rik_bronze_sample_bundle.txt
    python scripts/load_bronze.py --data-dir /opt/airflow/data/raw --init-sql-dir /opt/airflow/clickhouse/init

What it does, per file:
  1. maps the file name to a bronze table (scripts/bronze_schema.py; real RIK names and sample names
     are both recognised, a .zip with one CSV inside is read directly);
  2. validates the header against the expected columns and fails on missing or unexpected columns;
  3. parses the CSV with Python's csv module (';' separator, '"' quotes, CRLF, UTF-8 with optional BOM),
     fails on any row whose field count differs from the header;
  4. inserts all rows into a temporary table bronze._load_<table> (same structure), then
     ALTER TABLE bronze.<table> REPLACE PARTITION ... FROM the temporary table.

Idempotency: step 4 replaces the target partition atomically, so loading the same file twice
leaves exactly one copy. rik_report_elements is partitioned by report_year, so each yearly file only
replaces its own year; the other tables are unpartitioned and every load replaces the whole table
(each RIK file is a full snapshot). The tables are ReplacingMergeTree(_loaded_at), so if someone
loads with plain INSERTs instead (Olena's choice), reading with FINAL still gives one row per key.

Duplicates inside a source file: rows are inserted into the temporary table with
optimize_on_insert=0, so nothing collapses before they are counted. The loader then reports
  exact_duplicate_rows  rows identical to another row in all source columns,
  key_conflict_rows     rows sharing the ORDER BY key with another row but differing elsewhere
                        (these are the ones FINAL would hide; logged as a WARNING with examples).
After REPLACE PARTITION the target table may collapse them during merges, so source-level duplicate
checks must use these stats (returned by load_file and logged), not count() on bronze.

Connection settings come from the environment: CLICKHOUSE_HOST, CLICKHOUSE_PORT (HTTP, default 8123),
CLICKHOUSE_USER, CLICKHOUSE_PASSWORD. File version: taken from real RIK file names
("..._kuni_31102026.csv"), otherwise from --file-version or RIK_FILE_VERSION.

Olena's ingestion DAG can import and call load_file()/load_directory() instead of shelling out.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import sys
import uuid
import zipfile
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bronze_schema import BRONZE_DATABASE, BRONZE_TABLES, BronzeTable, match_file  # noqa: E402

log = logging.getLogger("load_bronze")

BATCH_ROWS = 100_000
BUNDLE_MARKER = "@@@FILE "


class BronzeLoadError(RuntimeError):
    """Raised for any problem that must stop the load (bad header, ragged row, empty file...)."""


# --------------------------------------------------------------------------------------------
# File handling
# --------------------------------------------------------------------------------------------

def split_bundle(bundle_path: Path, out_dir: Path) -> list:
    """Split a '@@@FILE <name>' bundle into separate files in out_dir. Bytes are kept as-is (CRLF)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written, current = [], None
    with open(bundle_path, "rb") as fh:
        for raw in fh:
            stripped = raw.rstrip(b"\r\n")
            if stripped.startswith(BUNDLE_MARKER.encode()):
                if current:
                    current.close()
                name = stripped[len(BUNDLE_MARKER):].decode("utf-8").strip()
                if not name or "/" in name or "\\" in name or name.startswith(".."):
                    raise BronzeLoadError(f"unsafe file name in bundle: {name!r}")
                target = out_dir / name
                current = open(target, "wb")
                written.append(target)
                continue
            if current is None:
                if stripped.strip():
                    raise BronzeLoadError(f"{bundle_path}: data before the first {BUNDLE_MARKER!r} line")
                continue
            current.write(raw)
    if current:
        current.close()
    log.info("split %s into %d files in %s", bundle_path, len(written), out_dir)
    return written


@contextmanager
def open_csv_text(path: Path):
    """Yield (member_name, text stream) for a .csv or a .zip containing exactly one .csv."""
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
            if len(members) != 1:
                raise BronzeLoadError(f"{path}: expected exactly one CSV in the zip, found {members}")
            with zf.open(members[0]) as raw:
                yield members[0].rsplit("/", 1)[-1], io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
    else:
        with open(path, encoding="utf-8-sig", newline="") as fh:
            yield path.name, fh


def csv_reader(stream, spec: BronzeTable):
    if spec.quoting == "none":
        return csv.reader(stream, delimiter=";", quoting=csv.QUOTE_NONE, strict=True)
    return csv.reader(stream, delimiter=";", quotechar='"', doublequote=True, strict=True)


def validate_header(header: list, spec: BronzeTable, source: str) -> list:
    """Return the list of column indexes in spec order; raise on missing/unexpected columns."""
    header = [h.strip() for h in header]
    expected = spec.headers
    missing = [h for h in expected if h not in header]
    unexpected = [h for h in header if h not in expected]
    duplicated = sorted({h for h in header if header.count(h) > 1})
    if missing or unexpected or duplicated:
        raise BronzeLoadError(
            f"{source}: header does not match bronze.{spec.table}: "
            f"missing={missing} unexpected={unexpected} duplicated={duplicated}"
        )
    if header != expected:
        log.warning("%s: columns are in a different order than expected; mapping by name", source)
    return [header.index(h) for h in expected]


# --------------------------------------------------------------------------------------------
# ClickHouse
# --------------------------------------------------------------------------------------------

def get_client():
    import clickhouse_connect  # imported lazily so the module imports without the driver

    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database="default",
    )


def run_init_sql(client, sql_dir: Path) -> None:
    """Execute clickhouse/init/*.sql (all statements are IF NOT EXISTS)."""
    for f in sorted(sql_dir.glob("*.sql")):
        text = "\n".join(l for l in f.read_text(encoding="utf-8").splitlines() if not l.strip().startswith("--"))
        for stmt in (s.strip() for s in text.split(";")):
            if stmt:
                client.command(stmt)
        log.info("executed %s", f.name)


def load_file(client, path: Path, file_version: str | None = None, allow_empty: bool = False) -> dict:
    """Load one CSV/zip into its bronze table, replacing the matching partition. Returns stats."""
    path = Path(path)
    with open_csv_text(path) as (member_name, stream):
        matched = match_file(member_name)
        if not matched:
            raise BronzeLoadError(f"{path}: file name {member_name!r} is not a known RIK bronze file")
        spec, info = matched
        version = info.get("version") or file_version or os.environ.get("RIK_FILE_VERSION", "")
        if not version:
            raise BronzeLoadError(f"{path}: no file version (name, --file-version or RIK_FILE_VERSION)")

        reader = csv_reader(stream, spec)
        try:
            header = next(reader)
        except StopIteration:
            raise BronzeLoadError(f"{path}: file is empty (no header)") from None
        idx = validate_header(header, spec, member_name)

        partition_value = int(info["year"]) if spec.partition_column else None
        columns = spec.columns + ([spec.partition_column] if spec.partition_column else []) + [
            "_source_file", "_file_version"]
        # unique name so parallel loads (e.g. one task per year) do not collide
        tmp = f"{BRONZE_DATABASE}._load_{spec.table}_{partition_value or 'all'}_{uuid.uuid4().hex[:8]}"
        client.command(f"DROP TABLE IF EXISTS {tmp}")
        client.command(f"CREATE TABLE {tmp} AS {spec.full_name}")
        client.command(f"SYSTEM STOP MERGES {tmp}")  # keep in-file duplicates until they are counted
        try:
            n_rows, batch = 0, []
            extra = ([partition_value] if spec.partition_column else []) + [member_name, version]
            for row in _rows(reader, member_name):
                if len(row) != len(header):
                    if not any(row):  # tolerate blank lines
                        continue
                    raise BronzeLoadError(
                        f"{member_name}: line {reader.line_num}: {len(row)} fields, expected {len(header)}")
                batch.append([row[i] for i in idx] + extra)
                if len(batch) >= BATCH_ROWS:
                    client.insert(tmp, batch, column_names=columns, settings=_NO_COLLAPSE)
                    n_rows += len(batch)
                    batch = []
            if batch:
                client.insert(tmp, batch, column_names=columns, settings=_NO_COLLAPSE)
                n_rows += len(batch)
            if n_rows == 0 and not allow_empty:
                raise BronzeLoadError(f"{member_name}: header only, no data rows (use --allow-empty to load)")

            dup_stats = duplicate_stats(client, tmp, spec) if n_rows else {}

            partition = str(partition_value) if spec.partition_column else "tuple()"
            if n_rows == 0:
                client.command(f"ALTER TABLE {spec.full_name} DROP PARTITION {partition}")
            else:
                client.command(f"ALTER TABLE {spec.full_name} REPLACE PARTITION {partition} FROM {tmp}")
        finally:
            client.command(f"DROP TABLE IF EXISTS {tmp}")

    stats = {"file": str(path), "table": spec.full_name, "rows": n_rows, "file_version": version,
             "partition": partition_value, **dup_stats}
    log.info("loaded %s", stats)
    return stats


_NO_COLLAPSE = {"optimize_on_insert": 0}


def duplicate_stats(client, table: str, spec: BronzeTable) -> dict:
    """Count in-file duplicates in the (not yet collapsed) temporary table."""
    key = ", ".join(f"`{c}`" for c in spec.order_by)
    allc = ", ".join(f"`{c}`" for c in spec.columns)
    total, n_key, n_all = client.query(
        f"SELECT count(), uniqExact(({key},)), uniqExact(({allc},)) FROM {table}").result_rows[0]
    stats = {"exact_duplicate_rows": int(total - n_all), "key_conflict_rows": int(n_all - n_key)}
    if stats["exact_duplicate_rows"]:
        log.warning("%s: %d exact duplicate rows in the source file (kept once after merges / FINAL)",
                    spec.table, stats["exact_duplicate_rows"])
    if stats["key_conflict_rows"]:
        examples = client.query(
            f"SELECT {key}, count() AS n FROM (SELECT DISTINCT {allc} FROM {table}) "
            f"GROUP BY {key} HAVING n > 1 LIMIT 5").result_rows
        log.warning("%s: %d rows share the ORDER BY key (%s) with a different row; FINAL keeps one. "
                    "Examples: %s", spec.table, stats["key_conflict_rows"], key, examples)
    return stats


def _rows(reader, source: str):
    """Iterate a csv reader, turning csv.Error (bad quoting, NUL bytes...) into BronzeLoadError."""
    while True:
        try:
            yield next(reader)
        except StopIteration:
            return
        except csv.Error as exc:
            raise BronzeLoadError(f"{source}: line {reader.line_num}: CSV parse error: {exc}") from exc


def discover_files(data_dir: Path) -> list:
    """Known RIK files (csv or zip) in data_dir, sorted; files starting with '_' are ignored."""
    files = []
    for p in sorted(Path(data_dir).iterdir()):
        if p.name.startswith(("_", ".")) or not p.is_file():
            continue
        if p.suffix.lower() == ".csv" and match_file(p.name):
            files.append(p)
        elif p.suffix.lower() == ".zip":
            files.append(p)  # name checked against the CSV member inside
    return files


def load_directory(data_dir: Path, file_version: str | None = None, client=None,
                   allow_empty: bool = False, only: list | None = None) -> list:
    client = client or get_client()
    files = discover_files(data_dir)
    if not files:
        raise BronzeLoadError(f"no RIK CSV/zip files found in {data_dir}")
    results = []
    for f in files:
        if only:
            m = match_file(f.name)
            if m and m[0].key not in only:
                continue
        results.append(load_file(client, f, file_version=file_version, allow_empty=allow_empty))
    return results


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--data-dir", type=Path, help="directory with RIK CSV/zip files (real or sample names)")
    src.add_argument("--bundle", type=Path, help="rik_bronze_sample_bundle.txt; split into --out-dir first")
    src.add_argument("--file", type=Path, action="append", help="load only this file (repeatable)")
    ap.add_argument("--out-dir", type=Path, default=None,
                    help="where --bundle is split to (default: the bundle's directory)")
    ap.add_argument("--file-version", default=None, help="overrides RIK_FILE_VERSION for sample names")
    ap.add_argument("--init-sql-dir", type=Path, default=None, help="run these *.sql files first")
    ap.add_argument("--allow-empty", action="store_true", help="allow header-only files (empties the partition)")
    ap.add_argument("--only", nargs="*", choices=sorted(BRONZE_TABLES), help="restrict to these tables")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        client = get_client()
        if args.init_sql_dir:
            run_init_sql(client, args.init_sql_dir)
        if args.bundle:
            out_dir = args.out_dir or args.bundle.parent
            split_bundle(args.bundle, out_dir)
            results = load_directory(out_dir, args.file_version, client, args.allow_empty, args.only)
        elif args.file:
            results = [load_file(client, f, args.file_version, args.allow_empty) for f in args.file]
        else:
            results = load_directory(args.data_dir, args.file_version, client, args.allow_empty, args.only)
    except BronzeLoadError as exc:
        log.error("%s", exc)
        return 2
    total = sum(r["rows"] for r in results)
    log.info("done: %d files, %d rows", len(results), total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
