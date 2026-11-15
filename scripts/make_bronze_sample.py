#!/usr/bin/env python3
"""Build the small, reproducible bronze sample from the real RIK open-data files.

Keeps companies with int(registrikood) % MODULO == REMAINDER (default 500 / 7) and annual reports with
aruandeaasta in --years (default 2022-2025), then keeps the matching rows of the EMTAK revenue file,
the yearly element files and the company basic-data file. Output files use the sample names that
scripts/load_bronze.py recognises (aruannete_yldandmed.csv, aruannete_elemendid_<year>.csv,
emtak_myygitulu.csv, ettevotja_lihtandmed.csv) plus _stats.txt and _element_counts_<last year>.txt.
Records are copied byte for byte (original quoting and CRLF are kept). With --bundle the same
content is also written as one '@@@FILE <name>' bundle file.

Downloads (streamed) from https://avaandmed.ariregister.rik.ee/sites/default/files/ into --cache-dir.
The date part of the file names (e.g. 31102026) is --file-version / RIK_FILE_VERSION; the extra
'_0' / '_1' suffix RIK adds changes from month to month, so the script tries '', '_0', '_1', '_2'
unless --suffix KEY=SUFFIX is given (KEY: report_general, report_emtak_revenue, report_elements).
With --local-zip-dir no download happens: the zips must already be there (used by the tests).

Example:
    python scripts/make_bronze_sample.py --file-version 31102026 --years 2022,2023,2024,2025 \
        --out-dir data/sample --cache-dir data/raw --bundle data/sample/rik_bronze_sample_bundle.txt
"""

from __future__ import annotations

import argparse
import collections
import csv
import io
import logging
import os
import shutil
import sys
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bronze_schema import BRONZE_TABLES, RIK_BASE_URL  # noqa: E402

log = logging.getLogger("make_bronze_sample")
SUFFIX_CANDIDATES = ["", "_0", "_1", "_2"]


# --------------------------------------------------------------------------------------------
# Reading records while keeping the original bytes
# --------------------------------------------------------------------------------------------

def iter_records(stream, quoting: str):
    """Yield (raw_record, fields). raw_record keeps its line ending; quoted newlines are joined."""
    pending = ""
    for line in stream:
        pending += line
        if quoting != "none" and pending.count('"') % 2 == 1:
            continue  # inside a quoted field that contains a newline
        if quoting == "none":
            fields = next(csv.reader(io.StringIO(pending, newline=""), delimiter=";", quoting=csv.QUOTE_NONE))
        else:
            fields = next(csv.reader(io.StringIO(pending, newline=""), delimiter=";", quotechar='"'), [])
        yield pending, fields
        pending = ""
    if pending:
        raise ValueError("file ends inside a quoted field")


def open_zip_member(zip_path: Path):
    zf = zipfile.ZipFile(zip_path)
    members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
    if len(members) != 1:
        raise ValueError(f"{zip_path}: expected one CSV, found {members}")
    return zf, io.TextIOWrapper(zf.open(members[0]), encoding="utf-8-sig", newline="")


def filter_zip(zip_path: Path, out_path: Path, quoting: str, keep) -> int:
    """Copy header + records for which keep(dict_row) is true. Returns kept data rows."""
    zf, stream = open_zip_member(zip_path)
    kept = 0
    with zf, stream, open(out_path, "w", encoding="utf-8", newline="") as out:
        records = iter_records(stream, quoting)
        raw_header, header = next(records)
        out.write(raw_header)
        for raw, fields in records:
            if not fields:
                continue
            if keep(dict(zip(header, fields))):
                out.write(raw)
                kept += 1
    log.info("%s -> %s: %d rows", zip_path.name, out_path.name, kept)
    return kept


# --------------------------------------------------------------------------------------------
# Locating / downloading the zips
# --------------------------------------------------------------------------------------------

def candidate_names(key: str, version: str, year: int | None, suffixes: dict) -> list:
    spec = BRONZE_TABLES[key]
    if key == "company_basic":
        return [spec.real_zip]
    sfx = [suffixes[key]] if key in suffixes else SUFFIX_CANDIDATES
    return [spec.real_zip.format(version=version, year=year, suffix=s) for s in sfx]


def fetch_zip(key: str, version: str, year, suffixes: dict, cache_dir: Path, local_only: bool) -> Path:
    names = candidate_names(key, version, year, suffixes)
    for name in names:
        local = cache_dir / name.rsplit("/", 1)[-1]
        if local.exists() and local.stat().st_size > 0:
            return local
    if local_only:
        raise FileNotFoundError(f"none of {names} found in {cache_dir}")
    cache_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        url = RIK_BASE_URL + name
        local = cache_dir / name.rsplit("/", 1)[-1]
        try:
            log.info("downloading %s", url)
            with urllib.request.urlopen(url, timeout=120) as resp, open(str(local) + ".part", "wb") as out:
                shutil.copyfileobj(resp, out, length=1 << 20)
            os.replace(str(local) + ".part", local)
            return local
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise
            log.info("not found: %s", url)
    raise FileNotFoundError(f"could not download any of {names}")


# --------------------------------------------------------------------------------------------

def build_sample(file_version: str, years: list, out_dir: Path, cache_dir: Path, modulo: int = 500,
                 remainder: int = 7, suffixes: dict | None = None, local_only: bool = False,
                 bundle: Path | None = None) -> dict:
    suffixes = suffixes or {}
    out_dir.mkdir(parents=True, exist_ok=True)
    years_s = {str(y) for y in years}
    report_ids, companies = set(), set()

    def keep_general(r):
        code = r.get("registrikood", "").strip()
        ok = code.isdigit() and int(code) % modulo == remainder and r.get("aruandeaasta", "").strip() in years_s
        if ok:
            report_ids.add(r["report_id"].strip())
            companies.add(code)
        return ok

    stats = collections.OrderedDict()
    written = []
    z = fetch_zip("report_general", file_version, None, suffixes, cache_dir, local_only)
    stats["aruannete_yldandmed.csv"] = filter_zip(z, out_dir / "aruannete_yldandmed.csv", "minimal", keep_general)
    written.append("aruannete_yldandmed.csv")

    for y in years:
        name = f"aruannete_elemendid_{y}.csv"
        z = fetch_zip("report_elements", file_version, y, suffixes, cache_dir, local_only)
        stats[name] = filter_zip(z, out_dir / name, "minimal", lambda r: r.get("report_id", "").strip() in report_ids)
        written.append(name)

    z = fetch_zip("report_emtak_revenue", file_version, None, suffixes, cache_dir, local_only)
    stats["emtak_myygitulu.csv"] = filter_zip(z, out_dir / "emtak_myygitulu.csv", "minimal",
                                              lambda r: r.get("report_id", "").strip() in report_ids)
    written.append("emtak_myygitulu.csv")

    z = fetch_zip("company_basic", file_version, None, suffixes, cache_dir, local_only)
    stats["ettevotja_lihtandmed.csv"] = filter_zip(z, out_dir / "ettevotja_lihtandmed.csv", "none",
                                                   lambda r: r.get("ariregistri_kood", "").strip() in companies)
    written.append("ettevotja_lihtandmed.csv")

    # element-name frequencies of the last year (helps choosing fact columns)
    last = max(years)
    counts = collections.Counter()
    with open(out_dir / f"aruannete_elemendid_{last}.csv", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh, delimiter=";"):
            counts[row["elemendi_nimetus"]] += 1
    with open(out_dir / f"_element_counts_{last}.txt", "w", encoding="utf-8", newline="") as fh:
        for name, n in counts.most_common():
            fh.write(f"{n}\t{name}\r\n")
    written.append(f"_element_counts_{last}.txt")

    with open(out_dir / "_stats.txt", "w", encoding="utf-8", newline="") as fh:
        fh.write(f"file_version={file_version}\r\nyears={','.join(map(str, years))}\r\n")
        fh.write(f"filter=int(registrikood) % {modulo} == {remainder}\r\n")
        fh.write(f"companies={len(companies)}\r\nreports={len(report_ids)}\r\n")
        for k, v in stats.items():
            fh.write(f"rows[{k}]={v}\r\n")
    written.append("_stats.txt")

    if bundle:
        with open(bundle, "wb") as out:
            for name in written:
                out.write(f"@@@FILE {name}\r\n".encode())
                out.write((out_dir / name).read_bytes())
        log.info("bundle written to %s", bundle)

    return {"companies": len(companies), "reports": len(report_ids), "rows": dict(stats)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--file-version", default=os.environ.get("RIK_FILE_VERSION"), required=False)
    ap.add_argument("--years", default=os.environ.get("RIK_YEARS", "2022,2023,2024,2025"))
    ap.add_argument("--out-dir", type=Path, default=Path("data/sample"))
    ap.add_argument("--cache-dir", type=Path, default=Path("data/raw"))
    ap.add_argument("--local-zip-dir", type=Path, default=None, help="use zips from here, never download")
    ap.add_argument("--modulo", type=int, default=500)
    ap.add_argument("--remainder", type=int, default=7)
    ap.add_argument("--suffix", action="append", default=[], help="KEY=SUFFIX, e.g. report_general=_1")
    ap.add_argument("--bundle", type=Path, default=None)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not args.file_version:
        ap.error("--file-version (or RIK_FILE_VERSION) is required")
    suffixes = dict(s.split("=", 1) for s in args.suffix)
    years = [int(y) for y in str(args.years).split(",") if y.strip()]
    result = build_sample(args.file_version, years, args.out_dir, args.local_zip_dir or args.cache_dir,
                          args.modulo, args.remainder, suffixes, local_only=args.local_zip_dir is not None,
                          bundle=args.bundle)
    log.info("sample: %s", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
