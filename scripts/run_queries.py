#!/usr/bin/env python3
"""Run the analytical queries Q1 to Q5 (sql/queries/*.sql) on gold and write the results as Markdown.

    python scripts/run_queries.py                       # print to stdout
    python scripts/run_queries.py --out docs/query_results.md --readme README.md

--readme replaces the text between <!-- QUERY_RESULTS_START --> and <!-- QUERY_RESULTS_END --> in the
README with the same tables, so the README always shows the results of the last full run.

Inside the Airflow container (CLICKHOUSE_* are set there):
    docker compose exec airflow-scheduler python /opt/airflow/scripts/run_queries.py \
        --sql-dir /opt/airflow/sql/queries
or from the host with the ClickHouse HTTP port (8123) and CLICKHOUSE_* exported.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

REPO = Path(__file__).resolve().parent.parent
START, END = "<!-- QUERY_RESULTS_START -->", "<!-- QUERY_RESULTS_END -->"


def _title(sql: str, fallback: str) -> str:
    m = re.search(r"--\s*(Q\d\..*)", sql)
    return m.group(1).strip() if m else fallback


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:,.1f}" if abs(v) < 1e15 else str(v)
    return str(v).replace("|", "\\|")


def run(client, sql_dir: Path, max_rows: int) -> str:
    parts = []
    for f in sorted(sql_dir.glob("q*.sql")):
        sql = f.read_text(encoding="utf-8").strip().rstrip(";")
        t0 = time.perf_counter()
        res = client.query(sql)
        ms = (time.perf_counter() - t0) * 1000
        rows = res.result_rows
        parts.append(f"### {_title(sql, f.stem)}\n")
        parts.append(f"`{f.relative_to(REPO) if f.is_relative_to(REPO) else f.name}`, "
                     f"{len(rows)} rows, {ms:.0f} ms\n")
        if not rows:
            parts.append("_No group reached the minimum of 30 companies._\n")
            continue
        parts.append("| " + " | ".join(res.column_names) + " |")
        parts.append("|" + "|".join("---" for _ in res.column_names) + "|")
        for r in rows[:max_rows]:
            parts.append("| " + " | ".join(_fmt(v) for v in r) + " |")
        if len(rows) > max_rows:
            parts.append(f"\n_{len(rows) - max_rows} more rows not shown._")
        parts.append("")
    return "\n".join(parts)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--sql-dir", type=Path, default=REPO / "sql" / "queries")
    ap.add_argument("--out", type=Path, default=None, help="write the Markdown here")
    ap.add_argument("--readme", type=Path, default=None, help="also update this README between the markers")
    ap.add_argument("--max-rows", type=int, default=15, help="rows shown per query (default 15)")
    ap.add_argument("--label", default=None, help="e.g. 'full RIK data, file version 31102026'")
    args = ap.parse_args(argv)

    import load_bronze

    client = load_bronze.get_client()
    version = client.command("SELECT version()")
    n_fact = client.command("SELECT count() FROM gold.fact_company_year")
    header = (f"Results of the last run ({dt.date.today().isoformat()}, ClickHouse {version}, "
              f"{int(n_fact):,} rows in gold.fact_company_year"
              + (f", {args.label}" if args.label else "") + ").\n")
    md = header + "\n" + run(client, args.sql_dir, args.max_rows)

    if args.out:
        args.out.write_text("# Query results\n\n" + md + "\n", encoding="utf-8")
    if args.readme:
        text = args.readme.read_text(encoding="utf-8")
        if START not in text or END not in text:
            print(f"markers {START} / {END} not found in {args.readme}", file=sys.stderr)
            return 1
        before, rest = text.split(START, 1)
        _, after = rest.split(END, 1)
        args.readme.write_text(before + START + "\n" + md + "\n" + END + after, encoding="utf-8")
    if not args.out and not args.readme:
        print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
