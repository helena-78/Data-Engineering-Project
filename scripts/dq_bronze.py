#!/usr/bin/env python3
"""Data-quality checks on the ClickHouse bronze layer (owner: Melih).

Runs after bronze is loaded and before dbt is triggered:

    load bronze  >>  dq_check_bronze  >>  trigger_dbt_refresh_gold

A failed check of severity "error" fails the Airflow task (AirflowFailException, no retries), so dbt never
builds silver and gold on top of broken raw data. Checks of severity "warn" are only logged.

Checks (see docs/data_quality.md for the reasoning behind each one):

  error  columns          every bronze table exists and has all expected columns (scripts/bronze_schema.py)
  error  not_empty        every bronze table has at least one row
  error  years_present    report_general and report_elements contain every year in RIK_YEARS
  error  report_id_filled report_id is never empty in report_general, report_elements, report_emtak_revenue
  error  registry_filled  registrikood / ariregistri_kood is never empty
  error  report_id_unique no duplicate report_id in the report_general source file
                          (loader stats exact_duplicate_rows + key_conflict_rows, see below)
  warn   key_conflicts    rows of other tables that share their ORDER BY key with a different row
  warn   numeric_values   report_elements.vaartus parses as a number
  warn   orphan_reports   report_elements / report_emtak_revenue rows whose report_id is not in report_general
  warn   one_report_per_company_year  a company has more than one report for the same aruandeaasta
  warn   one_main_activity            a report has more than one main EMTAK activity per EMTAK version

Why duplicates come from the loader and not from bronze: the bronze tables are ReplacingMergeTree, which
collapses rows with the same key already on insert and during merges, so count() - uniqExact(report_id)
on bronze is always 0. scripts/load_bronze.py counts duplicates in its temporary table before that happens
and returns them per file (Airflow XCom of the load task). This module reads those stats. Without stats
(CLI run on its own) the duplicate check is reported as "skipped".

Use in a DAG (scripts dir is mounted at SCRIPTS_DIR and imported at run time, like load_bronze):

    from airflow.operators.python import PythonOperator
    dq = PythonOperator(task_id="dq_check_bronze", python_callable=run_dq_task,   # see the DAG files
                        op_kwargs={"upstream_task_ids": ["load_bronze_sample"]}, retries=0)

Command line (CLICKHOUSE_* from the environment, exit code 1 if an error check fails):

    python scripts/dq_bronze.py --years 2022,2023,2024,2025 [--stats-json load_stats.json]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bronze_schema import BRONZE_TABLES, METADATA_COLUMNS  # noqa: E402

log = logging.getLogger("dq_bronze")

GENERAL = BRONZE_TABLES["report_general"]
ELEMENTS = BRONZE_TABLES["report_elements"]
EMTAK = BRONZE_TABLES["report_emtak_revenue"]
BASIC = BRONZE_TABLES["company_basic"]

# Shown in the task log; the first rows of a failing check are included as examples.
MAX_EXAMPLES = 5


class DataQualityError(RuntimeError):
    """At least one check with severity 'error' failed."""


@dataclass
class CheckResult:
    check: str
    table: str
    severity: str      # "error" or "warn"
    status: str        # "pass", "fail" or "skipped"
    value: object      # the measured number (failing rows, missing columns...)
    detail: str = ""

    @property
    def failed(self) -> bool:
        return self.status == "fail"


def _years_from_env() -> list:
    raw = os.environ.get("RIK_YEARS", "")
    return [int(y) for y in raw.replace(" ", "").split(",") if y]


def _scalar(client, sql: str):
    return client.query(sql).result_rows[0][0]


def _rows(client, sql: str) -> list:
    return client.query(sql).result_rows


# ---------------------------------------------------------------------------------------------
# Individual checks. Each returns a list of CheckResult.
# ---------------------------------------------------------------------------------------------

def check_columns(client) -> list:
    """All four tables exist and contain every expected column (missing = error, extra = warn)."""
    results = []
    for spec in BRONZE_TABLES.values():
        expected = spec.columns + ([spec.partition_column] if spec.partition_column else []) + METADATA_COLUMNS
        actual = [r[0] for r in _rows(
            client,
            f"SELECT name FROM system.columns WHERE database = 'bronze' AND table = '{spec.table}' "
            f"ORDER BY position")]
        if not actual:
            results.append(CheckResult("columns", spec.full_name, "error", "fail", "table missing",
                                       "table does not exist (clickhouse/init/02_create_bronze_tables.sql)"))
            continue
        missing = [c for c in expected if c not in actual]
        extra = [c for c in actual if c not in expected]
        results.append(CheckResult("columns", spec.full_name, "error", "fail" if missing else "pass",
                                   len(missing), f"missing={missing}" if missing else f"{len(actual)} columns"))
        if extra:
            results.append(CheckResult("columns_extra", spec.full_name, "warn", "fail", len(extra),
                                       f"unexpected={extra}"))
    return results


def check_not_empty(client, existing: set) -> list:
    results = []
    for spec in BRONZE_TABLES.values():
        if spec.full_name not in existing:
            continue
        n = int(_scalar(client, f"SELECT count() FROM {spec.full_name}"))
        results.append(CheckResult("not_empty", spec.full_name, "error", "pass" if n > 0 else "fail", n,
                                   f"{n} rows"))
    return results


def check_years_present(client, years: list, existing: set) -> list:
    """Every configured report year has reports (general file) and financial elements (one file per year)."""
    if not years:
        return [CheckResult("years_present", "bronze", "error", "skipped", None, "RIK_YEARS not set")]
    results = []
    if GENERAL.full_name in existing:
        counts = dict(_rows(client, f"SELECT toUInt16OrZero(aruandeaasta), count() FROM {GENERAL.full_name} FINAL "
                                    f"GROUP BY 1"))
        missing = [y for y in years if counts.get(y, 0) == 0]
        results.append(CheckResult("years_present", GENERAL.full_name, "error", "fail" if missing else "pass",
                                   len(missing),
                                   f"no reports for {missing}" if missing else
                                   ", ".join(f"{y}: {counts.get(y, 0)}" for y in years)))
    if ELEMENTS.full_name in existing:
        counts = dict(_rows(client, f"SELECT report_year, count() FROM {ELEMENTS.full_name} GROUP BY 1"))
        missing = [y for y in years if counts.get(y, 0) == 0]
        results.append(CheckResult("years_present", ELEMENTS.full_name, "error", "fail" if missing else "pass",
                                   len(missing),
                                   f"no elements file loaded for {missing}" if missing else
                                   ", ".join(f"{y}: {counts.get(y, 0)}" for y in years)))
    return results


def _empty_check(client, name: str, spec, column: str, existing: set) -> list:
    if spec.full_name not in existing:
        return []
    n = int(_scalar(client, f"SELECT countIf(trimBoth({column}) = '') FROM {spec.full_name} FINAL"))
    detail = f"{n} rows with empty {column}"
    if n:
        ex = _rows(client, f"SELECT * FROM {spec.full_name} FINAL WHERE trimBoth({column}) = '' "
                           f"LIMIT {MAX_EXAMPLES}")
        detail += f"; examples: {ex}"
    return [CheckResult(name, spec.full_name, "error", "fail" if n else "pass", n, detail)]


def check_keys_filled(client, existing: set) -> list:
    results = []
    for spec in (GENERAL, ELEMENTS, EMTAK):
        results += _empty_check(client, "report_id_filled", spec, "report_id", existing)
    results += _empty_check(client, "registry_filled", GENERAL, "registrikood", existing)
    results += _empty_check(client, "registry_filled", BASIC, "ariregistri_kood", existing)
    return results


def check_duplicates(load_stats: list | None) -> list:
    """Duplicate keys inside the source files, from the loader's per-file stats.

    report_general: any duplicate report_id fails (exact copies and conflicting rows), because report_id is
    the grain of that file and the key of every join. Other tables: conflicting rows are a warning (FINAL
    keeps one of them), exact copies are harmless and only reported.
    """
    if not load_stats:
        return [CheckResult("report_id_unique", GENERAL.full_name, "error", "skipped", None,
                            "no loader stats passed (run inside the DAG, or --stats-json)")]
    results = []
    by_table: dict = {}
    for s in load_stats:
        t = by_table.setdefault(s["table"], {"exact": 0, "conflict": 0, "files": 0})
        t["exact"] += int(s.get("exact_duplicate_rows", 0) or 0)
        t["conflict"] += int(s.get("key_conflict_rows", 0) or 0)
        t["files"] += 1
    if GENERAL.full_name in by_table:
        t = by_table[GENERAL.full_name]
        n = t["exact"] + t["conflict"]
        results.append(CheckResult("report_id_unique", GENERAL.full_name, "error", "fail" if n else "pass", n,
                                   f"exact_duplicate_rows={t['exact']}, key_conflict_rows={t['conflict']}"))
    else:
        results.append(CheckResult("report_id_unique", GENERAL.full_name, "error", "skipped", None,
                                   "report_general was not loaded in this run"))
    for table, t in sorted(by_table.items()):
        if table == GENERAL.full_name:
            continue
        results.append(CheckResult("key_conflicts", table, "warn", "fail" if t["conflict"] else "pass",
                                   t["conflict"],
                                   f"key_conflict_rows={t['conflict']}, exact_duplicate_rows={t['exact']} "
                                   f"over {t['files']} file(s)"))
    return results


def check_consistency(client, existing: set) -> list:
    """Warnings that matter for silver/gold but do not stop the pipeline."""
    results = []
    if ELEMENTS.full_name in existing:
        n = int(_scalar(client, f"SELECT countIf(vaartus != '' AND toFloat64OrNull(vaartus) IS NULL) "
                                f"FROM {ELEMENTS.full_name} FINAL"))
        results.append(CheckResult("numeric_values", ELEMENTS.full_name, "warn", "fail" if n else "pass", n,
                                   f"{n} non-numeric vaartus values"))
    if GENERAL.full_name in existing:
        for spec in (ELEMENTS, EMTAK):
            if spec.full_name not in existing:
                continue
            n = int(_scalar(client, f"SELECT count() FROM (SELECT DISTINCT report_id FROM {spec.full_name} FINAL) "
                                    f"WHERE report_id NOT IN (SELECT report_id FROM {GENERAL.full_name} FINAL)"))
            results.append(CheckResult("orphan_reports", spec.full_name, "warn", "fail" if n else "pass", n,
                                       f"{n} report_id values not found in {GENERAL.full_name}"))
        n = int(_scalar(client, f"SELECT count() FROM (SELECT registrikood, aruandeaasta FROM {GENERAL.full_name} FINAL "
                                f"GROUP BY 1, 2 HAVING count() > 1)"))
        results.append(CheckResult("one_report_per_company_year", GENERAL.full_name, "warn",
                                   "fail" if n else "pass", n,
                                   f"{n} company-years with more than one report (silver picks one)"))
    if EMTAK.full_name in existing:
        n = int(_scalar(client, f"SELECT count() FROM (SELECT report_id, emtak_version FROM {EMTAK.full_name} FINAL "
                                f"WHERE pohitegevusala = 'jah' GROUP BY 1, 2 HAVING count() > 1)"))
        results.append(CheckResult("one_main_activity", EMTAK.full_name, "warn", "fail" if n else "pass", n,
                                   f"{n} report/version pairs with more than one main activity"))
    return results


# ---------------------------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------------------------

def run_checks(client, years: list | None = None, load_stats: list | None = None) -> list:
    """Run every check and return the results (does not raise)."""
    years = years if years is not None else _years_from_env()
    results = check_columns(client)
    existing = {r.table for r in results if r.check == "columns" and r.value != "table missing"}
    results += check_not_empty(client, existing)
    results += check_years_present(client, years, existing)
    results += check_keys_filled(client, existing)
    results += check_duplicates(load_stats)
    results += check_consistency(client, existing)
    return results


def format_report(results: list) -> str:
    lines = [f"{'status':7} {'severity':8} {'check':28} {'table':34} detail"]
    for r in results:
        label = "WARN" if r.failed and r.severity == "warn" else r.status.upper()
        lines.append(f"{label:7} {r.severity:8} {r.check:28} {r.table:34} {r.detail}")
    errors = [r for r in results if r.failed and r.severity == "error"]
    warns = [r for r in results if r.failed and r.severity == "warn"]
    lines.append(f"{len(results)} checks: {len(errors)} failed errors, {len(warns)} warnings")
    return "\n".join(lines)


def run_and_raise(client, years: list | None = None, load_stats: list | None = None) -> list:
    results = run_checks(client, years, load_stats)
    log.info("bronze data-quality report\n%s", format_report(results))
    errors = [r for r in results if r.failed and r.severity == "error"]
    if errors:
        raise DataQualityError("bronze data-quality checks failed: " +
                               "; ".join(f"{r.check} on {r.table} ({r.detail})" for r in errors))
    return results


def _flatten_stats(values) -> list:
    """XCom values can be one dict per file (one task per file) or a list of dicts (load_directory)."""
    out = []
    for v in values or []:
        if isinstance(v, dict):
            out.append(v)
        elif isinstance(v, (list, tuple)):
            out += _flatten_stats(v)
    return out


def run_dq_task(upstream_task_ids=None, **context) -> list:
    """Airflow python_callable. Pulls loader stats from the upstream load task(s) via XCom."""
    from airflow.exceptions import AirflowFailException

    import load_bronze

    ids = [upstream_task_ids] if isinstance(upstream_task_ids, str) else list(upstream_task_ids or [])
    stats = _flatten_stats(context["ti"].xcom_pull(task_ids=ids)) if ids else []
    try:
        results = run_and_raise(load_bronze.get_client(), load_stats=stats)
    except DataQualityError as exc:
        raise AirflowFailException(str(exc)) from exc
    return [asdict(r) for r in results]   # stored as XCom, visible in the Airflow UI


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--years", default=None, help="comma separated report years (default: RIK_YEARS)")
    ap.add_argument("--stats-json", type=Path, default=None,
                    help="JSON list of load_bronze stats dicts, for the duplicate check")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    import load_bronze

    years = [int(y) for y in args.years.split(",")] if args.years else None
    stats = json.loads(args.stats_json.read_text()) if args.stats_json else None
    try:
        run_and_raise(load_bronze.get_client(), years, stats)
    except DataQualityError as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
