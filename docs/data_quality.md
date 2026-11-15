# Data quality

Owner: Melih. Two layers of checks, each at the point where it is cheapest to stop bad data.

| Where | What | Fails the run? | Code |
|---|---|---|---|
| Airflow, after bronze is loaded | raw files are complete and keys are usable | yes, dbt is not triggered | `scripts/dq_bronze.py`, task `dq_check_bronze` |
| dbt, on the bronze sources | raw values look like the RIK formats | mostly warnings | `dbt/models/bronze/_bronze__sources.yml` |
| dbt, on gold | grain, keys, SCD2 rules, value ranges | yes (`dbt_test` fails `dbt_refresh_gold`) | `dbt/tests/gold/*.sql` |
| dbt, bronze against gold | nothing is lost or changed on the way | warnings | `dbt/tests/cross_layer/*.sql` |

Teammates' own `unique` / `not_null` / `relationships` tests in their model YAML come on top of these.

## 1. Airflow check on bronze (`dq_check_bronze`)

Runs between the load task and `trigger_dbt_refresh_gold`
(`load >> dq_check_bronze >> trigger_dbt_refresh_gold`). Every check returns a row
(status, severity, table, detail) that is printed as a table in the task log and stored as XCom.
A failed `error` check raises `AirflowFailException` (no retries), so the run stops before dbt.

| Check | Severity | Rule |
|---|---|---|
| `columns` | error | each bronze table exists and has every column of `scripts/bronze_schema.py` (extra columns: warning) |
| `not_empty` | error | each bronze table has rows |
| `years_present` | error | every year in `RIK_YEARS` has reports and an elements file |
| `report_id_filled` | error | `report_id` is never empty (general, elements, EMTAK) |
| `registry_filled` | error | registry code is never empty (general, company basic data) |
| `report_id_unique` | error | no duplicate `report_id` in the report file (loader stats) |
| `key_conflicts` | warn | other tables: rows that share their key with a different row |
| `numeric_values` | warn | `vaartus` parses as a number |
| `orphan_reports` | warn | elements and EMTAK rows point to a known report |
| `one_report_per_company_year` | warn | a company has one report per year (silver picks one if not) |
| `one_main_activity` | warn | one main EMTAK activity per report and EMTAK version |

**Why duplicates come from the loader.** Bronze tables are `ReplacingMergeTree`. ClickHouse collapses
rows with the same key already on insert and during merges, so `count() - uniqExact(report_id)` on bronze
is always 0 and would hide duplicates in the file. `scripts/load_bronze.py` counts them in its temporary
table before that happens and returns `exact_duplicate_rows` / `key_conflict_rows` per file. The check
reads these from the XCom of the load task(s). Without stats it reports `skipped`, never `pass`.

**Use in an ingestion DAG** (Olena): put the task after the load tasks and before the trigger, and pass
the ids of all load tasks so their stats are collected:

```python
def dq_check_bronze(**context):
    sys.path.insert(0, SCRIPTS_DIR)
    import dq_bronze
    return dq_bronze.run_dq_task(upstream_task_ids=["load_general", "load_elements_2025", ...], **context)

dq = PythonOperator(task_id="dq_check_bronze", python_callable=dq_check_bronze, retries=0)
[load tasks] >> dq >> trigger_dbt_refresh_gold
```

**Command line:** `python scripts/dq_bronze.py --years 2022,2023,2024,2025 [--stats-json stats.json]`
(exit code 1 on a failed error check).

## 2. dbt tests on the bronze sources

`dbt_expectations` (package `metaplane/dbt_expectations`) plus dbt's built-in tests:
row count at least 1 for the three main tables, `report_id` / `registrikood` / `aruandeaasta` /
`period_end` / `emtak` / `vaartus` match their text formats (regex), `report_year` between 2000 and 2100,
`pohitegevusala` in (`jah`, `ei`), elements and EMTAK rows reference a known report, registry code unique
in the basic-data file. Format tests are warnings: bronze stores the files as they are, and the hard
stop already happened in Airflow.

`dbt_expectations` has no ClickHouse version of its `regexp_instr` helper (its default renders a function
ClickHouse does not have). `dbt/macros/dbt_expectations_clickhouse.sql` adds one with `match()`, and the
`dispatch` block in `dbt_project.yml` makes the package use it.

## 3. dbt tests on gold (`dbt/tests/gold/`)

| Test | Severity | Rule |
|---|---|---|
| `assert_fact_one_row_per_company_year` | error | one fact row per `company_id` and year (the grain; uses the durable id so two SCD2 versions in one year are caught) |
| `assert_fact_keys_match_dimensions` | error | every fact key exists in its dimension (ClickHouse has no foreign keys) |
| `assert_fact_avg_fte_in_range` | error | `0 <= avg_fte < 100000` |
| `assert_fact_amounts_not_negative` | warn | revenue, assets and cash not negative |
| `assert_dim_company_scd2_valid` | error | one current version per company, `valid_to > valid_from`, no overlapping versions |
| `assert_dims_have_unknown_member` | error | each dimension has its `UNKNOWN` member exactly once |
| `assert_dim_size_band_codes` | error | `dim_size_band` has exactly `micro`, `small`, `medium`, `large`, `unknown` |

Negative revenue is a warning on purpose: real filings contain a few (1 of about 1,200 reports in the
sample), and the queries already require `revenue > 0`. `employee_expense` and `operating_profit` are not
range-checked: in `Kasumiaruanne skeem 1` expenses are reported with a minus sign (all 626
`EmployeeExpense` values in the 2022 to 2025 sample are negative), and losses are negative.

## 4. Reconciliation bronze to gold (`dbt/tests/cross_layer/`)

| Test | Severity | Rule |
|---|---|---|
| `assert_bronze_company_years_reach_fact` | warn | every company-year with a report in bronze (years in `RIK_YEARS`) has a fact row; result per year |
| `assert_fact_revenue_matches_bronze` | warn | fact revenue equals the `Revenue` element of the selected report's income statement (within 1 euro) |

Warnings, because silver may drop or choose reports on purpose; the warning makes the number visible.

## Names the tests rely on

Written against `docs/pipeline_contracts.md` and `docs/surrogate_keys.md`:
`fact_company_year(company_year_key, company_id, company_key, year_key, industry_key, size_band_key,
report_id, revenue, assets, cash, avg_fte)`, `dim_company(company_key, registry_code, valid_from, valid_to,
is_current)`, `dim_year(year_key, year)`, `dim_industry(industry_key, section_code)`,
`dim_size_band(size_band_key, band_code)`. A singular test that refers to a model that does not exist yet
is skipped by dbt with a warning, so the tests switch on by themselves when the models are merged.
If a column is named differently, change the test or tell Melih.

## How it was tested (15 Nov 2026)

Environment: chDB 4.4.0 (embedded ClickHouse 26.9.2) behind `clickhouse-connect` 1.12.0, dbt-core 1.11.17 +
dbt-clickhouse 1.10.3, dbt_utils 1.4.1, dbt_expectations 0.10.10, Airflow 2.10.5 (Python 3.11). Not the
Docker stack: Docker Hub was not reachable from the test machine.

- Real RIK sample (`data/sample/rik_bronze_sample_bundle.txt`) loaded with `scripts/load_bronze.py`:
  1,959 reports, 28,829 elements, 1,778 EMTAK rows, 537 companies. All 24 Airflow checks pass.
- `airflow dags test bronze_sample_load`: `load_bronze_sample`, `dq_check_bronze`, `trigger_dbt_refresh_gold`
  all success. `airflow dags test dbt_refresh_gold`: all 6 tasks success, including `run_queries`.
- Synthetic fixture (`tests/fixtures/`, contains one duplicated report on purpose): `report_id_unique`
  fails with `exact_duplicate_rows=1`, as it should.
- Broken data on purpose: dropped a column, deleted the 2024 elements partition, inserted an empty
  `report_id` and a non-numeric value. The task raised with 4 failed error checks and 4 warnings.
- Gold tests against throwaway versions of the teammates' models (built from the contract snippets, not
  committed): all pass on clean data (1 expected warning: one negative revenue). After inserting a duplicate
  fact row, a row with unknown keys and negative FTE, an overlapping SCD2 version, deleting the unknown size
  band and changing one revenue, every test fails or warns as intended.
