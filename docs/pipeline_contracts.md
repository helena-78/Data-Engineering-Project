# Pipeline contracts: what each part needs from the others

Owner: Ege. If you change a name in this file, tell the team: other people's code depends on it.

## Run order

```
RIK zips ──> [Olena] ingestion DAG(s): download + load bronze ──> [Melih] data-quality task on bronze
        ──> TriggerDagRunOperator(trigger_dag_id="dbt_refresh_gold")
        ──> [Ege] dbt_refresh_gold: dbt_deps >> dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test
                  dbt run builds silver (Gabriel) then gold (Kristjan dims, Gabriel fact), dbt test checks all
        ──> [Melih] Q1 to Q5 on gold
```

`bronze_sample_load` (Ege) is a small working example of the first and last arrows for the sample.

## Names that are fixed

| What | Name |
|---|---|
| ClickHouse databases (= dbt schemas) | `bronze`, `silver`, `gold` (created by `clickhouse/init/01_create_databases.sql`) |
| Bronze tables | `bronze.rik_report_general`, `bronze.rik_report_elements`, `bronze.rik_report_emtak_revenue`, `bronze.rik_company_basic` (columns: `docs/bronze_tables.md`) |
| dbt sources | `source('bronze', '<table name above>')` (`dbt/models/bronze/_bronze__sources.yml`) |
| DAG to trigger after bronze is loaded and checked | `dbt_refresh_gold` (conf: `{"full_refresh": false}`) |
| Sample DAG | `bronze_sample_load` |
| dbt project / profile name | `observatory` |

## Olena (ingestion DAGs)

- Load with `scripts/load_bronze.py`, either `python /opt/airflow/scripts/load_bronze.py --data-dir ...` in a
  BashOperator or `load_bronze.load_file(client, path)` / `load_directory(dir)` in a PythonOperator
  (see `airflow/dags/bronze_sample_load.py`). It reads zips directly, so the downloaded
  `1.aruannete_yldandmed_kuni_<version>_<n>.zip` etc. can be passed as-is. Each yearly elements file can be its
  own task (temporary table names are unique).
- Put downloads in `${DATA_DIR}/raw` (host `./data/raw`, gitignored). File version: `RIK_FILE_VERSION`, years:
  `RIK_YEARS` (both in `.env`). The `_0`/`_1` suffix changes monthly; `scripts/make_bronze_sample.py` shows how
  to try the candidates.
- Idempotency: the loader replaces the partition (elements: one year; other tables: whole table). If you write
  your own insert instead, the tables are `ReplacingMergeTree(_loaded_at)` and readers use `FINAL`.
- Last task of the DAG, after Melih's check:
  ```python
  from airflow.operators.trigger_dagrun import TriggerDagRunOperator
  trigger = TriggerDagRunOperator(task_id="trigger_dbt_refresh_gold", trigger_dag_id="dbt_refresh_gold",
                                  conf={"full_refresh": False}, wait_for_completion=False)
  ```
  `wait_for_completion=True` (with `poke_interval=30`) makes the ingestion run fail if dbt fails, if you prefer.

## Melih (data-quality task, extra dbt tests, queries)

- Expected headers / columns: `from bronze_schema import BRONZE_TABLES, expected_headers` (scripts dir is
  mounted at `${SCRIPTS_DIR}` = `/opt/airflow/scripts`). The loader already fails on missing or unexpected
  columns and on empty files; your check can re-verify the loaded tables.
- Query bronze with `FINAL`. Source duplicates are collapsed by the engine, so for the duplicate `report_id`
  check use the loader stats `exact_duplicate_rows` / `key_conflict_rows` (returned by `load_file`, pushed to
  XCom by `bronze_sample_load`, logged in every case). Details: `docs/bronze_tables.md`.
- ClickHouse connection in Python: `load_bronze.get_client()` (reads `CLICKHOUSE_*` from the environment).
- Extra dbt tests: generic tests in model/source YAML, singular tests in `dbt/tests/`. They run in the
  `dbt_test` task automatically. `dbt_test` fails the DAG run on any failing test (severity `error`), so use
  `config(severity='warn')` for informational checks.
- dbt_expectations: add it to `dbt/packages.yml`, run `docker compose run --rm dbt deps` once (updates `package-lock.yml`),
  commit both files and run `docker compose build`, because the packages are baked into the image.
- Q1/Q5: partition year-over-year windows by `company_id` (durable, stored in the fact), not by `company_key`.

## Kristjan (dimensions) and Gabriel (silver, fact)

- Where files go: `dbt/models/silver/*.sql` → database `silver`; `dbt/models/gold/*.sql` → database `gold`;
  `dbt/seeds/*.csv` → `gold` (e.g. `dim_size_band.csv`); `dbt/snapshots/*.sql` → `gold` (e.g. `snap_company`).
  No schema config is needed in the model; the folder decides (`dbt_project.yml` + `macros/generate_schema_name.sql`
  use the custom schema as the database name as-is). Do not set `database=`.
- Default materialization is `table`. Gabriel's incremental fact:
  `{{ config(materialized='incremental', unique_key='company_year_key', order_by='company_year_key') }}`;
  `dbt_refresh_gold` with `full_refresh: true` rebuilds it.
- ClickHouse table settings per model: `order_by`, `partition_by`, `engine` in `config(...)` (dbt-clickhouse).
- Keys: only through the macros in `docs/surrogate_keys.md`. Read bronze with `final`.
- The snapshot runs before `dbt run`, so a snapshot can read bronze directly (or a silver model only if it is a
  view or already built; simplest is bronze plus light cleaning inside the snapshot query).

## Environment variables (set in `.env`, passed by `docker-compose.yml`)

| Variable | Used by | Default in `.env.example` |
|---|---|---|
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | ClickHouse server, dbt profile, loader | `observatory` / `change-me` |
| `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT` | dbt profile, loader (set in compose) | `clickhouse`, `8123` |
| `DBT_PROJECT_DIR`, `DBT_PROFILES_DIR`, `DBT_TARGET` | dbt DAG, dbt service | `/opt/airflow/dbt`, same, `dev` |
| `DATA_DIR`, `SCRIPTS_DIR`, `CLICKHOUSE_INIT_DIR` | DAGs, loader | `/opt/airflow/data`, `/opt/airflow/scripts`, `/opt/airflow/clickhouse/init` |
| `RIK_FILE_VERSION`, `RIK_YEARS` | loader, sample builder, ingestion | `31102026`, `2022,2023,2024,2025` |
| `DBT_PACKAGES_INSTALL_PATH` | dbt (`packages-install-path`) | set in the image: `/opt/dbt/packages` (outside the bind mount) |
| `DBT_SKIP_DEPS` | `dbt_deps` task: `true` never runs `dbt deps` | unset (= reinstall only if `packages.yml` changed since the image build) |
| `AIRFLOW_UID`, `AIRFLOW_ADMIN_*`, `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY`, `POSTGRES_*` | Airflow | see `.env.example` |

## Commands

```bash
cp .env.example .env                       # once; edit passwords
docker compose up -d --build               # UI: http://localhost:8080 (AIRFLOW_ADMIN_USER / _PASSWORD)
# sample: copy rik_bronze_sample_bundle.txt into ./data/sample/, then trigger the DAG
docker compose exec airflow-scheduler airflow dags trigger bronze_sample_load
docker compose exec airflow-scheduler airflow dags trigger dbt_refresh_gold --conf '{"full_refresh": true}'
# ad-hoc dbt (same image, profile "tools")
# dbt packages are baked into the image; after changing dbt/packages.yml: docker compose build
docker compose run --rm dbt build --select gold
docker compose run --rm dbt test
# loader from the host (needs: pip install clickhouse-connect; CLICKHOUSE_* exported, host port 8123)
python scripts/load_bronze.py --bundle rik_bronze_sample_bundle.txt --out-dir data/sample --init-sql-dir clickhouse/init
```

ClickHouse runs `clickhouse/init/*.sql` only on the first start with an empty volume. After changing the bronze
DDL either run the loader with `--init-sql-dir` (creates missing tables; it does not alter existing ones) or
`docker compose down -v` to start from scratch (deletes all ClickHouse data).
