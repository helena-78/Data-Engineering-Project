# How Ege's part was tested

All tests used the **synthetic** fixture `tests/fixtures/rik_synthetic_bundle.txt` (invented companies and
numbers in the exact RIK file formats; regenerate with `python tests/fixtures/make_synthetic_fixture.py`,
output is deterministic). It contains on purpose: quoted `;` and doubled quotes inside fields, empty strings,
Estonian letters (õ ä ö ü š ž), an exact duplicate report row, element rows with an empty element name, a UTF-8
BOM in one file, a 2021 report, companies outside the `% 500 == 7` rule and a company without a basic-data row.
The real sample bundle was not available in the test environment.

## Unit tests (no services needed)

```bash
python -m unittest discover -s tests -v      # 9 tests: schema/DDL drift, column names, CSV parsing, sample filter
```

## Integration checks that were run (Nov 2026, local binaries, not the Docker stack)

Environment: ClickHouse server 26.8.37.168 (official binary), dbt-core 1.11.17 + dbt-clickhouse 1.10.3,
dbt_utils 1.4.1 (cloned from GitHub because hub.getdbt.com was not reachable), Airflow 2.10.5 on Python 3.11
with SQLite metadata DB, clickhouse-connect 1.12.0.

| Check | Command (abridged) | Result |
|---|---|---|
| Init SQL via clickhouse-client | `clickhouse client --multiquery < clickhouse/init/0*.sql` | 3 databases, 4 ReplacingMergeTree tables |
| Load fixture | `load_bronze.py --bundle tests/fixtures/rik_synthetic_bundle.txt --init-sql-dir clickhouse/init` | 7 files, 595 rows; duplicate report reported (`exact_duplicate_rows=1`) |
| Rerun (idempotency) | same command again | `count()` unchanged: general 67, elements 438, emtak 71, basic 19; with `FINAL`: 66 / 438 / 71 / 19 |
| Header validation | renamed `vaartus`, removed `emtak_version`, short row | loader exit 2 with `missing=[...]` / `unexpected=[...]` / `3 fields, expected 5`; bronze unchanged, no temp tables left |
| Characters | queries on bronze | `Šokolaadi Proov OÜ`, `Testižürii Konsult OÜ`, `Märkusega; vt "Lisa 5"`, `Silt "jutumärkidega"` intact; no `\r` left; BOM stripped (`report_id` all digits) |
| Collapse check | `count() - uniqExact((report_id, tabel, elemendi_nimetus))` | 63 rows would be lost with the plan's key; 0 with `elemendi_label` in the key |
| Sample builder | `make_bronze_sample.py --local-zip-dir` on zips built from the fixture | 17 companies, 51 reports, 16 basic-data rows (as expected); CRLF kept; bundle re-splittable |
| dbt on empty project | `dbt debug / seed / snapshot / run / test` | all exit 0; test PASS=3 (2 source smoke tests + key-rule test with 28 checks) |
| Key-rule test can fail | changed one expected md5 | `FAIL 1`, exit 1 (then reverted) |
| Key rule end to end | throwaway snapshot + dim_company + dim_year + fact (built independently) | 12/12 tests PASS incl. 2 `relationships`; after a name change: 2 versions, old reports keep version 1 |
| Raw `generate_surrogate_key` with NULL | `dbt show --inline` | ClickHouse error 349 (Cannot convert NULL value to non-Nullable type) |
| DagBag | `DagBag(dag_folder=airflow/dags)` | 2 DAGs, 0 import errors |
| `airflow dags test dbt_refresh_gold` | with `DBT_SKIP_DEPS=true` (no hub access) | 5/5 tasks success, empty project and with throwaway models; `full_refresh=true` adds `--full-refresh` to seed and run |
| `airflow dags test bronze_sample_load` | bundle in `DATA_DIR/sample` | load + trigger success; a `dbt_refresh_gold` run was queued |
| Compose | `docker compose config -q` and `--profile tools config -q` | valid |

## Docker stack (15 Nov 2026, Windows 11 + Docker Desktop 29.4.1)

`scripts\docker_smoke_test.cmd`: **ALL STEPS PASSED** (build, up, bronze init, fixture load, `dbt debug`,
DAG import, `airflow dags test dbt_refresh_gold` 5/5 tasks, `docker compose run --rm dbt test` PASS=3).
Two problems found and fixed on the way:
- `dbt deps` into the bind-mounted `./dbt` left `dbt_utils` half-extracted (no `dbt_project.yml`) while exiting 0,
  so `dbt_seed` failed. Packages are now baked into the image at `/opt/dbt/packages`
  (`DBT_PACKAGES_INSTALL_PATH`, outside the bind mount); `dbt_deps` reinstalls only if `packages.yml` changed
  and fails on a package without `dbt_project.yml`. Reinstall tested inside the container (235 files).
- `dbt debug` failed on missing `git`; git is now installed in the image.

Real sample: `airflow dags test bronze_sample_load` with `data/sample/rik_bronze_sample_bundle.txt` → success.
Bronze: general 1,959 reports / 587 companies, elements 6,880 / 7,170 / 7,572 / 7,207 rows (2022 to 2025),
emtak 1,778, basic 537; no company has more than one report per `aruandeaasta`; Estonian letters intact.

Not tested: Linux/macOS hosts (set `AIRFLOW_UID` on Linux) and downloads from RIK (Olena's DAGs).
