"""dbt_refresh_gold: rebuild silver and gold in ClickHouse with dbt (owner: Ege).

Not scheduled. It is started by the ingestion DAGs after bronze is loaded and checked:

    TriggerDagRunOperator(task_id="trigger_dbt_refresh_gold", trigger_dag_id="dbt_refresh_gold",
                          conf={"full_refresh": False})

or by hand in the UI ("Trigger DAG w/ config") / CLI:
    airflow dags trigger dbt_refresh_gold --conf '{"full_refresh": true}'

Tasks: dbt_deps >> dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test >> run_queries
  * full_refresh=true adds --full-refresh to seed and run (rebuilds incremental models such as the fact).
  * run_queries (Melih) runs Q1 to Q5 (sql/queries/*.sql) on gold and writes DATA_DIR/query_results.md
    (host: ./data/query_results.md). It is skipped while gold.fact_company_year does not exist yet.
  * The DAG only calls dbt; which models exist is decided by the dbt project, so it works while
    silver/gold are still empty and keeps working as models, seeds and snapshots are added.

Configuration comes from the environment (docker-compose.yml / .env), nothing is hard-coded here:
  DBT_PROJECT_DIR, DBT_PROFILES_DIR, DBT_BIN (default "dbt", the separate venv on PATH),
  DBT_TARGET (profile target), CLICKHOUSE_HOST/PORT/USER/PASSWORD (read by dbt/profiles.yml),
  DBT_PACKAGES_INSTALL_PATH (set in the image: /opt/dbt/packages, packages baked in at build time),
  DBT_SKIP_DEPS=true never runs `dbt deps` (offline use; otherwise it runs only if packages.yml changed).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DBT_BIN = os.environ.get("DBT_BIN", "dbt")

# Passed to every task on top of the worker environment (append_env=True keeps CLICKHOUSE_* etc.).
DBT_ENV = {
    "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
}

# --no-use-colors keeps the Airflow task log readable.
DBT = f'"{DBT_BIN}" --no-use-colors'
DIRS = '--project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROFILES_DIR"'
FULL_REFRESH = "{{ '--full-refresh' if params.full_refresh else '' }}"

SCRIPTS_DIR = os.environ.get("SCRIPTS_DIR", "/opt/airflow/scripts")
DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")


def run_queries(**_context) -> str:
    """Q1 to Q5 on gold (scripts/run_queries.py, owner: Melih). Skipped until the fact table exists."""
    import sys
    from pathlib import Path

    from airflow.exceptions import AirflowSkipException

    if SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, SCRIPTS_DIR)
    import load_bronze  # noqa: E402
    import run_queries as rq  # noqa: E402

    client = load_bronze.get_client()
    if not int(client.command("EXISTS TABLE gold.fact_company_year")):
        raise AirflowSkipException("gold.fact_company_year does not exist yet")
    out = Path(DATA_DIR) / "query_results.md"
    rq.main(["--out", str(out)])
    text = out.read_text(encoding="utf-8")
    print(text)
    return str(out)


default_args = {
    "owner": "ege",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dbt_refresh_gold",
    description="dbt deps/seed/snapshot/run/test: refresh silver and gold in ClickHouse",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "full_refresh": Param(False, type="boolean",
                              description="Add --full-refresh to dbt seed and dbt run"),
    },
    tags=["dbt", "silver", "gold"],
    doc_md=__doc__,
) as dag:
    # The image already contains the packages of dbt/packages.yml in $DBT_PACKAGES_INSTALL_PATH
    # (/opt/dbt/packages, outside the bind mount, see airflow/Dockerfile). Reinstall only when
    # packages.yml changed since the image was built (e.g. someone added dbt_expectations) and verify
    # the result, because a half-extracted package would otherwise only fail later in dbt_seed.
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            'PKG="${DBT_PACKAGES_INSTALL_PATH:-$DBT_PROJECT_DIR/dbt_packages}"; '
            'if [ "${DBT_SKIP_DEPS:-false}" = "true" ] && [ -f "$PKG/dbt_utils/dbt_project.yml" ]; then '
            'echo "DBT_SKIP_DEPS=true: skipping dbt deps"; '
            'elif cmp -s "$DBT_PROJECT_DIR/packages.yml" "$PKG/.installed_packages.yml"; then '
            'echo "packages.yml unchanged since the image was built: using $PKG"; '
            f"else {DBT} deps {DIRS} || exit 1; cp \"$DBT_PROJECT_DIR/packages.yml\" \"$PKG/.installed_packages.yml\"; fi; "
            'for d in "$PKG"/*/; do [ -f "$d/dbt_project.yml" ] || { echo "broken package: $d"; exit 1; }; done'
        ),
        env=DBT_ENV,
        append_env=True,
        retries=2,  # a reinstall needs network (hub.getdbt.com)
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT} seed {DIRS} {FULL_REFRESH}",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT} snapshot {DIRS}",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run {DIRS} {FULL_REFRESH}",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DIRS}",
        env=DBT_ENV,
        append_env=True,
        retries=0,  # a failing data test is a real result, retrying does not help
    )

    query_results = PythonOperator(
        task_id="run_queries",
        python_callable=run_queries,
        retries=0,
    )

    dbt_deps >> dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test >> query_results
