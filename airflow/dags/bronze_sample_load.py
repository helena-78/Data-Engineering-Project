"""bronze_sample_load: load the small RIK sample into bronze, then trigger dbt_refresh_gold (owner: Ege).

Manual DAG (schedule=None), used for development and as a reference for the real ingestion DAGs.

Input, inside the containers (host folder ./data is mounted at DATA_DIR, default /opt/airflow/data):
  * DATA_DIR/sample/rik_bronze_sample_bundle.txt  (the bundle is split into DATA_DIR/sample/*.csv first), or
  * already split CSVs in DATA_DIR/sample/ (aruannete_yldandmed.csv, aruannete_elemendid_<year>.csv,
    emtak_myygitulu.csv, ettevotja_lihtandmed.csv).

Tasks:
  load_bronze_sample        scripts/load_bronze.py (header check, CSV parsing, partition replace -> idempotent)
  trigger_dbt_refresh_gold  TriggerDagRunOperator(trigger_dag_id="dbt_refresh_gold")

Olena's ingestion DAGs follow the same pattern for the full files (DATA_DIR/raw), with Melih's
data-quality task between the load and the trigger (see docs/pipeline_contracts.md).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
SCRIPTS_DIR = Path(os.environ.get("SCRIPTS_DIR", "/opt/airflow/scripts"))
CLICKHOUSE_INIT_DIR = Path(os.environ.get("CLICKHOUSE_INIT_DIR", "/opt/airflow/clickhouse/init"))
BUNDLE_NAME = "rik_bronze_sample_bundle.txt"


def load_bronze_sample(**_context) -> list:
    """Run the bronze loader on DATA_DIR/sample. Returns per-file stats (stored as XCom)."""
    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))
    import load_bronze  # noqa: E402  (scripts/load_bronze.py, imported at run time, not at DAG parse)

    sample_dir = DATA_DIR / "sample"
    client = load_bronze.get_client()
    if CLICKHOUSE_INIT_DIR.is_dir():
        load_bronze.run_init_sql(client, CLICKHOUSE_INIT_DIR)  # IF NOT EXISTS, safe on every run
    bundle = sample_dir / BUNDLE_NAME
    if bundle.exists():
        load_bronze.split_bundle(bundle, sample_dir)
    results = load_bronze.load_directory(sample_dir, client=client)
    for r in results:
        print(r)
    return results


with DAG(
    dag_id="bronze_sample_load",
    description="Load data/sample (RIK sample) into ClickHouse bronze, then trigger dbt_refresh_gold",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "ege", "retries": 0, "retry_delay": timedelta(minutes=1)},
    tags=["bronze", "sample"],
    doc_md=__doc__,
) as dag:
    load = PythonOperator(task_id="load_bronze_sample", python_callable=load_bronze_sample)

    trigger = TriggerDagRunOperator(
        task_id="trigger_dbt_refresh_gold",
        trigger_dag_id="dbt_refresh_gold",
        conf={"full_refresh": False},
        wait_for_completion=False,
    )

    load >> trigger
