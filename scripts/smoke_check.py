"""Print the bronze/silver/gold tables and row counts. Used by scripts/docker_smoke_test.cmd.

Run inside the Airflow container (CLICKHOUSE_* come from the environment):
    python $SCRIPTS_DIR/smoke_check.py
Exits 1 if any of the four bronze tables is missing.
"""
import os
import sys

import clickhouse_connect

from bronze_schema import BRONZE_TABLES  # noqa: F401  (import check: the schema module is on the path)

client = clickhouse_connect.get_client(
    host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
    port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
    username=os.environ.get("CLICKHOUSE_USER", "default"),
    password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
)
print("ClickHouse version:", client.command("SELECT version()"))
rows = client.query(
    "SELECT database, name, engine, total_rows FROM system.tables "
    "WHERE database IN ('bronze', 'silver', 'gold') ORDER BY database, name"
).result_rows
for r in rows:
    print("  %s.%s  engine=%s  rows=%s" % r)
expected = {"rik_report_general", "rik_report_elements", "rik_report_emtak_revenue", "rik_company_basic"}
missing = expected - {r[1] for r in rows if r[0] == "bronze"}
if missing:
    print("MISSING bronze tables:", sorted(missing))
    sys.exit(1)
print("bronze tables present")
