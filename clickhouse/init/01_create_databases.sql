-- Medallion layers as ClickHouse databases.
-- Runs once, on the first start of the clickhouse container (empty data volume),
-- via /docker-entrypoint-initdb.d. Safe to re-run: everything is IF NOT EXISTS.
-- bronze: raw RIK files, all columns String (Ege / Olena)
-- silver: cleaned and typed models built by dbt (Gabriel)
-- gold:   star schema built by dbt (Kristjan, Gabriel)
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
