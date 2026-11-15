-- Extra credit A (Melih): statements behind the measurements in extra_credit_clickhouse_vs_postgres.md.
-- Run after the sample is loaded into bronze (bronze_sample_load), e.g.
--   docker compose exec clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery < docs/report/extra_credit_benchmark.sql
-- Creates database bench (drop it afterwards: DROP DATABASE bench).

CREATE DATABASE IF NOT EXISTS bench;
DROP TABLE IF EXISTS bench.elements_scaled;
CREATE TABLE bench.elements_scaled AS bronze.rik_report_elements;   -- same engine, PARTITION BY, ORDER BY

-- 125 copies of the sample with new report ids: about 3.6 million rows, the size of one yearly RIK file
INSERT INTO bench.elements_scaled
SELECT concat(report_id, '-', toString(n)), tabel, elemendi_label, elemendi_nimetus,
       toString(toFloat64(vaartus) * (1 + n / 1000)), report_year, _source_file, _file_version, _loaded_at
FROM bronze.rik_report_elements, (SELECT number AS n FROM numbers(125)) AS r;
OPTIMIZE TABLE bench.elements_scaled FINAL;

-- parts / rows / marks that would be read
EXPLAIN ESTIMATE SELECT count() FROM bench.elements_scaled WHERE elemendi_nimetus = 'Revenue';
EXPLAIN ESTIMATE SELECT count() FROM bench.elements_scaled WHERE report_year = 2025 AND elemendi_nimetus = 'Revenue';
EXPLAIN ESTIMATE SELECT count() FROM bench.elements_scaled WHERE report_id = '2439180-7';
EXPLAIN indexes = 1 SELECT count() FROM bench.elements_scaled WHERE report_year = 2025 AND report_id = '2439180-7';

-- exact (R-7, same as PERCENTILE_CONT) vs approximate median
SELECT report_year, round(quantileExactInclusive(0.5)(toFloat64(vaartus))) FROM bench.elements_scaled
WHERE elemendi_nimetus = 'Revenue' GROUP BY report_year ORDER BY report_year;
SELECT report_year, round(median(toFloat64(vaartus))) FROM bench.elements_scaled
WHERE elemendi_nimetus = 'Revenue' GROUP BY report_year ORDER BY report_year;
