# Extra credit A: ClickHouse instead of PostgreSQL (Melih)

Project 1 planned PostgreSQL (`sql/project1_postgres/`). Project 2 runs on ClickHouse. This note ties the
ClickHouse features we use to our five questions and lists what we gave up. Report text: Section 9.

## Our query pattern

All five questions are aggregations over the whole company population for one or two years:
medians of ratios (growth, margin, revenue per employee) grouped by industry or size band, with at least
30 companies per group. They read a few columns of many rows. Bronze is the opposite shape of a typical
OLTP table: the financial elements file is long and narrow (about 3.6 to 3.9 million rows per year, 9
columns) and is loaded once a month as a whole file. There are no single-row updates.

## Features we use and why they fit

| Feature | Where | What it does for us |
|---|---|---|
| Column storage | all tables | a query reads only its columns. The median-per-year query on the elements table reads 3 of 9 columns. |
| MergeTree `PARTITION BY` | `bronze.rik_report_elements` by `report_year` | a year filter skips the other years' parts, and a monthly reload replaces one year (`REPLACE PARTITION`) without touching the rest. |
| Sparse primary index (`ORDER BY`) | bronze keys, fact `ORDER BY` | one index mark per 8,192 rows; a lookup by key reads one granule instead of the table. |
| `ReplacingMergeTree` + `FINAL` | bronze | idempotent reloads without a unique constraint. |
| `quantileExactInclusive(0.5)` | Q1 to Q4 | exact median with linear interpolation (R-7), the same value as PostgreSQL's `PERCENTILE_CONT(0.5)`. Checked against pandas on the sample: identical. |
| `anyIf` / `countIf` | Q1, Q5 | 2024 and 2025 values per company in one `GROUP BY` instead of a `LAG` window that sorts every company. |
| `ASOF LEFT JOIN` | fact (`docs/surrogate_keys.md`) | finds the SCD2 company version valid at the report date in one join. |

## Measurements

Embedded ClickHouse 26.9 (chDB 4.4.0) on the test machine. Table `bench.elements_scaled`: the real
sample's elements copied 125 times with new report ids, 3,603,625 rows (the size of one yearly file),
same engine, partition key and `ORDER BY` as `bronze.rik_report_elements`, 4 parts (one per year),
441 granules. Row and granule counts from `EXPLAIN ESTIMATE` / `EXPLAIN indexes = 1`, times are the best of 5.

| Query | Parts read | Rows read | Time |
|---|---|---|---|
| `count()` of Revenue rows, no filter | 4 of 4 | 3,603,625 | 11 ms |
| same with `report_year = 2025` | 1 of 4 | 900,875 (25 %) | 7 ms |
| lookup by `report_id` (primary key) | 1 | 8,192 (1 granule) | 2 ms |
| exact median revenue per year (`quantileExactInclusive`) | 4 of 4 | 3,603,625, 3 of 9 columns | 37 ms |
| approximate median per year (`median`) | 4 of 4 | same | 36 ms |

`median()` is not faster here, and it is approximate (reservoir sampling): its 2025 value was 14,839
against the exact 15,063, about 1.5 % off. So the queries use the exact function.

Q1 to Q5 on the gold tables of the sample (1,959 fact rows) take 6 to 25 ms each (`scripts/run_queries.py`
prints the time per query).

What this does not show: a measured PostgreSQL run. No PostgreSQL server was available on the test machine,
so the comparison below is by design, not by benchmark.

## Same question in both engines (Q1)

PostgreSQL (Project 1): a `LAG(revenue) OVER (PARTITION BY registry_code ORDER BY year)` window, then
`PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY growth)` per industry. Both steps sort.

ClickHouse (now): one `GROUP BY company_id` with `anyIf(revenue, year = 2024)` and
`anyIf(revenue, year = 2025)`, then `quantileExactInclusive(0.5)(growth)` per industry. The fact table is read
once, no window sort. `company_id` (durable hash of the registry code) replaces `registry_code` because the
fact can be built before `dim_company`.

## Trade-offs, and how we handled them

| PostgreSQL gives | ClickHouse | What we did |
|---|---|---|
| enforced primary and foreign keys | not enforced | dbt tests: grain, key-to-dimension, unknown members (`docs/data_quality.md`) |
| immediate uniqueness | `ReplacingMergeTree` removes duplicates only during merges, and already on insert, hiding duplicates in a file | readers use `FINAL`; the loader counts duplicates before insert, the Airflow check fails on them |
| cheap `UPDATE` / `DELETE` | mutations rewrite whole parts, asynchronously | yearly batch loads replace partitions; SCD2 history comes from dbt snapshots, not updates |
| sequences for integer keys | none | md5 surrogate keys from natural keys (Ege's macros) |
| mature join planner | joins are hash joins with the right table in memory | dimensions are small, so they sit on the right side; the fact stays on the left |
| `CAST(NULL AS text)` works | casting NULL to non-Nullable `String` fails | key macros replace NULL with `UNKNOWN` before hashing |

Partition pruning on its own is not unique to ClickHouse: PostgreSQL has declarative partitioning with
pruning too. The difference for us is the combination of column storage, compression and vectorised
aggregate functions, which is what makes full-population medians cheap.

## Suggestion for the fact table

Our queries filter the fact by year and group by industry or size band. `PARTITION BY` the report year and
an `ORDER BY` that starts with the year would let the year filter skip data in the fact as it does in
bronze. With `ORDER BY company_year_key` (a hash) the year filter can not use the index.

## References

- ClickHouse, quantileExactInclusive: https://clickhouse.com/docs/reference/functions/aggregate-functions/quantileExactInclusive
- ClickHouse, MergeTree engine and partitions: https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree
- ClickHouse, primary indexes: https://clickhouse.com/docs/primary-indexes
- PostgreSQL, aggregate functions (ordered-set aggregates, `percentile_cont`): https://www.postgresql.org/docs/current/functions-aggregate.html
- PostgreSQL, table partitioning and partition pruning: https://www.postgresql.org/docs/current/ddl-partitioning.html
