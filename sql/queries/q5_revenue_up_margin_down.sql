-- Q5. Which companies grew revenue from 2024 to 2025 while their operating margin declined?
-- ClickHouse version of sql/project1_postgres/queries/q5_*.sql.
-- Same per-company pairing as Q1 (anyIf per year, grouped by the durable company_id).
-- The company name is taken from the dim_company version linked to the 2025 report.
-- n_matching (window over the whole result) shows how many companies match in total,
-- because only the 50 with the highest growth are listed.
WITH company_pairs AS (
    SELECT
        f.company_id,
        anyIf(f.company_key, y.year = 2025)      AS company_key_2025,
        anyIf(f.revenue, y.year = 2024)          AS revenue_2024,
        anyIf(f.revenue, y.year = 2025)          AS revenue_2025,
        anyIf(f.operating_profit, y.year = 2024) AS operating_profit_2024,
        anyIf(f.operating_profit, y.year = 2025) AS operating_profit_2025
    FROM gold.fact_company_year AS f
    INNER JOIN gold.dim_year AS y ON y.year_key = f.year_key
    WHERE y.year IN (2024, 2025)
    GROUP BY f.company_id
    HAVING countIf(y.year = 2024) = 1 AND countIf(y.year = 2025) = 1
)
SELECT
    c.registry_code,
    c.company_name,
    round(100 * (p.revenue_2025 / p.revenue_2024 - 1), 1)    AS revenue_growth_pct,
    round(100 * p.operating_profit_2024 / p.revenue_2024, 1) AS operating_margin_2024_pct,
    round(100 * p.operating_profit_2025 / p.revenue_2025, 1) AS operating_margin_2025_pct,
    count() OVER ()                                          AS n_matching
FROM company_pairs AS p
INNER JOIN gold.dim_company AS c ON c.company_key = p.company_key_2025
WHERE p.revenue_2024 > 0
  AND p.revenue_2025 > p.revenue_2024
  AND p.operating_profit_2024 IS NOT NULL
  AND p.operating_profit_2025 IS NOT NULL
  AND p.operating_profit_2025 / p.revenue_2025 < p.operating_profit_2024 / p.revenue_2024
ORDER BY revenue_growth_pct DESC, c.registry_code
LIMIT 50;
