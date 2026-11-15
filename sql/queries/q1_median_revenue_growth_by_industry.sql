-- Q1. Which industries have the highest median revenue growth (2024 -> 2025)?
-- ClickHouse version of sql/project1_postgres/queries/q1_*.sql.
-- * One row per company with its 2024 and 2025 revenue, built with anyIf/countIf in a single GROUP BY
--   instead of a LAG window: ClickHouse reads the fact once and needs no sort per company.
-- * Grouped by company_id (durable key in the fact), not company_key: dim_company is SCD Type 2,
--   so a company can have a different company_key in 2024 and 2025.
-- * The industry is the one of the 2025 report.
-- * quantileExactInclusive(0.5) = exact median with linear interpolation, the same result as
--   PostgreSQL's PERCENTILE_CONT(0.5). median() would be faster but is approximate.
-- KPI rules: revenue 2024 > 0, groups with fewer than 30 companies are left out.
WITH company_pairs AS (
    SELECT
        f.company_id,
        anyIf(f.industry_key, y.year = 2025) AS industry_key,
        anyIf(f.revenue, y.year = 2024)      AS revenue_2024,
        anyIf(f.revenue, y.year = 2025)      AS revenue_2025
    FROM gold.fact_company_year AS f
    INNER JOIN gold.dim_year AS y ON y.year_key = f.year_key
    WHERE y.year IN (2024, 2025)
    GROUP BY f.company_id
    HAVING countIf(y.year = 2024) = 1 AND countIf(y.year = 2025) = 1
)
SELECT
    i.section_code,
    i.section_name,
    count() AS n_companies,
    round(100 * quantileExactInclusive(0.5)(p.revenue_2025 / p.revenue_2024 - 1), 1) AS median_revenue_growth_pct
FROM company_pairs AS p
INNER JOIN gold.dim_industry AS i ON i.industry_key = p.industry_key
WHERE p.revenue_2024 > 0
  AND p.revenue_2025 IS NOT NULL
  AND i.section_code != 'UNKNOWN'
GROUP BY i.section_code, i.section_name
HAVING n_companies >= 30
ORDER BY median_revenue_growth_pct DESC;
