-- Q2. Which industries have the highest median operating margin (2025)?
-- ClickHouse version of sql/project1_postgres/queries/q2_*.sql.
-- Operating margin = operating_profit / revenue (operating profit = RIK element TotalProfitLoss, "Ärikasum").
-- quantileExactInclusive(0.5) gives the same median as PostgreSQL's PERCENTILE_CONT(0.5).
-- KPI rules: revenue > 0, groups with fewer than 30 companies are left out.
SELECT
    i.section_code,
    i.section_name,
    count() AS n_companies,
    round(100 * quantileExactInclusive(0.5)(f.operating_profit / f.revenue), 1) AS median_operating_margin_pct
FROM gold.fact_company_year AS f
INNER JOIN gold.dim_year AS y     ON y.year_key = f.year_key
INNER JOIN gold.dim_industry AS i ON i.industry_key = f.industry_key
WHERE y.year = 2025
  AND f.revenue > 0
  AND f.operating_profit IS NOT NULL
  AND i.section_code != 'UNKNOWN'
GROUP BY i.section_code, i.section_name
HAVING n_companies >= 30
ORDER BY median_operating_margin_pct DESC;
