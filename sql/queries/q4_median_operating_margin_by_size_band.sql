-- Q4. How does the median operating margin vary by company size (2025)?
-- ClickHouse version of sql/project1_postgres/queries/q4_*.sql.
-- KPI rules: revenue > 0, size band known, groups with fewer than 30 companies are left out.
SELECT
    s.band_label,
    count() AS n_companies,
    round(100 * quantileExactInclusive(0.5)(f.operating_profit / f.revenue), 1) AS median_operating_margin_pct
FROM gold.fact_company_year AS f
INNER JOIN gold.dim_year AS y      ON y.year_key = f.year_key
INNER JOIN gold.dim_size_band AS s ON s.size_band_key = f.size_band_key
WHERE y.year = 2025
  AND f.revenue > 0
  AND f.operating_profit IS NOT NULL
  AND s.band_code != 'unknown'
GROUP BY s.band_label, s.sort_order
HAVING n_companies >= 30
ORDER BY s.sort_order;
