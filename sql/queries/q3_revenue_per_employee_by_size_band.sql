-- Q3. How does revenue per employee differ by employee size band (2025)?
-- ClickHouse version of sql/project1_postgres/queries/q3_*.sql.
-- Revenue per employee = revenue / average FTE employees. Medians, because a few very large ratios
-- (holding companies with one employee) would dominate an average.
-- KPI rules: avg_fte > 0, groups with fewer than 30 companies are left out.
SELECT
    s.band_label,
    count() AS n_companies,
    round(quantileExactInclusive(0.5)(f.revenue / f.avg_fte), 0) AS median_revenue_per_employee_eur
FROM gold.fact_company_year AS f
INNER JOIN gold.dim_year AS y      ON y.year_key = f.year_key
INNER JOIN gold.dim_size_band AS s ON s.size_band_key = f.size_band_key
WHERE y.year = 2025
  AND f.avg_fte > 0
  AND f.revenue IS NOT NULL
GROUP BY s.band_label, s.sort_order
HAVING n_companies >= 30
ORDER BY s.sort_order;
