-- Q3. How does revenue per employee differ by employee size band?
SELECT s.band_label,
       COUNT(*) AS n_companies,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
             (ORDER BY f.revenue / f.avg_fte)::numeric, 0)
         AS median_revenue_per_employee_eur
FROM fact_company_year f
JOIN dim_year y      ON y.year_key = f.year_key
JOIN dim_size_band s ON s.size_band_key = f.size_band_key
WHERE y.year = 2025 AND f.avg_fte > 0
GROUP BY s.band_label, s.sort_order
ORDER BY s.sort_order;
