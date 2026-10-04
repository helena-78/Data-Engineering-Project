-- Q4. How does median operating margin vary by company size?
SELECT s.band_label,
       COUNT(*) AS n_companies,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
             (ORDER BY 100.0 * f.operating_profit / f.revenue)::numeric, 1)
         AS median_operating_margin_pct
FROM fact_company_year f
JOIN dim_year y      ON y.year_key = f.year_key
JOIN dim_size_band s ON s.size_band_key = f.size_band_key
WHERE y.year = 2025 AND f.revenue > 0
GROUP BY s.band_label, s.sort_order
ORDER BY s.sort_order;
