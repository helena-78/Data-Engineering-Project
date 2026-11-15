-- Q2. Which industries have the highest median operating margin?
SELECT i.section_code, i.section_name,
       COUNT(*) AS n_companies,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
             (ORDER BY 100.0 * f.operating_profit / f.revenue)::numeric, 1)
         AS median_operating_margin_pct
FROM fact_company_year f
JOIN dim_year y     ON y.year_key = f.year_key
JOIN dim_industry i ON i.industry_key = f.industry_key
WHERE y.year = 2025 AND f.revenue > 0
GROUP BY i.section_code, i.section_name
HAVING COUNT(*) >= 30
ORDER BY median_operating_margin_pct DESC;
