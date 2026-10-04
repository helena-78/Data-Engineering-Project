-- Q1. Which industries have the highest median revenue growth?
-- Growth needs two consecutive years, so 2025 is compared with 2024.
-- Partitioned by registry_code (natural key) because dim_company is SCD Type 2
-- and a company can get a new company_key when its name or county changes.
WITH cy AS (
  SELECT c.registry_code, y.year, f.industry_key, f.revenue,
         LAG(f.revenue) OVER w AS prev_revenue,
         LAG(y.year)    OVER w AS prev_year
  FROM fact_company_year f
  JOIN dim_year y     ON y.year_key = f.year_key
  JOIN dim_company c  ON c.company_key = f.company_key
  WINDOW w AS (PARTITION BY c.registry_code ORDER BY y.year)
)
SELECT i.section_code, i.section_name,
       COUNT(*) AS n_companies,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
             (ORDER BY 100.0 * (cy.revenue / cy.prev_revenue - 1))::numeric, 1)
         AS median_revenue_growth_pct
FROM cy
JOIN dim_industry i ON i.industry_key = cy.industry_key
WHERE cy.year = 2025 AND cy.prev_year = 2024 AND cy.prev_revenue > 0
GROUP BY i.section_code, i.section_name
HAVING COUNT(*) >= 30
ORDER BY median_revenue_growth_pct DESC;
