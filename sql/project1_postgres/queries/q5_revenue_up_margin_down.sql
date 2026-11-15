-- Q5. Which companies grew revenue while their operating margin declined?
-- Partitioned by registry_code for the same SCD Type 2 reason as Q1.
WITH cy AS (
  SELECT c.registry_code, c.company_name, y.year, f.revenue, f.operating_profit,
         LAG(f.revenue)          OVER w AS prev_revenue,
         LAG(f.operating_profit) OVER w AS prev_operating_profit,
         LAG(y.year)             OVER w AS prev_year
  FROM fact_company_year f
  JOIN dim_year y    ON y.year_key = f.year_key
  JOIN dim_company c ON c.company_key = f.company_key
  WINDOW w AS (PARTITION BY c.registry_code ORDER BY y.year)
)
SELECT cy.registry_code, cy.company_name,
       ROUND(100.0 * (cy.revenue / cy.prev_revenue - 1), 1)         AS revenue_growth_pct,
       ROUND(100.0 * cy.prev_operating_profit / cy.prev_revenue, 1) AS operating_margin_2024_pct,
       ROUND(100.0 * cy.operating_profit / cy.revenue, 1)           AS operating_margin_2025_pct
FROM cy
WHERE cy.year = 2025 AND cy.prev_year = 2024
  AND cy.prev_revenue > 0
  AND cy.revenue > cy.prev_revenue
  AND cy.operating_profit / cy.revenue < cy.prev_operating_profit / cy.prev_revenue
ORDER BY revenue_growth_pct DESC
LIMIT 50;
