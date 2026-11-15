{{ config(severity='warn') }}
{#- Value range (Melih): revenue, total assets and cash should not be negative.
    Warning only: the real filings contain a few negative revenue lines (corrections, returns), about
    1 in 1,200 reports in the sample. Q1 to Q5 already require revenue > 0 for every ratio.
    Not checked: employee_expense and operating_profit, which are negative by design
    (expenses are reported with a minus sign in Kasumiaruanne skeem 1, and losses are negative). -#}
select company_year_key, report_id, revenue, assets, cash
from {{ ref('fact_company_year') }}
where revenue < 0 or assets < 0 or cash < 0
