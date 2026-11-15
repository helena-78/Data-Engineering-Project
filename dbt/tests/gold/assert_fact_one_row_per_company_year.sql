{#- Grain of the fact (Melih): one row per company per year. Checked on the durable company_id, so two
    SCD2 versions of the same company in one year are also caught (company_key would hide them).
    Returns the company-years that have more than one row. -#}
select company_id, year_key, count() as n_rows, groupArray(report_id) as report_ids
from {{ ref('fact_company_year') }}
group by company_id, year_key
having n_rows > 1
