{{ config(severity='warn') }}
{#- Reconciliation bronze -> gold (Melih): every company-year that has an annual report in bronze for the
    configured years (RIK_YEARS) should have a row in fact_company_year.
    Warning only, because silver may drop reports on purpose (e.g. consolidated or liquidation reports);
    the warning shows how many and which years, so the drop is visible and can be explained.
    Returns one row per year with the number of company-years missing from the fact. -#}
{%- set years = env_var('RIK_YEARS', '2022,2023,2024,2025').split(',') -%}
with bronze_cy as (
    select distinct {{ company_id('registrikood') }} as company_id, toUInt16(aruandeaasta) as year
    from {{ source('bronze', 'rik_report_general') }} final
    where aruandeaasta in ({% for y in years %}'{{ y | trim }}'{% if not loop.last %}, {% endif %}{% endfor %})
),
fact_cy as (
    select f.company_id as company_id, y.year as year
    from {{ ref('fact_company_year') }} as f
    inner join {{ ref('dim_year') }} as y on y.year_key = f.year_key
)
select year, count() as missing_company_years
from bronze_cy
where (company_id, year) not in (select company_id, year from fact_cy)
group by year
