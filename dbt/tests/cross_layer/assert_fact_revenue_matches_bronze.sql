{{ config(severity='warn') }}
{#- Reconciliation bronze -> gold (Melih): the revenue in the fact must be the 'Revenue' element
    ("Müügitulu") of the selected report in its income statement (Kasumiaruanne skeem 1 or 2, also
    the non-consolidated variants). Catches pivot errors such as picking the wrong statement table or
    summing duplicates. Returns the fact rows whose revenue differs by more than 1 euro. -#}
with bronze_revenue as (
    select report_id, max(toFloat64OrNull(vaartus)) as bronze_revenue
    from {{ source('bronze', 'rik_report_elements') }} final
    where elemendi_nimetus = 'Revenue'
      and tabel in ('Kasumiaruanne skeem 1', 'Kasumiaruanne skeem 2',
                    'Konsolideerimata kasumiaruanne (skeem 1)', 'Konsolideerimata kasumiaruanne (skeem 2)')
    group by report_id
)
select f.company_year_key as company_year_key, f.report_id as report_id,
       f.revenue as fact_revenue, b.bronze_revenue as bronze_revenue
from {{ ref('fact_company_year') }} as f
inner join bronze_revenue as b on b.report_id = f.report_id
where f.revenue is null or abs(f.revenue - b.bronze_revenue) > 1
