{{ config(materialized='table', engine='MergeTree()', order_by='(year, registry_code)') }}

{#-
  One report per company and year (aruandeaasta), from the in-scope reports of silver_report.
  Choice when a company filed several reports for the same year:
    has_financials desc, esitatud_kpv desc (latest submission), report_id desc.
  n_candidate_reports = how many in-scope reports the company had for that year.
-#}

with candidates as (
    select
        *,
        count() over (partition by registry_code, year) as n_candidate_reports,
        row_number() over (
            partition by registry_code, year
            order by has_financials desc, esitatud_kpv desc nulls last, toUInt64OrZero(report_id) desc, report_id desc
        ) as rn
    from {{ ref('silver_report') }}
    where is_in_scope = 1
)

select
    report_id,
    registry_code,
    legal_form,
    staatus,
    period_start,
    period_end,
    esitatud_kpv,
    kas_konsolideeritud,
    cast(assumeNotNull(year) as UInt16) as year,   -- not NULL: is_in_scope requires year 2022 to 2025
    revenue,
    operating_profit,
    assets,
    equity,
    cash,
    current_assets,
    current_liabilities,
    employee_expense,
    avg_fte,
    has_financials,
    is_in_scope,
    toUInt32(n_candidate_reports) as n_candidate_reports
from candidates
where rn = 1
