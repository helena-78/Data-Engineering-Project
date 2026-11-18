{{ config(materialized='table', engine='MergeTree()', order_by='report_id',
          query_settings={'join_use_nulls': 1}) }}

{#-
  One row per annual report (bronze.rik_report_general), typed, with its measures and main EMTAK activity.

  * year = aruandeaasta (the report year as filed), not the year of period_end.
  * Main EMTAK activity (bronze.rik_report_emtak_revenue): the pohitegevusala = 'jah' row; otherwise the
    row with the highest jaotatud_myygitulu, ties by lowest code; no rows -> NULL (UNKNOWN in gold).
  * is_in_scope: legal form in OÜ, AS, TÜH, UÜ, TÜ; not consolidated; year 2022 to 2025.
    The bronze files have no currency column, so there is no currency condition.
    staatus is not filtered on; taidetud_aruanne_report_id is ignored.
-#}

with general as (
    select
        report_id,
        trimBoth(registrikood) as registry_code,
        trimBoth(oiguslik_vorm) as legal_form,
        trimBoth(staatus) as staatus,
        toDate(parseDateTimeOrNull(trimBoth(period_start), '%d.%m.%Y')) as period_start,
        toDate(parseDateTimeOrNull(trimBoth(period_end), '%d.%m.%Y')) as period_end,
        toDate(parseDateTimeOrNull(trimBoth(esitatud_kpv), '%d.%m.%Y')) as esitatud_kpv,
        trimBoth(kas_konsolideeritud) as kas_konsolideeritud,
        toUInt16OrNull(trimBoth(aruandeaasta)) as year
    from {{ source('bronze', 'rik_report_general') }} final
),

emtak_ranked as (
    select
        report_id,
        nullIf(trimBoth(emtak), '') as emtak_code,
        nullIf(trimBoth(emtak_version), '') as emtak_version,
        row_number() over (
            partition by report_id
            order by
                lower(trimBoth(pohitegevusala)) = 'jah' desc,
                toFloat64OrNull(trimBoth(jaotatud_myygitulu)) desc nulls last,
                trimBoth(emtak) asc
        ) as rn
    from {{ source('bronze', 'rik_report_emtak_revenue') }} final
),

main_emtak as (
    select report_id, emtak_code, emtak_version
    from emtak_ranked
    where rn = 1
)

select
    g.report_id as report_id,
    g.registry_code as registry_code,
    g.legal_form as legal_form,
    g.staatus as staatus,
    g.period_start as period_start,
    g.period_end as period_end,
    g.esitatud_kpv as esitatud_kpv,
    g.kas_konsolideeritud as kas_konsolideeritud,
    g.year as year,
    e.emtak_code as emtak_code,
    e.emtak_version as emtak_version,
    i.revenue as revenue,
    i.operating_profit as operating_profit,
    i.assets as assets,
    i.equity as equity,
    i.cash as cash,
    i.current_assets as current_assets,
    i.current_liabilities as current_liabilities,
    i.employee_expense as employee_expense,
    i.avg_fte as avg_fte,
    toUInt8(coalesce(i.has_financials, 0)) as has_financials,
    toUInt8(
        g.legal_form in ('OÜ', 'AS', 'TÜH', 'UÜ', 'TÜ')
        and g.kas_konsolideeritud = 'Ei'
        and coalesce(g.year between 2022 and 2025, false)
    ) as is_in_scope
from general as g
left join main_emtak as e on e.report_id = g.report_id
left join {{ ref('silver_report_indicators') }} as i on i.report_id = g.report_id
