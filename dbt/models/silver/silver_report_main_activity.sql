{{ config(order_by='report_id') }}
{#-
  Main activity of each annual report (one row per report_id = one company-year). Owner: Kristjan.
  Source: bronze.rik_report_emtak_revenue, one row per report x EMTAK code (revenue split by activity).
  Rule (as planned in Project 1): the activity with the highest allocated revenue, ties broken by the
  lowest code. is_declared_main shows whether that row is also the one RIK marks as main activity
  (pohitegevusala = 'jah'); in the sample the two rules differ for 2 of 1,412 reports.
  Section: seed emtak_sections (division = first two digits). The same division can have a different
  section letter in EMTAK 2008 and EMTAK 2025, so the join is on (emtak_version, division).
  Reports without any EMTAK row are not in this model; the fact maps them to the unknown industry.
-#}
with emtak_rows as (
    select
        trimBoth(report_id)                      as report_id,
        trimBoth(emtak)                          as emtak_code,
        trimBoth(emtak_version)                  as emtak_version,
        lower(trimBoth(pohitegevusala)) = 'jah'  as is_main,
        toFloat64OrNull(jaotatud_myygitulu)      as allocated_revenue
    from {{ source('bronze', 'rik_report_emtak_revenue') }} final
    where trimBoth(report_id) != ''
),
ranked as (
    select
        *,
        row_number() over (partition by report_id
                           order by coalesce(allocated_revenue, -1) desc, emtak_code) as rn
    from emtak_rows
),
main as (
    select report_id, emtak_code, emtak_version, allocated_revenue, is_main,
           toUInt8OrNull(substring(emtak_code, 1, 2)) as division
    from ranked
    where rn = 1
),
-- The seed stores ranges; arrayJoin expands them to one row per division, so the join is a plain equi-join.
divisions as (
    select emtak_version,
           toUInt8(arrayJoin(range(division_from, division_to + 1))) as division,
           section_code
    from {{ ref('emtak_sections') }}
)
select
    m.report_id                  as report_id,
    m.emtak_code                 as emtak_code,
    m.emtak_version              as emtak_version,
    m.division                   as division,
    nullIf(d.section_code, '')   as section_code,
    m.allocated_revenue          as allocated_revenue,
    m.is_main                    as is_declared_main,
    -- Code without a section -> both key parts NULL -> unknown member (md5('UNKNOWN-UNKNOWN')).
    -- Otherwise we would get md5('UNKNOWN-EMTAK 2008'), which is not in dim_industry.
    {{ industry_key("nullIf(d.section_code, '')", "if(d.section_code = '', NULL, m.emtak_version)") }} as industry_key
from main as m
left join divisions as d
    on d.emtak_version = m.emtak_version and d.division = m.division