{{ config(order_by='year_key') }}
{#- Year dimension (static). Owner: Kristjan. One row per report year in RIK_YEARS (the same variable
    the ingestion and the cross-layer test use), plus the unknown member (year 0). -#}
{%- set years = env_var('RIK_YEARS', '2022,2023,2024,2025').split(',') %}
select
    {{ year_key('y') }} as year_key,
    toUInt16(y)         as year
from (select arrayJoin([{{ years | map('trim') | join(', ') }}]) as y)
union all
select {{ year_key('NULL') }}, toUInt16(0)