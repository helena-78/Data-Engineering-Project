{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='MergeTree()',
    partition_by='year',
    order_by='(industry_key, size_band_key, company_id)'
) }}

{#-
  fact_company_year: one row per company and year (aruandeaasta), from the report chosen in
  silver.silver_company_year. Measures are EUR as Nullable(Float64); NULL = not reported; ratios are not stored.

  Keys only through the project macros (docs/surrogate_keys.md, dbt/macros/surrogate_keys.sql).

  Incremental: insert_overwrite replaces whole year partitions (ALTER TABLE ... REPLACE PARTITION).
  An incremental run rebuilds only the years in RIK_YEARS; other year partitions are left as they are.
  dbt_refresh_gold with full_refresh=true rebuilds the whole table.
-#}

{%- set rik_years = env_var('RIK_YEARS', '2022,2023,2024,2025').split(',') | map('trim') | reject('equalto', '') | list -%}

with company_years as (
    select *
    from {{ ref('silver_company_year') }}
    {% if is_incremental() %}
    where year in ({{ rik_years | join(', ') }})
    {% endif %}
)

select
    {{ company_year_key('registry_code', 'year') }} as company_year_key,
    {{ company_id('registry_code') }} as company_id,
    registry_code,
    cast(year as UInt16) as year,
    report_id,
    -- TODO(Kristjan: snap_company / dim_company): replace the placeholder with the contract snippet in
    --   docs/surrogate_keys.md ("Fact"): versions = company_versions(ref('snap_company')), then
    --   ASOF LEFT JOIN versions on company_id and toDateTime64(period_end, 6, 'UTC') >= lookup_from,
    --   if(v.company_key = '', unknown_company_key(), v.company_key).
    {{ unknown_company_key() }} as company_key,
    {{ year_key('year') }} as year_key,
    -- TODO(Kristjan: dim_industry + EMTAK code -> section mapping): replace the placeholder with
    --   industry_key(section_code, emtak_version) (docs/surrogate_keys.md), section_code looked up from
    --   emtak_code + emtak_version; no EMTAK row or no match -> industry_key('NULL', 'NULL').
    {{ industry_key('NULL', 'NULL') }} as industry_key,
    {{ size_band_key(size_band_code('avg_fte')) }} as size_band_key,
    cast(revenue as Nullable(Float64)) as revenue,
    cast(operating_profit as Nullable(Float64)) as operating_profit,
    cast(assets as Nullable(Float64)) as assets,
    cast(equity as Nullable(Float64)) as equity,
    cast(cash as Nullable(Float64)) as cash,
    cast(current_assets as Nullable(Float64)) as current_assets,
    cast(current_liabilities as Nullable(Float64)) as current_liabilities,
    cast(employee_expense as Nullable(Float64)) as employee_expense,
    cast(avg_fte as Nullable(Float64)) as avg_fte
from company_years
