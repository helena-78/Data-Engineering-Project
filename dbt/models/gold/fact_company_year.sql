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
  company_key: ASOF LEFT JOIN on the snap_company versions (company_versions macro), version valid at period_end.
  industry_key: taken from silver_report_main_activity (Kristjan). No match on either -> unknown member.

  Incremental: insert_overwrite replaces whole year partitions (ALTER TABLE ... REPLACE PARTITION).
  An incremental run rebuilds only the years in RIK_YEARS; other year partitions are left as they are.
  dbt_refresh_gold with full_refresh=true rebuilds the whole table.
-#}

{%- set rik_years = env_var('RIK_YEARS', '2022,2023,2024,2025').split(',') | map('trim') | reject('equalto', '') | list -%}

with company_years as (
    select
        *,
        {{ company_id('registry_code') }} as company_id,
        toDateTime64(period_end, 6, 'UTC') as period_end_ts
    from {{ ref('silver_company_year') }}
    {% if is_incremental() %}
    where year in ({{ rik_years | join(', ') }})
    {% endif %}
),

-- SCD2 versions of each company, from the snapshot through the same macro as dim_company.
versions as (
    select company_id, company_key, lookup_from
    from {{ company_versions(ref('snap_company')) }}
)

select
    {{ company_year_key('cy.registry_code', 'cy.year') }} as company_year_key,
    cy.company_id as company_id,
    cy.registry_code as registry_code,
    cast(cy.year as UInt16) as year,
    cy.report_id as report_id,
    -- Company version valid at period_end (latest version with lookup_from <= period_end).
    -- No match: ClickHouse fills the String with '' -> unknown member.
    if(v.company_key = '', {{ unknown_company_key() }}, v.company_key) as company_key,
    {{ year_key('cy.year') }} as year_key,
    -- Industry of the main activity, computed in silver_report_main_activity (not recomputed here).
    -- No EMTAK row for the report: '' -> unknown member.
    if(m.industry_key = '', {{ industry_key('NULL', 'NULL') }}, m.industry_key) as industry_key,
    {{ size_band_key(size_band_code('cy.avg_fte')) }} as size_band_key,
    cast(cy.revenue as Nullable(Float64)) as revenue,
    cast(cy.operating_profit as Nullable(Float64)) as operating_profit,
    cast(cy.assets as Nullable(Float64)) as assets,
    cast(cy.equity as Nullable(Float64)) as equity,
    cast(cy.cash as Nullable(Float64)) as cash,
    cast(cy.current_assets as Nullable(Float64)) as current_assets,
    cast(cy.current_liabilities as Nullable(Float64)) as current_liabilities,
    cast(cy.employee_expense as Nullable(Float64)) as employee_expense,
    cast(cy.avg_fte as Nullable(Float64)) as avg_fte
from company_years as cy
asof left join versions as v
    on v.company_id = cy.company_id
   and cy.period_end_ts >= v.lookup_from
left join {{ ref('silver_report_main_activity') }} as m
    on m.report_id = cy.report_id
