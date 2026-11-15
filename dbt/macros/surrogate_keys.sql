{#-
  Surrogate key rule (docs/surrogate_keys.md)
  ===========================================
  Every surrogate key = dbt_utils.generate_surrogate_key([...natural key parts...]):
  md5 of the parts joined with '-', as a 32-character lower-case hex String.
  The macros below normalise each part the same way on the dimension side and on the fact side,
  so fact and dimension models can be built independently and still join.

  Normalisation of one part (sk_text):  toString -> trim -> '' or NULL becomes 'UNKNOWN'.
  Year part (sk_year):                  4-digit year string ('2023'); invalid/NULL -> 'UNKNOWN'.
  Section part:                         upper case (sk_text(..., upper=true)).
  Timestamp part (sk_timestamp):        'YYYY-MM-DD hh:mm:ss.ffffff' in UTC.
  Because parts are never NULL, the NULL handling of generate_surrogate_key is never reached
  (on ClickHouse its cast(NULL as String) would fail, see docs/surrogate_keys.md).
-#}

{%- macro sk_text(expr, upper=false) -%}
    {%- set e = "trimBoth(toString(" ~ expr ~ "))" -%}
    {%- if upper %}{% set e = "upper(" ~ e ~ ")" %}{% endif -%}
    coalesce(nullIf({{ e }}, ''), '{{ var("unknown_natural_key", "UNKNOWN") }}')
{%- endmacro -%}

{%- macro sk_year(expr) -%}
    coalesce(toString(toUInt16OrNull(trimBoth(toString({{ expr }})))), '{{ var("unknown_natural_key", "UNKNOWN") }}')
{%- endmacro -%}

{%- macro sk_timestamp(expr) -%}
    coalesce(formatDateTime(toDateTime64({{ expr }}, 6, 'UTC'), '%Y-%m-%d %H:%i:%S.%f', 'UTC'), '{{ var("unknown_natural_key", "UNKNOWN") }}')
{%- endmacro -%}


{#- ---------------- one macro per key ---------------- -#}

{#- Durable company key: one value per company for its whole life (all SCD2 versions share it). -#}
{%- macro company_id(registry_code) -%}
    {{ dbt_utils.generate_surrogate_key([sk_text(registry_code)]) }}
{%- endmacro -%}

{#- SCD2 version key of dim_company: registry code + dbt_valid_from of the snapshot row. -#}
{%- macro company_key(registry_code, valid_from) -%}
    {{ dbt_utils.generate_surrogate_key([sk_text(registry_code), sk_timestamp(valid_from)]) }}
{%- endmacro -%}

{%- macro year_key(year) -%}
    {{ dbt_utils.generate_surrogate_key([sk_year(year)]) }}
{%- endmacro -%}

{%- macro industry_key(section_code, emtak_version) -%}
    {{ dbt_utils.generate_surrogate_key([sk_text(section_code, upper=true), sk_text(emtak_version)]) }}
{%- endmacro -%}

{#- Size band from average FTE employees: micro <10, small 10-49, medium 50-249, large 250+. -#}
{%- macro size_band_code(avg_fte) -%}
    multiIf(
        isNull({{ avg_fte }}), 'unknown',
        toFloat64({{ avg_fte }}) < 10, 'micro',
        toFloat64({{ avg_fte }}) < 50, 'small',
        toFloat64({{ avg_fte }}) < 250, 'medium',
        'large')
{%- endmacro -%}

{#- Band codes are lower case; the unknown member of dim_size_band is band_code 'unknown'. -#}
{%- macro size_band_key(band_code) -%}
    {{ dbt_utils.generate_surrogate_key(["coalesce(nullIf(lower(trimBoth(toString(" ~ band_code ~ "))), ''), 'unknown')"]) }}
{%- endmacro -%}

{#- Grain key of fact_company_year. -#}
{%- macro company_year_key(registry_code, year) -%}
    {{ dbt_utils.generate_surrogate_key([sk_text(registry_code), sk_year(year)]) }}
{%- endmacro -%}

{#- Unknown-member keys, for the extra row each dimension carries and for fact-side fallbacks. -#}
{%- macro unknown_company_key() -%}{{ company_key('NULL', 'NULL') }}{%- endmacro -%}


{#-
  company_versions(snapshot_relation, registry_code_column='registry_code')
  ------------------------------------------------------------------------
  One row per SCD2 version of a company snapshot, with the keys and the lookup range.
  Used by dim_company AND by fact_company_year (both select from the snapshot, not from each other),
  so the company_key on both sides comes from the same expression.

  lookup_from: the RIK basic-data file holds the current state only, so history starts with the first
  snapshot run. For the earliest version of each company lookup_from is 1900-01-01, so reports from
  years before the first snapshot still find a company version. lookup_to: dbt_valid_to, or 2299-12-31 23:59:59 (DateTime64 maximum) for the current version.
-#}
{%- macro company_versions(snapshot_relation, registry_code_column='registry_code') -%}
(
    select
        *,
        {{ company_id(registry_code_column) }} as company_id,
        {{ company_key(registry_code_column, 'dbt_valid_from') }} as company_key,
        toDateTime64(dbt_valid_from, 6, 'UTC') as valid_from,
        toDateTime64(dbt_valid_to, 6, 'UTC') as valid_to,
        if(row_number() over (partition by {{ registry_code_column }} order by dbt_valid_from) = 1,
           toDateTime64('1900-01-01 00:00:00', 6, 'UTC'),
           toDateTime64(dbt_valid_from, 6, 'UTC')) as lookup_from,
        coalesce(toDateTime64(dbt_valid_to, 6, 'UTC'), toDateTime64('2299-12-31 23:59:59', 6, 'UTC')) as lookup_to,
        isNull(dbt_valid_to) as is_current
    from {{ snapshot_relation }}
)
{%- endmacro -%}
