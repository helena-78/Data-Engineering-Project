{#- Unknown members (Melih): the key macros map a missing natural key to the 'UNKNOWN' member
    (docs/surrogate_keys.md), so each dimension must contain that row exactly once,
    otherwise fact rows with a missing key would fall out of inner joins.
    Returns the dimensions where the unknown member is missing or duplicated. -#}
select * from (
    select 'dim_company' as dimension, count() as n
    from {{ ref('dim_company') }} where company_key = {{ unknown_company_key() }}
    union all
    select 'dim_year', count() from {{ ref('dim_year') }} where year_key = {{ year_key('NULL') }}
    union all
    select 'dim_industry', count() from {{ ref('dim_industry') }} where industry_key = {{ industry_key('NULL', 'NULL') }}
    union all
    select 'dim_size_band', count() from {{ ref('dim_size_band') }} where size_band_key = {{ size_band_key("'unknown'") }}
)
where n != 1
