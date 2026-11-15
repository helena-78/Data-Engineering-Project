{#- Foreign keys of the fact (Melih). ClickHouse has no enforced foreign keys, so this test does their job:
    every key in fact_company_year must exist in its dimension. Missing natural keys are mapped to the
    unknown member of each dimension by the key macros, so this should always return 0 rows.
    Returns one row per dimension with the number of fact rows whose key is not found. -#}
with fact as (select * from {{ ref('fact_company_year') }})
select * from (
    select 'dim_company' as dimension, count() as orphan_rows from fact
    where company_key not in (select company_key from {{ ref('dim_company') }})
    union all
    select 'dim_year', count() from fact
    where year_key not in (select year_key from {{ ref('dim_year') }})
    union all
    select 'dim_industry', count() from fact
    where industry_key not in (select industry_key from {{ ref('dim_industry') }})
    union all
    select 'dim_size_band', count() from fact
    where size_band_key not in (select size_band_key from {{ ref('dim_size_band') }})
)
where orphan_rows > 0
