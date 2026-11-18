{{ config(order_by='industry_key') }}
{#- Industry dimension (Type 1, static). Owner: Kristjan.
    Grain: one EMTAK section per EMTAK version. emtak_version is part of the natural key, because
    EMTAK 2025 (NACE Rev. 2.1) split the old section J into J and K and shifted the later letters,
    so e.g. 'K' means Financial activities in 2008 but Telecom/IT in 2025. Plus the unknown member. -#}
select
    {{ industry_key('section_code', 'emtak_version') }} as industry_key,
    section_code,
    section_name,
    emtak_version,
    division_from,
    division_to
from {{ ref('emtak_sections') }}
union all
select {{ industry_key('NULL', 'NULL') }}, 'UNKNOWN', 'Unknown', 'UNKNOWN', toUInt8(0), toUInt8(0)