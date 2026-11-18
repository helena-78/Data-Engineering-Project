{{ config(order_by='company_key') }}
{#- Company dimension, SCD Type 2: one row per version from snap_company. Owner: Kristjan.
    Keys and validity come from the shared macro company_versions(), which the fact uses too,
    so both sides compute company_key from the same expression (docs/surrogate_keys.md). -#}
select company_key, company_id, registry_code, company_name, legal_form, county,
       valid_from, valid_to, is_current
from {{ company_versions(ref('snap_company')) }}
union all
select {{ unknown_company_key() }}, {{ company_id('NULL') }}, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown',
       toDateTime64('1900-01-01 00:00:00', 6, 'UTC'), CAST(NULL AS Nullable(DateTime64(6, 'UTC'))), true