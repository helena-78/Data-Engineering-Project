{#- SCD Type 2 history of companies. Owner: Kristjan.
    The RIK basic-data file holds the current state only, so dbt snapshot builds the history itself:
    each run compares company_name, legal_form and county with the stored version; on a change it closes
    the old row (dbt_valid_to) and adds a new one. Companies deleted from the register are NOT invalidated
    (no hard_deletes): their last version stays current, so old reports still find it and every company
    keeps exactly one current version (test assert_dim_company_scd2_valid). -#}
{% snapshot snap_company %}
{{ config(unique_key='registry_code', strategy='check', check_cols=['company_name', 'legal_form', 'county']) }}
select
    trimBoth(ariregistri_kood)          as registry_code,
    trimBoth(nimi)                      as company_name,
    trimBoth(ettevotja_oiguslik_vorm)   as legal_form,
    trimBoth(arrayElement(splitByString(', ', asukoha_ehak_tekstina), -1)) as county
from {{ source('bronze', 'rik_company_basic') }} final
where trimBoth(ariregistri_kood) != ''
{% endsnapshot %}