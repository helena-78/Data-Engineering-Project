{#- SCD Type 2 rules for dim_company (Melih):
    1. every company has exactly one current version,
    2. valid_to is after valid_from (or NULL for the current version),
    3. versions of one company do not overlap.
    Returns the registry codes that break a rule, with the rule. -#}
with d as (
    select registry_code, valid_from, valid_to, is_current
    from {{ ref('dim_company') }}
    where registry_code != 'UNKNOWN'
),
ordered as (
    select registry_code, valid_from, valid_to,
           leadInFrame(valid_from) over (partition by registry_code order by valid_from
                                         rows between current row and 1 following) as next_valid_from,
           row_number() over (partition by registry_code order by valid_from desc) as rn
    from d
)
select registry_code, 'not exactly one current version' as rule
from d group by registry_code having countIf(is_current) != 1
union all
select registry_code, 'valid_to not after valid_from'
from d where valid_to is not null and valid_to <= valid_from
union all
select registry_code, 'overlapping versions'
from ordered where rn > 1 and (valid_to is null or valid_to > next_valid_from)
