{#- dim_size_band holds exactly the five band codes produced by the size_band_code() macro
    (micro < 10, small 10-49, medium 50-249, large 250+, unknown), each once (Melih).
    Returns codes that are missing, unexpected or duplicated. -#}
with expected as (select arrayJoin(['micro', 'small', 'medium', 'large', 'unknown']) as band_code),
actual as (select band_code, count() as n from {{ ref('dim_size_band') }} group by band_code)
select e.band_code as band_code, 'missing' as problem
from expected e where e.band_code not in (select band_code from actual)
union all
select band_code, 'unexpected' from actual where band_code not in (select band_code from expected)
union all
select band_code, 'duplicated' from actual where n > 1
