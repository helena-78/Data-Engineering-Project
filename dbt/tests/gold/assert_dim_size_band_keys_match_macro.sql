{#- The keys in the dim_size_band seed are typed by hand (a CSV cannot call a macro).
    They must equal what the fact computes with size_band_key(); returns the rows that differ. -#}
select band_code, size_band_key, {{ size_band_key('band_code') }} as expected_key
from {{ ref('dim_size_band') }}
where size_band_key != {{ size_band_key('band_code') }}