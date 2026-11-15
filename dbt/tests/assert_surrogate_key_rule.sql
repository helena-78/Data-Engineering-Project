{#-
  Singular test for the surrogate key rule (docs/surrogate_keys.md). Returns one row per failed check.
  1. Known inputs give the md5 computed by hand (Python: hashlib.md5('10000018'.encode()).hexdigest()).
  2. The same natural key written differently on the dim side and the fact side gives the same key
     (String vs number, surrounding spaces, lower vs upper section code, Date vs DateTime).
  3. The macros are deterministic (same input twice -> same key) and NULL/'' map to the UNKNOWN member.
-#}
with checks as (
    -- 1. hand-computed md5 values
    select 'company_id literal' as check_name,
           {{ company_id("'10000018'") }} as actual, '827e9c4b6b35341601983da8d9ea2137' as expected
    union all select 'year_key literal', {{ year_key('2023') }}, '5531a5834816222280f20d1ef9e95f69'
    union all select 'company_year_key literal', {{ company_year_key("'10000018'", '2023') }},
                     '572c997f5a294b8e29fff6d3233e7c3e'
    union all select 'industry_key literal', {{ industry_key("'C'", "'EMTAK 2008'") }},
                     '427644faa2db91655c29d5d1d0c0168f'
    union all select 'size_band_key literal', {{ size_band_key("'micro'") }}, '1c21d9b19298645fa2534d9828ee0778'
    union all select 'company_key literal',
                     {{ company_key("'10000018'", "toDateTime64('2026-11-14 10:00:00', 6, 'UTC')") }},
                     'f0401159bf7c1fa69dd0a801e3bd7351'

    -- 2. dim-side vs fact-side spellings of the same natural key
    union all select 'company_id String vs UInt32',
                     {{ company_id("' 10000018 '") }}, {{ company_id('toUInt32(10000018)') }}
    union all select 'year_key String vs UInt16 vs toYear(Date)',
                     {{ year_key("'2023'") }}, {{ year_key("toYear(toDate('2023-12-31'))") }}
    union all select 'year_key Int64', {{ year_key('toInt64(2023)') }}, {{ year_key("'2023'") }}
    union all select 'company_year_key mixed types',
                     {{ company_year_key("'10000018'", "'2023'") }}, {{ company_year_key('toUInt32(10000018)', 'toUInt16(2023)') }}
    union all select 'industry_key case and spaces',
                     {{ industry_key("' c '", "'EMTAK 2008'") }}, {{ industry_key("'C'", "' EMTAK 2008'") }}
    union all select 'company_key DateTime vs DateTime64',
                     {{ company_key("'10000018'", "toDateTime('2026-11-14 10:00:00', 'UTC')") }},
                     {{ company_key('toUInt32(10000018)', "toDateTime64('2026-11-14 10:00:00.000000', 6, 'UTC')") }}
    union all select 'size_band_key case', {{ size_band_key("' MICRO '") }}, {{ size_band_key("'micro'") }}

    -- 3. determinism and the unknown member
    union all select 'deterministic', {{ company_id("'10000018'") }}, {{ company_id("'10000018'") }}
    union all select 'NULL registry code -> UNKNOWN',
                     {{ company_id('CAST(NULL AS Nullable(String))') }}, '696b031073e74bf2cb98e5ef201d4aa3'
    union all select 'empty registry code -> UNKNOWN', {{ company_id("''") }}, {{ company_id('NULL') }}
    union all select 'invalid year -> UNKNOWN', {{ year_key("'n/a'") }}, {{ year_key('NULL') }}
    union all select 'unknown company_key', {{ unknown_company_key() }}, '80c42cdba55d859e15482e351bdf3147'
    union all select 'NULL size band -> unknown', {{ size_band_key('NULL') }}, 'ad921d60486366258809553a3db49a4a'

    -- size band boundaries (string compare, not keys)
    union all select 'band 9.99', {{ size_band_code('9.99') }}, 'micro'
    union all select 'band 10', {{ size_band_code('10') }}, 'small'
    union all select 'band 49.9', {{ size_band_code('49.9') }}, 'small'
    union all select 'band 50', {{ size_band_code('50') }}, 'medium'
    union all select 'band 249.5', {{ size_band_code('249.5') }}, 'medium'
    union all select 'band 250', {{ size_band_code('250') }}, 'large'
    union all select 'band NULL', {{ size_band_code('CAST(NULL AS Nullable(Float64))') }}, 'unknown'
)
select check_name, actual, expected
from checks
where actual != expected or actual is null or length(actual) = 0
