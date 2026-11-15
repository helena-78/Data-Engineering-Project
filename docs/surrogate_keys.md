# Surrogate key rule

Owner: Ege. Code: `dbt/macros/surrogate_keys.sql`. Test: `dbt/tests/assert_surrogate_key_rule.sql`.

## The rule

1. **Every surrogate key is a hash of its natural key**:
   `dbt_utils.generate_surrogate_key([...natural key parts...])` = md5 of the parts joined with `-`,
   returned as a 32-character lower-case hex `String` (`lower(hex(MD5(...)))` on ClickHouse).
2. **Never call `generate_surrogate_key` directly in a model.** Use the project macros below. They normalise
   every part in the same way (cast to String, trim; year as a 4-digit string; section code upper case), so the
   dimension side and the fact side produce the same key even if one side has `registrikood` as String and the
   other as UInt32.
3. **Unknown member.** A missing natural key (NULL or `''`) becomes the literal natural key `'UNKNOWN'`.
   Every dimension carries one extra row built from `'UNKNOWN'`, so fact rows with a missing or unmatched key
   still join (the `relationships` tests stay green and no fact row disappears in an inner join).

Because keys are computed from data, not assigned by a sequence, **the fact and the dimensions can be built
separately and in any order**, and a rebuild gives the same keys again.

### Change from Project 1

Project 1 (`sql/ddl/01_create_star_schema.sql`, PostgreSQL) used `INTEGER` keys (`year_key` 1..4,
`size_band_key` 1..4) with `REFERENCES` constraints. In ClickHouse there are no sequences and no enforced
foreign keys, and integer keys would force the fact to be built after the dimensions to look them up. The keys
are now `String` md5 hashes and referential integrity is checked by dbt `relationships` tests instead of
constraints. Kristjan's DBML/README types change from `int` to `varchar(32)` / `String` (README is Melih's;
please update it there).

## Macros

| Key | Macro | Natural key parts | Unknown member |
|---|---|---|---|
| durable company id | `company_id(registry_code)` | registry code | `company_id('NULL')` = md5(`UNKNOWN`) |
| SCD2 company version | `company_key(registry_code, valid_from)` | registry code, snapshot `dbt_valid_from` as `YYYY-MM-DD hh:mm:ss.ffffff` UTC | `unknown_company_key()` = md5(`UNKNOWN-UNKNOWN`) |
| year | `year_key(year)` | 4-digit year | `year_key('NULL')` |
| industry | `industry_key(section_code, emtak_version)` | section letter (upper), EMTAK version text | `industry_key('NULL', 'NULL')` |
| size band | `size_band_key(band_code)` | band code (lower case) | band code `'unknown'` |
| fact grain | `company_year_key(registry_code, year)` | registry code, 4-digit year | n/a |

Helper: `size_band_code(avg_fte)` returns `micro` (<10), `small` (10 to <50), `medium` (50 to <250),
`large` (250 and more) or `unknown` (NULL). Kristjan's `dim_size_band` seed should contain exactly these five
`band_code`s (plus label and sort order); Gabriel computes `size_band_key(size_band_code(avg_fte))` in the fact.

Hand-checked values (also in the singular test):
`company_id('10000018')` = `827e9c4b6b35341601983da8d9ea2137` = Python `hashlib.md5(b"10000018").hexdigest()`;
`company_year_key('10000018', 2023)` = md5(`10000018-2023`) = `572c997f5a294b8e29fff6d3233e7c3e`.

### NULLs and `generate_surrogate_key` on ClickHouse

dbt_utils 1.4.1 renders `coalesce(cast(<part> as String), '_dbt_utils_surrogate_key_null_')`. On ClickHouse
(26.8) the cast of a NULL to non-Nullable `String` fails before `coalesce` runs:
`Code: 349 ... Cannot convert NULL value to non-Nullable type` (tested with a literal NULL and with a
Nullable column). So calling it directly on a Nullable column breaks the model. The project macros replace
NULL and `''` with `'UNKNOWN'` first, so the cast never sees a NULL.

## SCD2: `company_id` versus `company_key`

- `company_id` is durable: one value per company across all versions. The fact stores it so it can be built
  before `dim_company` exists, and Q1/Q5 can `PARTITION BY company_id` (instead of `registry_code`) for
  year-over-year comparisons.
- `company_key` identifies one version of the company in `dim_company`. It is computed from the snapshot row's
  `dbt_valid_from`. Both `dim_company` and `fact_company_year` select from the **snapshot** through the macro
  `company_versions(ref('snap_company'), 'registry_code')`; neither refs the other.
- History caveat: the RIK basic-data file only has the current state, so history starts at the first snapshot
  run (all 2022 to 2025 reports are older than that). `company_versions` therefore sets `lookup_from` of each
  company's first version to 1900-01-01; later versions are valid from their `dbt_valid_from`.
- Fact lookup: the version valid at the report's `period_end`, done with a ClickHouse `ASOF LEFT JOIN`
  (latest version with `lookup_from <= period_end`). No match (company missing from the basic-data file,
  e.g. deleted) → `unknown_company_key()`.

## Copy-paste snippets

The following were run end to end on ClickHouse 26.8 with the synthetic fixture (throwaway models, removed
afterwards): snapshot → dim → fact → `unique`, `not_null` and `relationships` tests all passed (12 of 12), also
after a company name change created a second snapshot version (old reports kept pointing to version 1).

**Snapshot** (Kristjan, `dbt/snapshots/snap_company.sql`; source columns are bronze, silver can replace it):

```sql
{% snapshot snap_company %}
{{ config(unique_key='registry_code', strategy='check', check_cols=['company_name', 'legal_form', 'county']) }}
select
    ariregistri_kood as registry_code,
    nimi as company_name,
    ettevotja_oiguslik_vorm as legal_form,
    trimBoth(arrayElement(splitByString(', ', asukoha_ehak_tekstina), -1)) as county
from {{ source('bronze', 'rik_company_basic') }} final
{% endsnapshot %}
```

**Dimension** (Kristjan, `dbt/models/gold/dim_company.sql`):

```sql
select company_key, company_id, registry_code, company_name, legal_form, county, valid_from, valid_to, is_current
from {{ company_versions(ref('snap_company')) }}
union all
select {{ unknown_company_key() }}, {{ company_id('NULL') }}, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown',
       toDateTime64('1900-01-01 00:00:00', 6, 'UTC'), CAST(NULL AS Nullable(DateTime64(6, 'UTC'))), true
```

Other dimensions, same pattern, e.g. `dim_year`:

```sql
select {{ year_key('y') }} as year_key, toUInt16(y) as year
from (select arrayJoin(range(2022, 2026)) as y)
union all
select {{ year_key('NULL') }}, toUInt16(0)   -- unknown member
```

**Fact** (Gabriel, `dbt/models/gold/fact_company_year.sql`; `reports` would normally be a silver model):

```sql
with reports as (
    select report_id, registry_code, report_year, section_code, emtak_version, avg_fte,
           {{ company_id('registry_code') }} as company_id,
           toDateTime64(period_end, 6, 'UTC') as period_end_ts      -- period_end as Date in silver
    from {{ ref('silver_report') }}
),
versions as (
    select company_id, company_key, lookup_from
    from {{ company_versions(ref('snap_company')) }}
)
select
    {{ company_year_key('r.registry_code', 'r.report_year') }} as company_year_key,
    r.company_id as company_id,
    if(v.company_key = '', {{ unknown_company_key() }}, v.company_key) as company_key,
    {{ year_key('r.report_year') }} as year_key,
    {{ industry_key('r.section_code', 'r.emtak_version') }} as industry_key,
    {{ size_band_key(size_band_code('r.avg_fte')) }} as size_band_key,
    r.report_id
from reports as r
asof left join versions as v
    on r.company_id = v.company_id and r.period_end_ts >= v.lookup_from
```

(`ASOF LEFT JOIN` fills unmatched String columns with `''` unless `join_use_nulls=1`, hence the `if`.)

**Tests** (in the model YAML):

```yaml
- name: fact_company_year
  columns:
    - name: company_year_key
      data_tests: [unique, not_null]
    - name: company_key
      data_tests:
        - not_null
        - relationships: {to: ref('dim_company'), field: company_key}
    - name: year_key
      data_tests:
        - relationships: {to: ref('dim_year'), field: year_key}
```
