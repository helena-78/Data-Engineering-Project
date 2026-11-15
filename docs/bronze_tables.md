# Bronze tables (ClickHouse database `bronze`)

Owner: Ege. Loaded by `scripts/load_bronze.py` (sample, `bronze_sample_load` DAG) and by Olena's ingestion DAGs (full files).
The machine-readable version of everything below is `scripts/bronze_schema.py`
(`BRONZE_TABLES`, `expected_headers(key)`); `clickhouse/init/02_create_bronze_tables.sql` is generated from it
and a unit test fails if the two drift apart. Melih's "missing columns" check can import it:

```python
import sys; sys.path.insert(0, "/opt/airflow/scripts")
from bronze_schema import BRONZE_TABLES, expected_headers
expected_headers("report_general")           # exact CSV header list, in file order
BRONZE_TABLES["report_general"].columns      # bronze column names, same order
BRONZE_TABLES["report_general"].full_name    # 'bronze.rik_report_general'
```

## Rules for all bronze tables

- **Raw fidelity.** Every source column is `String`, stored exactly as in the file: no casting, no trimming,
  an empty field stays `''` (never NULL). Dates stay `dd.mm.yyyy`, numbers stay text with `.` decimals.
  Typing and cleaning happen in silver (Gabriel).
- **Column names** = source header lower-cased, Estonian letters transliterated (õ→o, ä→a, ö→o, ü→y, š→s, ž→z),
  spaces → `_`, `?` and parentheses dropped. ü→y follows RIK's own spelling (`EMTAK_myygitulu`), which gives
  `jaotatud_myygitulu` as listed in the plan.
- **Metadata:** `_source_file` (CSV name), `_file_version` (the `kuni_<ddmmyyyy>` part, or `RIK_FILE_VERSION`
  for sample files), `_loaded_at DateTime DEFAULT now()`.
- **Source format** (all files): UTF-8, `;` separator, CRLF, header row; string fields double-quoted except
  in the company basic-data file, which is unquoted. The loader accepts an optional UTF-8 BOM, quoted `;`,
  doubled quotes (`""`) and CRLF, and fails on any row with the wrong number of fields.

## Reloads, duplicates and how to read

- **Engine `ReplacingMergeTree(_loaded_at)`.** Rows with the same `ORDER BY` key collapse to the one with the
  latest `_loaded_at` when ClickHouse merges parts. Merges happen in the background, so a plain `SELECT` can
  briefly see old and new copies. **Readers (silver models, DQ checks, ad-hoc queries) use `FINAL`**, e.g.
  `select ... from {{ source('bronze', 'rik_report_general') }} final`, or `argMax(col, _loaded_at)` grouped by the key.
- **Idempotent reloads, two options (Olena chooses):**
  1. *Partition replace* (what `load_bronze.py` does): load the file into a temporary table, then
     `ALTER TABLE ... REPLACE PARTITION`. `rik_report_elements` replaces only that year; the other three tables
     are unpartitioned, so the whole table is replaced (each RIK file is a full snapshot). Rerunning leaves
     exactly one copy even before merges (tested: `count()` stayed 67 / 438 / 71 / 19 after a second load).
  2. *Plain `INSERT` + dedup*: insert again and rely on ReplacingMergeTree + `FINAL`. Simpler, but rows that
     disappeared from a newer file stay in bronze, and `count()` without `FINAL` over-counts until merges.
- **In-file duplicates are collapsed by design.** ClickHouse applies the replacing logic already at insert
  time (`optimize_on_insert=1` by default; verified on 26.8: 3 rows with 2 equal keys → 2 rows). So
  `bronze` never shows source duplicates reliably. `load_bronze.py` therefore inserts into its temporary table
  with `optimize_on_insert=0` and merges stopped, and returns per file:
  `exact_duplicate_rows` (identical rows) and `key_conflict_rows` (same key, different values: real data
  loss under `FINAL`, logged as a WARNING with examples). **Melih: use these stats (task log / XCom) for the
  duplicate `report_id` check; `count() - uniqExact(report_id)` on bronze will be 0 after merges.**
  With the synthetic fixture: `rik_report_general` 67 rows read, `exact_duplicate_rows=1`, 66 rows with `FINAL`.
- **Elements key includes `elemendi_label`.** About 9k rows in the 2025 file have an empty `elemendi_nimetus`.
  With the plan's key `(report_id, tabel, elemendi_nimetus)` all empty-name rows of one report table would
  collapse into one. In the fixture this would have lost 63 of 438 rows; with `elemendi_label` added, 0 rows
  are lost. Whether the real files still contain key conflicts is reported by the loader when the real sample
  is loaded (`key_conflict_rows`).

## `bronze.rik_report_general`

- **Source file:** `1.aruannete_yldandmed_kuni_<version>.csv` (zip: `1.aruannete_yldandmed_kuni_{version}{suffix}.zip`)
- **Sample file name:** `aruannete_yldandmed.csv`
- **Grain:** one row per annual report (report_id)
- **Engine / key:** `ReplacingMergeTree(_loaded_at)`, `ORDER BY (report_id)`

| # | Column | Original header |
|---|---|---|
| 1 | `report_id` | `report_id` |
| 2 | `taidetud_aruanne_report_id` | `taidetud_aruanne_report_id` |
| 3 | `registrikood` | `registrikood` |
| 4 | `oiguslik_vorm` | `õiguslik vorm` |
| 5 | `staatus` | `staatus` |
| 6 | `aruandeaasta` | `aruandeaasta` |
| 7 | `kas_konsolideeritud` | `kas konsolideeritud?` |
| 8 | `period_start` | `period_start` |
| 9 | `period_end` | `period_end` |
| 10 | `esitatud_kpv` | `esitatud_kpv` |
| 11 | `kas_auditeeritud` | `kas auditeeritud?` |
| 12 | `valitud_aruanne_kategooria` | `valitud aruanne kategooria` |
| 13 | `minimaalne_kategooria_andmete_alusel` | `minimaalne kategooria andmete alusel` |
| 14 | `auditi_toovotu_liik` | `auditi töövõtu liik` |
| 15 | `audiitori_otsuse_tyyp` | `audiitori otsuse tüüp` |
| 16 | `modifikatsioon_asjaolu_rohutamine` | `modifikatsioon asjaolu rõhutamine` |
| 17 | `modifikatsioon_muu_asjaolu` | `modifikatsioon muu asjaolu` |
| 18 | `modifikatsioon_tegevuse_jatkuvus` | `modifikatsioon tegevuse jatkuvus` |
| 19 | `audiitorettevotja_aev` | `Audiitorettevõtja (AEV)` |
| + | `_source_file`, `_file_version`, `_loaded_at` | loader metadata (CSV member name, e.g. `31102026`, load time) |

Gotchas: **there is no company name column** (Project 1 assumed one); name, legal form and address come from
`rik_company_basic`. `report_id` identifies a report. Whether a company can have more than one report for the
same `aruandeaasta` (and what `taidetud_aruanne_report_id` means exactly) is not verified yet; the fact grain is
one row per company-year, so silver must make that choice explicit and test it. `aruandeaasta` is the report year; `period_end` (`dd.mm.yyyy`) is used for the SCD2
company lookup. 1,629,842 data rows in the `kuni_31102026` file.

## `bronze.rik_report_elements`

- **Source file:** `4.<year>_aruannete_elemendid_kuni_<version>.csv (one file per report year)` (zip: `4.{year}_aruannete_elemendid_kuni_{version}{suffix}.zip`)
- **Sample file name:** `aruannete_elemendid_<year>.csv`
- **Grain:** one row per report x tabel x element (long format)
- **Engine / key:** `ReplacingMergeTree(_loaded_at)`, `PARTITION BY report_year`, `ORDER BY (report_id, tabel, elemendi_nimetus, elemendi_label)`

| # | Column | Original header |
|---|---|---|
| 1 | `report_id` | `report_id` |
| 2 | `tabel` | `tabel` |
| 3 | `elemendi_label` | `elemendi_label` |
| 4 | `elemendi_nimetus` | `elemendi_nimetus` |
| 5 | `vaartus` | `vaartus` |
| + | `report_year` (UInt16) | from the file name (`4.<year>_...`) |
| + | `_source_file`, `_file_version`, `_loaded_at` | loader metadata (CSV member name, e.g. `31102026`, load time) |

Gotchas: **long format**: one row per report × statement table (`tabel`, 43 distinct values in 2025) × element.
Silver pivots the needed `elemendi_nimetus` values into columns. Values are text (`"0.0"`). About 3.6 to 3.9
million rows per yearly file; we load 2022 to 2025. Most frequent element names in 2025: Assets, CurrentAssets,
AverageNumberOfEmployeesInFullTimeEquivalentUnits, CashAndCashEquivalents, LaborExpense, Equity,
TotalAnnualPeriodProfitLoss, TotalProfitLoss, IssuedCapital, TotalProfitLossBeforeTax, CurrentLiabilities,
Revenue, NonCurrentAssets, EmployeeExpense, DepreciationAndImpairmentLossReversal, NonCurrentLiabilities,
RetainedEarningsLoss; non-profit organisations use other names (NetAssets, TotalRevenue,
SurplusDeficitFromOperatingActivities, ...). Elements belong to a statement table, and some tables are special cases (real RIK rows
include `Likvideerimise kasumiaruanne` and `Lõppbilanss`, i.e. liquidation statements), so silver should filter
on `tabel` explicitly rather than on the element name alone.
**Operating profit = `TotalProfitLoss`** (confirmed in the real sample: `elemendi_label` is
`Ärikasum (kahjum)`). Do not confuse it with `TotalProfitLossBeforeTax` (`Kasum (kahjum) enne tulumaksustamist`)
or `TotalAnnualPeriodProfitLoss` (`Aruandeaasta kasum (kahjum)`, net profit). Revenue = `Revenue` (`Müügitulu`).
For commercial companies take these from `tabel` in (`Kasumiaruanne skeem 1`, `Kasumiaruanne skeem 2`), plus
`Konsolideerimata kasumiaruanne (skeem 1/2)` (a group parent's own, non-consolidated figures); skip the
`Konsolideeritud...` (consolidated, out of scope) and `Likvideerimis...` (liquidation) tables.

## `bronze.rik_report_emtak_revenue`

- **Source file:** `2.EMTAK_myygitulu_kuni_<version>.csv` (zip: `2.EMTAK_myygitulu_kuni_{version}{suffix}.zip`)
- **Sample file name:** `emtak_myygitulu.csv`
- **Grain:** one row per report x EMTAK code x EMTAK version
- **Engine / key:** `ReplacingMergeTree(_loaded_at)`, `ORDER BY (report_id, emtak, emtak_version)`

| # | Column | Original header |
|---|---|---|
| 1 | `report_id` | `report_id` |
| 2 | `emtak` | `emtak` |
| 3 | `jaotatud_myygitulu` | `Jaotatud müügitulu` |
| 4 | `pohitegevusala` | `põhitegevusala` |
| 5 | `emtak_version` | `emtak_version` |
| + | `_source_file`, `_file_version`, `_loaded_at` | loader metadata (CSV member name, e.g. `31102026`, load time) |

Gotchas: several rows per report when revenue is split across activities; `pohitegevusala` = `jah` marks the
main activity. `emtak` is a 5-digit EMTAK code as text (keep leading zeros: `01241`); the section letter
comes from the EMTAK classification (Kristjan, `dim_industry`). `emtak_version` (e.g. `EMTAK 2008`) is part of
the industry key. 1,553,677 rows in the `kuni_31102026` file.

## `bronze.rik_company_basic`

- **Source file:** `avaandmed/ettevotja_rekvisiidid__lihtandmed.csv (updated daily, current state only)` (zip: `avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip`)
- **Sample file name:** `ettevotja_lihtandmed.csv`
- **Grain:** one row per company (ariregistri_kood), current state
- **Engine / key:** `ReplacingMergeTree(_loaded_at)`, `ORDER BY (ariregistri_kood)`

| # | Column | Original header |
|---|---|---|
| 1 | `nimi` | `nimi` |
| 2 | `ariregistri_kood` | `ariregistri_kood` |
| 3 | `ettevotja_oiguslik_vorm` | `ettevotja_oiguslik_vorm` |
| 4 | `ettevotja_oigusliku_vormi_alaliik` | `ettevotja_oigusliku_vormi_alaliik` |
| 5 | `kmkr_nr` | `kmkr_nr` |
| 6 | `ettevotja_staatus` | `ettevotja_staatus` |
| 7 | `ettevotja_staatus_tekstina` | `ettevotja_staatus_tekstina` |
| 8 | `ettevotja_esmakande_kpv` | `ettevotja_esmakande_kpv` |
| 9 | `ettevotja_aadress` | `ettevotja_aadress` |
| 10 | `asukoht_ettevotja_aadressis` | `asukoht_ettevotja_aadressis` |
| 11 | `asukoha_ehak_kood` | `asukoha_ehak_kood` |
| 12 | `asukoha_ehak_tekstina` | `asukoha_ehak_tekstina` |
| 13 | `indeks_ettevotja_aadressis` | `indeks_ettevotja_aadressis` |
| 14 | `ads_adr_id` | `ads_adr_id` |
| 15 | `ads_ads_oid` | `ads_ads_oid` |
| 16 | `ads_normaliseeritud_taisaadress` | `ads_normaliseeritud_taisaadress` |
| 17 | `teabesysteemi_link` | `teabesysteemi_link` |
| + | `_source_file`, `_file_version`, `_loaded_at` | loader metadata (CSV member name, e.g. `31102026`, load time) |

Gotchas: the file is updated daily and holds the **current state only**. SCD2 history for `dim_company`
therefore starts with the first `dbt snapshot` run; earlier changes of name or county are not recoverable.
For the fact lookup the first snapshot version of each company is treated as valid from 1900-01-01
(see `docs/surrogate_keys.md`). Companies deleted from the register are missing from this file but still
have reports; they map to the unknown company member. County = last part of `asukoha_ehak_tekstina`
(`Pirita linnaosa, Tallinn, Harju maakond` → `Harju maakond`). 379,417 rows on the day it was checked.
