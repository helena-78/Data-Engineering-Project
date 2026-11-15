# data/sample

Small, reproducible sample of the real RIK open-data files for development and tests.

- Put `rik_bronze_sample_bundle.txt` here (2.9 MB; companies with `registrikood % 500 == 7`, report years
  2022 to 2025: 587 companies, 1,959 reports; `_stats.txt` counts include the header line). The bundle is committed; the CSVs split from it are gitignored.
- Load it: trigger the Airflow DAG `bronze_sample_load` (splits the bundle and loads bronze, then triggers
  `dbt_refresh_gold`), or from the host:
  `python scripts/load_bronze.py --bundle data/sample/rik_bronze_sample_bundle.txt`
- Rebuild it from the live RIK files (needs internet access to avaandmed.ariregister.rik.ee):
  `python scripts/make_bronze_sample.py --file-version 31102026 --out-dir data/sample --cache-dir data/raw --bundle data/sample/rik_bronze_sample_bundle.txt`

After splitting, this folder holds `aruannete_yldandmed.csv`, `aruannete_elemendid_<year>.csv`,
`emtak_myygitulu.csv`, `ettevotja_lihtandmed.csv`, `_element_counts_2025.txt` and `_stats.txt`
(files starting with `_` are not loaded).
