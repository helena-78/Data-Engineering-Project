"""Single source of truth for the bronze layer: RIK file -> ClickHouse table mapping.

Used by:
  * scripts/load_bronze.py        (header validation and column order on insert)
  * scripts/make_bronze_sample.py (which files to read and filter)
  * tests/test_bronze_unit.py     (checks clickhouse/init/02_create_bronze_tables.sql matches this file)
  * Melih's data-quality task     (import BRONZE_TABLES / expected_headers() for the "missing columns" check)

Rules (see docs/bronze_tables.md):
  * every source column is stored as String, unchanged (no casting, empty string stays empty);
  * column name = source header lower-cased, Estonian letters transliterated
    (o-tilde -> o, a-umlaut -> a, o-umlaut -> o, u-umlaut -> y, s-caron -> s, z-caron -> z),
    e.g. 'Jaotatud müügitulu' -> jaotatud_myygitulu,
    spaces -> "_", "?" and parentheses dropped;
  * metadata columns _source_file, _file_version, _loaded_at are added by the loader / table default;
  * engine ReplacingMergeTree(_loaded_at): rows with the same ORDER BY key collapse to the newest load,
    so readers use FINAL (or argMax) to get exactly one row per key.

Regenerate clickhouse/init/02_create_bronze_tables.sql after a change here:
    python scripts/bronze_schema.py > clickhouse/init/02_create_bronze_tables.sql

This module has no third-party dependencies so it can be imported anywhere (Airflow DAG, tests).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

BRONZE_DATABASE = "bronze"

# u-umlaut -> y follows RIK's own file names (EMTAK_myygitulu); see docs/bronze_tables.md.
_TRANSLIT = str.maketrans({"õ": "o", "ä": "a", "ö": "o", "ü": "y", "š": "s", "ž": "z"})

METADATA_COLUMNS = ["_source_file", "_file_version", "_loaded_at"]


def to_column_name(header: str) -> str:
    """Derive the bronze column name from an original RIK CSV header."""
    name = header.strip().lower().translate(_TRANSLIT)
    name = re.sub(r"[?()]", "", name)
    name = re.sub(r"\s+", "_", name.strip())
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
        raise ValueError(f"header {header!r} does not map to a safe column name ({name!r})")
    return name


@dataclass(frozen=True)
class BronzeTable:
    key: str                      # short logical name used in code and docs
    table: str                    # ClickHouse table name (database = BRONZE_DATABASE)
    headers: list                 # exact source header, in file order
    sample_name: re.Pattern       # file name inside the sample bundle / data/sample
    real_name: re.Pattern         # file name of the extracted real RIK CSV
    real_zip: str                 # zip name template on avaandmed.ariregister.rik.ee
    quoting: str = "minimal"      # "minimal" (quoted strings) or "none" (lihtandmed is unquoted)
    partition_column: Optional[str] = None   # extra column derived from the file name
    order_by: tuple = field(default_factory=tuple)
    source_desc: str = ""
    grain: str = ""

    @property
    def full_name(self) -> str:
        return f"{BRONZE_DATABASE}.{self.table}"

    @property
    def columns(self) -> list:
        return [to_column_name(h) for h in self.headers]

    @property
    def header_to_column(self) -> dict:
        return dict(zip(self.headers, self.columns))


BRONZE_TABLES = {
    "report_general": BronzeTable(
        key="report_general",
        table="rik_report_general",
        headers=[
            "report_id", "taidetud_aruanne_report_id", "registrikood", "õiguslik vorm", "staatus",
            "aruandeaasta", "kas konsolideeritud?", "period_start", "period_end", "esitatud_kpv",
            "kas auditeeritud?", "valitud aruanne kategooria", "minimaalne kategooria andmete alusel",
            "auditi töövõtu liik", "audiitori otsuse tüüp", "modifikatsioon asjaolu rõhutamine",
            "modifikatsioon muu asjaolu", "modifikatsioon tegevuse jatkuvus", "Audiitorettevõtja (AEV)",
        ],
        sample_name=re.compile(r"^aruannete_yldandmed\.csv$"),
        real_name=re.compile(r"^1\.aruannete_yldandmed_kuni_(?P<version>\d{8})\.csv$"),
        real_zip="1.aruannete_yldandmed_kuni_{version}{suffix}.zip",
        source_desc="1.aruannete_yldandmed_kuni_<version>.csv",
        grain="one row per annual report (report_id)",
        order_by=("report_id",),
    ),
    "report_elements": BronzeTable(
        key="report_elements",
        table="rik_report_elements",
        headers=["report_id", "tabel", "elemendi_label", "elemendi_nimetus", "vaartus"],
        sample_name=re.compile(r"^aruannete_elemendid_(?P<year>\d{4})\.csv$"),
        real_name=re.compile(r"^4\.(?P<year>\d{4})_aruannete_elemendid_kuni_(?P<version>\d{8})\.csv$"),
        real_zip="4.{year}_aruannete_elemendid_kuni_{version}{suffix}.zip",
        partition_column="report_year",
        source_desc="4.<year>_aruannete_elemendid_kuni_<version>.csv (one file per report year)",
        grain="one row per report x tabel x element (long format)",
        # elemendi_label is part of the key on purpose: ~9k real rows have an empty elemendi_nimetus,
        # and without the label ReplacingMergeTree would collapse distinct rows (see docs/bronze_tables.md).
        order_by=("report_id", "tabel", "elemendi_nimetus", "elemendi_label"),
    ),
    "report_emtak_revenue": BronzeTable(
        key="report_emtak_revenue",
        table="rik_report_emtak_revenue",
        headers=["report_id", "emtak", "Jaotatud müügitulu", "põhitegevusala", "emtak_version"],
        sample_name=re.compile(r"^emtak_myygitulu\.csv$"),
        real_name=re.compile(r"^2\.EMTAK_myygitulu_kuni_(?P<version>\d{8})\.csv$"),
        real_zip="2.EMTAK_myygitulu_kuni_{version}{suffix}.zip",
        source_desc="2.EMTAK_myygitulu_kuni_<version>.csv",
        grain="one row per report x EMTAK code x EMTAK version",
        order_by=("report_id", "emtak", "emtak_version"),
    ),
    "company_basic": BronzeTable(
        key="company_basic",
        table="rik_company_basic",
        headers=[
            "nimi", "ariregistri_kood", "ettevotja_oiguslik_vorm", "ettevotja_oigusliku_vormi_alaliik",
            "kmkr_nr", "ettevotja_staatus", "ettevotja_staatus_tekstina", "ettevotja_esmakande_kpv",
            "ettevotja_aadress", "asukoht_ettevotja_aadressis", "asukoha_ehak_kood", "asukoha_ehak_tekstina",
            "indeks_ettevotja_aadressis", "ads_adr_id", "ads_ads_oid", "ads_normaliseeritud_taisaadress",
            "teabesysteemi_link",
        ],
        sample_name=re.compile(r"^ettevotja_lihtandmed\.csv$"),
        real_name=re.compile(r"^ettevotja_rekvisiidid__lihtandmed\.csv$"),
        real_zip="avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip",
        quoting="none",
        source_desc="avaandmed/ettevotja_rekvisiidid__lihtandmed.csv (updated daily, current state only)",
        grain="one row per company (ariregistri_kood), current state",
        order_by=("ariregistri_kood",),
    ),
}

RIK_BASE_URL = "https://avaandmed.ariregister.rik.ee/sites/default/files/"


def expected_headers(key: str) -> list:
    """Exact header list for a bronze table key (for data-quality checks)."""
    return list(BRONZE_TABLES[key].headers)


def match_file(file_name: str):
    """Map a CSV file name (sample or real) to (BronzeTable, info dict) or None.

    info may contain 'year' (elements files) and 'version' (real RIK file names).
    For a zip, pass the name of the CSV member inside it (the loader does this).
    """
    base = file_name.rsplit("/", 1)[-1]
    for spec in BRONZE_TABLES.values():
        for pattern in (spec.real_name, spec.sample_name):
            m = pattern.match(base)
            if m:
                return spec, {k: v for k, v in m.groupdict().items() if v is not None}
    return None


# --------------------------------------------------------------------------------------------
# DDL generation (clickhouse/init/02_create_bronze_tables.sql is the output of this function)
# --------------------------------------------------------------------------------------------

def create_table_sql(spec: BronzeTable) -> str:
    cols = []
    for header, col in zip(spec.headers, spec.columns):
        comment = header.replace("\\", "\\\\").replace("'", "\\'")
        cols.append(f"    `{col}` String COMMENT '{comment}'")
    if spec.partition_column:
        cols.append(f"    `{spec.partition_column}` UInt16 COMMENT 'from the file name'")
    cols += ["    `_source_file` String", "    `_file_version` String", "    `_loaded_at` DateTime DEFAULT now()"]
    lines = [f"-- Source: {spec.source_desc}", f"-- Grain:  {spec.grain}",
             f"CREATE TABLE IF NOT EXISTS {spec.full_name}", "(", ",\n".join(cols), ")",
             "ENGINE = ReplacingMergeTree(_loaded_at)"]
    if spec.partition_column:
        lines.append(f"PARTITION BY {spec.partition_column}")
    lines.append(f"ORDER BY ({', '.join(spec.order_by)});")
    return "\n".join(lines)


DDL_HEADER = """-- Bronze tables: raw copies of the RIK open-data CSV files.
-- GENERATED by `python scripts/bronze_schema.py` (single source of truth for header -> column mapping).
-- Do not edit by hand; tests/test_bronze_unit.py fails if this file and bronze_schema.py drift apart.
--
-- * Every source column is String, stored exactly as in the file (no casting; '' stays '').
-- * _source_file / _file_version are filled by the loader, _loaded_at defaults to now().
-- * ReplacingMergeTree(_loaded_at): rows sharing the ORDER BY key collapse to the newest load.
--   Read with FINAL (or argMax(..., _loaded_at)) to get one row per key.
-- * scripts/load_bronze.py additionally replaces the whole partition per file, so reloads are
--   idempotent even before background merges. See docs/bronze_tables.md.
"""


def render_ddl() -> str:
    return DDL_HEADER + "\n" + "\n\n".join(create_table_sql(t) for t in BRONZE_TABLES.values()) + "\n"


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    sys.stdout.write(render_ddl())
