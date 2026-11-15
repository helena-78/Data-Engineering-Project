"""Unit tests for the bronze layer helpers (no ClickHouse, no network needed).

Run from the repo root:   python -m unittest discover -s tests -v
The ClickHouse / dbt / Airflow integration checks are described in tests/README.md.
"""

from __future__ import annotations

import csv
import io
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import bronze_schema  # noqa: E402
import load_bronze  # noqa: E402
import make_bronze_sample  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "rik_synthetic_bundle.txt"
VERSION = "31102026"
REAL_NAMES = {  # sample name -> (real csv name inside the zip, zip name)
    "aruannete_yldandmed.csv": (f"1.aruannete_yldandmed_kuni_{VERSION}.csv", f"1.aruannete_yldandmed_kuni_{VERSION}_1.zip"),
    "emtak_myygitulu.csv": (f"2.EMTAK_myygitulu_kuni_{VERSION}.csv", f"2.EMTAK_myygitulu_kuni_{VERSION}_0.zip"),
    "ettevotja_lihtandmed.csv": ("ettevotja_rekvisiidid__lihtandmed.csv", "ettevotja_rekvisiidid__lihtandmed.csv.zip"),
    **{f"aruannete_elemendid_{y}.csv": (f"4.{y}_aruannete_elemendid_kuni_{VERSION}.csv",
                                        f"4.{y}_aruannete_elemendid_kuni_{VERSION}_0.zip") for y in range(2022, 2026)},
}


class SchemaTests(unittest.TestCase):
    def test_ddl_file_matches_schema_module(self):
        ddl = (ROOT / "clickhouse" / "init" / "02_create_bronze_tables.sql").read_text(encoding="utf-8")
        self.assertEqual(ddl, bronze_schema.render_ddl(),
                         "regenerate: python scripts/bronze_schema.py > clickhouse/init/02_create_bronze_tables.sql")

    def test_column_names(self):
        f = bronze_schema.to_column_name
        self.assertEqual(f("õiguslik vorm"), "oiguslik_vorm")
        self.assertEqual(f("kas konsolideeritud?"), "kas_konsolideeritud")
        self.assertEqual(f("Audiitorettevõtja (AEV)"), "audiitorettevotja_aev")
        self.assertEqual(f("Jaotatud müügitulu"), "jaotatud_myygitulu")
        self.assertEqual(f("põhitegevusala"), "pohitegevusala")
        self.assertEqual(f("audiitori otsuse tüüp"), "audiitori_otsuse_tyyp")

    def test_table_shapes(self):
        t = bronze_schema.BRONZE_TABLES
        self.assertEqual(len(t["report_general"].headers), 19)
        self.assertEqual(len(t["report_elements"].headers), 5)
        self.assertEqual(len(t["report_emtak_revenue"].headers), 5)
        self.assertEqual(len(t["company_basic"].headers), 17)
        for spec in t.values():
            self.assertEqual(len(set(spec.columns)), len(spec.columns), spec.table)
            self.assertTrue(set(spec.order_by) <= set(spec.columns) | {spec.partition_column}, spec.table)

    def test_file_name_matching(self):
        m = bronze_schema.match_file
        self.assertEqual(m(f"4.2024_aruannete_elemendid_kuni_{VERSION}.csv")[1], {"year": "2024", "version": VERSION})
        self.assertEqual(m("aruannete_elemendid_2023.csv")[1], {"year": "2023"})
        self.assertEqual(m(f"1.aruannete_yldandmed_kuni_{VERSION}.csv")[0].key, "report_general")
        self.assertEqual(m("ettevotja_rekvisiidid__lihtandmed.csv")[0].key, "company_basic")
        self.assertIsNone(m("_stats.txt"))


class LoaderParsingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        load_bronze.split_bundle(FIXTURE, self.dir)

    def tearDown(self):
        self.tmp.cleanup()

    def _parse(self, name):
        spec, _ = bronze_schema.match_file(name)
        with load_bronze.open_csv_text(self.dir / name) as (_, stream):
            rows = list(load_bronze.csv_reader(stream, spec))
        return spec, rows

    def test_bundle_split_keeps_bytes(self):
        raw = (self.dir / "aruannete_yldandmed.csv").read_bytes()
        self.assertIn(b"\r\n", raw)
        self.assertTrue((self.dir / "emtak_myygitulu.csv").read_bytes().startswith(b"\xef\xbb\xbf"))

    def test_headers_validate_and_bom_is_stripped(self):
        for name in REAL_NAMES:
            spec, rows = self._parse(name)
            idx = load_bronze.validate_header(rows[0], spec, name)
            self.assertEqual(idx, list(range(len(spec.headers))), name)
            self.assertTrue(all(len(r) == len(rows[0]) for r in rows), name)

    def test_quoted_semicolons_quotes_and_estonian_letters(self):
        _, rows = self._parse("aruannete_yldandmed.csv")
        self.assertIn('Märkusega; vt "Lisa 5"', {r[14] for r in rows})
        _, rows = self._parse("aruannete_elemendid_2025.csv")
        labels = {r[2] for r in rows}
        self.assertIn("Muud tulud; kulud (sünteetiline)", labels)
        self.assertIn('Silt "jutumärkidega"', labels)
        _, rows = self._parse("ettevotja_lihtandmed.csv")
        self.assertIn("Šokolaadi Proov OÜ", {r[0] for r in rows})

    def test_missing_column_fails(self):
        spec = bronze_schema.BRONZE_TABLES["report_elements"]
        with self.assertRaises(load_bronze.BronzeLoadError):
            load_bronze.validate_header(spec.headers[:-1], spec, "x.csv")
        with self.assertRaises(load_bronze.BronzeLoadError):
            load_bronze.validate_header(spec.headers + ["extra"], spec, "x.csv")


class SampleBuilderTests(unittest.TestCase):
    """make_bronze_sample.py on local zips built from the fixture (RIK itself is not reachable in tests)."""

    def test_filter(self):
        with tempfile.TemporaryDirectory() as t:
            t = Path(t)
            split, zips, out = t / "split", t / "zips", t / "out"
            load_bronze.split_bundle(FIXTURE, split)
            zips.mkdir()
            for sample_name, (csv_name, zip_name) in REAL_NAMES.items():
                with zipfile.ZipFile(zips / zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(csv_name, (split / sample_name).read_bytes())
            res = make_bronze_sample.build_sample(VERSION, [2023, 2024, 2025], out, zips, local_only=True,
                                                  bundle=t / "bundle.txt")
            # 20 synthetic companies, 3 fail the % 500 == 7 rule; 2021/2022 reports are outside the years
            self.assertEqual(res["companies"], 17)
            self.assertEqual(res["reports"], 51)
            self.assertEqual(res["rows"]["ettevotja_lihtandmed.csv"], 16)  # one company has no basic-data row
            with open(out / "aruannete_yldandmed.csv", encoding="utf-8", newline="") as fh:
                rows = list(csv.DictReader(fh, delimiter=";"))
            self.assertTrue(all(int(r["registrikood"]) % 500 == 7 for r in rows))
            self.assertTrue(all(r["aruandeaasta"] in {"2023", "2024", "2025"} for r in rows))
            self.assertIn(b"\r\n", (out / "aruannete_yldandmed.csv").read_bytes())
            # the bundle it writes can be split again by the loader
            again = load_bronze.split_bundle(t / "bundle.txt", t / "again")
            self.assertIn("emtak_myygitulu.csv", {p.name for p in again})


if __name__ == "__main__":
    unittest.main()
