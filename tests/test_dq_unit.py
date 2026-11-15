"""Unit tests for scripts/dq_bronze.py and scripts/run_queries.py (no ClickHouse needed). Owner: Melih.

    python -m unittest discover -s tests -v
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import dq_bronze  # noqa: E402
from dq_bronze import CheckResult, check_duplicates, _flatten_stats, format_report  # noqa: E402

GEN = "bronze.rik_report_general"
EL = "bronze.rik_report_elements"


class DuplicateCheckTest(unittest.TestCase):
    def test_clean_stats_pass(self):
        res = check_duplicates([{"table": GEN, "exact_duplicate_rows": 0, "key_conflict_rows": 0}])
        self.assertEqual([(r.check, r.status) for r in res], [("report_id_unique", "pass")])

    def test_exact_duplicate_report_fails(self):
        # the synthetic fixture has one exact duplicate report row: report_id is not unique in the file
        res = check_duplicates([{"table": GEN, "exact_duplicate_rows": 1, "key_conflict_rows": 0}])
        self.assertEqual(res[0].status, "fail")
        self.assertEqual(res[0].severity, "error")

    def test_conflicts_in_other_tables_only_warn(self):
        res = check_duplicates([{"table": GEN, "exact_duplicate_rows": 0, "key_conflict_rows": 0},
                                {"table": EL, "exact_duplicate_rows": 0, "key_conflict_rows": 2},
                                {"table": EL, "exact_duplicate_rows": 0, "key_conflict_rows": 1}])
        el = [r for r in res if r.table == EL][0]
        self.assertEqual((el.severity, el.status, el.value), ("warn", "fail", 3))

    def test_no_stats_is_skipped_not_passed(self):
        self.assertEqual(check_duplicates(None)[0].status, "skipped")
        self.assertEqual(check_duplicates([{"table": EL, "key_conflict_rows": 0}])[0].status, "skipped")


class HelpersTest(unittest.TestCase):
    def test_flatten_xcom_values(self):
        a, b, c = {"table": "a"}, {"table": "b"}, {"table": "c"}
        # load_directory returns a list per task, one task per file returns a dict
        self.assertEqual(_flatten_stats([[a, b], c, None]), [a, b, c])

    def test_report_counts_errors_and_warnings(self):
        rs = [CheckResult("x", "t", "error", "fail", 1), CheckResult("y", "t", "warn", "fail", 1),
              CheckResult("z", "t", "error", "pass", 0)]
        text = format_report(rs)
        self.assertIn("1 failed errors, 1 warnings", text)
        self.assertIn("WARN", text)


class QueryFilesTest(unittest.TestCase):
    def test_five_queries_with_titles(self):
        files = sorted((ROOT / "sql" / "queries").glob("q*.sql"))
        self.assertEqual(len(files), 5)
        for i, f in enumerate(files, 1):
            sql = f.read_text(encoding="utf-8")
            self.assertTrue(sql.lstrip().startswith(f"-- Q{i}."), f.name)
            self.assertIn("gold.fact_company_year", sql, f.name)
            if i <= 4:  # medians: exact, interpolated like PostgreSQL's PERCENTILE_CONT
                self.assertIn("quantileExactInclusive(0.5)", sql, f.name)


if __name__ == "__main__":
    unittest.main()
