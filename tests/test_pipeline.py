"""Tests for the ETL pipeline.

Run with `python -m pytest` if pytest is installed, or `python -m unittest`
with no dependencies at all.
"""

import json
import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path

from etl import transforms
from etl.pipeline import PROJECT_ROOT, load_config, run, transform_and_validate
from etl.validate import FieldError, validate_field

CONFIG_PATH = PROJECT_ROOT / "config" / "members_mapping.json"
TODAY = date(2026, 1, 1)


class TestTransforms(unittest.TestCase):
    def test_strip_collapses_internal_whitespace(self):
        self.assertEqual(transforms.strip("  maria   elena  "), "maria elena")

    def test_title_case_preserves_apostrophes_and_hyphens(self):
        self.assertEqual(transforms.title_case("o'brien"), "O'Brien")
        self.assertEqual(transforms.title_case("SMITH-JONES"), "Smith-Jones")

    def test_strip_currency_handles_symbols_and_separators(self):
        self.assertEqual(transforms.strip_currency("$ 1,025.00 "), "1025.00")

    def test_unknown_transform_fails_loudly(self):
        with self.assertRaises(KeyError):
            transforms.apply_all("x", ["does_not_exist"])


class TestFieldValidation(unittest.TestCase):
    def setUp(self):
        self.config = load_config(CONFIG_PATH)
        self.specs = {f["target"]: f for f in self.config["fields"]}

    def test_required_field_rejects_empty(self):
        with self.assertRaises(FieldError) as ctx:
            validate_field("", self.specs["first_name"], today=TODAY)
        self.assertEqual(ctx.exception.code, "REQUIRED_MISSING")

    def test_optional_field_returns_none_when_empty(self):
        self.assertIsNone(validate_field("", self.specs["monthly_premium"], today=TODAY))

    def test_member_id_pattern_enforced(self):
        self.assertEqual(validate_field("M000101", self.specs["member_id"], today=TODAY), "M000101")
        with self.assertRaises(FieldError) as ctx:
            validate_field("BADID11", self.specs["member_id"], today=TODAY)
        self.assertEqual(ctx.exception.code, "PATTERN_MISMATCH")

    def test_multiple_date_formats_accepted(self):
        spec = self.specs["date_of_birth"]
        for raw in ("1958-03-12", "03/12/1958", "12-Mar-1958"):
            self.assertEqual(validate_field(raw, spec, today=TODAY), date(1958, 3, 12))

    def test_future_date_rejected(self):
        with self.assertRaises(FieldError) as ctx:
            validate_field("2030-01-01", self.specs["date_of_birth"], today=TODAY)
        self.assertEqual(ctx.exception.code, "FUTURE_DATE")

    def test_status_value_map_normalizes_codes(self):
        spec = self.specs["status"]
        self.assertEqual(validate_field("A", spec, today=TODAY), "ACTIVE")
        self.assertEqual(validate_field("TERM", spec, today=TODAY), "TERMINATED")

    def test_unmapped_status_rejected(self):
        with self.assertRaises(FieldError) as ctx:
            validate_field("Z", self.specs["status"], today=TODAY)
        self.assertEqual(ctx.exception.code, "NOT_ALLOWED")

    def test_premium_out_of_range_rejected(self):
        with self.assertRaises(FieldError) as ctx:
            validate_field("9999.00", self.specs["monthly_premium"], today=TODAY)
        self.assertEqual(ctx.exception.code, "OUT_OF_RANGE")


class TestRecordRules(unittest.TestCase):
    def setUp(self):
        self.config = load_config(CONFIG_PATH)

    def _row(self, **overrides):
        row = {
            "MBR_ID": "M000999",
            "FIRST_NM": "test",
            "LAST_NM": "person",
            "DOB": "1970-01-01",
            "ENROLL_DT": "2021-01-01",
            "PLAN_CD": "HMO",
            "PREMIUM_AMT": "1000.00",
            "STATUS": "A",
        }
        row.update(overrides)
        return row

    def test_clean_row_produces_no_errors(self):
        record, errors = transform_and_validate(self._row(), self.config, today=TODAY)
        self.assertEqual(errors, [])
        self.assertEqual(record["status"], "ACTIVE")
        self.assertEqual(record["first_name"], "Test")

    def test_enrollment_before_birth_rejected(self):
        row = self._row(DOB="1966-10-11", ENROLL_DT="1960-01-01")
        _, errors = transform_and_validate(row, self.config, today=TODAY)
        self.assertEqual([e["code"] for e in errors], ["DATE_ORDER"])


class TestEndToEnd(unittest.TestCase):
    def test_run_balances_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "warehouse.db"
            report = Path(tmp) / "reconciliation.md"

            first = run(CONFIG_PATH, db, report)
            self.assertEqual(first["accepted"] + first["rejected"], first["source_rows"])

            conn = sqlite3.connect(db)
            count_after_first = conn.execute("SELECT COUNT(*) FROM members").fetchone()[0]
            conn.close()

            # Re-running the same source must not duplicate rows.
            run(CONFIG_PATH, db, report)
            conn = sqlite3.connect(db)
            count_after_second = conn.execute("SELECT COUNT(*) FROM members").fetchone()[0]
            quarantined = conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]
            conn.close()

            self.assertEqual(count_after_first, count_after_second)
            self.assertGreater(quarantined, 0)
            self.assertIn("PASS", report.read_text())

    def test_every_reject_has_a_known_reason_code(self):
        from etl.validate import REASON_CODES

        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "warehouse.db"
            run(CONFIG_PATH, db, Path(tmp) / "r.md")
            conn = sqlite3.connect(db)
            codes = {r[0] for r in conn.execute("SELECT DISTINCT reason_code FROM quarantine")}
            conn.close()
        self.assertTrue(codes.issubset(set(REASON_CODES)), f"Unknown codes: {codes - set(REASON_CODES)}")

    def test_quarantined_rows_keep_their_raw_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "warehouse.db"
            run(CONFIG_PATH, db, Path(tmp) / "r.md")
            conn = sqlite3.connect(db)
            raw = conn.execute("SELECT raw_row FROM quarantine LIMIT 1").fetchone()[0]
            conn.close()
        self.assertIn("MBR_ID", json.loads(raw))


if __name__ == "__main__":
    unittest.main()
