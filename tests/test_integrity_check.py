"""Tests for the vault integrity checker."""
from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import MagicMock

from healthbot.data.models import LabResult
from healthbot.vault_ops.integrity_check import (
    IntegrityChecker,
    IntegrityIssue,
    IntegrityReport,
)


class TestIntegrityChecker:
    """Vault integrity verification."""

    def test_empty_vault_passes(self, db):
        checker = IntegrityChecker(db)
        report = checker.check_all()
        assert report.ok
        assert report.tables_checked == 4
        assert len(report.issues) == 0

    def test_with_valid_data_passes(self, db):
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=130.0,
            unit="mg/dL",
            date_collected=date.today(),
        )
        db.insert_observation(lab)

        checker = IntegrityChecker(db)
        report = checker.check_all()
        assert report.ok
        assert report.rows_checked >= 1

    def test_decrypt_failure_reports_error(self):
        db = MagicMock()
        db.query_observations = MagicMock(
            side_effect=Exception("Decryption failed"),
        )
        db.get_active_medications = MagicMock(return_value=[])
        db.get_ltm_by_category = MagicMock(return_value=[])
        db.query_workouts = MagicMock(return_value=[])

        checker = IntegrityChecker(db)
        report = checker.check_all()
        assert not report.ok
        assert any(i.issue_type == "decrypt_failure" for i in report.issues)

    def test_format_report_pass(self):
        db = MagicMock()
        checker = IntegrityChecker(db)
        report = IntegrityReport(tables_checked=4, rows_checked=100, ok=True)
        text = checker.format_report(report)
        assert "PASS" in text
        assert "100" in text

    def test_format_report_fail(self):
        db = MagicMock()
        checker = IntegrityChecker(db)
        report = IntegrityReport(
            tables_checked=4,
            rows_checked=50,
            ok=False,
            issues=[IntegrityIssue(
                table="observations",
                issue_type="decrypt_failure",
                description="Cannot decrypt row 42",
                severity="error",
            )],
        )
        text = checker.format_report(report)
        assert "FAIL" in text
        assert "[X]" in text
        assert "observations" in text
