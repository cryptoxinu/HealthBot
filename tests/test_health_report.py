"""Tests for periodic health reports (Phase T3)."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import LabResult, Medication, WhoopDaily
from healthbot.export.health_report import (
    HealthReport,
    HealthReportBuilder,
    format_report,
)


class TestHealthReportBuilder:
    """Test report building from various data sources."""

    def test_empty_report(self, db) -> None:
        """No data should produce empty report."""
        builder = HealthReportBuilder(db)
        report = builder.build_weekly(user_id=1)
        assert report.period == "weekly"
        assert report.sections == []

    def test_weekly_report_dates(self, db) -> None:
        """Weekly report should cover 7 days."""
        builder = HealthReportBuilder(db)
        report = builder.build_weekly(user_id=1)
        start = date.fromisoformat(report.start_date)
        end = date.fromisoformat(report.end_date)
        assert (end - start).days == 7

    def test_monthly_report_dates(self, db) -> None:
        """Monthly report should cover 30 days."""
        builder = HealthReportBuilder(db)
        report = builder.build_monthly(user_id=1)
        start = date.fromisoformat(report.start_date)
        end = date.fromisoformat(report.end_date)
        assert (end - start).days == 30

    def test_lab_section(self, db) -> None:
        """Labs in period should appear in report."""
        builder = HealthReportBuilder(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=130.0,
            unit="mg/dL",
            date_collected=date.today(),
            flag="H",
        )
        db.insert_observation(lab, user_id=1)

        report = builder.build_weekly(user_id=1)
        lab_sections = [s for s in report.sections if s.title == "Lab Results"]
        assert len(lab_sections) == 1
        assert any("LDL" in item for item in lab_sections[0].items)

    def test_wearable_section(self, db) -> None:
        """Wearable data should appear in report."""
        builder = HealthReportBuilder(db)
        for i in range(3):
            wd = WhoopDaily(
                id=uuid.uuid4().hex,
                date=date.today() - timedelta(days=i),
                hrv=55.0 + i,
                rhr=58.0,
                sleep_score=75.0,
                recovery_score=65.0,
            )
            db.insert_wearable_daily(wd, user_id=1)

        report = builder.build_weekly(user_id=1)
        wear_sections = [
            s for s in report.sections if s.title == "Wearable Summary"
        ]
        assert len(wear_sections) == 1
        assert any("HRV" in item for item in wear_sections[0].items)

    def test_medication_section(self, db) -> None:
        """Active medications should appear in report."""
        builder = HealthReportBuilder(db)
        med = Medication(
            id=uuid.uuid4().hex,
            name="Atorvastatin",
            dose="20",
            unit="mg",
            frequency="daily",
            start_date=date.today(),
        )
        db.insert_medication(med, user_id=1)

        report = builder.build_monthly(user_id=1)
        med_sections = [
            s for s in report.sections if s.title == "Active Medications"
        ]
        assert len(med_sections) == 1
        assert any("Atorvastatin" in item for item in med_sections[0].items)

    def test_goal_section(self, db) -> None:
        """Goal progress should appear if goals exist."""
        from healthbot.reasoning.goals import GoalTracker

        builder = HealthReportBuilder(db)
        tracker = GoalTracker(db)
        tracker.add_goal(1, "ldl", 100.0, "below", "LDL")

        report = builder.build_monthly(user_id=1)
        goal_sections = [
            s for s in report.sections if s.title == "Health Goals"
        ]
        assert len(goal_sections) == 1

    def test_old_labs_excluded(self, db) -> None:
        """Labs outside the period should not appear."""
        builder = HealthReportBuilder(db)
        old_lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=2.5,
            unit="mIU/L",
            date_collected=date.today() - timedelta(days=60),
        )
        db.insert_observation(old_lab, user_id=1)

        report = builder.build_weekly(user_id=1)
        lab_sections = [s for s in report.sections if s.title == "Lab Results"]
        assert len(lab_sections) == 0

    def test_generated_at_set(self, db) -> None:
        """Report should have generated_at timestamp."""
        builder = HealthReportBuilder(db)
        report = builder.build_weekly(user_id=1)
        assert report.generated_at
        assert "UTC" in report.generated_at


class TestFormatReport:
    """Test report formatting."""

    def test_format_empty(self) -> None:
        """Empty report should show help text."""
        report = HealthReport(
            period="weekly",
            start_date="2025-01-01",
            end_date="2025-01-08",
            generated_at="2025-01-08 12:00 UTC",
        )
        text = format_report(report)
        assert "No data" in text

    def test_format_with_sections(self, db) -> None:
        """Report with data should show sections."""
        builder = HealthReportBuilder(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=date.today(),
        )
        db.insert_observation(lab, user_id=1)

        report = builder.build_weekly(user_id=1)
        text = format_report(report)
        assert "WEEKLY HEALTH REPORT" in text
        assert "Lab Results" in text
        assert "Generated:" in text

    def test_format_monthly(self, db) -> None:
        """Monthly report title should say MONTHLY."""
        builder = HealthReportBuilder(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=2.0,
            unit="mIU/L",
            date_collected=date.today(),
        )
        db.insert_observation(lab, user_id=1)

        report = builder.build_monthly(user_id=1)
        text = format_report(report)
        assert "MONTHLY HEALTH REPORT" in text
