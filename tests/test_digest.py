"""Tests for the daily health digest module."""
from __future__ import annotations

import uuid
from datetime import date

from healthbot.reasoning.digest import DigestReport, build_daily_digest, format_digest


class TestFormatDigest:
    """Test digest formatting."""

    def test_empty_report(self) -> None:
        """Empty report should say all clear."""
        report = DigestReport()
        output = format_digest(report)
        assert "DAILY HEALTH DIGEST" in output
        assert "All clear" in output

    def test_wearable_section(self) -> None:
        """Wearable data should appear in output."""
        report = DigestReport(wearable_summary="2025-12-01: HRV: 45ms, RHR: 58bpm")
        output = format_digest(report)
        assert "Wearable Data" in output
        assert "HRV: 45ms" in output

    def test_medications_section(self) -> None:
        """Medications should appear in output."""
        report = DigestReport(medications=["Metformin 1000mg twice daily"])
        output = format_digest(report)
        assert "Active Medications" in output
        assert "Metformin" in output

    def test_drug_lab_flags_section(self) -> None:
        """Drug-lab flags should appear in output."""
        report = DigestReport(
            drug_lab_flags=["Metformin -> Vitamin B12: 180 (LOW)"]
        )
        output = format_digest(report)
        assert "Drug-Lab Flags" in output
        assert "Vitamin B12" in output

    def test_alerts_section(self) -> None:
        """Health alerts should appear in output."""
        report = DigestReport(alerts=["~ Overdue: TSH check"])
        output = format_digest(report)
        assert "Health Alerts" in output
        assert "TSH" in output

    def test_hypotheses_section(self) -> None:
        """Active hypotheses should appear in output."""
        report = DigestReport(hypotheses=["Iron deficiency anemia (55%)"])
        output = format_digest(report)
        assert "Active Hypotheses" in output
        assert "Iron deficiency" in output

    def test_full_report(self) -> None:
        """Full report with all sections should render correctly."""
        report = DigestReport(
            wearable_summary="2025-12-01: HRV: 45ms",
            medications=["Lisinopril 10mg daily"],
            drug_lab_flags=["Lisinopril -> Potassium: 5.8 (HIGH)"],
            alerts=["! Trend: LDL rose 22%"],
            hypotheses=["Hypothyroidism (50%)"],
        )
        output = format_digest(report)
        assert "Wearable Data" in output
        assert "Active Medications" in output
        assert "Drug-Lab Flags" in output
        assert "Health Alerts" in output
        assert "Active Hypotheses" in output
        assert "All clear" not in output


class TestBuildDigest:
    """Test digest building against a real DB."""

    def test_build_empty_db(self, db) -> None:
        """Empty DB should produce report with no content."""
        report = build_daily_digest(db, user_id=0)
        assert report.wearable_summary == ""
        assert report.medications == []
        assert report.alerts == []
        assert report.drug_lab_flags == []
        assert report.hypotheses == []

    def test_build_with_medications(self, db) -> None:
        """Medications should appear in digest."""
        from healthbot.data.models import Medication

        med = Medication(
            id=uuid.uuid4().hex, name="Atorvastatin", dose="20mg",
            frequency="daily", status="active",
        )
        db.insert_medication(med)

        report = build_daily_digest(db, user_id=0)
        assert any("Atorvastatin" in m for m in report.medications)

    def test_build_with_drug_lab_flag(self, db) -> None:
        """Drug-lab flags should appear when medication + abnormal lab."""
        from healthbot.data.models import LabResult, Medication

        med = Medication(
            id=uuid.uuid4().hex, name="Metformin", dose="1000mg",
            frequency="twice daily", status="active",
        )
        db.insert_medication(med)

        lab = LabResult(
            id=uuid.uuid4().hex, test_name="Vitamin B12",
            canonical_name="vitamin_b12", value=180,
            unit="pg/mL", flag="L",
            date_collected=date(2025, 12, 1),
        )
        db.insert_observation(lab)

        report = build_daily_digest(db, user_id=0)
        assert len(report.drug_lab_flags) >= 1
        assert any("Vitamin B12" in f for f in report.drug_lab_flags)


class TestWorkoutsInDigest:
    """Test workout section and streak in daily digest."""

    def test_workout_section_in_format(self) -> None:
        """Workouts list should appear in formatted output."""
        report = DigestReport(
            workouts=["Running | 30min | 300cal"]
        )
        output = format_digest(report)
        assert "Recent Workouts" in output
        assert "Running" in output

    def test_streak_display(self) -> None:
        """Workout streak > 1 should display in formatted output."""
        report = DigestReport(
            workouts=["Running | 30min"],
            workout_streak=5,
        )
        output = format_digest(report)
        assert "Streak: 5 consecutive days" in output

    def test_no_streak_when_zero(self) -> None:
        """No streak line when streak is 0 or 1."""
        report = DigestReport(
            workouts=["Running | 30min"],
            workout_streak=1,
        )
        output = format_digest(report)
        assert "Streak" not in output
