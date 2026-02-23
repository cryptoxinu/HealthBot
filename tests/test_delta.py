"""Tests for delta engine — 'what changed since last time.'"""
from __future__ import annotations

import uuid
from datetime import date

from healthbot.data.models import LabResult, TriageLevel
from healthbot.reasoning.delta import DeltaEngine


def _make_lab(
    canonical: str,
    value: float | str,
    unit: str,
    day: int,
    month: int = 1,
    triage: TriageLevel = TriageLevel.NORMAL,
) -> LabResult:
    return LabResult(
        id=uuid.uuid4().hex,
        test_name=canonical.upper().replace("_", " "),
        canonical_name=canonical,
        value=value,
        unit=unit,
        date_collected=date(2024, month, day),
        triage_level=triage,
    )


class TestDeltaEngine:
    """Core delta computation tests."""

    def test_no_data_returns_none(self, db):
        d = DeltaEngine(db)
        assert d.compute_delta() is None

    def test_single_date_returns_none(self, db):
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", 15))
        d = DeltaEngine(db)
        assert d.compute_delta() is None

    def test_two_dates_produces_report(self, db):
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("glucose", 108, "mg/dL", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()

        assert report is not None
        assert report.current_date == "2024-03-15"
        assert report.previous_date == "2024-01-15"
        assert len(report.items) == 1
        assert report.items[0].canonical_name == "glucose"
        assert report.items[0].current_value == 108.0
        assert report.items[0].previous_value == 95.0

    def test_new_test_detected(self, db):
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("glucose", 100, "mg/dL", 15, month=3))
        db.insert_observation(_make_lab("tsh", 2.1, "mIU/L", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()
        assert report is not None

        statuses = {item.canonical_name: item.status for item in report.items}
        assert statuses["tsh"] == "new"

    def test_resolved_test_detected(self, db):
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("tsh", 2.1, "mIU/L", 15, month=1))
        db.insert_observation(_make_lab("glucose", 100, "mg/dL", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()
        assert report is not None

        statuses = {item.canonical_name: item.status for item in report.items}
        assert statuses["tsh"] == "resolved"

    def test_improving_detected(self, db):
        # Glucose moving toward midpoint (85 = midpoint of 70-100)
        db.insert_observation(_make_lab("glucose", 120, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("glucose", 100, "mg/dL", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()
        assert report is not None
        assert report.items[0].status == "improving"

    def test_worsening_detected(self, db):
        # Glucose moving away from midpoint
        db.insert_observation(_make_lab("glucose", 100, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("glucose", 130, "mg/dL", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()
        assert report is not None
        assert report.items[0].status == "worsening"

    def test_stable_detected(self, db):
        db.insert_observation(_make_lab("glucose", 85, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("glucose", 86, "mg/dL", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()
        assert report is not None
        assert report.items[0].status == "stable"

    def test_multiple_tests_in_panel(self, db):
        # Panel 1
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("ldl", 110, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("hdl", 50, "mg/dL", 15, month=1))

        # Panel 2
        db.insert_observation(_make_lab("glucose", 108, "mg/dL", 15, month=3))
        db.insert_observation(_make_lab("ldl", 95, "mg/dL", 15, month=3))
        db.insert_observation(_make_lab("hdl", 55, "mg/dL", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()
        assert report is not None
        assert len(report.items) == 3


class TestDeltaFormat:
    """format_delta output tests."""

    def test_format_includes_dates(self, db):
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("glucose", 108, "mg/dL", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()
        text = d.format_delta(report)

        assert "2024-03-15" in text
        assert "2024-01-15" in text
        assert "WHAT CHANGED" in text

    def test_format_shows_status(self, db):
        db.insert_observation(_make_lab("glucose", 120, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("glucose", 100, "mg/dL", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()
        text = d.format_delta(report)

        assert "improving" in text

    def test_format_qualitative_changed(self, db):
        db.insert_observation(_make_lab("jak2_v617f_mutation", "Not Detected", "", 15, month=1))
        db.insert_observation(_make_lab("jak2_v617f_mutation", "Detected", "", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()
        text = d.format_delta(report)

        assert "Not Detected" in text
        assert "Detected" in text
        assert "CHANGED" in text

    def test_format_qualitative_stable(self, db):
        db.insert_observation(_make_lab("jak2_v617f_mutation", "Not Detected", "", 15, month=1))
        db.insert_observation(_make_lab("jak2_v617f_mutation", "Not Detected", "", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()
        text = d.format_delta(report)

        assert "stable" in text


class TestDeltaQualitative:
    """Qualitative (string) value delta tests."""

    def test_qualitative_changed_status(self, db):
        """Different qualitative values produce status='changed'."""
        db.insert_observation(_make_lab("jak2_v617f_mutation", "Not Detected", "", 15, month=1))
        db.insert_observation(_make_lab("jak2_v617f_mutation", "Detected", "", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()

        assert report is not None
        assert len(report.items) == 1
        item = report.items[0]
        assert item.status == "changed"
        assert item.current_value == "Detected"
        assert item.previous_value == "Not Detected"

    def test_qualitative_stable_status(self, db):
        """Same qualitative values produce status='stable'."""
        db.insert_observation(_make_lab("hbsag", "Negative", "", 15, month=1))
        db.insert_observation(_make_lab("hbsag", "Negative", "", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()

        assert report is not None
        assert len(report.items) == 1
        assert report.items[0].status == "stable"

    def test_qualitative_case_insensitive(self, db):
        """Case differences treated as stable."""
        db.insert_observation(_make_lab("hcv_antibody", "Non-Reactive", "", 15, month=1))
        db.insert_observation(_make_lab("hcv_antibody", "non-reactive", "", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()

        assert report is not None
        assert report.items[0].status == "stable"

    def test_qualitative_new_test(self, db):
        """New qualitative test shows raw string value, not None."""
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("glucose", 100, "mg/dL", 15, month=3))
        db.insert_observation(_make_lab("jak2_v617f_mutation", "Detected", "", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()

        assert report is not None
        jak2 = [i for i in report.items if i.canonical_name == "jak2_v617f_mutation"]
        assert len(jak2) == 1
        assert jak2[0].status == "new"
        assert jak2[0].current_value == "Detected"

    def test_qualitative_resolved_test(self, db):
        """Resolved qualitative test shows raw string value, not None."""
        db.insert_observation(_make_lab("jak2_v617f_mutation", "Detected", "", 15, month=1))
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("glucose", 100, "mg/dL", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()

        assert report is not None
        jak2 = [i for i in report.items if i.canonical_name == "jak2_v617f_mutation"]
        assert len(jak2) == 1
        assert jak2[0].status == "resolved"
        assert jak2[0].previous_value == "Detected"

    def test_mixed_qualitative_and_numeric(self, db):
        """Mixed panel with both numeric and qualitative changes."""
        # Panel 1
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", 15, month=1))
        db.insert_observation(_make_lab("jak2_v617f_mutation", "Not Detected", "", 15, month=1))

        # Panel 2
        db.insert_observation(_make_lab("glucose", 108, "mg/dL", 15, month=3))
        db.insert_observation(_make_lab("jak2_v617f_mutation", "Detected", "", 15, month=3))

        d = DeltaEngine(db)
        report = d.compute_delta()

        assert report is not None
        assert len(report.items) == 2

        items = {i.canonical_name: i for i in report.items}
        # Numeric test should have numeric values
        assert isinstance(items["glucose"].current_value, float)
        # Qualitative test should have string values
        assert items["jak2_v617f_mutation"].status == "changed"
        assert items["jak2_v617f_mutation"].current_value == "Detected"
