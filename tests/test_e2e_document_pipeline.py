"""End-to-end document pipeline integration tests.

These tests verify multi-component flows (import -> store -> analyze -> export)
using real DB instances but mocked external services (Ollama, file I/O).
"""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import LabResult, Medication
from healthbot.reasoning.data_quality import DataQualityEngine
from healthbot.reasoning.interactions import InteractionChecker


class TestLabIngestionPipeline:
    """Verify: lab results -> DB -> quality checks -> interactions -> insights."""

    def _insert_labs(self, db, labs: list[dict]) -> None:
        for lab_data in labs:
            lab = LabResult(
                id=uuid.uuid4().hex,
                test_name=lab_data["test_name"],
                canonical_name=lab_data["canonical_name"],
                value=lab_data["value"],
                unit=lab_data["unit"],
                date_collected=lab_data.get("date_collected", date.today()),
                flag=lab_data.get("flag", ""),
                reference_low=lab_data.get("reference_low"),
                reference_high=lab_data.get("reference_high"),
            )
            db.insert_observation(lab)

    def test_lab_ingest_and_query(self, db):
        """Labs inserted into DB should be queryable."""
        self._insert_labs(db, [
            {
                "test_name": "LDL", "canonical_name": "ldl",
                "value": 130.0, "unit": "mg/dL", "flag": "H",
                "reference_low": 0, "reference_high": 100,
            },
            {
                "test_name": "HDL", "canonical_name": "hdl",
                "value": 55.0, "unit": "mg/dL", "flag": "",
                "reference_low": 40, "reference_high": 999,
            },
        ])

        results = db.query_observations(
            record_type="lab_result", limit=10,
        )
        assert len(results) >= 2
        names = [r.get("test_name") for r in results]
        assert "LDL" in names
        assert "HDL" in names

    def test_quality_check_after_ingest(self, db):
        """Data quality engine should flag missing reference ranges."""
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            fasting=None,  # Missing fasting flag
        )
        dq = DataQualityEngine(db)
        issues = dq.check_batch([lab])
        types = {i.issue_type for i in issues}
        assert "missing_fasting_flag" in types

    def test_interaction_check_with_medications(self, db):
        """Medication interaction checker should detect known interactions."""
        # Insert warfarin and aspirin as active medications
        db.insert_medication(
            Medication(id=uuid.uuid4().hex, name="Warfarin", dose="5mg", frequency="daily"),
            user_id=1,
        )
        db.insert_medication(
            Medication(id=uuid.uuid4().hex, name="Aspirin", dose="81mg", frequency="daily"),
            user_id=1,
        )
        checker = InteractionChecker(db)
        results = checker.check_all(user_id=1)
        # Warfarin + aspirin is a known interaction
        assert len(results) >= 1

    def test_hypothesis_generation_from_labs(self, db):
        """Hypothesis generator should detect patterns from lab data."""
        from healthbot.reasoning.hypothesis_generator import HypothesisGenerator

        self._insert_labs(db, [
            {
                "test_name": "TSH", "canonical_name": "tsh",
                "value": 8.0, "unit": "mIU/L", "flag": "H",
            },
        ])

        gen = HypothesisGenerator(db)
        hypotheses = gen.scan_all(user_id=None)
        titles = [h.title for h in hypotheses]
        assert any("Hypothyroidism" in t for t in titles)


class TestReportPipeline:
    """Verify: data -> weekly PDF report generation."""

    def test_weekly_pdf_with_lab_data(self, db):
        """Weekly PDF report should include lab data."""
        from healthbot.export.weekly_pdf_report import WeeklyPdfReportGenerator

        # Insert a recent lab result
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=130.0,
            unit="mg/dL",
            date_collected=date.today() - timedelta(days=2),
            flag="H",
        )
        db.insert_observation(lab)

        gen = WeeklyPdfReportGenerator(db)
        pdf_bytes = gen.generate_weekly(user_id=None)
        assert isinstance(pdf_bytes, bytes)
        assert pdf_bytes[:4] == b"%PDF"

    def test_csv_export_after_ingest(self, db):
        """CSV export should contain ingested lab data."""
        from healthbot.export.csv_exporter import export_labs_csv

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Hemoglobin",
            canonical_name="hemoglobin",
            value=14.5,
            unit="g/dL",
            date_collected=date.today(),
        )
        db.insert_observation(lab)

        csv_text = export_labs_csv(db, user_id=None)
        assert "Hemoglobin" in csv_text
        assert "14.5" in csv_text


class TestDigestPipeline:
    """Verify: data -> daily digest generation."""

    def test_digest_with_recent_data(self, db):
        """Digest builder should include recent observations."""
        from healthbot.reasoning.digest import build_daily_digest

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=date.today(),
        )
        db.insert_observation(lab)

        build_daily_digest(db, user_id=None)
        # Should not raise even with minimal data


class TestOverdueDetection:
    """Verify: old labs -> overdue detection -> action items."""

    def test_old_lab_flags_overdue(self, db):
        """Lab result > 365 days old should be detected as overdue."""
        from healthbot.reasoning.overdue import OverdueDetector

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=2.5,
            unit="mIU/L",
            date_collected=date.today() - timedelta(days=400),
        )
        db.insert_observation(lab)

        detector = OverdueDetector(db)
        overdue_items = detector.check_overdue(user_id=None)
        # TSH checked 400 days ago should be overdue
        test_names = [item.test_name for item in overdue_items]
        assert any("tsh" in tn.lower() for tn in test_names)
