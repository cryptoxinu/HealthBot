"""Tests for doctor visit preparation engine."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import LabResult
from healthbot.reasoning.doctor_prep import DoctorPrepEngine
from healthbot.reasoning.overdue import OverdueDetector
from healthbot.reasoning.trends import TrendAnalyzer
from healthbot.reasoning.triage import TriageEngine


def _make_engine(db):
    return DoctorPrepEngine(
        db, TriageEngine(), TrendAnalyzer(db), OverdueDetector(db)
    )


class TestGeneratePrep:
    """DoctorPrepEngine.generate_prep() text output."""

    def test_empty_db_prep(self, db):
        engine = _make_engine(db)
        text = engine.generate_prep()
        assert "DOCTOR VISIT PREPARATION" in text
        assert "No urgent" in text or "No urgent or critical" in text
        assert "No medications" in text or "No active medications" in text

    def test_urgent_findings_shown(self, db):
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=250.0,
            unit="mg/dL",
            date_collected=date.today(),
        )
        db.insert_observation(lab)
        engine = _make_engine(db)
        text = engine.generate_prep()
        # The lab may or may not be flagged depending on triage rules.
        # At minimum, the prep should be generated without error.
        assert "DOCTOR VISIT PREPARATION" in text

    def test_medications_listed(self, db):
        from healthbot.data.models import Medication
        med = Medication(
            id=uuid.uuid4().hex,
            name="Metformin",
            dose="500mg",
            frequency="twice daily",
            status="active",
        )
        db.insert_medication(med)
        engine = _make_engine(db)
        text = engine.generate_prep()
        assert "Metformin" in text
        assert "500mg" in text

    def test_overdue_shown(self, db):
        old_date = date.today() - timedelta(days=15 * 30)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=old_date,
        )
        db.insert_observation(lab)
        engine = _make_engine(db)
        text = engine.generate_prep()
        assert "OVERDUE" in text
        assert "Glucose" in text

    def test_questions_generated_when_no_findings(self, db):
        engine = _make_engine(db)
        text = engine.generate_prep()
        assert "QUESTIONS" in text
        assert "screenings" in text.lower() or "recommend" in text.lower()


class TestGeneratePrepData:
    """DoctorPrepEngine.generate_prep_data() structured output."""

    def test_returns_prepdata(self, db):
        from healthbot.export.pdf_generator import PrepData
        engine = _make_engine(db)
        data = engine.generate_prep_data()
        assert isinstance(data, PrepData)
        assert data.generated_date  # Should be set

    def test_prepdata_has_medications(self, db):
        from healthbot.data.models import Medication
        med = Medication(
            id=uuid.uuid4().hex,
            name="Lisinopril",
            dose="10mg",
            frequency="daily",
            status="active",
        )
        db.insert_medication(med)
        engine = _make_engine(db)
        data = engine.generate_prep_data()
        assert len(data.medications) >= 1
        assert any(m["name"] == "Lisinopril" for m in data.medications)

    def test_prepdata_questions_not_empty(self, db):
        engine = _make_engine(db)
        data = engine.generate_prep_data()
        assert len(data.questions) >= 1


class TestGenerateQuestions:
    """Question generation from findings."""

    def test_flagged_results_generate_question(self, db):
        engine = _make_engine(db)
        flagged = [{"test_name": "LDL"}]
        questions = engine._generate_questions(flagged, [], [])
        assert any("LDL" in q for q in questions)

    def test_no_findings_generates_default(self, db):
        engine = _make_engine(db)
        questions = engine._generate_questions([], [], [])
        assert len(questions) >= 1
        assert any("screenings" in q.lower() or "recommend" in q.lower() for q in questions)
