"""Tests for the structured health review engine."""
from __future__ import annotations

import uuid
from datetime import date

from healthbot.data.models import LabResult, TriageLevel
from healthbot.reasoning.delta import DeltaEngine
from healthbot.reasoning.health_review import HealthReviewEngine
from healthbot.reasoning.overdue import OverdueDetector
from healthbot.reasoning.trends import TrendAnalyzer
from healthbot.reasoning.triage import TriageEngine


def _make_lab(
    canonical: str,
    value: float,
    unit: str,
    day: int = 15,
    month: int = 1,
    triage: TriageLevel = TriageLevel.NORMAL,
    ref_low: float | None = None,
    ref_high: float | None = None,
) -> LabResult:
    return LabResult(
        id=uuid.uuid4().hex,
        test_name=canonical.replace("_", " ").title(),
        canonical_name=canonical,
        value=value,
        unit=unit,
        date_collected=date(2024, month, day),
        triage_level=triage,
        reference_low=ref_low,
        reference_high=ref_high,
    )


def _build_engine(db) -> HealthReviewEngine:
    triage = TriageEngine()
    trends = TrendAnalyzer(db)
    overdue = OverdueDetector(db)
    delta = DeltaEngine(db)
    return HealthReviewEngine(db, triage, trends, overdue, delta)


class TestHealthReview:
    """Core review generation tests."""

    def test_empty_db_produces_review(self, db):
        engine = _build_engine(db)
        packet = engine.generate_review()
        assert packet is not None
        assert packet.overall_score == 0.0 or packet.overall_score >= 0
        assert isinstance(packet.domains, list)

    def test_review_with_data(self, db):
        # Insert a basic metabolic panel
        db.insert_observation(_make_lab("glucose", 95, "mg/dL"))
        db.insert_observation(_make_lab("sodium", 140, "mEq/L"))
        db.insert_observation(_make_lab("creatinine", 1.0, "mg/dL"))

        engine = _build_engine(db)
        packet = engine.generate_review()

        assert packet.overall_score > 0
        domain_names = {d.domain for d in packet.domains}
        assert "metabolic" in domain_names

    def test_low_score_generates_action(self, db):
        # Multiple urgent results in metabolic domain -> score < 70
        db.insert_observation(
            _make_lab("glucose", 250, "mg/dL", triage=TriageLevel.URGENT)
        )
        db.insert_observation(
            _make_lab("creatinine", 5.0, "mg/dL", triage=TriageLevel.CRITICAL)
        )
        db.insert_observation(
            _make_lab("bun", 60, "mg/dL", triage=TriageLevel.URGENT)
        )
        db.insert_observation(
            _make_lab("egfr", 25, "mL/min/1.73m2", triage=TriageLevel.CRITICAL)
        )

        engine = _build_engine(db)
        packet = engine.generate_review()

        # Should have a P1 action for metabolic
        p1_actions = [a for a in packet.actions if a.priority == 1]
        assert len(p1_actions) > 0
        assert any("metabolic" in a.message.lower() for a in p1_actions)

    def test_domains_have_drivers_when_flagged(self, db):
        db.insert_observation(
            _make_lab("glucose", 250, "mg/dL", triage=TriageLevel.URGENT)
        )

        engine = _build_engine(db)
        packet = engine.generate_review()

        metabolic = next((d for d in packet.domains if d.domain == "metabolic"), None)
        assert metabolic is not None
        assert len(metabolic.drivers) > 0

    def test_doctor_questions_generated(self, db):
        db.insert_observation(
            _make_lab("glucose", 250, "mg/dL", triage=TriageLevel.URGENT)
        )

        engine = _build_engine(db)
        packet = engine.generate_review()
        assert len(packet.doctor_questions) > 0

    def test_delta_included_with_two_panels(self, db):
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", month=1))
        db.insert_observation(_make_lab("glucose", 108, "mg/dL", month=3))

        engine = _build_engine(db)
        packet = engine.generate_review()
        assert packet.delta_summary != ""
        assert "WHAT CHANGED" in packet.delta_summary

    def test_supplement_action_for_low_vitamins(self, db):
        db.insert_observation(
            _make_lab("vitamin_d", 12, "ng/mL", triage=TriageLevel.URGENT)
        )

        engine = _build_engine(db)
        packet = engine.generate_review()

        supplement_actions = [a for a in packet.actions if a.category == "supplement"]
        assert len(supplement_actions) > 0


class TestHealthReviewFormat:
    """format_review output tests."""

    def test_format_includes_all_sections(self, db):
        db.insert_observation(_make_lab("glucose", 95, "mg/dL", month=1))
        db.insert_observation(_make_lab("glucose", 250, "mg/dL", month=3,
                                        triage=TriageLevel.URGENT))

        engine = _build_engine(db)
        packet = engine.generate_review()
        text = engine.format_review(packet)

        assert "HEALTH REVIEW" in text
        assert "DOMAIN SCORES" in text
        assert "ACTION PLAN" in text
        assert "Overall Health Score" in text

    def test_format_empty_db(self, db):
        engine = _build_engine(db)
        packet = engine.generate_review()
        text = engine.format_review(packet)
        assert "HEALTH REVIEW" in text

    def test_overall_score_is_weighted(self, db):
        # All normal -> score should be ~100
        db.insert_observation(_make_lab("glucose", 85, "mg/dL"))
        db.insert_observation(_make_lab("ldl", 90, "mg/dL"))
        db.insert_observation(_make_lab("hemoglobin", 15, "g/dL"))
        db.insert_observation(_make_lab("alt", 25, "U/L"))
        db.insert_observation(_make_lab("tsh", 2.0, "mIU/L"))
        db.insert_observation(_make_lab("vitamin_d", 50, "ng/mL"))
        db.insert_observation(_make_lab("crp", 1.0, "mg/L"))

        engine = _build_engine(db)
        packet = engine.generate_review()
        assert packet.overall_score >= 95.0
