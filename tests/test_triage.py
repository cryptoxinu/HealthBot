"""Tests for triage red-flag detection."""
from __future__ import annotations

import pytest

from healthbot.data.models import LabResult, TriageLevel
from healthbot.reasoning.triage import TriageEngine


@pytest.fixture
def triage() -> TriageEngine:
    return TriageEngine()


class TestTriageClassification:
    """Test deterministic triage level classification."""

    def test_normal_glucose(self, triage: TriageEngine) -> None:
        lab = LabResult(id="1", test_name="Glucose", value=85.0, unit="mg/dL",
                        reference_low=70, reference_high=100)
        result = triage.classify(lab)
        assert result == TriageLevel.NORMAL

    def test_watch_glucose(self, triage: TriageEngine) -> None:
        """Value within 10% of boundary should be WATCH."""
        lab = LabResult(id="2", test_name="Glucose", value=95.0, unit="mg/dL",
                        reference_low=70, reference_high=100)
        result = triage.classify(lab)
        assert result == TriageLevel.WATCH

    def test_urgent_glucose(self, triage: TriageEngine) -> None:
        lab = LabResult(id="3", test_name="Glucose", value=150.0, unit="mg/dL",
                        reference_low=70, reference_high=100)
        result = triage.classify(lab)
        assert result == TriageLevel.URGENT

    def test_critical_glucose(self, triage: TriageEngine) -> None:
        lab = LabResult(id="4", test_name="Glucose", value=520.0, unit="mg/dL",
                        reference_low=70, reference_high=100)
        result = triage.classify(lab)
        assert result == TriageLevel.CRITICAL

    def test_critical_potassium_high(self, triage: TriageEngine) -> None:
        lab = LabResult(id="5", test_name="Potassium", value=7.1, unit="mEq/L",
                        reference_low=3.5, reference_high=5.0)
        result = triage.classify(lab)
        assert result == TriageLevel.CRITICAL

    def test_critical_potassium_low(self, triage: TriageEngine) -> None:
        lab = LabResult(id="6", test_name="K+", value=2.3, unit="mEq/L",
                        reference_low=3.5, reference_high=5.0)
        result = triage.classify(lab)
        assert result == TriageLevel.CRITICAL

    def test_ldl_increasing_triggers_concern(self, triage: TriageEngine) -> None:
        """LDL above reference should be URGENT."""
        lab = LabResult(id="7", test_name="LDL Cholesterol", value=190.0,
                        unit="mg/dL", reference_high=100)
        result = triage.classify(lab)
        assert result == TriageLevel.URGENT


class TestTriageEmergencyKeywords:
    """Test emergency keyword short-circuit."""

    def test_chest_pain_emergency(self, triage: TriageEngine) -> None:
        level, msg = triage.check_emergency_keywords("I'm having chest pain")
        assert level == TriageLevel.EMERGENCY
        assert "911" in msg or "emergency" in msg.lower()

    def test_difficulty_breathing_emergency(self, triage: TriageEngine) -> None:
        level, msg = triage.check_emergency_keywords("difficulty breathing")
        assert level == TriageLevel.EMERGENCY

    def test_suicidal_ideation_emergency(self, triage: TriageEngine) -> None:
        level, msg = triage.check_emergency_keywords("suicidal thoughts")
        assert level == TriageLevel.EMERGENCY
        assert "988" in msg or "helpline" in msg.lower() or "crisis" in msg.lower()

    def test_normal_text_no_emergency(self, triage: TriageEngine) -> None:
        level, msg = triage.check_emergency_keywords("What is my LDL trend?")
        assert level is None
        assert msg == ""
