"""Tests for the family risk engine."""
from __future__ import annotations

from healthbot.reasoning.family_risk import (
    FamilyCondition,
    FamilyRiskEngine,
    parse_family_history,
)


class TestParseFamily:
    """Parse free-text family history into structured conditions."""

    def test_parse_first_degree_with_age(self):
        facts = ["Family history: father had heart attack at 55"]
        conditions = parse_family_history(facts)
        assert len(conditions) >= 1
        c = conditions[0]
        assert c.relationship == "first_degree"
        assert c.age_onset == 55

    def test_parse_second_degree(self):
        facts = ["grandmother had diabetes"]
        conditions = parse_family_history(facts)
        assert len(conditions) >= 1
        assert conditions[0].relationship == "second_degree"

    def test_parse_no_age_onset(self):
        facts = ["diabetes in mother"]
        conditions = parse_family_history(facts)
        assert len(conditions) >= 1
        assert conditions[0].age_onset is None
        assert conditions[0].relationship == "first_degree"

    def test_parse_multiple_conditions(self):
        facts = [
            "father had heart attack at 55",
            "mother has diabetes",
        ]
        conditions = parse_family_history(facts)
        assert len(conditions) >= 2


class TestFamilyRiskEngine:
    """Risk assessment based on family history."""

    def test_elevated_risk_ldl_with_heart_disease(self):
        engine = FamilyRiskEngine()
        conditions = [FamilyCondition("heart disease", "first_degree", 55)]
        result = engine.assess(conditions, "ldl")
        assert result.risk_level == "elevated"
        assert result.aggressive_range is not None
        assert "aggressive_high" in result.aggressive_range

    def test_standard_risk_unrelated_test(self):
        engine = FamilyRiskEngine()
        conditions = [FamilyCondition("heart disease", "first_degree", 55)]
        result = engine.assess(conditions, "tsh")
        assert result.risk_level == "standard"

    def test_screening_implications_diabetes(self):
        engine = FamilyRiskEngine()
        conditions = [FamilyCondition("diabetes", "first_degree", None)]
        implications = engine.get_all_screening_implications(conditions)
        assert len(implications) > 0
        assert any("HbA1c" in imp for imp in implications)
