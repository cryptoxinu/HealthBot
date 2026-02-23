"""Tests for comorbidity cross-analysis engine."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.comorbidity import (
    COMORBIDITY_KB,
    ComorbidityAnalyzer,
    format_comorbidities,
)


def _make_db(
    conditions: list[str] | None = None,
    hypotheses: list[dict] | None = None,
    observations: list[dict] | None = None,
) -> MagicMock:
    db = MagicMock()
    conditions = conditions or []
    hypotheses = hypotheses or []
    observations = observations or []

    condition_facts = [{"fact": c} for c in conditions]

    def get_ltm(user_id, category):
        if category == "condition":
            return condition_facts
        return []

    db.get_ltm_by_category.side_effect = get_ltm
    db.get_active_hypotheses.return_value = hypotheses

    def query_obs(
        record_type=None, canonical_name=None,
        start_date=None, end_date=None,
        triage_level=None, limit=200, user_id=None,
    ):
        results = []
        for obs in observations:
            if canonical_name and obs.get("canonical_name") != canonical_name:
                continue
            results.append(obs)
        return results[:limit]

    db.query_observations.side_effect = query_obs
    return db


class TestKBCoverage:
    def test_all_have_evidence(self):
        for i in COMORBIDITY_KB:
            assert i.evidence, f"{i.condition_a}/{i.condition_b} missing evidence"

    def test_all_have_clinical_implication(self):
        for i in COMORBIDITY_KB:
            assert i.clinical_implication

    def test_all_valid_types(self):
        for i in COMORBIDITY_KB:
            assert i.interaction_type in (
                "causal", "bidirectional", "shared_mechanism",
            )

    def test_all_valid_priorities(self):
        for i in COMORBIDITY_KB:
            assert i.priority in ("high", "medium", "low")


class TestDetection:
    def test_hypothyroidism_hyperlipidemia(self):
        db = _make_db(conditions=["hypothyroidism", "hyperlipidemia"])
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)

        pairs = [
            (f.interaction.condition_a, f.interaction.condition_b)
            for f in findings
        ]
        assert ("hypothyroidism", "hyperlipidemia") in pairs

    def test_diabetes_hypertension(self):
        db = _make_db(conditions=["diabetes", "hypertension"])
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)

        assert len(findings) >= 1
        assert any(
            f.interaction.condition_a == "diabetes"
            and f.interaction.condition_b == "hypertension"
            for f in findings
        )

    def test_alias_matching(self):
        """'type 2 diabetes' should match 'diabetes' in KB."""
        db = _make_db(conditions=["type 2 diabetes", "high blood pressure"])
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)

        assert len(findings) >= 1

    def test_no_match_single_condition(self):
        db = _make_db(conditions=["hypothyroidism"])
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)
        assert findings == []

    def test_no_match_unrelated(self):
        db = _make_db(conditions=["gout", "depression"])
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)
        assert findings == []

    def test_hypothesis_as_condition(self):
        """High-confidence hypotheses should be treated as conditions."""
        hyps = [
            {"title": "hypothyroidism", "confidence": 0.8, "_confidence": 0.8},
        ]
        db = _make_db(conditions=["hyperlipidemia"], hypotheses=hyps)
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)

        assert len(findings) >= 1

    def test_low_confidence_hypothesis_excluded(self):
        hyps = [
            {"title": "hypothyroidism", "confidence": 0.3, "_confidence": 0.3},
        ]
        db = _make_db(conditions=["hyperlipidemia"], hypotheses=hyps)
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)

        # hypothyroidism at 30% confidence should not trigger
        thyroid_findings = [
            f for f in findings
            if "hypothyroidism" in (
                f.interaction.condition_a, f.interaction.condition_b,
            )
        ]
        assert len(thyroid_findings) == 0

    def test_lab_detected_deficiency(self):
        """Low vitamin D in labs + osteoporosis condition should trigger."""
        obs = [{"canonical_name": "vitamin_d", "value": 15.0}]
        db = _make_db(conditions=["osteoporosis"], observations=obs)
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)

        vd = [
            f for f in findings
            if "vitamin d deficiency" in (
                f.interaction.condition_a, f.interaction.condition_b,
            )
        ]
        assert len(vd) == 1

    def test_multiple_comorbidities(self):
        """Multiple interacting conditions."""
        db = _make_db(conditions=[
            "diabetes", "hypertension", "chronic kidney disease",
        ])
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)

        # Should find: diabetes↔hypertension, diabetes↔CKD, hypertension↔CKD
        assert len(findings) >= 3


class TestSorting:
    def test_high_priority_first(self):
        db = _make_db(conditions=[
            "hypothyroidism", "hyperlipidemia", "depression",
        ])
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)

        if len(findings) >= 2:
            assert findings[0].interaction.priority == "high"


class TestFormatting:
    def test_format_empty(self):
        result = format_comorbidities([])
        assert "No significant" in result

    def test_format_with_findings(self):
        db = _make_db(conditions=["hypothyroidism", "hyperlipidemia"])
        analyzer = ComorbidityAnalyzer(db)
        findings = analyzer.analyze(user_id=1)

        result = format_comorbidities(findings)
        assert "COMORBIDITY" in result
        assert "Hypothyroidism" in result
        assert "Hyperlipidemia" in result
