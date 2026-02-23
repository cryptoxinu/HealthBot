"""Tests for the condition extractor."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.condition_extractor import extract_conditions


class TestExtractConditions:
    """Extract user conditions from hypotheses, LTM, and flagged labs."""

    def _mock_db(
        self,
        hypotheses: list | None = None,
        conditions: list | None = None,
        labs: list | None = None,
    ) -> MagicMock:
        db = MagicMock()
        db.get_active_hypotheses = MagicMock(return_value=hypotheses or [])
        db.get_ltm_by_category = MagicMock(return_value=conditions or [])
        db.query_observations = MagicMock(return_value=labs or [])
        return db

    def test_extracts_from_hypothesis(self):
        db = self._mock_db(hypotheses=[{"title": "Possible iron deficiency"}])
        result = extract_conditions(db, user_id=1)
        assert "iron deficiency" in result

    def test_extracts_from_ltm_condition(self):
        db = self._mock_db(conditions=[{"fact": "Diagnosed with hypothyroidism"}])
        result = extract_conditions(db, user_id=1)
        assert "hypothyroidism" in result

    def test_extracts_from_flagged_labs(self):
        db = self._mock_db(labs=[{
            "canonical_name": "ferritin",
            "flag": "L",
        }])
        result = extract_conditions(db, user_id=1)
        assert "iron deficiency" in result

    def test_dedup_across_sources(self):
        db = self._mock_db(
            hypotheses=[{"title": "iron deficiency anemia"}],
            labs=[{"canonical_name": "ferritin", "flag": "L"}],
        )
        result = extract_conditions(db, user_id=1)
        # "iron deficiency" from both sources should appear only once
        assert result.count("iron deficiency") <= 1

    def test_max_conditions_limits(self):
        db = self._mock_db(hypotheses=[
            {"title": "iron deficiency"},
            {"title": "hypothyroidism"},
            {"title": "diabetes"},
            {"title": "dyslipidemia"},
        ])
        result = extract_conditions(db, user_id=1, max_conditions=2)
        assert len(result) <= 2

    def test_empty_db_returns_empty(self):
        db = self._mock_db()
        result = extract_conditions(db, user_id=1)
        assert result == []
