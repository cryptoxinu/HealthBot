"""Tests for the intelligence auditor."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.intelligence_auditor import (
    IntelligenceAuditor,
    IntelligenceGap,
)


class TestIntelligenceAuditor:
    """Self-audit for health data gaps."""

    def _mock_db(
        self,
        conditions: list | None = None,
        labs: list | None = None,
        hypotheses: list | None = None,
    ) -> MagicMock:
        db = MagicMock()
        db.get_ltm_by_category = MagicMock(return_value=conditions or [])
        db.query_observations = MagicMock(return_value=labs or [])
        db.get_active_hypotheses = MagicMock(return_value=hypotheses or [])
        return db

    def test_condition_without_tests_flags_gap(self):
        db = self._mock_db(
            conditions=[{"fact": "diagnosed with diabetes"}],
            labs=[],  # no lab data at all
        )
        auditor = IntelligenceAuditor(db)
        gaps = auditor._check_condition_test_gaps(user_id=1)
        assert len(gaps) >= 1
        assert gaps[0].gap_type == "missing_test"
        assert gaps[0].priority == "high"

    def test_unfollowed_flag_detected(self):
        db = self._mock_db(labs=[
            {
                "canonical_name": "ldl",
                "test_name": "LDL",
                "date_collected": "2024-01-15",
                "flag": "H",
                "value": "180",
                "unit": "mg/dL",
                "_meta": {},
            },
        ])
        auditor = IntelligenceAuditor(db)
        gaps = auditor._check_unfollowed_flags(user_id=1)
        assert len(gaps) >= 1
        assert gaps[0].gap_type == "unfollowed_flag"

    def test_followed_flag_no_gap(self):
        db = self._mock_db(labs=[
            {
                "canonical_name": "ldl",
                "test_name": "LDL",
                "date_collected": "2024-01-15",
                "flag": "H",
                "value": "180",
                "unit": "mg/dL",
                "_meta": {},
            },
            {
                "canonical_name": "ldl",
                "test_name": "LDL",
                "date_collected": "2024-06-15",
                "flag": "",
                "value": "95",
                "unit": "mg/dL",
                "_meta": {},
            },
        ])
        auditor = IntelligenceAuditor(db)
        gaps = auditor._check_unfollowed_flags(user_id=1)
        # The flagged result was rechecked (later date exists)
        assert not any(g.gap_type == "unfollowed_flag" for g in gaps)

    def test_age_screening_gap(self):
        db = self._mock_db(labs=[])
        auditor = IntelligenceAuditor(db)
        gaps = auditor._check_age_screenings(
            user_id=1,
            demographics={"age": 40, "sex": "male"},
        )
        assert len(gaps) >= 1
        assert all(g.gap_type == "age_screening" for g in gaps)

    def test_age_screening_young_no_gap(self):
        db = self._mock_db(labs=[])
        auditor = IntelligenceAuditor(db)
        gaps = auditor._check_age_screenings(
            user_id=1,
            demographics={"age": 18, "sex": "male"},
        )
        # No screening needed at 18 — start ages are 20+
        assert len(gaps) == 0

    def test_hypothesis_missing_tests(self):
        db = self._mock_db(hypotheses=[{
            "title": "Iron deficiency anemia",
            "missing_tests": ["iron", "tibc"],
        }])
        auditor = IntelligenceAuditor(db)
        gaps = auditor._check_hypothesis_gaps(user_id=1)
        assert len(gaps) >= 1
        assert "iron" in gaps[0].related_tests

    def test_format_gaps_empty(self):
        db = self._mock_db()
        auditor = IntelligenceAuditor(db)
        text = auditor.format_gaps([])
        assert "No intelligence gaps" in text

    def test_format_gaps_with_items(self):
        db = self._mock_db()
        auditor = IntelligenceAuditor(db)
        gaps = [
            IntelligenceGap(
                gap_type="missing_test",
                description="Missing ferritin test",
                priority="high",
                related_tests=["ferritin"],
            ),
        ]
        text = auditor.format_gaps(gaps)
        assert "Missing ferritin" in text
        assert "!" in text  # high priority icon
