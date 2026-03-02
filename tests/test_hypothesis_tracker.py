"""Tests for reasoning/hypothesis_tracker.py — fuzzy matching, upsert, status changes."""
from __future__ import annotations

from unittest.mock import patch

import pytest

from healthbot.reasoning.hypothesis_tracker import HypothesisTracker


@pytest.fixture
def tracker(config, key_manager, db) -> HypothesisTracker:
    db.run_migrations()
    return HypothesisTracker(db)


@pytest.fixture
def user_id() -> int:
    return 123


class TestFindMatchingHypothesis:
    def test_no_hypotheses_returns_none(self, tracker, user_id):
        assert tracker.find_matching_hypothesis("anything", user_id) is None

    def test_exact_match(self, tracker, user_id, db):
        db.insert_hypothesis(user_id, {"title": "Pre-diabetes", "confidence": 0.6})
        result = tracker.find_matching_hypothesis("Pre-diabetes", user_id)
        assert result is not None
        assert result.get("title") == "Pre-diabetes"

    def test_fuzzy_match(self, tracker, user_id, db):
        db.insert_hypothesis(user_id, {"title": "Hashimoto's Thyroiditis", "confidence": 0.5})
        result = tracker.find_matching_hypothesis("Hashimotos Thyroiditis", user_id)
        assert result is not None

    def test_case_insensitive(self, tracker, user_id, db):
        db.insert_hypothesis(user_id, {"title": "POTS", "confidence": 0.7})
        result = tracker.find_matching_hypothesis("pots", user_id)
        assert result is not None

    def test_no_match_below_threshold(self, tracker, user_id, db):
        db.insert_hypothesis(user_id, {"title": "Anemia", "confidence": 0.5})
        result = tracker.find_matching_hypothesis("Diabetes", user_id)
        assert result is None


class TestUpsertHypothesis:
    def test_creates_new_when_no_match(self, tracker, user_id, db):
        hyp_id = tracker.upsert_hypothesis(user_id, {
            "title": "Iron deficiency",
            "confidence": 0.4,
            "evidence_for": ["low ferritin"],
            "missing_tests": ["iron panel"],
        })
        assert hyp_id
        hyps = db.get_active_hypotheses(user_id)
        assert len(hyps) == 1
        assert hyps[0].get("title") == "Iron deficiency"

    def test_merges_when_match_found(self, tracker, user_id, db):
        db.insert_hypothesis(user_id, {
            "title": "Pre-diabetes",
            "confidence": 0.5,
            "evidence_for": ["glucose 108"],
            "evidence_against": [],
            "missing_tests": ["HbA1c"],
        })

        tracker.upsert_hypothesis(user_id, {
            "title": "Pre-diabetes",
            "confidence": 0.7,
            "evidence_for": ["glucose trending up"],
            "evidence_against": ["normal fasting"],
            "missing_tests": ["fasting insulin"],
        })

        hyps = db.get_active_hypotheses(user_id)
        assert len(hyps) == 1
        h = hyps[0]
        # Weighted average: 0.5 * 0.7 + 0.7 * 0.3 = 0.56
        assert abs(h.get("confidence") - 0.56) < 0.01
        assert "glucose 108" in h.get("evidence_for", [])
        assert "glucose trending up" in h.get("evidence_for", [])
        assert "normal fasting" in h.get("evidence_against", [])
        assert "HbA1c" in h.get("missing_tests", [])
        assert "fasting insulin" in h.get("missing_tests", [])

    def test_evidence_dedup(self, tracker, user_id, db):
        db.insert_hypothesis(user_id, {
            "title": "POTS",
            "confidence": 0.6,
            "evidence_for": ["elevated HR"],
        })

        tracker.upsert_hypothesis(user_id, {
            "title": "POTS",
            "confidence": 0.6,
            "evidence_for": ["elevated HR", "new evidence"],
        })

        hyps = db.get_active_hypotheses(user_id)
        ev_for = hyps[0].get("evidence_for", [])
        assert ev_for.count("elevated HR") == 1
        assert "new evidence" in ev_for

    def test_requires_title(self, tracker, user_id):
        with pytest.raises(ValueError, match="title"):
            tracker.upsert_hypothesis(user_id, {"confidence": 0.5})


class TestCheckFulfilledTests:
    @patch.object(HypothesisTracker, "_has_lab_data")
    def test_finds_fulfilled_tests(self, mock_has_lab, tracker, user_id, db):
        db.insert_hypothesis(user_id, {
            "title": "Thyroid issue",
            "confidence": 0.5,
            "evidence_for": [],
            "missing_tests": ["TSH", "Free T4"],
        })

        mock_has_lab.side_effect = lambda name, user_id=None: name == "TSH"

        updated = tracker.check_fulfilled_tests(user_id)
        assert len(updated) == 1
        h = updated[0]
        assert "Free T4" in h.get("missing_tests", [])
        assert "TSH" not in h.get("missing_tests", [])
        assert any("TSH" in e for e in h.get("evidence_for", []))

    @patch.object(HypothesisTracker, "_has_lab_data", return_value=False)
    def test_no_fulfilled_returns_empty(self, mock_has_lab, tracker, user_id, db):
        db.insert_hypothesis(user_id, {
            "title": "Anemia",
            "confidence": 0.4,
            "missing_tests": ["iron", "ferritin"],
        })
        updated = tracker.check_fulfilled_tests(user_id)
        assert updated == []

    def test_no_missing_tests_skipped(self, tracker, user_id, db):
        db.insert_hypothesis(user_id, {
            "title": "Known condition",
            "confidence": 0.9,
            "missing_tests": [],
        })
        updated = tracker.check_fulfilled_tests(user_id)
        assert updated == []


class TestRuleout:
    def test_ruleout_sets_status(self, tracker, user_id, db):
        hyp_id = db.insert_hypothesis(user_id, {
            "title": "POTS",
            "confidence": 0.6,
        })
        tracker.ruleout(hyp_id, "Tilt table test negative")

        hyp = db.get_hypothesis(hyp_id)
        assert hyp["_status"] == "ruled_out"
        assert hyp.get("confidence") == 0.0
        assert "Tilt table test negative" in hyp.get("evidence_against", [])

    def test_ruleout_nonexistent_raises(self, tracker):
        with pytest.raises(ValueError, match="not found"):
            tracker.ruleout("nonexistent_id")


class TestConfirm:
    def test_confirm_sets_status(self, tracker, user_id, db):
        hyp_id = db.insert_hypothesis(user_id, {
            "title": "Pre-diabetes",
            "confidence": 0.8,
            "missing_tests": ["oral glucose tolerance"],
        })
        tracker.confirm(hyp_id, "Doctor confirmed")

        hyp = db.get_hypothesis(hyp_id)
        assert hyp["_status"] == "confirmed"
        assert hyp.get("confidence") == 1.0
        assert hyp.get("missing_tests") == []
        assert "Doctor confirmed" in hyp.get("evidence_for", [])

    def test_confirm_nonexistent_raises(self, tracker):
        with pytest.raises(ValueError, match="not found"):
            tracker.confirm("nonexistent_id")
