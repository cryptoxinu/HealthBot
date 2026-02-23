"""Tests for post-ingestion targeted analysis."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from healthbot.data.models import LabResult
from healthbot.reasoning.targeted_analysis import (
    TargetedAnalysisResult,
    TargetedAnalyzer,
)

# ── Fixtures ──────────────────────────────────────────────


@pytest.fixture()
def mock_db():
    db = MagicMock()
    db.get_user_demographics.return_value = {"sex": "male", "age": 45}
    db.get_active_hypotheses.return_value = []
    return db


@pytest.fixture()
def analyzer(mock_db):
    return TargetedAnalyzer(mock_db)


def _make_lab(canonical_name: str, value: float = 5.0) -> LabResult:
    return LabResult(
        id="lab1",
        test_name=canonical_name.replace("_", " ").title(),
        canonical_name=canonical_name,
        value=value,
        unit="mg/dL",
    )


# ── Basic behavior ────────────────────────────────────────


class TestAnalyzeNewLabs:
    def test_empty_lab_list_returns_empty(self, analyzer):
        result = analyzer.analyze_new_labs([], user_id=1)
        assert isinstance(result, TargetedAnalysisResult)
        assert result.trends_found == []
        assert result.interactions_found == []

    def test_labs_without_canonical_name_returns_empty(self, analyzer):
        lab = LabResult(id="x", test_name="Unknown", canonical_name="")
        result = analyzer.analyze_new_labs([lab], user_id=1)
        assert result.trends_found == []

    def test_calls_demographics(self, analyzer, mock_db):
        lab = _make_lab("glucose", 95)
        analyzer.analyze_new_labs([lab], user_id=1)
        mock_db.get_user_demographics.assert_called_with(1)

    @patch("healthbot.reasoning.targeted_analysis.TargetedAnalyzer._check_trends")
    @patch("healthbot.reasoning.targeted_analysis.TargetedAnalyzer._check_interactions")
    @patch("healthbot.reasoning.targeted_analysis.TargetedAnalyzer._run_hypotheses")
    @patch("healthbot.reasoning.targeted_analysis.TargetedAnalyzer._validate_hypotheses")
    @patch("healthbot.reasoning.targeted_analysis.TargetedAnalyzer._check_fulfilled_tests")
    def test_runs_all_engines(
        self, mock_fulfilled, mock_validate, mock_hyp,
        mock_interact, mock_trends, analyzer,
    ):
        mock_trends.return_value = ["glucose: increasing (+15.0%)"]
        mock_interact.return_value = []
        mock_hyp.return_value = (1, 0)
        mock_validate.return_value = 2
        mock_fulfilled.return_value = ["Iron deficiency: ferritin"]

        lab = _make_lab("glucose", 110)
        result = analyzer.analyze_new_labs([lab], user_id=1)

        assert len(result.trends_found) == 1
        assert result.hypotheses_created == 1
        assert result.hypotheses_updated == 2
        assert len(result.fulfilled_tests) == 1


class TestCheckTrends:
    @patch("healthbot.reasoning.trends.TrendAnalyzer")
    def test_returns_significant_trends(self, mock_analyzer_cls, mock_db):
        mock_trend = MagicMock()
        mock_trend.data_points = 5
        mock_trend.direction = "increasing"
        mock_trend.pct_change = 25.0
        mock_analyzer_cls.return_value.analyze_test.return_value = mock_trend

        analyzer = TargetedAnalyzer(mock_db)
        findings = analyzer._check_trends({"glucose"}, user_id=1)
        assert len(findings) == 1
        assert "increasing" in findings[0]

    @patch("healthbot.reasoning.trends.TrendAnalyzer")
    def test_skips_stable_trends(self, mock_analyzer_cls, mock_db):
        mock_trend = MagicMock()
        mock_trend.data_points = 5
        mock_trend.direction = "stable"
        mock_trend.pct_change = 2.0
        mock_analyzer_cls.return_value.analyze_test.return_value = mock_trend

        analyzer = TargetedAnalyzer(mock_db)
        findings = analyzer._check_trends({"glucose"}, user_id=1)
        assert findings == []

    @patch("healthbot.reasoning.trends.TrendAnalyzer")
    def test_skips_insufficient_data(self, mock_analyzer_cls, mock_db):
        mock_trend = MagicMock()
        mock_trend.data_points = 2
        mock_trend.direction = "increasing"
        mock_trend.pct_change = 50.0
        mock_analyzer_cls.return_value.analyze_test.return_value = mock_trend

        analyzer = TargetedAnalyzer(mock_db)
        findings = analyzer._check_trends({"glucose"}, user_id=1)
        assert findings == []


class TestCheckInteractions:
    @patch("healthbot.reasoning.interactions.InteractionChecker")
    def test_filters_to_ingested_tests(self, mock_checker_cls, mock_db):
        result1 = MagicMock()
        result1.lab_name = "glucose"
        result1.med_name = "metformin"
        result1.interaction.effect = "may lower glucose"

        result2 = MagicMock()
        result2.lab_name = "potassium"
        result2.med_name = "lisinopril"
        result2.interaction.effect = "may raise potassium"

        mock_checker_cls.return_value.check_drug_lab.return_value = [result1, result2]

        analyzer = TargetedAnalyzer(mock_db)
        findings = analyzer._check_interactions({"glucose"}, user_id=1)
        assert len(findings) == 1
        assert "glucose" in findings[0]


class TestCheckFulfilledTests:
    def test_detects_fulfilled_missing_tests(self, mock_db):
        mock_db.get_active_hypotheses.return_value = [
            {
                "status": "active",
                "title": "Iron deficiency",
                "missing_tests": ["ferritin", "tibc"],
            },
        ]
        analyzer = TargetedAnalyzer(mock_db)
        fulfilled = analyzer._check_fulfilled_tests({"ferritin"}, user_id=1)
        assert len(fulfilled) == 1
        assert "Iron deficiency" in fulfilled[0]
        assert "ferritin" in fulfilled[0]

    def test_ignores_ruled_out_hypotheses(self, mock_db):
        mock_db.get_active_hypotheses.return_value = [
            {
                "status": "ruled_out",
                "title": "Old hypothesis",
                "missing_tests": ["ferritin"],
            },
        ]
        analyzer = TargetedAnalyzer(mock_db)
        fulfilled = analyzer._check_fulfilled_tests({"ferritin"}, user_id=1)
        assert fulfilled == []

    def test_no_missing_tests(self, mock_db):
        mock_db.get_active_hypotheses.return_value = [
            {
                "status": "active",
                "title": "Some hypothesis",
                "missing_tests": [],
            },
        ]
        analyzer = TargetedAnalyzer(mock_db)
        fulfilled = analyzer._check_fulfilled_tests({"ferritin"}, user_id=1)
        assert fulfilled == []


class TestValidateHypotheses:
    """Tests for automatic hypothesis validation against new data."""

    @patch("healthbot.reasoning.hypothesis_tracker.HypothesisTracker")
    def test_validate_wired_into_analyzer(self, mock_tracker_cls, mock_db):
        """_validate_hypotheses delegates to HypothesisTracker."""
        mock_tracker_cls.return_value.validate_against_new_data.return_value = [
            {"hyp_id": "h1", "title": "Iron deficiency", "confidence": 0.65},
        ]
        analyzer = TargetedAnalyzer(mock_db)
        count = analyzer._validate_hypotheses({"ferritin"}, user_id=1)
        assert count == 1
        mock_tracker_cls.return_value.validate_against_new_data.assert_called_once_with(
            1, {"ferritin"},
        )

    @patch("healthbot.reasoning.reference_ranges.get_range")
    def test_abnormal_lab_boosts_confidence(self, mock_get_range, mock_db):
        """Low ferritin matches iron_deficiency_anemia trigger -> +0.10."""
        from healthbot.reasoning.hypothesis_tracker import HypothesisTracker

        mock_db.get_active_hypotheses.return_value = [
            {
                "_id": "h1",
                "_status": "active",
                "title": "Iron deficiency anemia",
                "confidence": 0.55,
                "pattern_id": "iron_deficiency_anemia",
                "evidence_for": ["ferritin is low (8)"],
                "evidence_against": [],
                "missing_tests": ["iron", "tibc"],
                "notes": "",
            },
        ]
        # hemoglobin is a trigger for iron_deficiency_anemia, direction="low"
        mock_db.query_observations.return_value = [{"value": 10.5}]
        mock_get_range.return_value = {"low": 12.0, "high": 17.5}

        tracker = HypothesisTracker(mock_db)
        updates = tracker.validate_against_new_data(1, {"hemoglobin"})

        assert len(updates) == 1
        assert updates[0]["confidence"] == pytest.approx(0.65)
        mock_db.update_hypothesis.assert_called_once()
        call_data = mock_db.update_hypothesis.call_args[0][1]
        assert any("supports" in e for e in call_data["evidence_for"])

    @patch("healthbot.reasoning.reference_ranges.get_range")
    def test_normal_lab_reduces_confidence(self, mock_get_range, mock_db):
        """Normal ferritin contradicts iron_deficiency_anemia -> -0.15."""
        from healthbot.reasoning.hypothesis_tracker import HypothesisTracker

        mock_db.get_active_hypotheses.return_value = [
            {
                "_id": "h2",
                "_status": "active",
                "title": "Iron deficiency anemia",
                "confidence": 0.55,
                "pattern_id": "iron_deficiency_anemia",
                "evidence_for": [],
                "evidence_against": [],
                "missing_tests": ["iron", "tibc"],
                "notes": "",
            },
        ]
        # hemoglobin is normal (within range)
        mock_db.query_observations.return_value = [{"value": 14.0}]
        mock_get_range.return_value = {"low": 12.0, "high": 17.5}

        tracker = HypothesisTracker(mock_db)
        updates = tracker.validate_against_new_data(1, {"hemoglobin"})

        assert len(updates) == 1
        assert updates[0]["confidence"] == pytest.approx(0.40)
        call_data = mock_db.update_hypothesis.call_args[0][1]
        assert any("contradicts" in e for e in call_data["evidence_against"])

    @patch("healthbot.reasoning.reference_ranges.get_range")
    def test_confidence_below_threshold_rules_out(self, mock_get_range, mock_db):
        """Confidence dropping below 0.10 sets status='ruled_out'."""
        from healthbot.reasoning.hypothesis_tracker import HypothesisTracker

        mock_db.get_active_hypotheses.return_value = [
            {
                "_id": "h3",
                "_status": "active",
                "title": "Hypothyroidism",
                "confidence": 0.15,
                "pattern_id": "hypothyroidism",
                "evidence_for": [],
                "evidence_against": [],
                "missing_tests": ["free_t4"],
                "notes": "",
            },
        ]
        # tsh is a trigger for hypothyroidism, direction="high"
        # normal TSH contradicts -> -0.15, new confidence = 0.0
        mock_db.query_observations.return_value = [{"value": 2.5}]
        mock_get_range.return_value = {"low": 0.4, "high": 4.0}

        tracker = HypothesisTracker(mock_db)
        updates = tracker.validate_against_new_data(1, {"tsh"})

        assert len(updates) == 1
        assert updates[0]["status"] == "ruled_out"
        assert updates[0]["confidence"] < 0.10

    @patch("healthbot.reasoning.reference_ranges.get_range")
    def test_removes_fulfilled_from_missing_tests(self, mock_get_range, mock_db):
        """Ingested test is removed from missing_tests list."""
        from healthbot.reasoning.hypothesis_tracker import HypothesisTracker

        mock_db.get_active_hypotheses.return_value = [
            {
                "_id": "h4",
                "_status": "active",
                "title": "Iron deficiency anemia",
                "confidence": 0.55,
                "pattern_id": "iron_deficiency_anemia",
                "evidence_for": [],
                "evidence_against": [],
                "missing_tests": ["iron", "tibc", "transferrin_saturation"],
                "notes": "",
            },
        ]
        # iron is both a trigger (direction="low") AND in missing_tests
        mock_db.query_observations.return_value = [{"value": 30.0}]
        mock_get_range.return_value = {"low": 60.0, "high": 170.0}

        tracker = HypothesisTracker(mock_db)
        updates = tracker.validate_against_new_data(1, {"iron"})

        assert len(updates) == 1
        call_data = mock_db.update_hypothesis.call_args[0][1]
        assert "iron" not in call_data["missing_tests"]
        assert "tibc" in call_data["missing_tests"]

    def test_skips_ruled_out_hypotheses(self, mock_db):
        """Hypotheses with ruled_out status are not validated."""
        from healthbot.reasoning.hypothesis_tracker import HypothesisTracker

        mock_db.get_active_hypotheses.return_value = [
            {
                "_id": "h5",
                "_status": "ruled_out",
                "title": "Old hypothesis",
                "confidence": 0.0,
                "pattern_id": "hypothyroidism",
                "evidence_for": [],
                "evidence_against": [],
                "missing_tests": [],
                "notes": "",
            },
        ]
        tracker = HypothesisTracker(mock_db)
        updates = tracker.validate_against_new_data(1, {"tsh"})
        assert updates == []

    def test_skips_no_pattern_id(self, mock_db):
        """Hypotheses without pattern_id are skipped."""
        from healthbot.reasoning.hypothesis_tracker import HypothesisTracker

        mock_db.get_active_hypotheses.return_value = [
            {
                "_id": "h6",
                "_status": "active",
                "title": "Custom hypothesis",
                "confidence": 0.5,
                "pattern_id": "",
                "evidence_for": [],
                "evidence_against": [],
                "missing_tests": [],
                "notes": "",
            },
        ]
        tracker = HypothesisTracker(mock_db)
        updates = tracker.validate_against_new_data(1, {"glucose"})
        assert updates == []


class TestRunHypotheses:
    @patch("healthbot.reasoning.hypothesis_tracker.HypothesisTracker")
    @patch("healthbot.reasoning.hypothesis_generator.HypothesisGenerator")
    def test_generates_and_upserts(self, mock_gen_cls, mock_tracker_cls, mock_db):
        hyp = MagicMock()
        hyp.title = "Iron deficiency"
        hyp.confidence = 0.6
        hyp.evidence_for = ["low ferritin"]
        hyp.evidence_against = []
        hyp.missing_tests = ["tibc"]
        hyp.pattern_id = "iron_deficiency_anemia"
        mock_gen_cls.return_value.scan_all.return_value = [hyp]
        mock_tracker_cls.return_value.upsert_hypothesis.return_value = "hyp-123"

        analyzer = TargetedAnalyzer(mock_db)
        created, updated = analyzer._run_hypotheses(1, {"sex": "female", "age": 30})
        assert created == 1
        mock_tracker_cls.return_value.upsert_hypothesis.assert_called_once()

    @patch("healthbot.reasoning.hypothesis_generator.HypothesisGenerator")
    def test_no_hypotheses_found(self, mock_gen_cls, mock_db):
        mock_gen_cls.return_value.scan_all.return_value = []
        analyzer = TargetedAnalyzer(mock_db)
        created, updated = analyzer._run_hypotheses(1, {"sex": "male", "age": 50})
        assert created == 0
        assert updated == 0
