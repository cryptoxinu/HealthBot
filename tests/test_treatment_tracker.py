"""Tests for treatment effectiveness tracker."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

from healthbot.reasoning.treatment_tracker import (
    DRUG_BIOMARKER_LINKS,
    EffectivenessReport,
    TreatmentTracker,
    format_effectiveness,
)


def _make_db(meds: list[dict], observations: list[dict]) -> MagicMock:
    """Create a mock HealthDB with medications and observations."""
    db = MagicMock()
    db.get_active_medications.return_value = meds

    def query_obs(
        record_type=None, canonical_name=None,
        start_date=None, end_date=None,
        triage_level=None, limit=200, user_id=None,
    ):
        results = []
        for obs in observations:
            if canonical_name and obs.get("canonical_name") != canonical_name:
                continue
            obs_date = obs.get(
                "date_collected", obs.get("_date_effective", ""),
            )
            if start_date and str(obs_date) < str(start_date):
                continue
            if end_date and str(obs_date) > str(end_date):
                continue
            results.append(obs)
        results.sort(
            key=lambda x: x.get("date_collected", ""), reverse=True,
        )
        return results[:limit]

    db.query_observations.side_effect = query_obs
    return db


def _obs(name: str, value: float, dt: str) -> dict:
    """Shorthand for creating an observation dict."""
    return {
        "canonical_name": name,
        "value": value,
        "date_collected": dt,
        "_date_effective": dt,
    }


class TestDrugKeyResolution:
    def test_resolves_brand_name(self):
        assert TreatmentTracker._resolve_drug_key("Lipitor") == "statin"

    def test_resolves_generic_name(self):
        assert TreatmentTracker._resolve_drug_key("atorvastatin") == "statin"

    def test_resolves_with_dose(self):
        key = TreatmentTracker._resolve_drug_key("atorvastatin 40mg")
        assert key == "statin"

    def test_unknown_drug(self):
        assert TreatmentTracker._resolve_drug_key("somethingweird") == ""

    def test_metformin(self):
        key = TreatmentTracker._resolve_drug_key("metformin 500mg")
        assert key == "metformin"

    def test_levothyroxine(self):
        key = TreatmentTracker._resolve_drug_key("Synthroid 75mcg")
        assert key == "levothyroxine"

    def test_vitamin_d_multiword(self):
        key = TreatmentTracker._resolve_drug_key("vitamin d 5000iu")
        assert key == "vitamin_d"


class TestVerdictComputation:
    def test_too_early(self):
        v = TreatmentTracker._compute_verdict(
            pct_change=-10.0, expected_direction="decrease",
            expected_pct=-30.0, weeks_elapsed=3, typical_weeks=6,
        )
        assert v == "too_early"

    def test_effective_decrease(self):
        v = TreatmentTracker._compute_verdict(
            pct_change=-25.0, expected_direction="decrease",
            expected_pct=-30.0, weeks_elapsed=8, typical_weeks=6,
        )
        assert v == "effective"

    def test_very_effective(self):
        v = TreatmentTracker._compute_verdict(
            pct_change=-45.0, expected_direction="decrease",
            expected_pct=-30.0, weeks_elapsed=8, typical_weeks=6,
        )
        assert v == "very_effective"

    def test_insufficient(self):
        v = TreatmentTracker._compute_verdict(
            pct_change=-5.0, expected_direction="decrease",
            expected_pct=-30.0, weeks_elapsed=8, typical_weeks=6,
        )
        assert v == "insufficient"

    def test_worsening_decrease(self):
        v = TreatmentTracker._compute_verdict(
            pct_change=15.0, expected_direction="decrease",
            expected_pct=-30.0, weeks_elapsed=8, typical_weeks=6,
        )
        assert v == "worsening"

    def test_effective_increase(self):
        v = TreatmentTracker._compute_verdict(
            pct_change=60.0, expected_direction="increase",
            expected_pct=50.0, weeks_elapsed=10, typical_weeks=8,
        )
        assert v == "effective"

    def test_worsening_increase(self):
        v = TreatmentTracker._compute_verdict(
            pct_change=-10.0, expected_direction="increase",
            expected_pct=50.0, weeks_elapsed=10, typical_weeks=8,
        )
        assert v == "worsening"

    def test_early_but_already_effective(self):
        v = TreatmentTracker._compute_verdict(
            pct_change=-35.0, expected_direction="decrease",
            expected_pct=-30.0, weeks_elapsed=3, typical_weeks=6,
        )
        assert v == "effective"


class TestAssessAll:
    def test_statin_working(self):
        start = date.today() - timedelta(weeks=10)
        bl = (start - timedelta(days=5)).isoformat()
        cur = date.today().isoformat()

        meds = [{
            "name": "atorvastatin 40mg",
            "start_date": start.isoformat(),
            "status": "active",
        }]
        obs = [_obs("ldl", 160.0, bl), _obs("ldl", 105.0, cur)]
        db = _make_db(meds, obs)
        tracker = TreatmentTracker(db)
        reports = tracker.assess_all(user_id=1)

        ldl = [r for r in reports if r.biomarker == "ldl"]
        assert len(ldl) == 1
        assert ldl[0].verdict in ("effective", "very_effective")
        assert ldl[0].pct_change < 0

    def test_no_baseline(self):
        start = date.today() - timedelta(weeks=10)
        cur = date.today().isoformat()
        meds = [{"name": "atorvastatin", "start_date": start.isoformat()}]
        obs = [_obs("ldl", 105.0, cur)]
        db = _make_db(meds, obs)
        tracker = TreatmentTracker(db)
        reports = tracker.assess_all(user_id=1)
        ldl = [r for r in reports if r.biomarker == "ldl"]
        assert len(ldl) == 0

    def test_unknown_drug_skipped(self):
        meds = [{
            "name": "randomdrug",
            "start_date": date.today().isoformat(),
        }]
        db = _make_db(meds, [])
        tracker = TreatmentTracker(db)
        assert tracker.assess_all(user_id=1) == []

    def test_no_start_date_skipped(self):
        meds = [{"name": "atorvastatin", "start_date": ""}]
        db = _make_db(meds, [])
        tracker = TreatmentTracker(db)
        assert tracker.assess_all(user_id=1) == []

    def test_vitamin_d_supplement(self):
        start = date.today() - timedelta(weeks=12)
        bl = (start - timedelta(days=3)).isoformat()
        cur = date.today().isoformat()

        meds = [{
            "name": "vitamin d 5000iu",
            "start_date": start.isoformat(),
        }]
        obs = [_obs("vitamin_d", 18.0, bl), _obs("vitamin_d", 42.0, cur)]
        db = _make_db(meds, obs)
        tracker = TreatmentTracker(db)
        reports = tracker.assess_all(user_id=1)

        vd = [r for r in reports if r.biomarker == "vitamin_d"]
        assert len(vd) == 1
        assert vd[0].verdict in ("effective", "very_effective")

    def test_too_early_assessment(self):
        start = date.today() - timedelta(weeks=2)
        bl = (start - timedelta(days=1)).isoformat()
        cur = date.today().isoformat()

        meds = [{"name": "metformin", "start_date": start.isoformat()}]
        obs = [_obs("hba1c", 7.2, bl), _obs("hba1c", 7.0, cur)]
        db = _make_db(meds, obs)
        tracker = TreatmentTracker(db)
        reports = tracker.assess_all(user_id=1)

        hba1c = [r for r in reports if r.biomarker == "hba1c"]
        assert len(hba1c) == 1
        assert hba1c[0].verdict == "too_early"


class TestFormatEffectiveness:
    def test_empty_reports(self):
        result = format_effectiveness([])
        assert "No trackable medications" in result

    def test_with_reports(self):
        reports = [
            EffectivenessReport(
                med_name="atorvastatin 40mg",
                drug_key="statin",
                biomarker="ldl",
                start_date="2025-06-01",
                baseline_value=160.0,
                baseline_date="2025-05-28",
                current_value=105.0,
                current_date="2025-09-15",
                pct_change=-34.4,
                expected_pct=-30.0,
                weeks_elapsed=15,
                typical_weeks=6,
                verdict="very_effective",
                citation="Weng TC et al. 2010.",
            ),
        ]
        result = format_effectiveness(reports)
        assert "TREATMENT EFFECTIVENESS" in result
        assert "atorvastatin" in result
        assert "Ldl" in result
        assert "Exceeding expectations" in result
        assert "-34.4%" in result
        assert "Weng TC" in result


class TestKBCoverage:
    def test_all_links_have_citations(self):
        for link in DRUG_BIOMARKER_LINKS:
            assert link.citation, (
                f"{link.drug_key} -> {link.target_biomarker} missing"
            )

    def test_all_links_have_valid_direction(self):
        for link in DRUG_BIOMARKER_LINKS:
            assert link.expected_direction in ("increase", "decrease")

    def test_all_links_have_positive_weeks(self):
        for link in DRUG_BIOMARKER_LINKS:
            assert link.typical_weeks > 0
