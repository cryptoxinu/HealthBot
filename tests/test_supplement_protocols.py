"""Tests for supplement dosing protocols."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.supplement_protocols import (
    SUPPLEMENT_PROTOCOLS,
    SupplementAdvisor,
    format_recommendations,
)


def _make_db(
    observations: list[dict] | None = None,
    meds: list[dict] | None = None,
) -> MagicMock:
    db = MagicMock()
    observations = observations or []
    meds = meds or []
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
            results.append(obs)
        results.sort(
            key=lambda x: x.get("date_collected", ""), reverse=True,
        )
        return results[:limit]

    db.query_observations.side_effect = query_obs
    return db


def _obs(name: str, value: float, dt: str = "2024-06-01") -> dict:
    return {
        "canonical_name": name,
        "value": value,
        "date_collected": dt,
    }


class TestProtocolKB:
    def test_all_protocols_have_citations(self):
        for p in SUPPLEMENT_PROTOCOLS:
            assert p.citation, f"{p.deficiency_marker} missing citation"

    def test_all_thresholds_valid(self):
        for p in SUPPLEMENT_PROTOCOLS:
            assert p.threshold_deficient < p.threshold_insufficient
            assert p.threshold_deficient > 0

    def test_all_have_loading_and_maintenance(self):
        for p in SUPPLEMENT_PROTOCOLS:
            assert p.loading_dose
            assert p.maintenance_dose
            assert p.loading_weeks > 0
            assert p.retest_weeks > 0

    def test_six_protocols_exist(self):
        markers = {p.deficiency_marker for p in SUPPLEMENT_PROTOCOLS}
        assert "vitamin_d" in markers
        assert "vitamin_b12" in markers
        assert "ferritin" in markers
        assert "folate" in markers
        assert "magnesium" in markers
        assert "zinc" in markers


class TestDeficiencyDetection:
    def test_vitamin_d_deficient(self):
        obs = [_obs("vitamin_d", 12.0)]
        db = _make_db(obs)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        vd = [r for r in recs if r.protocol.deficiency_marker == "vitamin_d"]
        assert len(vd) == 1
        assert vd[0].severity == "deficient"
        assert "5,000 IU" in vd[0].recommended_dose

    def test_vitamin_d_insufficient(self):
        obs = [_obs("vitamin_d", 25.0)]
        db = _make_db(obs)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        vd = [r for r in recs if r.protocol.deficiency_marker == "vitamin_d"]
        assert len(vd) == 1
        assert vd[0].severity == "insufficient"
        assert "1,000-2,000 IU" in vd[0].recommended_dose

    def test_vitamin_d_sufficient_no_rec(self):
        obs = [_obs("vitamin_d", 45.0)]
        db = _make_db(obs)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        vd = [r for r in recs if r.protocol.deficiency_marker == "vitamin_d"]
        assert len(vd) == 0

    def test_b12_deficient(self):
        obs = [_obs("vitamin_b12", 150.0)]
        db = _make_db(obs)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        b12 = [r for r in recs if r.protocol.deficiency_marker == "vitamin_b12"]
        assert len(b12) == 1
        assert b12[0].severity == "deficient"

    def test_ferritin_deficient(self):
        obs = [_obs("ferritin", 8.0)]
        db = _make_db(obs)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        fe = [r for r in recs if r.protocol.deficiency_marker == "ferritin"]
        assert len(fe) == 1
        assert fe[0].severity == "deficient"
        assert "every other day" in fe[0].recommended_dose.lower()

    def test_no_data_no_rec(self):
        db = _make_db([])
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)
        assert recs == []


class TestInteractionWarnings:
    def test_metformin_b12_warning(self):
        obs = [_obs("vitamin_b12", 180.0)]
        meds = [{"name": "metformin 1000mg"}]
        db = _make_db(obs, meds)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        b12 = [r for r in recs if r.protocol.deficiency_marker == "vitamin_b12"]
        assert len(b12) == 1
        assert len(b12[0].warnings) >= 1
        assert any("metformin" in w.lower() for w in b12[0].warnings)

    def test_iron_levothyroxine_warning(self):
        obs = [_obs("ferritin", 10.0)]
        meds = [{"name": "levothyroxine 100mcg"}]
        db = _make_db(obs, meds)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        fe = [r for r in recs if r.protocol.deficiency_marker == "ferritin"]
        assert len(fe) == 1
        assert any("levothyroxine" in w.lower() for w in fe[0].warnings)

    def test_no_interaction_no_warning(self):
        obs = [_obs("vitamin_d", 15.0)]
        meds = [{"name": "atorvastatin 40mg"}]
        db = _make_db(obs, meds)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        vd = [r for r in recs if r.protocol.deficiency_marker == "vitamin_d"]
        assert len(vd) == 1
        assert len(vd[0].warnings) == 0


class TestAlreadySupplementing:
    def test_already_taking_vitamin_d_skip(self):
        obs = [_obs("vitamin_d", 18.0)]
        meds = [{"name": "vitamin D3 5000IU"}]
        db = _make_db(obs, meds)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        vd = [r for r in recs if r.protocol.deficiency_marker == "vitamin_d"]
        assert len(vd) == 0

    def test_already_taking_iron_skip(self):
        obs = [_obs("ferritin", 10.0)]
        meds = [{"name": "ferrous sulfate 325mg"}]
        db = _make_db(obs, meds)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        fe = [r for r in recs if r.protocol.deficiency_marker == "ferritin"]
        assert len(fe) == 0

    def test_not_supplementing_shows_rec(self):
        obs = [_obs("ferritin", 10.0)]
        meds = [{"name": "atorvastatin 40mg"}]
        db = _make_db(obs, meds)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        fe = [r for r in recs if r.protocol.deficiency_marker == "ferritin"]
        assert len(fe) == 1


class TestMultipleDeficiencies:
    def test_multiple_deficiencies_sorted(self):
        obs = [
            _obs("vitamin_d", 15.0),       # deficient
            _obs("vitamin_b12", 350.0),     # insufficient
            _obs("ferritin", 10.0),         # deficient
        ]
        db = _make_db(obs)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        assert len(recs) == 3
        # Deficient should come first
        deficient = [r for r in recs if r.severity == "deficient"]
        insufficient = [r for r in recs if r.severity == "insufficient"]
        assert len(deficient) == 2
        assert len(insufficient) == 1
        # Deficient items should be before insufficient
        assert recs[0].severity == "deficient"
        assert recs[1].severity == "deficient"
        assert recs[2].severity == "insufficient"


class TestFormatting:
    def test_format_empty(self):
        result = format_recommendations([])
        assert "No supplement" in result

    def test_format_with_rec(self):
        obs = [_obs("vitamin_d", 12.0)]
        db = _make_db(obs)
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=1)

        result = format_recommendations(recs)
        assert "SUPPLEMENT RECOMMENDATIONS" in result
        assert "Vitamin D" in result
        assert "DEFICIENT" in result
        assert "5,000 IU" in result
        assert "Holick" in result
