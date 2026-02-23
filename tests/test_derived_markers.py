"""Tests for derived markers engine."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.derived_markers import DerivedMarkerEngine


def _make_db(labs: dict[str, float] | None = None):
    """Create a mock DB returning specified lab values."""
    db = MagicMock()
    observations = []
    for name, value in (labs or {}).items():
        observations.append({
            "canonical_name": name,
            "value": value,
            "unit": "",
            "test_name": name.upper(),
            "_meta": {"date_effective": "2024-06-15"},
        })
    db.query_observations.return_value = observations
    db.get_user_demographics.return_value = {}
    return db


class TestDerivedMarkers:

    def test_no_labs_returns_empty(self):
        db = _make_db()
        engine = DerivedMarkerEngine(db)
        report = engine.compute_all(user_id=0)
        assert report.markers == []

    def test_homa_ir_computes(self):
        db = _make_db({"glucose": 95, "insulin": 8})
        engine = DerivedMarkerEngine(db)
        report = engine.compute_all(user_id=0)
        homa = next((m for m in report.markers if m.name == "HOMA-IR"), None)
        assert homa is not None
        assert abs(homa.value - (95 * 8 / 405.0)) < 0.01

    def test_tg_hdl_ratio(self):
        db = _make_db({"triglycerides": 150, "hdl": 50})
        engine = DerivedMarkerEngine(db)
        report = engine.compute_all(user_id=0)
        ratio = next((m for m in report.markers if m.name == "TG/HDL Ratio"), None)
        assert ratio is not None
        assert ratio.value == 3.0

    def test_anion_gap(self):
        db = _make_db({"sodium": 140, "chloride": 104, "carbon_dioxide": 24})
        engine = DerivedMarkerEngine(db)
        report = engine.compute_all(user_id=0)
        gap = next((m for m in report.markers if m.name == "Anion Gap"), None)
        assert gap is not None
        assert gap.value == 12.0

    def test_non_hdl_cholesterol(self):
        db = _make_db({"cholesterol_total": 220, "hdl": 55})
        engine = DerivedMarkerEngine(db)
        report = engine.compute_all(user_id=0)
        non_hdl = next((m for m in report.markers if m.name == "Non-HDL Cholesterol"), None)
        assert non_hdl is not None
        assert non_hdl.value == 165.0

    def test_missing_components_reported(self):
        db = _make_db({"glucose": 95})  # insulin missing -> HOMA-IR can't compute
        engine = DerivedMarkerEngine(db)
        report = engine.compute_all(user_id=0)
        assert "homa_ir" in report.missing
