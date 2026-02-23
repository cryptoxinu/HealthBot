"""Tests for the hypothesis generator."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from healthbot.reasoning.hypothesis_generator import HypothesisGenerator


class TestHypothesisGenerator:
    """Pattern-based hypothesis generation from lab results."""

    def _make_generator(self) -> HypothesisGenerator:
        db = MagicMock()
        return HypothesisGenerator(db)

    def test_iron_deficiency_pattern(self):
        gen = self._make_generator()
        # ferritin low (<20) and hemoglobin low (<13.5)
        latest = {"ferritin": 10.0, "hemoglobin": 11.0}
        with patch.object(gen, "_get_latest_values", return_value=latest):
            results = gen.scan_all(user_id=1)
        titles = [h.title for h in results]
        assert any("Iron deficiency" in t for t in titles)

    def test_hypothyroidism_pattern(self):
        gen = self._make_generator()
        # tsh high (>4.0)
        latest = {"tsh": 8.0}
        with patch.object(gen, "_get_latest_values", return_value=latest):
            results = gen.scan_all(user_id=1)
        titles = [h.title for h in results]
        assert any("Hypothyroidism" in t for t in titles)

    def test_optional_boosts_confidence(self):
        gen = self._make_generator()
        # Iron deficiency triggers only
        base_latest = {"ferritin": 10.0, "hemoglobin": 11.0}
        # Iron deficiency triggers + optional mcv low (<80)
        boosted_latest = {"ferritin": 10.0, "hemoglobin": 11.0, "mcv": 75.0}

        with patch.object(gen, "_get_latest_values", return_value=base_latest):
            base_results = gen.scan_all(user_id=1)
        with patch.object(gen, "_get_latest_values", return_value=boosted_latest):
            boosted_results = gen.scan_all(user_id=1)

        base_hyp = next(h for h in base_results if "Iron" in h.title)
        boosted_hyp = next(h for h in boosted_results if "Iron" in h.title)
        assert boosted_hyp.confidence > base_hyp.confidence

    def test_confidence_capped_at_095(self):
        gen = self._make_generator()
        # Iron deficiency with ALL optional tests abnormal
        latest = {
            "ferritin": 10.0, "hemoglobin": 11.0,
            "mcv": 75.0, "rdw": 16.0,
            "iron": 30.0, "tibc": 500.0,
            "transferrin_saturation": 10.0,
        }
        with patch.object(gen, "_get_latest_values", return_value=latest):
            results = gen.scan_all(user_id=1)
        hyp = next(h for h in results if "Iron" in h.title)
        assert hyp.confidence <= 0.95

    def test_missing_trigger_skips(self):
        gen = self._make_generator()
        # Only ferritin low but hemoglobin normal -> no iron deficiency
        latest = {"ferritin": 10.0, "hemoglobin": 15.0}
        with patch.object(gen, "_get_latest_values", return_value=latest):
            results = gen.scan_all(user_id=1)
        titles = [h.title for h in results]
        assert not any("Iron deficiency anemia" in t for t in titles)

    def test_no_data_returns_empty(self):
        gen = self._make_generator()
        with patch.object(gen, "_get_latest_values", return_value={}):
            results = gen.scan_all(user_id=1)
        assert results == []

    def test_sex_filter_pcos_female(self):
        gen = self._make_generator()
        # Female testosterone_total range: 15-70 ng/dL, so 90 is "high"
        latest = {"testosterone_total": 90.0}
        with patch.object(gen, "_get_latest_values", return_value=latest):
            results = gen.scan_all(user_id=1, sex="female")
        titles = [h.title for h in results]
        assert any("PCOS" in t for t in titles)

    def test_sex_filter_excludes_pcos_for_male(self):
        gen = self._make_generator()
        latest = {"testosterone_total": 1100.0}  # High for male range (>1070)
        with patch.object(gen, "_get_latest_values", return_value=latest):
            results = gen.scan_all(user_id=1, sex="male")
        titles = [h.title for h in results]
        assert not any("PCOS" in t for t in titles)
