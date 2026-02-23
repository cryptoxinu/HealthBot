"""Tests for reference ranges and unit conversion."""
from __future__ import annotations

import pytest

from healthbot.reasoning.reference_ranges import (
    DEFAULT_RANGES,
    FASTING_TESTS,
    convert_unit,
    get_default_range,
    get_range,
)


class TestGetDefaultRange:
    def test_known_test(self):
        r = get_default_range("glucose")
        assert r is not None
        assert r["low"] == 70.0
        assert r["high"] == 100.0
        assert r["unit"] == "mg/dL"

    def test_unknown_test(self):
        assert get_default_range("unknown_test_xyz") is None

    def test_all_ranges_have_required_keys(self):
        for name, r in DEFAULT_RANGES.items():
            assert "low" in r, f"{name} missing 'low'"
            assert "high" in r, f"{name} missing 'high'"
            assert "unit" in r, f"{name} missing 'unit'"

    def test_ranges_low_less_than_high(self):
        for name, r in DEFAULT_RANGES.items():
            assert r["low"] <= r["high"], f"{name}: low > high"

    @pytest.mark.parametrize("test_name", [
        "glucose", "sodium", "potassium", "creatinine",
        "cholesterol_total", "hdl", "ldl", "triglycerides",
        "wbc", "hemoglobin", "platelets", "hba1c", "tsh",
    ])
    def test_common_tests_present(self, test_name):
        assert get_default_range(test_name) is not None


class TestConvertUnit:
    def test_same_unit_passthrough(self):
        assert convert_unit(100.0, "mg/dL", "mg/dL") == 100.0

    def test_same_unit_case_insensitive(self):
        assert convert_unit(100.0, "MG/DL", "mg/dl") == 100.0

    def test_mgdl_to_mmoll(self):
        result = convert_unit(100.0, "mg/dL", "mmol/L")
        assert result == pytest.approx(5.55, abs=0.1)

    def test_mmoll_to_mgdl(self):
        result = convert_unit(5.55, "mmol/L", "mg/dL")
        assert result == pytest.approx(100.0, abs=1.0)

    def test_unknown_conversion(self):
        assert convert_unit(100.0, "gallons", "liters") is None

    def test_strips_whitespace(self):
        result = convert_unit(100.0, " mg/dL ", " mmol/L ")
        assert result is not None


class TestGetRange:
    """Test age/sex-adjusted reference ranges."""

    def test_unknown_test_returns_none(self):
        assert get_range("unknown_test_xyz") is None

    def test_no_demographics_returns_default(self):
        r = get_range("glucose")
        assert r is not None
        assert r["low"] == 70.0
        assert r["high"] == 100.0

    def test_male_hemoglobin_uses_default(self):
        r = get_range("hemoglobin", sex="male")
        assert r["low"] == 13.5
        assert r["high"] == 17.5

    def test_female_hemoglobin_override(self):
        r = get_range("hemoglobin", sex="female")
        assert r["low"] == 12.0
        assert r["high"] == 16.0

    def test_female_ferritin_override(self):
        r = get_range("ferritin", sex="female")
        assert r["high"] == 150.0

    def test_female_uric_acid_override(self):
        r = get_range("uric_acid", sex="female")
        assert r["high"] == 6.0

    def test_child_creatinine(self):
        r = get_range("creatinine", age=12)
        assert r["low"] == 0.2  # age 0-12 bracket
        assert r["high"] == 0.5

    def test_child_alkaline_phosphatase(self):
        r = get_range("alkaline_phosphatase", age=15)
        assert r["high"] == 500  # age 13-17 bracket (adolescent bone growth)

    def test_child_bun(self):
        r = get_range("bun", age=10)
        assert r["high"] == 18  # age 0-12 bracket

    def test_elderly_creatinine(self):
        r = get_range("creatinine", age=70)
        assert r["high"] == 1.5

    def test_elderly_psa(self):
        r = get_range("psa", age=68)
        assert r["high"] == 4.5  # age 60-69 bracket

    def test_elderly_psa_over_70(self):
        r = get_range("psa", age=72)
        assert r["high"] == 6.5  # age 70+ bracket

    def test_elderly_male_esr(self):
        r = get_range("esr", sex="male", age=70)
        assert r["high"] == 30  # age 51-70 bracket

    def test_elderly_female_esr(self):
        r = get_range("esr", sex="female", age=70)
        assert r["high"] == 40  # female age 51-70 bracket

    def test_age_overrides_sex(self):
        """Child override should take priority over female override."""
        r = get_range("hemoglobin", sex="female", age=12)
        assert r["low"] == 11.5  # child age-stratified, not female 12.0

    def test_adult_female_not_child(self):
        """Adult female should get female override, not child."""
        r = get_range("hemoglobin", sex="female", age=30)
        assert r["low"] == 12.0

    def test_non_overridden_test_unaffected(self):
        """Tests without overrides should return default regardless of demographics."""
        r = get_range("glucose", sex="female", age=12)
        assert r["low"] == 70.0
        assert r["high"] == 100.0


class TestFastingTests:
    def test_glucose_requires_fasting(self):
        assert "glucose" in FASTING_TESTS

    def test_triglycerides_requires_fasting(self):
        assert "triglycerides" in FASTING_TESTS

    def test_hemoglobin_no_fasting(self):
        assert "hemoglobin" not in FASTING_TESTS
