"""Tests for lab name normalization."""
from __future__ import annotations

import pytest

from healthbot.normalize.lab_normalizer import (
    LOINC_MAP,
    TEST_NAME_MAP,
    get_loinc,
    normalize_test_name,
)


class TestNormalizeTestName:
    @pytest.mark.parametrize("input_name,expected", [
        ("glucose", "glucose"),
        ("Glucose", "glucose"),
        ("GLUCOSE", "glucose"),
        ("glu", "glucose"),
        ("blood sugar", "glucose"),
        ("fasting glucose", "glucose"),
    ])
    def test_glucose_variations(self, input_name, expected):
        assert normalize_test_name(input_name) == expected

    @pytest.mark.parametrize("input_name,expected", [
        ("hdl", "hdl"),
        ("HDL Cholesterol", "hdl"),
        ("ldl", "ldl"),
        ("LDL Calculated", "ldl"),
        ("triglycerides", "triglycerides"),
        ("trig", "triglycerides"),
    ])
    def test_lipid_panel(self, input_name, expected):
        assert normalize_test_name(input_name) == expected

    @pytest.mark.parametrize("input_name,expected", [
        ("hemoglobin a1c", "hba1c"),
        ("HbA1c", "hba1c"),
        ("a1c", "hba1c"),
        ("glycated hemoglobin", "hba1c"),
    ])
    def test_a1c_variations(self, input_name, expected):
        assert normalize_test_name(input_name) == expected

    def test_strips_trailing_punctuation(self):
        assert normalize_test_name("glucose.") == "glucose"
        assert normalize_test_name("glucose,") == "glucose"
        assert normalize_test_name("glucose-") == "glucose"

    def test_collapses_whitespace(self):
        assert normalize_test_name("blood  sugar") == "glucose"
        assert normalize_test_name("  glucose  ") == "glucose"

    def test_unknown_returns_cleaned(self):
        assert normalize_test_name("xylophone test") == "xylophone test"

    def test_sodium_aliases(self):
        assert normalize_test_name("Na") == "sodium"
        assert normalize_test_name("na+") == "sodium"

    def test_potassium_aliases(self):
        assert normalize_test_name("K") == "potassium"
        assert normalize_test_name("k+") == "potassium"


class TestGetLoinc:
    def test_known_test(self):
        assert get_loinc("glucose") == "2345-7"

    def test_unknown_test(self):
        assert get_loinc("xylophone") is None

    def test_all_loinc_entries_have_canonical_names(self):
        """Every LOINC entry should correspond to a canonical name in TEST_NAME_MAP values."""
        canonical_names = set(TEST_NAME_MAP.values())
        for name in LOINC_MAP:
            assert name in canonical_names, f"LOINC entry '{name}' not in canonical names"
