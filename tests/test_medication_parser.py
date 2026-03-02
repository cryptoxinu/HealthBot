"""Tests for healthbot.nlu.medication_parser — smart medication parsing."""
from __future__ import annotations

from healthbot.nlu.medication_parser import parse_medication


class TestBasicParsing:
    """Basic medication name + dose extraction."""

    def test_simple_medication(self) -> None:
        r = parse_medication("metformin 500mg")
        assert r.name.lower() == "metformin"
        assert r.prescribed_dose == "500mg"
        assert r.actual_dose == "500mg"
        assert r.actual_dose_mg == 500.0
        assert r.modifier == ""

    def test_medication_with_frequency(self) -> None:
        r = parse_medication("metformin 500mg twice daily")
        assert r.name.lower() == "metformin"
        assert r.prescribed_dose == "500mg"
        assert "twice" in r.frequency.lower() or "2x" in r.frequency.lower()

    def test_medication_with_prefix(self) -> None:
        r = parse_medication("I take lisinopril 10mg daily")
        assert r.name.lower() == "lisinopril"
        assert r.prescribed_dose == "10mg"

    def test_no_dose(self) -> None:
        r = parse_medication("aspirin daily")
        assert r.name.lower() == "aspirin"
        assert r.prescribed_dose == ""
        assert r.actual_dose_mg is None


class TestDoseModifiers:
    """Dose modifier detection and calculation."""

    def test_break_in_half(self) -> None:
        r = parse_medication("lisinopril 10mg but I break it in half")
        assert r.name.lower() == "lisinopril"
        assert r.prescribed_dose == "10mg"
        assert r.modifier == "half"
        assert r.actual_dose_mg == 5.0
        assert r.actual_dose == "5mg"

    def test_cut_in_half(self) -> None:
        r = parse_medication("metformin 500mg cut in half")
        assert r.modifier == "half"
        assert r.actual_dose_mg == 250.0
        assert r.actual_dose == "250mg"

    def test_split_in_half(self) -> None:
        r = parse_medication("atorvastatin 20mg split in half")
        assert r.modifier == "half"
        assert r.actual_dose_mg == 10.0

    def test_half_a_pill(self) -> None:
        r = parse_medication("half a pill of metoprolol 50mg")
        assert r.modifier == "half"
        assert r.actual_dose_mg == 25.0

    def test_quarter(self) -> None:
        r = parse_medication("I cut my 100mg pill into quarters")
        assert r.modifier == "quarter"
        assert r.actual_dose_mg == 25.0

    def test_double_dose(self) -> None:
        r = parse_medication("take two pills of ibuprofen 200mg")
        assert r.modifier == "double"
        assert r.actual_dose_mg == 400.0
        assert r.actual_dose == "400mg"

    def test_no_modifier(self) -> None:
        r = parse_medication("metformin 500mg twice daily")
        assert r.modifier == ""
        assert r.actual_dose_mg == 500.0


class TestEdgeCases:
    """Edge cases and robustness."""

    def test_mcg_unit(self) -> None:
        r = parse_medication("levothyroxine 50mcg daily")
        assert r.prescribed_dose == "50mcg"
        # mcg is converted to mg: 50mcg / 1000 = 0.05mg
        assert r.actual_dose_mg == 0.05

    def test_decimal_dose(self) -> None:
        r = parse_medication("warfarin 2.5mg daily")
        assert r.actual_dose_mg == 2.5

    def test_raw_text_preserved(self) -> None:
        text = "lisinopril 10mg but I break it in half"
        r = parse_medication(text)
        assert r.raw_text == text

    def test_empty_name_fallback(self) -> None:
        r = parse_medication("500mg daily")
        # Should still return something
        assert r.prescribed_dose == "500mg"
