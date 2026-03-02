"""Pytest-runnable eval checks — deterministic, no LLM.

Tests lab unit conversion (reference_ranges), privacy detection (phi_firewall),
and the eval runner itself.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from healthbot.reasoning.reference_ranges import convert_unit
from healthbot.security.phi_firewall import PhiFirewall

EVAL_DIR = Path(__file__).parent.parent / "eval"


# ---------------------------------------------------------------------------
# Unit Normalization — Lab Conversions
# ---------------------------------------------------------------------------


class TestLabUnitConversion:
    """reference_ranges unit conversions."""

    @pytest.mark.parametrize(
        "value,from_unit,to_unit,expected",
        [
            (5.5, "mmol/L", "mg/dL", 99.1),
            (100.0, "mg/dL", "mmol/L", 5.55),
            (88.4, "umol/L", "mg/dL", 1.0),
        ],
    )
    def test_lab_conversion(self, value: float, from_unit: str, to_unit: str, expected: float):
        result = convert_unit(value, from_unit, to_unit)
        assert result is not None, f"No conversion for {from_unit} -> {to_unit}"
        assert abs(result - expected) < 0.1, f"Got {result}, expected {expected}"


# ---------------------------------------------------------------------------
# Privacy — PHI Detection
# ---------------------------------------------------------------------------


class TestPhiDetection:
    """PHI firewall pattern detection via eval cases."""

    @pytest.fixture
    def fw(self):
        return PhiFirewall()

    def test_ssn_detected(self, fw):
        assert fw.contains_phi("My SSN is 123-45-6789")
        assert fw.contains_phi("SSN: 078-05-1120")

    def test_mrn_detected(self, fw):
        assert fw.contains_phi("MRN: 12345678")
        assert fw.contains_phi("Medical Record #987654321")

    def test_phone_detected(self, fw):
        assert fw.contains_phi("Call me at (555) 123-4567")
        assert fw.contains_phi("Phone: 555-123-4567")

    def test_email_detected(self, fw):
        assert fw.contains_phi("Email me at john.doe@hospital.com")

    def test_dob_detected(self, fw):
        assert fw.contains_phi("DOB: 01/15/1990")

    def test_name_labeled_detected(self, fw):
        assert fw.contains_phi("Patient: John Smith")
        assert fw.contains_phi("Patient Name: Jane Doe")

    def test_name_intro_detected(self, fw):
        assert fw.contains_phi("My name is Sarah Johnson")

    def test_clean_lab_data_not_flagged(self, fw):
        assert not fw.contains_phi("glucose 95 mg/dL normal range 70-100")
        assert not fw.contains_phi("LDL cholesterol trending up 15% over 6 months")
        assert not fw.contains_phi("hemoglobin A1C 6.2% prediabetic range")

    def test_multiple_phi_types(self, fw):
        text = "Patient: John Smith, DOB: 01/15/1990, SSN: 123-45-6789, MRN: 12345678"
        matches = fw.scan(text)
        categories = {m.category for m in matches}
        assert "ssn" in categories
        assert "mrn" in categories
        assert "name_labeled" in categories


# ---------------------------------------------------------------------------
# Eval Runner Smoke Test
# ---------------------------------------------------------------------------


class TestEvalRunner:
    """Verify the eval runner loads and runs cases."""

    def test_load_golden_cases(self):
        from eval.runner import EvalRunner
        runner = EvalRunner()
        cases = runner.load_cases(EVAL_DIR / "golden_cases.jsonl")
        assert len(cases) >= 15

    def test_load_privacy_cases(self):
        from eval.runner import EvalRunner
        runner = EvalRunner()
        cases = runner.load_cases(EVAL_DIR / "privacy_cases.jsonl")
        assert len(cases) >= 15

    def test_load_unit_cases(self):
        from eval.runner import EvalRunner
        runner = EvalRunner()
        cases = runner.load_cases(EVAL_DIR / "unit_cases.jsonl")
        assert len(cases) >= 15

    def test_run_contains_check(self):
        from eval.runner import EvalCase, EvalRunner
        runner = EvalRunner()
        case = EvalCase(
            id="test1", category="test", input="hello world",
            check_type="contains", expected="hello",
        )
        report = runner.run([case])
        assert report.passed == 1

    def test_run_not_contains_check(self):
        from eval.runner import EvalCase, EvalRunner
        runner = EvalRunner()
        case = EvalCase(
            id="test2", category="test", input="hello world",
            check_type="not_contains", expected="goodbye",
        )
        report = runner.run([case])
        assert report.passed == 1

    def test_run_equals_check(self):
        from eval.runner import EvalCase, EvalRunner
        runner = EvalRunner()
        case = EvalCase(
            id="test3", category="test", input="42",
            check_type="equals", expected="42",
        )
        report = runner.run([case])
        assert report.passed == 1

    def test_format_report(self):
        from eval.runner import EvalReport, EvalRunner
        runner = EvalRunner()
        report = EvalReport(total=10, passed=8, failed=2)
        text = runner.format_report(report)
        assert "80.0%" in text
        assert "EVAL REPORT" in text
