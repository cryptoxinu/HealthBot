"""Tests for PHI firewall detection and redaction."""
from __future__ import annotations

import pytest

from healthbot.security.phi_firewall import PhiDetectedError, PhiFirewall


@pytest.fixture
def fw() -> PhiFirewall:
    return PhiFirewall()


class TestPhiDetection:
    """Test PHI pattern detection."""

    def test_detects_ssn(self, fw: PhiFirewall) -> None:
        assert fw.contains_phi("SSN: 123-45-6789")

    def test_detects_mrn(self, fw: PhiFirewall) -> None:
        assert fw.contains_phi("MRN: 123456789")
        assert fw.contains_phi("Medical Record #1234567")
        assert fw.contains_phi("MR#12345678")

    def test_detects_phone(self, fw: PhiFirewall) -> None:
        assert fw.contains_phi("Call 555-123-4567")
        assert fw.contains_phi("(555) 123-4567")
        assert fw.contains_phi("+1-555-123-4567")

    def test_detects_email(self, fw: PhiFirewall) -> None:
        assert fw.contains_phi("Email: john@example.com")

    def test_detects_dob_slash(self, fw: PhiFirewall) -> None:
        assert fw.contains_phi("DOB: 01/15/1985")
        assert fw.contains_phi("Date of Birth: 12/31/1990")

    def test_detects_dob_slash_single_digit(self, fw: PhiFirewall) -> None:
        """Single-digit month/day DOBs without leading zeros."""
        assert fw.contains_phi("born on 1/5/1990")
        assert fw.contains_phi("date was 3/15/1985")
        assert fw.contains_phi("seen on 9/1/2001")

    def test_detects_labeled_name(self, fw: PhiFirewall) -> None:
        assert fw.contains_phi("Patient: John Smith")
        assert fw.contains_phi("Name: Jane A. Doe")
        assert fw.contains_phi("Patient Name: Robert Johnson")

    def test_detects_allcaps_patient_name(self, fw: PhiFirewall) -> None:
        """Lab PDFs often use ALL-CAPS: SMITH, JOHN A."""
        assert fw.contains_phi("Patient: SMITH, JOHN")
        assert fw.contains_phi("Patient: JOHN SMITH")
        assert fw.contains_phi("Pt: WILSON, SARAH")

    def test_detects_allcaps_provider(self, fw: PhiFirewall) -> None:
        """Lab PDFs: Ordering Provider: WILSON, SARAH MD."""
        assert fw.contains_phi("Ordering Provider: WILSON, SARAH MD")
        assert fw.contains_phi("Provider: SMITH, JOHN")
        assert fw.contains_phi("Physician: DR JONES")

    def test_detects_allcaps_doctor(self, fw: PhiFirewall) -> None:
        assert fw.contains_phi("DR. SMITH ordered labs")
        assert fw.contains_phi("DR WILSON JONES")

    def test_detects_account_id(self, fw: PhiFirewall) -> None:
        assert fw.contains_phi("Account #: 12345678")
        assert fw.contains_phi("Patient ID: ABC12345")
        assert fw.contains_phi("Encounter: 98765432")

    def test_detects_address(self, fw: PhiFirewall) -> None:
        assert fw.contains_phi("123 Main St")
        assert fw.contains_phi("456 Oak Ave")

    def test_no_false_positive_on_clean_text(self, fw: PhiFirewall) -> None:
        clean_texts = [
            "Hemoglobin A1c is elevated at 7.2%",
            "LDL cholesterol trending upward",
            "Recommend follow-up in 3 months",
            "Take metformin twice daily",
            "WBC 5.0 K/uL",
            "RBC 4.50 M/uL",
            "HDL cholesterol 55 mg/dL",
            "MCHC 33.5 g/dL",
        ]
        for text in clean_texts:
            assert not fw.contains_phi(text), f"False positive on: {text}"

    def test_scan_returns_all_matches(self, fw: PhiFirewall) -> None:
        text = "Patient: John Smith, SSN: 123-45-6789, Email: john@test.com"
        matches = fw.scan(text)
        categories = {m.category for m in matches}
        assert "name_labeled" in categories
        assert "ssn" in categories
        assert "email" in categories


class TestPhiRedaction:
    """Test PHI redaction."""

    def test_redacts_ssn(self, fw: PhiFirewall) -> None:
        result = fw.redact("SSN: 123-45-6789")
        assert "123-45-6789" not in result
        assert "[REDACTED-ssn]" in result

    def test_redacts_email(self, fw: PhiFirewall) -> None:
        result = fw.redact("Contact: user@hospital.com for records")
        assert "user@hospital.com" not in result
        assert "[REDACTED-email]" in result

    def test_redacts_multiple(self, fw: PhiFirewall) -> None:
        text = "Patient: Jane Doe, SSN: 111-22-3333"
        result = fw.redact(text)
        assert "Jane Doe" not in result
        assert "111-22-3333" not in result

    def test_clean_text_unchanged(self, fw: PhiFirewall) -> None:
        text = "LDL cholesterol is 130 mg/dL"
        assert fw.redact(text) == text


class TestPhiAssert:
    """Test assert_no_phi hard-block."""

    def test_raises_on_phi(self, fw: PhiFirewall) -> None:
        with pytest.raises(PhiDetectedError):
            fw.assert_no_phi("Patient: John Smith")

    def test_passes_on_clean(self, fw: PhiFirewall) -> None:
        fw.assert_no_phi("What are normal hemoglobin levels?")

    def test_blocks_outbound_research(self, fw: PhiFirewall) -> None:
        """PHI in research query must be hard-blocked, not sanitized."""
        query = "What does John Smith's glucose of 250 on 01/15/2024 mean?"
        with pytest.raises(PhiDetectedError):
            fw.assert_no_phi(query, context="research_query")
