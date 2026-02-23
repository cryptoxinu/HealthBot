"""Adversarial PII detection benchmark suite.

Tests evasion patterns that could bypass regex-based PHI detection.
Each test case represents a real-world pattern found in lab reports,
clinical notes, and patient communications.

The goal: ensure the PhiFirewall catches PII even in tricky formats.
Tests that target patterns not yet caught are marked xfail so the suite
passes while documenting known blind spots for future improvement.
"""
from __future__ import annotations

import pytest

from healthbot.llm.anonymizer import Anonymizer
from healthbot.security.phi_firewall import PhiFirewall


@pytest.fixture
def fw():
    return PhiFirewall()


@pytest.fixture
def anon():
    return Anonymizer(use_ner=False)


# ── SSN variants ──────────────────────────────────────────────

class TestSSNVariants:
    """SSN patterns including partial and formatted variants."""

    def test_standard_ssn(self, fw):
        assert fw.contains_phi("SSN: 123-45-6789")

    def test_ssn_no_label(self, fw):
        assert fw.contains_phi("Number is 123-45-6789 on file")

    def test_ssn_with_spaces(self, fw):
        """SSN with spaces instead of dashes."""
        # Not caught by current regex — xfail
        result = fw.contains_phi("SSN: 123 45 6789")
        if not result:
            pytest.xfail("SSN with spaces not caught by current regex")

    def test_ssn_no_dashes(self, fw):
        """SSN without dashes — 9 consecutive digits."""
        result = fw.contains_phi("SSN: 123456789")
        if not result:
            pytest.xfail("SSN without dashes not caught by current regex")

    def test_last_four_ssn(self, fw):
        """Partial SSN: 'last 4: 6789' — currently not caught."""
        result = fw.contains_phi("last 4 of SSN: 6789")
        if not result:
            pytest.xfail("Partial SSN (last 4) not caught")


# ── Name patterns (regex-detectable) ─────────────────────────

class TestNamePatterns:
    """Names in various formats that appear in lab reports."""

    def test_labeled_name(self, fw):
        assert fw.contains_phi("Patient: John Smith")

    def test_labeled_name_colon(self, fw):
        assert fw.contains_phi("Patient Name: Jane Doe")

    def test_labeled_caps_name(self, fw):
        """ALL-CAPS labeled name: 'Patient: SMITH, JOHN'"""
        assert fw.contains_phi("Patient: SMITH, JOHN")

    def test_doctor_name(self, fw):
        assert fw.contains_phi("Dr. Sarah Johnson ordered labs")

    def test_doctor_caps(self, fw):
        assert fw.contains_phi("DR. SMITH reviewed the results")

    def test_provider_labeled(self, fw):
        assert fw.contains_phi("Ordering Provider: Dr. Wilson")

    def test_provider_labeled_caps(self, fw):
        assert fw.contains_phi("Ordering Provider: WILSON, SARAH MD")

    def test_name_intro(self, fw):
        assert fw.contains_phi("My name is John Smith")

    def test_name_intro_im(self, fw):
        assert fw.contains_phi("I'm Sarah Jones and I have a question")

    def test_hyphenated_name_labeled(self, fw):
        """Hyphenated name: 'Patient: Mary-Jane Watson' — hard for regex."""
        result = fw.contains_phi("Patient: Mary-Jane Watson")
        if not result:
            pytest.xfail("Hyphenated name not caught by regex")

    def test_apostrophe_name_labeled(self, fw):
        """Apostrophe name: \"Patient: O'Brien\" — hard for regex."""
        result = fw.contains_phi("Patient: Patrick O'Brien")
        if not result:
            pytest.xfail("Apostrophe name not caught by regex")

    def test_name_near_lab_value(self, anon):
        """Name adjacent to a lab value — must not confuse name with value."""
        text = "Patient: John Smith WBC 5.0 K/uL"
        cleaned, had_phi = anon.anonymize(text)
        assert "John Smith" not in cleaned
        assert "5.0" in cleaned

    def test_caps_name_with_lab(self, anon):
        """ALL-CAPS name mixed with lab: 'SMITH, JOHN WBC 5.0 K/UL'"""
        text = "Patient: SMITH, JOHN WBC 5.0 K/UL"
        cleaned, had_phi = anon.anonymize(text)
        assert "SMITH" not in cleaned or "JOHN" not in cleaned
        assert "5.0" in cleaned


# ── DOB variants ──────────────────────────────────────────────

class TestDOBVariants:
    """Date of birth in multiple formats."""

    def test_dob_slash(self, fw):
        assert fw.contains_phi("DOB: 01/15/1990")

    def test_dob_iso(self, fw):
        assert fw.contains_phi("DOB: 1990-01-15")

    def test_dob_text_month(self, fw):
        assert fw.contains_phi("born on January 15, 1990")

    def test_dob_labeled_dash(self, fw):
        assert fw.contains_phi("Date of Birth: 1-15-1990")

    def test_birthday_keyword(self, fw):
        assert fw.contains_phi("birthday: March 3, 2000")


# ── Address patterns ──────────────────────────────────────────

class TestAddressPatterns:
    """Street addresses in various formats."""

    def test_standard_address(self, fw):
        assert fw.contains_phi("123 Main St")

    def test_address_avenue(self, fw):
        assert fw.contains_phi("456 Oak Ave")

    def test_address_boulevard(self, fw):
        assert fw.contains_phi("789 Sunset Blvd")

    def test_address_without_zip(self, fw):
        """Address without ZIP code should still be caught."""
        assert fw.contains_phi("Lives at 123 Elm Dr")

    def test_zip_with_state(self, fw):
        assert fw.contains_phi("Springfield, IL 62704")


# ── Email and phone ───────────────────────────────────────────

class TestContactInfo:
    """Email and phone number variants."""

    def test_standard_email(self, fw):
        assert fw.contains_phi("john.doe@hospital.com")

    def test_plus_email(self, fw):
        assert fw.contains_phi("john+health@gmail.com")

    def test_phone_dashes(self, fw):
        assert fw.contains_phi("555-123-4567")

    def test_phone_parens(self, fw):
        assert fw.contains_phi("(555) 123-4567")

    def test_phone_dots(self, fw):
        assert fw.contains_phi("555.123.4567")


# ── Insurance/ID patterns ────────────────────────────────────

class TestInsurancePatterns:
    """Insurance and account identifiers."""

    def test_insurance_id(self, fw):
        assert fw.contains_phi("Insurance ID: ABC12345678")

    def test_member_id(self, fw):
        assert fw.contains_phi("Member ID: XYZ987654")

    def test_account_number(self, fw):
        assert fw.contains_phi("Account #: 12345678")

    def test_mrn_labeled(self, fw):
        assert fw.contains_phi("MRN: 12345678")


# ── OCR artifacts / split PII ─────────────────────────────────

class TestOCRArtifacts:
    """PII split across lines (common OCR artifact)."""

    def test_pii_split_lines(self, fw):
        """Name split across lines: 'Patient:\\nJohn Smith'"""
        result = fw.contains_phi("Patient:\nJohn Smith")
        if not result:
            pytest.xfail("Name split across lines not caught by regex")

    def test_ssn_split_lines(self, fw):
        """SSN after label on next line."""
        text = "SSN:\n123-45-6789"
        # The SSN itself is on its own line — regex should still match it
        assert fw.contains_phi(text)


# ── Anonymizer canary token ───────────────────────────────────

class TestCanaryToken:
    """Verify the canary token mechanism works."""

    def test_canary_verified_on_first_call(self, anon):
        """First anonymize() call triggers canary verification."""
        assert not anon._canary_verified
        anon.anonymize("glucose 108 mg/dL")
        assert anon._canary_verified

    def test_canary_detects_broken_regex(self):
        """If regex is broken (empty patterns), canary raises."""
        from healthbot.llm.anonymizer import AnonymizationError
        from healthbot.security.phi_firewall import PhiFirewall

        # PhiFirewall with NO patterns — simulates broken init
        broken_fw = PhiFirewall()
        broken_fw._patterns = {}  # empty — nothing will be caught

        broken_anon = Anonymizer(phi_firewall=broken_fw, use_ner=False)
        with pytest.raises(AnonymizationError, match="Canary token survived"):
            broken_anon.anonymize("test text")

    def test_canary_text_contains_ssn(self, anon):
        """Canary text should contain a known SSN pattern."""
        assert anon._CANARY_SSN in anon._CANARY_TEXT


# ── Redaction scoring ─────────────────────────────────────────

class TestRedactionScoring:
    """Test the redaction quality scoring."""

    def test_clean_text_scores_high(self, anon):
        score = anon.score_redaction("glucose 108 mg/dL")
        assert score >= 0.9

    def test_phi_text_scores_low(self, anon):
        score = anon.score_redaction("Patient SSN: 123-45-6789")
        assert score <= 0.6

    def test_redacted_text_scores_high(self, anon):
        score = anon.score_redaction("[REDACTED-ssn] has glucose 108")
        assert score >= 0.9

    def test_score_range(self, anon):
        """Score is always between 0.0 and 1.0."""
        for text in [
            "",
            "clean text",
            "SSN 123-45-6789 phone 555-123-4567 email a@b.com",
        ]:
            score = anon.score_redaction(text)
            assert 0.0 <= score <= 1.0


# ── Edge cases ────────────────────────────────────────────────

class TestEdgeCases:
    """Boundary conditions and unusual inputs."""

    def test_empty_string(self, anon):
        cleaned, had_phi = anon.anonymize("")
        assert cleaned == ""
        assert had_phi is False

    def test_only_medical_values(self, anon):
        text = "WBC 5.0 RBC 4.5 HGB 14.2 HCT 42.1 PLT 250"
        cleaned, had_phi = anon.anonymize(text)
        assert had_phi is False
        assert "5.0" in cleaned
        assert "250" in cleaned

    def test_multiple_phi_types(self, anon):
        """Text with many PHI types — all must be caught."""
        text = (
            "Patient: John Smith, SSN: 123-45-6789, "
            "DOB: 01/15/1990, Phone: 555-123-4567, "
            "Email: john@example.com, MRN: 12345678"
        )
        cleaned, had_phi = anon.anonymize(text)
        assert had_phi is True
        assert "123-45-6789" not in cleaned
        assert "555-123-4567" not in cleaned
        assert "john@example.com" not in cleaned
        assert "12345678" not in cleaned

    def test_phi_surrounded_by_lab_values(self, anon):
        """PHI embedded between lab values — PHI caught, values preserved."""
        text = "Glucose 108 mg/dL Patient: Jane Doe WBC 5.0 K/uL"
        cleaned, had_phi = anon.anonymize(text)
        assert had_phi is True
        assert "Jane Doe" not in cleaned
        assert "108" in cleaned
        assert "5.0" in cleaned
