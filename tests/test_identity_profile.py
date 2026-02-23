"""Tests for encrypted identity profile and pattern compilation."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from healthbot.security.identity_profile import IdentityProfile


@pytest.fixture
def mock_db():
    """Mock HealthDB with identity field methods."""
    db = MagicMock()
    db._fields: list[dict] = []

    def upsert(user_id, field_key, value, field_type):
        # Remove existing
        db._fields[:] = [f for f in db._fields if not (
            f["field_key"] == field_key
        )]
        db._fields.append({
            "field_key": field_key,
            "value": value,
            "type": field_type,
        })
        return "test-id"

    def get_fields(user_id):
        return list(db._fields)

    def delete_field(user_id, field_key):
        before = len(db._fields)
        db._fields[:] = [f for f in db._fields if f["field_key"] != field_key]
        return len(db._fields) < before

    def delete_all(user_id):
        count = len(db._fields)
        db._fields.clear()
        return count

    db.upsert_identity_field = MagicMock(side_effect=upsert)
    db.get_identity_fields = MagicMock(side_effect=get_fields)
    db.delete_identity_field = MagicMock(side_effect=delete_field)
    db.delete_all_identity_fields = MagicMock(side_effect=delete_all)
    return db


@pytest.fixture
def profile(mock_db) -> IdentityProfile:
    return IdentityProfile(db=mock_db)


class TestIdentityCRUD:
    """Test store/retrieve/delete operations."""

    def test_store_and_retrieve(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "full_name", "John Smith", "name")
        fields = profile.get_all_fields(1)
        assert len(fields) == 1
        assert fields[0]["value"] == "John Smith"
        assert fields[0]["type"] == "name"

    def test_delete_field(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "email", "test@example.com", "email")
        assert profile.delete_field(1, "email")
        assert len(profile.get_all_fields(1)) == 0

    def test_delete_all(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "full_name", "John Smith", "name")
        profile.store_field(1, "email", "test@example.com", "email")
        count = profile.delete_all(1)
        assert count == 2
        assert len(profile.get_all_fields(1)) == 0

    def test_cache_invalidated_on_store(self, profile: IdentityProfile, mock_db) -> None:
        profile.get_all_fields(1)  # Populate cache
        profile.store_field(1, "full_name", "New Name", "name")
        fields = profile.get_all_fields(1)
        assert fields[0]["value"] == "New Name"


class TestNamePatterns:
    """Test name pattern compilation."""

    def test_forward_name(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "full_name", "John Smith", "name")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("John Smith") for p in patterns.values())

    def test_forward_name_case_insensitive(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "full_name", "John Smith", "name")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("john smith") for p in patterns.values())

    def test_reversed_name(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "full_name", "John Smith", "name")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("Smith, John") for p in patterns.values())

    def test_initial_form(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "full_name", "John Smith", "name")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("J. Smith") for p in patterns.values())
        assert any(p.search("J Smith") for p in patterns.values())

    def test_first_name_alone_long(self, profile: IdentityProfile, mock_db) -> None:
        """First names 4+ chars get a standalone pattern."""
        profile.store_field(1, "full_name", "John Smith", "name")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("John") for p in patterns.values())

    def test_first_name_short_no_standalone(self, profile: IdentityProfile, mock_db) -> None:
        """First names < 4 chars do NOT get a standalone pattern to avoid false positives."""
        profile.store_field(1, "full_name", "Kai Lin", "name")
        patterns = profile.compile_phi_patterns(1)
        # Should match "Kai Lin" forward but NOT "Kai" alone
        assert any(p.search("Kai Lin") for p in patterns.values())
        # Find all pattern matches for "Kai" alone — should not match first_name pattern
        first_patterns = {k: v for k, v in patterns.items() if k.endswith("_first")}
        assert not any(p.search("Kai") for p in first_patterns.values())

    def test_no_patterns_for_empty(self, profile: IdentityProfile, mock_db) -> None:
        patterns = profile.compile_phi_patterns(1)
        assert patterns == {}


class TestDOBPatterns:
    """Test DOB pattern compilation."""

    def test_dob_slash_formats(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "dob", "1990-03-15", "dob")
        patterns = profile.compile_phi_patterns(1)
        # Should match various date formats
        assert any(p.search("03/15/1990") for p in patterns.values())
        assert any(p.search("3/15/1990") for p in patterns.values())

    def test_dob_iso_format(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "dob", "1990-03-15", "dob")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("1990-03-15") for p in patterns.values())

    def test_dob_text_month(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "dob", "1990-03-15", "dob")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("March 15, 1990") for p in patterns.values())
        assert any(p.search("March 15 1990") for p in patterns.values())

    def test_dob_mm_dd_yyyy_input(self, profile: IdentityProfile, mock_db) -> None:
        """DOB provided as MM/DD/YYYY should also compile."""
        profile.store_field(1, "dob", "03/15/1990", "dob")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("03/15/1990") for p in patterns.values())
        assert any(p.search("3/15/1990") for p in patterns.values())


class TestEmailPatterns:
    """Test email pattern compilation."""

    def test_email_exact_match(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "email", "john@example.com", "email")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("john@example.com") for p in patterns.values())

    def test_email_case_insensitive(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "email", "John@Example.com", "email")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("john@example.com") for p in patterns.values())


class TestFamilyNames:
    """Test family name handling."""

    def test_family_names_multi_value(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "family:0", "Sarah Johnson", "name")
        profile.store_field(1, "family:1", "Mike Thompson", "name")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("Sarah Johnson") for p in patterns.values())
        assert any(p.search("Mike Thompson") for p in patterns.values())

    def test_family_in_ner_known_names(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "family:0", "Sarah Johnson", "name")
        names = profile.compile_ner_known_names(1)
        assert "Sarah Johnson" in names
        assert "Sarah" in names
        assert "Johnson" in names


class TestCustomPatterns:
    """Test custom PII pattern compilation."""

    def test_custom_word_boundary(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "custom:0", "Acme Corp", "custom")
        patterns = profile.compile_phi_patterns(1)
        assert any(p.search("I work at Acme Corp downtown") for p in patterns.values())

    def test_custom_no_partial_match(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "custom:0", "Acme", "custom")
        patterns = profile.compile_phi_patterns(1)
        # Should NOT match "Acmeologist" (word boundary)
        assert not any(p.search("Acmeologist") for p in patterns.values())


class TestNerKnownNames:
    """Test known names compilation for NER boosting."""

    def test_full_name_in_known(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "full_name", "John Smith", "name")
        names = profile.compile_ner_known_names(1)
        assert "John Smith" in names
        assert "John" in names
        assert "Smith" in names

    def test_short_parts_excluded(self, profile: IdentityProfile, mock_db) -> None:
        """Name parts < 3 chars excluded from NER boosting."""
        profile.store_field(1, "full_name", "Li Wei", "name")
        names = profile.compile_ner_known_names(1)
        assert "Li Wei" in names
        assert "Wei" in names
        assert "Li" not in names  # 2 chars — excluded

    def test_email_not_in_known_names(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "email", "test@example.com", "email")
        names = profile.compile_ner_known_names(1)
        assert len(names) == 0


class TestAnonymizationTest:
    """Test the test_anonymization method."""

    def test_detects_name_in_sample(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "full_name", "John Smith", "name")
        results = profile.test_anonymization(
            1, "Patient John Smith visited the clinic."
        )
        assert len(results) > 0
        assert any(r.matched_text == "John Smith" for r in results)

    def test_no_match_on_clean_text(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "full_name", "John Smith", "name")
        results = profile.test_anonymization(
            1, "Hemoglobin A1c is 5.7%"
        )
        assert len(results) == 0

    def test_results_sorted_by_position(self, profile: IdentityProfile, mock_db) -> None:
        profile.store_field(1, "full_name", "John Smith", "name")
        profile.store_field(1, "email", "john@example.com", "email")
        results = profile.test_anonymization(
            1, "Email john@example.com for John Smith"
        )
        assert len(results) >= 2
        positions = [r.start for r in results]
        assert positions == sorted(positions)


class TestPhiFirewallIntegration:
    """Test identity patterns work with PhiFirewall."""

    def test_firewall_with_identity_patterns(self, profile: IdentityProfile, mock_db) -> None:
        from healthbot.security.phi_firewall import PhiFirewall

        profile.store_field(1, "full_name", "Alex Morgan", "name")
        patterns = profile.compile_phi_patterns(1)
        fw = PhiFirewall(extra_patterns=patterns)

        assert fw.contains_phi("Alex Morgan had blood work done")
        assert not fw.contains_phi("Hemoglobin is 14.2 g/dL")

    def test_firewall_redacts_identity(self, profile: IdentityProfile, mock_db) -> None:
        from healthbot.security.phi_firewall import PhiFirewall

        profile.store_field(1, "full_name", "Jane Doe", "name")
        patterns = profile.compile_phi_patterns(1)
        fw = PhiFirewall(extra_patterns=patterns)

        result = fw.redact("Results for Jane Doe")
        assert "Jane Doe" not in result
        assert "[REDACTED-" in result
