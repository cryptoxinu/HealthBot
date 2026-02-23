"""Tests for the anonymizer module."""
from __future__ import annotations

import pytest

from healthbot.llm.anonymizer import AnonymizationError, Anonymizer


class TestAnonymizer:
    """Test regex-based PII stripping and medical value preservation."""

    def setup_method(self):
        self.anon = Anonymizer(use_ner=False)

    def test_strips_ssn(self):
        text = "Patient SSN: 123-45-6789 has glucose 108"
        cleaned, had_phi = self.anon.anonymize(text)
        assert "123-45-6789" not in cleaned
        assert had_phi is True
        assert "108" in cleaned

    def test_strips_phone(self):
        text = "Call 555-123-4567 for results"
        cleaned, had_phi = self.anon.anonymize(text)
        assert "555-123-4567" not in cleaned
        assert had_phi is True

    def test_strips_email(self):
        text = "Send to patient@example.com"
        cleaned, had_phi = self.anon.anonymize(text)
        assert "patient@example.com" not in cleaned
        assert had_phi is True

    def test_strips_mrn(self):
        text = "MRN: 12345678 glucose 108 mg/dL"
        cleaned, had_phi = self.anon.anonymize(text)
        assert "12345678" not in cleaned
        assert had_phi is True
        assert "108" in cleaned

    def test_preserves_medical_values(self):
        text = "glucose 108 mg/dL HRV 55ms LDL 145 mg/dL"
        cleaned, had_phi = self.anon.anonymize(text)
        assert had_phi is False
        assert "108" in cleaned
        assert "55" in cleaned
        assert "145" in cleaned

    def test_preserves_lab_names(self):
        text = "Hemoglobin A1c: 5.7% TSH: 2.5 mIU/L"
        cleaned, had_phi = self.anon.anonymize(text)
        assert had_phi is False
        assert "Hemoglobin" in cleaned
        assert "TSH" in cleaned

    def test_empty_text(self):
        cleaned, had_phi = self.anon.anonymize("")
        assert cleaned == ""
        assert had_phi is False

    def test_no_phi_text(self):
        text = "My glucose is 108 and I feel dizzy"
        cleaned, had_phi = self.anon.anonymize(text)
        assert had_phi is False
        assert cleaned == text

    def test_mixed_phi_and_medical(self):
        text = "Patient Name: John Smith has glucose 108 mg/dL and SSN 123-45-6789"
        cleaned, had_phi = self.anon.anonymize(text)
        assert had_phi is True
        assert "John Smith" not in cleaned
        assert "123-45-6789" not in cleaned
        assert "108" in cleaned

    def test_assert_safe_passes(self):
        self.anon.assert_safe("glucose 108 mg/dL")

    def test_assert_safe_raises(self):
        with pytest.raises(AnonymizationError):
            self.anon.assert_safe("SSN: 123-45-6789")


class TestCanaryMultiLayer:
    """Test multi-layer canary verification."""

    def test_regex_canary_catches_ssn(self):
        anon = Anonymizer(use_ner=False)
        # Force canary check via first anonymize call
        anon.anonymize("test text")
        assert anon._canary_verified is True

    def test_regex_canary_fails_on_broken_firewall(self):
        from unittest.mock import MagicMock

        broken_fw = MagicMock()
        broken_fw.contains_phi.return_value = False  # Broken — misses SSN
        anon = Anonymizer(phi_firewall=broken_fw, use_ner=False)
        with pytest.raises(AnonymizationError, match="Canary token survived"):
            anon.anonymize("test text")

    def test_ner_canary_warns_on_miss(self):
        """NER canary missing person should log warning, not error."""
        from unittest.mock import MagicMock

        anon = Anonymizer(use_ner=False)
        mock_ner = MagicMock()
        mock_ner.detect.return_value = []  # NER misses person
        anon._ner = mock_ner
        # Should NOT raise — NER is aid, not gate
        anon.anonymize("test text")
        assert anon._canary_verified is True

    def test_ollama_canary_warns_on_miss(self):
        """Ollama canary missing SSN should log warning, not error."""
        from unittest.mock import MagicMock

        anon = Anonymizer(use_ner=False)
        mock_ollama = MagicMock()
        mock_ollama.scan.return_value = []  # Ollama misses SSN
        anon._ollama_layer = mock_ollama
        # Should NOT raise — Ollama is enhancement
        anon.anonymize("test text")
        assert anon._canary_verified is True


class TestCaching:
    """Test anonymizer result caching."""

    def test_same_text_returns_cached(self):
        anon = Anonymizer(use_ner=False)
        text = "glucose 108 mg/dL"
        result1 = anon.anonymize(text)
        result2 = anon.anonymize(text)
        assert result1 == result2
        # Cache should have exactly 1 entry for this text
        assert len(anon._cache) == 1

    def test_different_texts_separate_cache_entries(self):
        anon = Anonymizer(use_ner=False)
        anon.anonymize("glucose 108")
        anon.anonymize("HRV 55ms")
        assert len(anon._cache) == 2

    def test_cache_eviction_at_max_size(self):
        anon = Anonymizer(use_ner=False)
        anon._CACHE_MAX_SIZE = 5  # Small for testing
        for i in range(10):
            anon.anonymize(f"glucose {100 + i} mg/dL")
        assert len(anon._cache) <= 5

    def test_cached_phi_result_correct(self):
        anon = Anonymizer(use_ner=False)
        text = "SSN: 123-45-6789 glucose 108"
        result1 = anon.anonymize(text)
        result2 = anon.anonymize(text)
        assert result1 == result2
        assert result1[1] is True  # had_phi
        assert "123-45-6789" not in result1[0]
        assert "108" in result1[0]


class TestAnonymizePhased:
    """Test the anonymize_phased() method that returns PiiSpan metadata."""

    def test_phased_returns_spans(self):
        from healthbot.llm.anonymizer import PiiSpan

        anon = Anonymizer(use_ner=False)
        text = "SSN: 123-45-6789 glucose 108"
        cleaned, spans = anon.anonymize_phased(text)
        assert "123-45-6789" not in cleaned
        assert "108" in cleaned
        assert len(spans) > 0
        assert all(isinstance(s, PiiSpan) for s in spans)

    def test_phased_span_metadata(self):
        anon = Anonymizer(use_ner=False)
        text = "SSN: 123-45-6789"
        _, spans = anon.anonymize_phased(text)
        assert len(spans) >= 1
        span = spans[0]
        assert span.layer == "regex"
        assert span.confidence == 1.0
        assert len(span.text_hash) == 12  # SHA256[:12]

    def test_phased_empty_text(self):
        anon = Anonymizer(use_ner=False)
        cleaned, spans = anon.anonymize_phased("")
        assert cleaned == ""
        assert spans == []

    def test_phased_no_phi(self):
        anon = Anonymizer(use_ner=False)
        text = "glucose 108 mg/dL"
        cleaned, spans = anon.anonymize_phased(text)
        assert cleaned == text
        assert spans == []
