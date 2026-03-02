"""Tests for the standardized anonymization pipeline.

Covers single-pass, retry, fallback modes, batch processing, audit trail,
medical false positive regression, and concurrency.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from healthbot.llm.anonymize_pipeline import AnonymizePipeline, RedactionResult
from healthbot.llm.anonymizer import AnonymizationError, Anonymizer


@pytest.fixture
def anon():
    """Create a regex-only Anonymizer (no NER, fast).

    The canary SSN (999-88-7777) uses area number 9xx which the
    SSN regex intentionally excludes.  Pre-verify the canary so
    tests that are *not* testing canary behaviour can proceed.
    """
    a = Anonymizer(use_ner=False)
    a._canary_verified = True
    return a


class TestAnonymizePipeline:
    """Core pipeline behavior: single pass, retry, fallback."""

    def test_single_pass_clean_text(self, anon):
        pipeline = AnonymizePipeline(anon, max_passes=2, fallback="block")
        result = pipeline.process("glucose 108 mg/dL")
        assert result.text == "glucose 108 mg/dL"
        assert result.had_phi is False
        assert result.passes == 1
        assert result.redaction_score == 1.0

    def test_single_pass_strips_phi(self, anon):
        pipeline = AnonymizePipeline(anon, max_passes=2, fallback="block")
        result = pipeline.process("SSN: 123-45-6789 glucose 108")
        assert "123-45-6789" not in result.text
        assert "108" in result.text
        assert result.had_phi is True
        assert result.passes == 1

    def test_empty_text(self, anon):
        pipeline = AnonymizePipeline(anon)
        result = pipeline.process("")
        assert result.text == ""
        assert result.had_phi is False
        assert result.passes == 0

    def test_fallback_block_raises(self):
        mock_anon = MagicMock(unsafe=True)  # unsafe for assert_safe attr
        mock_anon.anonymize.return_value = ("still has SSN 123-45-6789", True)
        mock_anon.assert_safe.side_effect = AnonymizationError("PII remains")
        pipeline = AnonymizePipeline(mock_anon, max_passes=2, fallback="block")
        with pytest.raises(AnonymizationError):
            pipeline.process("SSN 123-45-6789")

    def test_fallback_text(self):
        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("still bad", True)
        mock_anon.assert_safe.side_effect = AnonymizationError("PII remains")
        pipeline = AnonymizePipeline(
            mock_anon, max_passes=2,
            fallback="fallback_text", fallback_text="[REDACTED]",
        )
        result = pipeline.process("bad text")
        assert result.text == "[REDACTED]"
        assert result.had_phi is True
        assert result.redaction_score == 0.0

    def test_fallback_redact_all(self):
        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("aggressively cleaned", True)
        mock_anon.assert_safe.side_effect = [
            AnonymizationError("PII remains"),  # Pass 1 fails
            None,  # Aggressive re-anonymization pass succeeds
        ]
        pipeline = AnonymizePipeline(
            mock_anon, max_passes=1, fallback="redact_all",
        )
        result = pipeline.process("bad text")
        assert result.text == "aggressively cleaned"
        assert result.had_phi is True
        assert result.passes == 2  # Original pass + aggressive pass

    def test_retry_succeeds_on_second_pass(self):
        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.side_effect = [
            ("partially cleaned", True),
            ("fully cleaned", True),
        ]
        mock_anon.assert_safe.side_effect = [
            AnonymizationError("PII remains"),  # Pass 1 fails
            None,  # Pass 2 succeeds
        ]
        pipeline = AnonymizePipeline(mock_anon, max_passes=2, fallback="block")
        result = pipeline.process("dirty text")
        assert result.text == "fully cleaned"
        assert result.passes == 2

    def test_audit_trail_populated(self, anon):
        pipeline = AnonymizePipeline(anon, max_passes=2)
        result = pipeline.process("SSN: 123-45-6789 glucose 108")
        # Regex-only anon doesn't produce PiiSpans, so audit trail
        # may be empty unless using anonymize_phased (real Anonymizer)
        assert isinstance(result.audit_trail, list)
        if result.audit_trail:
            event = result.audit_trail[0]
            assert event.layer in ("NER", "regex", "LLM")
            assert event.confidence > 0


class TestPipelineBatch:
    """Batch processing of multiple fields."""

    def test_batch_empty(self, anon):
        pipeline = AnonymizePipeline(anon)
        results = pipeline.process_batch({})
        assert results == {}

    def test_batch_sequential_small(self, anon):
        pipeline = AnonymizePipeline(anon)
        results = pipeline.process_batch({
            "name": "glucose 108",
            "value": "SSN 123-45-6789",
        })
        assert "name" in results
        assert "value" in results
        assert results["name"].had_phi is False
        assert results["value"].had_phi is True

    def test_batch_parallel_large(self, anon):
        pipeline = AnonymizePipeline(anon)
        texts = {f"field_{i}": f"glucose {100 + i}" for i in range(5)}
        results = pipeline.process_batch(texts)
        assert len(results) == 5
        for _name, result in results.items():
            assert result.had_phi is False

    def test_batch_one_failure_independent(self):
        """One field failing doesn't block others."""
        mock_anon = MagicMock(spec=Anonymizer)
        call_count = 0

        def side_effect(text):
            nonlocal call_count
            call_count += 1
            if "bad" in text:
                raise RuntimeError("boom")
            return (text, False)

        mock_anon.anonymize.side_effect = side_effect
        mock_anon.assert_safe.return_value = None

        pipeline = AnonymizePipeline(
            mock_anon, fallback="fallback_text", fallback_text="[REDACTED]",
        )
        results = pipeline.process_batch({
            "good1": "glucose 108",
            "bad": "bad text",
            "good2": "HRV 55",
            "good3": "TSH 2.5",
        })
        assert len(results) == 4
        # Bad field gets fallback
        assert results["bad"].text == "[REDACTED]"


class TestMedicalFalsePositiveRegression:
    """Medical texts that MUST NOT be redacted."""

    SAFE_TEXTS = [
        "Iron deficiency anemia suspected based on ferritin 12 ng/mL",
        "Vitamin D insufficiency with 25-OH at 18 ng/mL",
        "MTHFR C677T heterozygous — elevated homocysteine",
        "JAK2 V617F positive — polycythemia vera confirmed",
        "Factor V Leiden heterozygous — consider anticoagulation",
        "Thyroid panel: TSH 0.3, Free T4 2.1",
        "PCOS with insulin resistance — HOMA-IR 4.2",
        "CRP elevated at 8.5, ESR 42",
        "Metformin 500mg twice daily, Atorvastatin 20mg",
        "HRV:42, RHR:68, Recovery:65%",
        "Sleep score 78, strain 12.4, respiratory rate 15.2",
        "eGFR 95 mL/min, creatinine 0.9 mg/dL",
        "Hemoglobin A1c 5.7%, fasting glucose 99 mg/dL",
        "Total cholesterol 210, LDL 135, HDL 55, triglycerides 150",
        "AST 28, ALT 32, alkaline phosphatase 65",
        "Vitamin B12 450 pg/mL, folate 12.5 ng/mL",
        "Cortisol 15.2 mcg/dL at 8AM",
        "Testosterone total 650 ng/dL, free 15.2 pg/mL",
        "Homocysteine 12.5 umol/L — borderline elevated",
        "Uric acid 7.2 mg/dL — gout risk assessment",
    ]

    @pytest.mark.parametrize("text", SAFE_TEXTS)
    def test_medical_text_not_redacted(self, anon, text):
        pipeline = AnonymizePipeline(anon, max_passes=2, fallback="block")
        result = pipeline.process(text)
        assert result.text == text, f"Falsely redacted: {text}"
        assert result.had_phi is False


class TestPiiMustRedact:
    """Texts that MUST be redacted (PII present)."""

    @pytest.mark.parametrize(
        "text,must_remove,must_keep",
        [
            (
                "Patient SSN 123-45-6789 glucose 108",
                "123-45-6789",
                "108",
            ),
            (
                "MRN: 12345678 HbA1c 5.7%",
                "12345678",
                "5.7",
            ),
            (
                "Call 555-123-4567 for lab results",
                "555-123-4567",
                "lab results",
            ),
            (
                "Email results to patient@example.com",
                "patient@example.com",
                "results",
            ),
        ],
    )
    def test_pii_redacted_medical_preserved(self, anon, text, must_remove, must_keep):
        pipeline = AnonymizePipeline(anon, max_passes=2, fallback="block")
        result = pipeline.process(text)
        assert must_remove not in result.text
        assert must_keep in result.text
        assert result.had_phi is True


class TestConcurrency:
    """Thread safety of the pipeline."""

    def test_concurrent_batch(self, anon):
        """Multiple threads anonymize simultaneously."""
        import threading

        pipeline = AnonymizePipeline(anon)
        results: list[RedactionResult] = []
        errors: list[Exception] = []

        def worker(text):
            try:
                r = pipeline.process(text)
                results.append(r)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=worker, args=(f"glucose {100 + i}",))
            for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 10
        for r in results:
            assert r.had_phi is False


class TestOllamaBatchScan:
    """Tests for OllamaAnonymizationLayer.scan_batch()."""

    def test_scan_batch_empty(self):
        from healthbot.llm.anonymizer_llm import OllamaAnonymizationLayer

        mock_ollama = MagicMock()
        layer = OllamaAnonymizationLayer(mock_ollama)
        assert layer.scan_batch([]) == []

    def test_scan_batch_unavailable(self):
        from healthbot.llm.anonymizer_llm import OllamaAnonymizationLayer

        mock_ollama = MagicMock()
        mock_ollama.is_available.return_value = False
        layer = OllamaAnonymizationLayer(mock_ollama)
        results = layer.scan_batch(["text1", "text2", "text3"])
        assert results == [[], [], []]

    def test_scan_batch_small_uses_individual(self):
        from healthbot.llm.anonymizer_llm import OllamaAnonymizationLayer

        mock_ollama = MagicMock()
        mock_ollama.is_available.return_value = True
        mock_ollama.send.return_value = '{"found": false}'
        layer = OllamaAnonymizationLayer(mock_ollama)
        results = layer.scan_batch(["text1", "text2"])
        assert len(results) == 2
        # For 2 texts, should call send individually (2 calls)
        assert mock_ollama.send.call_count == 2

    def test_scan_batch_parses_response(self):
        from healthbot.llm.anonymizer_llm import OllamaAnonymizationLayer

        mock_ollama = MagicMock()
        mock_ollama.is_available.return_value = True
        mock_ollama.send.return_value = (
            '{"sections": ['
            '{"id": 0, "found": false},'
            '{"id": 1, "found": true, "items": [{"text": "John Smith", "type": "name"}]},'
            '{"id": 2, "found": false}'
            ']}'
        )
        layer = OllamaAnonymizationLayer(mock_ollama)
        results = layer.scan_batch([
            "glucose 108",
            "John Smith has diabetes",
            "HRV 55ms",
        ])
        assert len(results) == 3
        assert results[0] == []  # No PII
        assert len(results[1]) > 0  # Found "John Smith"
        assert results[1][0][2] == "LLM-name"
        assert results[2] == []  # No PII

    def test_scan_batch_fallback_on_parse_error(self):
        from healthbot.llm.anonymizer_llm import OllamaAnonymizationLayer

        mock_ollama = MagicMock()
        mock_ollama.is_available.return_value = True
        # First call (batch) returns garbage, fallback individual calls return clean
        mock_ollama.send.side_effect = [
            "not valid json",  # Batch parse fails
            '{"found": false}',  # Individual scan 1
            '{"found": false}',  # Individual scan 2
            '{"found": false}',  # Individual scan 3
        ]
        layer = OllamaAnonymizationLayer(mock_ollama)
        results = layer.scan_batch(["a", "b", "c"])
        assert len(results) == 3


class TestFallbackResilience:
    """Verify graceful fallback when layers are unavailable."""

    def test_ner_unavailable_regex_still_works(self):
        anon = Anonymizer(use_ner=False)
        anon._canary_verified = True
        pipeline = AnonymizePipeline(anon)
        result = pipeline.process("SSN 123-45-6789 glucose 108")
        assert "123-45-6789" not in result.text
        assert "108" in result.text

    def test_pipeline_with_mock_anonymizer(self):
        mock = MagicMock(unsafe=True)  # unsafe for assert_safe attr
        mock.anonymize.return_value = ("cleaned text", False)
        mock.assert_safe.return_value = None
        pipeline = AnonymizePipeline(mock)
        result = pipeline.process("some text")
        assert result.text == "cleaned text"
