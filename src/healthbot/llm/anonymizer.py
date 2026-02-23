"""Anonymize outbound data for Claude CLI calls.

Up to three layers of PII stripping that preserves medical values:
  Layer 1: GLiNER NER (optional, smart — catches names, cities, organizations)
  Layer 2: PhiFirewall regex (always — catches SSN, MRN, insurance IDs, DOBs)
  Layer 3: Ollama LLM scan (optional — catches subtle, context-dependent PII)

All layers analyze the ORIGINAL text, then all detected spans are merged
and redacted in a single pass. This avoids ordering issues where one layer's
redaction could break the other layer's pattern matching.

If GLiNER is not installed, falls back to regex-only (Layer 2).
If Ollama layer is not provided, Layer 3 is skipped.
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass

from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")


class AnonymizationError(Exception):
    """Raised when PII is detected in a final outbound payload."""


@dataclass
class PiiSpan:
    """A single detected PII span with metadata for audit trail."""

    start: int
    end: int
    tag: str
    layer: str          # "NER", "regex", "LLM"
    confidence: float   # NER score, 1.0 for regex, 0.8 for LLM
    text_hash: str      # SHA256[:12] of redacted text (no PHI stored)


class Anonymizer:
    """Strip PII from text before sending to external APIs.

    Uses up to three layers:
    1. GLiNER NER (optional, smart, catches contextual PII like names/cities)
    2. PhiFirewall regex (always, deterministic, catches health-specific patterns)
    3. Ollama LLM scan (optional, catches subtle context-dependent PII)

    All layers run on the original text; results are merged before redaction.

    Canary token: a known fake PII string is injected before anonymization.
    If it survives, the pipeline is broken and an error is raised.
    """

    # Canary token — a known fake SSN used to verify the pipeline works.
    # If this survives anonymization, the pipeline failed catastrophically.
    _CANARY_SSN = "999-88-7777"
    _CANARY_TEXT = f"Patient SSN: {_CANARY_SSN}"

    _CACHE_MAX_SIZE = 500

    def __init__(
        self,
        phi_firewall: PhiFirewall | None = None,
        use_ner: bool = True,
        ollama_layer: object | None = None,
    ) -> None:
        self._fw = phi_firewall or PhiFirewall()
        self._ner = None
        self._ollama_layer = ollama_layer
        self._canary_verified = False
        # LRU-style cache: SHA256(text) -> (cleaned_text, had_phi)
        # Bounded dict with FIFO eviction. Never stores original text.
        self._cache: dict[str, tuple[str, bool]] = {}

        if use_ner:
            self._ner = self._try_init_ner()

        if not self._ner and not self._ollama_layer:
            logger.warning(
                "Running regex-only PII detection. Install GLiNER "
                "(make setup-nlp) or Ollama for enhanced name/city/org detection."
            )

    def _try_init_ner(self):
        """Attempt to initialize NER layer. Returns None on failure."""
        try:
            from healthbot.security.ner_layer import NerLayer

            if NerLayer.is_available():
                layer = NerLayer()
                logger.info("NER layer active (GLiNER)")
                return layer
        except Exception as e:
            logger.info("NER layer unavailable, using regex-only: %s", e)
        return None

    def _cache_put(self, key: str, value: tuple[str, bool]) -> None:
        """Store result in cache with FIFO eviction at max size."""
        if len(self._cache) >= self._CACHE_MAX_SIZE:
            # Evict oldest entry (first key in dict — insertion order)
            oldest = next(iter(self._cache))
            del self._cache[oldest]
        self._cache[key] = value

    @property
    def has_ner(self) -> bool:
        """Whether NER layer is active."""
        return self._ner is not None

    # NER canary: a name in medical context that NER should catch
    _NER_CANARY_TEXT = "Contact Dr. Sarah Johnson for results"
    _NER_CANARY_NAME = "Sarah Johnson"

    def _verify_canary(self) -> None:
        """One-time verification that the PII pipeline is functional.

        Multi-layer canary:
        - Regex canary (SSN): HARD FAIL if not caught
        - NER canary (person name): WARNING if not caught (NER is aid, not gate)
        - Ollama canary (SSN text): WARNING if not caught

        Called once on first anonymize() call.
        """
        if self._canary_verified:
            return

        # Layer 2 (regex) — must catch SSN. Hard fail.
        if not self._fw.contains_phi(self._CANARY_TEXT):
            raise AnonymizationError(
                "Canary token survived — PhiFirewall regex is not "
                "detecting SSN patterns. Pipeline is broken."
            )

        # Layer 1 (NER) — should catch person name. Warning only.
        if self._ner:
            try:
                entities = self._ner.detect(self._NER_CANARY_TEXT)
                names = [e for e in entities if e.label == "person"]
                if not any(self._NER_CANARY_NAME in e.text for e in names):
                    logger.warning(
                        "NER canary: 'Dr. Sarah Johnson' not detected as person. "
                        "NER may have reduced accuracy."
                    )
            except Exception as e:
                logger.warning("NER canary check failed: %s", e)

        # Layer 3 (Ollama) — should catch SSN in text. Warning only.
        if self._ollama_layer:
            try:
                llm_spans = self._ollama_layer.scan(self._CANARY_TEXT)
                if not llm_spans:
                    logger.warning(
                        "Ollama canary: SSN in '%s' not detected. "
                        "Ollama layer may have reduced accuracy.",
                        self._CANARY_TEXT,
                    )
            except Exception as e:
                logger.warning("Ollama canary check failed: %s", e)

        self._canary_verified = True

    def anonymize(self, text: str) -> tuple[str, bool]:
        """Remove PII patterns from text.

        Returns (cleaned_text, had_phi). Medical values are preserved.
        Both layers analyze the original text; spans are merged and redacted once.

        On first call, runs a canary check to verify the pipeline is functional.
        """
        if not text:
            return text, False

        # One-time canary verification
        self._verify_canary()

        # Cache lookup
        cache_key = hashlib.sha256(text.encode()).hexdigest()
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Collect all PII spans: (start, end, tag)
        spans: list[tuple[int, int, str]] = []

        # Layer 1: NER (if available)
        if self._ner:
            for e in self._ner.detect(text):
                tag = f"NER-{e.label.replace(' ', '_')}"
                spans.append((e.start, e.end, tag))

        # Layer 2: Regex (always)
        for m in self._fw.scan(text):
            spans.append((m.start, m.end, m.category))

        # Layer 3: Ollama LLM (optional, enhancement only)
        if self._ollama_layer:
            try:
                llm_spans = self._ollama_layer.scan(text)
                spans.extend(llm_spans)
            except Exception as e:
                logger.warning("Ollama anonymization layer failed (non-fatal): %s", e)

        if not spans:
            self._cache_put(cache_key, (text, False))
            return text, False

        # Cross-layer disagreement alerting
        if self._ner and spans:
            ner_positions = set()
            regex_positions = set()
            for start, end, tag in spans:
                if tag.startswith("NER-"):
                    ner_positions.add((start, end))
                else:
                    regex_positions.add((start, end))
            ner_only = len(ner_positions - regex_positions)
            regex_only = len(regex_positions - ner_positions)
            if ner_only or regex_only:
                logger.info(
                    "Layer disagreement: NER-only=%d, regex-only=%d",
                    ner_only, regex_only,
                )

        # Merge overlapping spans, keeping the largest
        merged = self._merge_spans(spans)

        # Redact from end to start to preserve positions
        result = text
        for start, end, tag in reversed(merged):
            result = result[:start] + f"[REDACTED-{tag}]" + result[end:]

        logger.info("Anonymizer: redacted PII from outbound text")
        self._cache_put(cache_key, (result, True))
        return result, True

    def anonymize_phased(self, text: str) -> tuple[str, list[PiiSpan]]:
        """Like anonymize(), but returns per-span metadata for audit trail.

        NER spans get confidence=entity.score, layer="NER".
        Regex spans get confidence=1.0, layer="regex".
        LLM spans get confidence=0.8, layer="LLM".
        """
        if not text:
            return text, []

        self._verify_canary()

        # Collect all PII spans with metadata
        pii_spans: list[PiiSpan] = []

        # Layer 1: NER (if available)
        if self._ner:
            for e in self._ner.detect(text):
                tag = f"NER-{e.label.replace(' ', '_')}"
                text_hash = hashlib.sha256(
                    text[e.start:e.end].encode(),
                ).hexdigest()[:12]
                pii_spans.append(PiiSpan(
                    start=e.start, end=e.end, tag=tag,
                    layer="NER", confidence=e.score, text_hash=text_hash,
                ))

        # Layer 2: Regex (always)
        for m in self._fw.scan(text):
            text_hash = hashlib.sha256(
                text[m.start:m.end].encode(),
            ).hexdigest()[:12]
            pii_spans.append(PiiSpan(
                start=m.start, end=m.end, tag=m.category,
                layer="regex", confidence=1.0, text_hash=text_hash,
            ))

        # Layer 3: Ollama LLM (optional)
        if self._ollama_layer:
            try:
                llm_spans = self._ollama_layer.scan(text)
                for start, end, tag in llm_spans:
                    text_hash = hashlib.sha256(
                        text[start:end].encode(),
                    ).hexdigest()[:12]
                    pii_spans.append(PiiSpan(
                        start=start, end=end, tag=tag,
                        layer="LLM", confidence=0.8, text_hash=text_hash,
                    ))
            except Exception as e:
                logger.warning("Ollama anonymization layer failed (non-fatal): %s", e)

        if not pii_spans:
            return text, []

        # Merge overlapping spans, preserving highest confidence
        merged = self._merge_pii_spans(pii_spans)

        # Redact from end to start to preserve positions
        result = text
        for span in reversed(merged):
            result = result[:span.start] + f"[REDACTED-{span.tag}]" + result[span.end:]

        logger.info("Anonymizer: redacted PII from outbound text (phased)")
        return result, merged

    def anonymize_fast_only(
        self, text: str,
    ) -> tuple[str, list[PiiSpan], list[PiiSpan]]:
        """Run NER + regex only (no Ollama).

        Returns (cleaned_text, merged_spans, raw_spans).
        raw_spans are pre-merge for uncertainty evaluation.

        Used by hybrid mode: keeps _ollama_layer intact for selective pass 2.
        Like anonymize_phased() but explicitly skips Layer 3.
        """
        if not text:
            return text, [], []

        self._verify_canary()

        pii_spans: list[PiiSpan] = []

        # Layer 1: NER (if available)
        if self._ner:
            for e in self._ner.detect(text):
                tag = f"NER-{e.label.replace(' ', '_')}"
                text_hash = hashlib.sha256(
                    text[e.start:e.end].encode(),
                ).hexdigest()[:12]
                pii_spans.append(PiiSpan(
                    start=e.start, end=e.end, tag=tag,
                    layer="NER", confidence=e.score, text_hash=text_hash,
                ))

        # Layer 2: Regex (always — includes identity profile patterns)
        for m in self._fw.scan(text):
            text_hash = hashlib.sha256(
                text[m.start:m.end].encode(),
            ).hexdigest()[:12]
            pii_spans.append(PiiSpan(
                start=m.start, end=m.end, tag=m.category,
                layer="regex", confidence=1.0, text_hash=text_hash,
            ))

        # Layer 3: Explicitly skipped (Ollama reserved for hybrid pass 2)

        if not pii_spans:
            return text, [], []

        merged = self._merge_pii_spans(pii_spans)

        result = text
        for span in reversed(merged):
            result = result[:span.start] + f"[REDACTED-{span.tag}]" + result[span.end:]

        return result, merged, pii_spans

    @staticmethod
    def _merge_pii_spans(spans: list[PiiSpan]) -> list[PiiSpan]:
        """Merge overlapping PiiSpan objects, keeping highest confidence."""
        if not spans:
            return []

        sorted_spans = sorted(spans, key=lambda s: (s.start, -s.end))

        merged: list[PiiSpan] = [sorted_spans[0]]
        for span in sorted_spans[1:]:
            prev = merged[-1]
            if span.start < prev.end:
                # Overlapping — extend, keep higher confidence span's metadata
                new_end = max(prev.end, span.end)
                if span.confidence > prev.confidence:
                    merged[-1] = PiiSpan(
                        start=prev.start, end=new_end, tag=span.tag,
                        layer=span.layer, confidence=span.confidence,
                        text_hash=span.text_hash,
                    )
                else:
                    merged[-1] = PiiSpan(
                        start=prev.start, end=new_end, tag=prev.tag,
                        layer=prev.layer, confidence=prev.confidence,
                        text_hash=prev.text_hash,
                    )
            else:
                merged.append(span)

        return merged

    def score_redaction(self, text: str) -> float:
        """Score redaction quality: 1.0 = all layers agree text is clean.

        Checks each active layer independently. A score below 0.6 indicates
        a layer detected residual PII — the text should not be sent outbound.
        """
        score = 1.0

        # Strip existing redaction tags for checking
        stripped = self._REDACTED_TAG.sub("", text)

        # NER check
        if self._ner:
            entities = self._ner.detect(stripped)
            real = [
                e for e in entities
                if e.text.strip() not in self._FIELD_LABELS
                and len(e.text.strip()) > 2
            ]
            if real:
                score -= 0.4

        # Regex check
        if self._fw.contains_phi(stripped):
            score -= 0.4

        # Ollama check (if available)
        if self._ollama_layer:
            try:
                findings = self._ollama_layer.scan(stripped)
                if findings:
                    score -= 0.2
            except Exception:
                pass  # Layer unavailable — don't penalize score

        # Log score with hash for audit trail
        text_hash = hashlib.sha256(text.encode()).hexdigest()[:12]
        logger.info("Redaction score: %.2f (hash=%s)", score, text_hash)

        return max(score, 0.0)

    _REDACTED_TAG = re.compile(r"\[REDACTED-[^\]]+\]")
    # Labels that remain after redaction and are not PII themselves
    _FIELD_LABELS = {"MRN", "SSN", "Phone", "Email", "Address"}

    def assert_safe(self, text: str) -> None:
        """Raise AnonymizationError if PII is still present.

        Call this as a final safety check on the assembled payload.
        Handles already-redacted text: strips redaction tags and filters
        out NER false positives from field labels and placeholders.
        Checks both NER (if available) and regex.
        """
        # Strip redaction tags for checking
        stripped = self._REDACTED_TAG.sub("", text)

        issues: list[str] = []

        # Check NER — filter out field labels and short noise
        if self._ner:
            entities = self._ner.detect(stripped)
            real = [
                e for e in entities
                if e.text.strip() not in self._FIELD_LABELS
                and len(e.text.strip()) > 2
            ]
            if real:
                labels = {e.label for e in real}
                issues.append(f"NER: {', '.join(labels)}")

        # Check regex
        if self._fw.contains_phi(stripped):
            matches = self._fw.scan(stripped)
            categories = {m.category for m in matches}
            issues.append(f"regex: {', '.join(categories)}")

        # Layer 3: Ollama LLM (optional, same pattern as anonymize())
        if self._ollama_layer:
            try:
                llm_findings = self._ollama_layer.scan(stripped)
                if llm_findings:
                    issues.append("ollama_llm")
            except Exception as e:
                logger.warning("Ollama assert_safe layer failed (non-fatal): %s", e)

        if issues:
            # Trigger PII alert
            try:
                from healthbot.security.pii_alert import PiiAlertService
                svc = PiiAlertService.get_instance()
                for issue in issues:
                    svc.record(category=issue, destination="outbound")
            except Exception:
                pass  # Alert is best-effort; never block the security gate

            raise AnonymizationError(
                f"PII detected in outbound payload ({'; '.join(issues)}). "
                f"Blocked to prevent data leakage."
            )

    @staticmethod
    def _merge_spans(
        spans: list[tuple[int, int, str]],
    ) -> list[tuple[int, int, str]]:
        """Merge overlapping spans, keeping the widest coverage."""
        if not spans:
            return []

        # Sort by start position, then by end (descending) for longest first
        sorted_spans = sorted(spans, key=lambda s: (s[0], -s[1]))

        merged: list[tuple[int, int, str]] = [sorted_spans[0]]
        for start, end, tag in sorted_spans[1:]:
            prev_start, prev_end, prev_tag = merged[-1]
            if start < prev_end:
                # Overlapping — extend to cover both, keep the wider tag
                new_end = max(prev_end, end)
                wider_tag = tag if (end - start) > (prev_end - prev_start) else prev_tag
                merged[-1] = (prev_start, new_end, wider_tag)
            else:
                merged.append((start, end, tag))

        return merged
