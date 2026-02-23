"""Standardized anonymization pipeline with retry, batch, and audit trail.

Single implementation used by ALL callers (clean_sync, claude_conversation,
ai_export). Replaces 3 divergent retry/fallback patterns with one pipeline.

Usage:
    pipeline = AnonymizePipeline(anonymizer, max_passes=2, fallback="block")
    result = pipeline.process(text)
    if result.had_phi:
        log_redaction(result.audit_trail)
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from healthbot.llm.anonymizer import AnonymizationError, Anonymizer

logger = logging.getLogger("healthbot")


@dataclass
class RedactionEvent:
    """A single redaction action in the audit trail."""

    layer: str          # "NER", "regex", "LLM"
    category: str       # "person", "ssn", "LLM-name"
    original_hash: str  # SHA256[:12] of redacted text (no PHI stored)
    span_start: int
    span_end: int
    confidence: float   # 1.0 for regex, NER score, 0.8 for LLM


@dataclass
class RedactionResult:
    """Result from a single anonymization run."""

    text: str
    had_phi: bool
    passes: int                         # How many passes needed (1 or 2)
    layer_hits: dict[str, int] = field(default_factory=dict)
    redaction_score: float = 1.0        # 0.0–1.0
    audit_trail: list[RedactionEvent] = field(default_factory=list)


class AnonymizePipeline:
    """Standardized anonymization with retry, batch, and audit trail.

    Replaces the divergent retry patterns in clean_sync, claude_conversation,
    and ai_export with one implementation.

    Fallback modes:
        "block"         — raise AnonymizationError on exhaustion
        "fallback_text" — return fallback_text on exhaustion
        "redact_all"    — re-anonymize aggressively on exhaustion
    """

    def __init__(
        self,
        anonymizer: Anonymizer,
        max_passes: int = 2,
        fallback: str = "block",
        fallback_text: str = "[REDACTED]",
    ) -> None:
        self._anon = anonymizer
        self._max_passes = max_passes
        self._fallback = fallback
        self._fallback_text = fallback_text

    def process(self, text: str) -> RedactionResult:
        """Anonymize text with retry and fallback.

        Flow: anonymize → assert_safe → retry if needed → fallback on exhaustion.
        """
        if not text:
            return RedactionResult(text=text or "", had_phi=False, passes=0)

        all_events: list[RedactionEvent] = []
        layer_hits: dict[str, int] = {}
        current = text
        had_phi = False

        # Use phased method only on real Anonymizer (MagicMock fails)
        use_phased = isinstance(self._anon, Anonymizer)

        for pass_num in range(1, self._max_passes + 1):
            if use_phased:
                cleaned, spans = self._anon.anonymize_phased(current)
                pass_had_phi = bool(spans)
                for span in spans:
                    layer = span.layer
                    layer_hits[layer] = layer_hits.get(layer, 0) + 1
                    all_events.append(RedactionEvent(
                        layer=layer,
                        category=span.tag,
                        original_hash=span.text_hash,
                        span_start=span.start,
                        span_end=span.end,
                        confidence=span.confidence,
                    ))
            else:
                cleaned, pass_had_phi = self._anon.anonymize(current)

            if pass_had_phi:
                had_phi = True
            current = cleaned

            # Verify safety
            try:
                self._anon.assert_safe(current)
                return RedactionResult(
                    text=current,
                    had_phi=had_phi,
                    passes=pass_num,
                    layer_hits=layer_hits,
                    redaction_score=1.0,
                    audit_trail=all_events,
                )
            except AnonymizationError:
                if pass_num < self._max_passes:
                    continue  # Retry
                # Exhausted all passes — apply fallback
                return self._apply_fallback(
                    current, had_phi, pass_num, layer_hits, all_events,
                )

        # Should not reach here, but safety net
        return RedactionResult(
            text=current, had_phi=had_phi, passes=self._max_passes,
            layer_hits=layer_hits, audit_trail=all_events,
        )

    def process_batch(
        self, texts: dict[str, str],
    ) -> dict[str, RedactionResult]:
        """Anonymize multiple named fields. One failure doesn't block others.

        Uses ThreadPoolExecutor when len > 3 for parallel processing.
        """
        if not texts:
            return {}

        results: dict[str, RedactionResult] = {}

        if len(texts) <= 3:
            # Sequential for small batches
            for name, text in texts.items():
                results[name] = self.process(text)
        else:
            # Parallel for larger batches
            with ThreadPoolExecutor(max_workers=min(len(texts), 6)) as pool:
                futures = {
                    pool.submit(self.process, text): name
                    for name, text in texts.items()
                }
                for future in futures:
                    name = futures[future]
                    try:
                        results[name] = future.result()
                    except Exception as e:
                        logger.warning(
                            "Batch anonymization failed for '%s': %s", name, e,
                        )
                        results[name] = RedactionResult(
                            text=self._fallback_text,
                            had_phi=True,
                            passes=0,
                            redaction_score=0.0,
                        )

        return results

    def _apply_fallback(
        self,
        current: str,
        had_phi: bool,
        passes: int,
        layer_hits: dict[str, int],
        events: list[RedactionEvent],
    ) -> RedactionResult:
        """Apply fallback strategy when all passes are exhausted."""
        if self._fallback == "block":
            raise AnonymizationError(
                f"PII remains after {passes} anonymization passes. Blocked."
            )

        if self._fallback == "fallback_text":
            logger.warning(
                "Redaction fallback after %d passes, returning fallback text",
                passes,
            )
            return RedactionResult(
                text=self._fallback_text,
                had_phi=True,
                passes=passes,
                layer_hits=layer_hits,
                redaction_score=0.0,
                audit_trail=events,
            )

        if self._fallback == "redact_all":
            # One more aggressive pass
            cleaned, _ = self._anon.anonymize(current)
            logger.warning(
                "Aggressive re-anonymization after %d passes", passes,
            )
            return RedactionResult(
                text=cleaned,
                had_phi=True,
                passes=passes + 1,
                layer_hits=layer_hits,
                redaction_score=0.0,
                audit_trail=events,
            )

        raise ValueError(f"Unknown fallback mode: {self._fallback}")

    def _log_audit_trail(self, events: list[RedactionEvent]) -> None:
        """Log each redaction event at DEBUG level. No PHI in logs."""
        for event in events:
            logger.debug(
                "Redaction: layer=%s cat=%s hash=%s confidence=%.2f",
                event.layer, event.category,
                event.original_hash, event.confidence,
            )
