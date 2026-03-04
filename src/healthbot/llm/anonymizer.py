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
import threading
from dataclasses import dataclass

from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")

# Circuit breaker: disable NER after this many consecutive failures
_NER_CIRCUIT_BREAKER_THRESHOLD = 3


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
    # Uses 078-05-1120 (a valid-format SSN in the detectable range, historically
    # voided by SSA). Previous value 999-xx-xxxx was excluded by the regex's
    # 900-999 area number filter.
    _CANARY_SSN = "078-05-1120"
    _CANARY_TEXT = f"Patient SSN: {_CANARY_SSN}"

    _CACHE_MAX_SIZE = 500

    # ── Heuristic name detection (fallback when NER unavailable) ──

    # Disease-eponym surnames: Title Case words that are medical, not person names.
    _MEDICAL_EPONYMS: frozenset[str] = frozenset({
        "Graves", "Addison", "Cushing", "Hashimoto", "Parkinson",
        "Alzheimer", "Hodgkin", "Crohn", "Raynaud", "Marfan",
        "Turner", "Bell", "Wilson", "Huntington", "Paget",
        "Sjögren", "Sjogren", "Behçet", "Behcet", "Kawasaki",
        "Reiter", "Wegener", "Barrett", "Dupuytren", "Kaposi",
        "Ménière", "Meniere", "Tourette", "Addisonian", "Down",
        "Ehlers", "Danlos", "Guillain", "Barré", "Barre",
        "Conn", "Klinefelter", "Whipple",
        # Additional medical surnames in condition names
        "Bright", "Hodgkins", "Perthes", "Legg", "Calve",
        "Charcot", "Marie", "Tooth", "Tay", "Sachs",
        "Wernicke", "Korsakoff", "Mallory", "Weiss",
        "Osler", "Weber", "Rendu", "Henoch", "Schonlein",
        "Takayasu", "Bechet", "Bowen", "Ewing", "Wilms",
        "Hirschsprung", "Meckel", "Zenker",
        "Bartter", "Gitelman", "Liddle", "Fanconi",
        "Goodpasture", "Fabry", "Gaucher",
        "Niemann", "Pick", "Pompe", "Krabbe",
    })

    # Two-word medical terms that look like person names in Title Case
    _MEDICAL_TERM_PAIRS: frozenset[str] = frozenset({
        "Diabetes Mellitus", "Chronic Fatigue", "Blood Pressure",
        "Heart Rate", "Heart Failure", "Lung Cancer",
        "Breast Cancer", "Liver Disease", "Kidney Disease",
        "Celiac Disease", "Multiple Sclerosis", "Cystic Fibrosis",
        "Sickle Cell", "Rheumatoid Arthritis", "Atrial Fibrillation",
        "Pulmonary Embolism", "Aortic Stenosis", "Mitral Valve",
        "Bone Marrow", "Stem Cell", "White Blood",
        "Red Blood", "Spinal Cord", "Nerve Growth",
        "Insulin Resistance", "Glucose Tolerance",
    })

    # Matches two consecutive Title Case words (potential name)
    _CAPITALIZED_PAIR: re.Pattern[str] = re.compile(
        r"\b([A-Z][a-z]{2,})\s+([A-Z][a-z]{2,})\b",
    )

    # Common English words that appear Title Case in headers/sentences but aren't names.
    # These are document structure words, status labels, and general vocabulary
    # that commonly appear in Title Case but are not person names.
    _COMMON_TITLE_WORDS: frozenset[str] = frozenset({
        # Document structure
        "Health", "Data", "Lab", "Results", "Active", "Medications",
        "Medical", "Trends", "Panel", "Gaps", "What", "Changed",
        "Since", "Last", "Drug", "Interactions", "Intelligence",
        "Export", "Ready", "Wearable", "Genetic", "Risk", "Profile",
        "Current", "Recent", "Summary", "History", "Review",
        "Daily", "Weekly", "Monthly", "Annual", "Clinical",
        "Patient", "Blood", "Test", "Report", "Total", "High",
        "Low", "Normal", "Healthy", "Complete", "General",
        "Body", "Weight", "Heart", "Rate", "Sleep", "Recovery",
        "Vitamin", "Supplement", "Index", "Score", "Level",
        "Left", "Right", "Upper", "Lower", "New", "Old",
        "First", "Second", "Third", "Primary", "Secondary",
        "Red", "White", "Green", "Black", "Blue",
        "Instructions", "Guidelines", "Protocol", "Context",
        "Discovered", "Correlations", "Hypotheses",
        "Medication", "Therapeutic", "Response",
        "Journal", "Demographics", "Observation",
        # Status and connection
        "Connection", "Status", "Integration", "Validation",
        "Schema", "Evolution", "Table", "Database",
        "System", "Improvement", "Analysis", "Rule",
        "Quality", "Check", "Overdue", "Screening",
        "Action", "Research", "Finding", "Insight",
        "Average", "Averages", "Maximum", "Minimum", "Standard",
        "Not", "Available", "Running", "Installed",
        "Emergency", "Contact", "Information", "Card",
        "Anonymized", "Encrypted", "Processed",
        "All", "Any", "The", "For", "From", "With",
        "This", "That", "These", "Those", "Each",
        "Key", "Important", "Note", "Warning",
        # Time and measurement
        "Day", "Night", "Morning", "Evening", "Hour",
        "Week", "Month", "Year", "Minute", "Strain",
        "Step", "Steps", "Calories", "Distance", "Duration",
        "Variability", "Resting", "Deep", "Light", "Cycle",
        "Peak", "Zone", "Baseline", "Target", "Range",
        # Medical context words — prevents false positives on medical terms
        "Disease", "Syndrome", "Disorder", "Therapy",
        "Receptor", "Enzyme", "Pathway",
        "Deficiency", "Tolerance",
        "Assay", "Diagnosis", "Symptom",
        "Pathology", "Lesion", "Tumor", "Carcinoma", "Biopsy",
        "Chronic", "Acute", "Benign", "Malignant",
        "Serum", "Plasma", "Platelet", "Hemoglobin",
        "Insulin", "Glucose", "Cortisol", "Thyroid",
        "Renal", "Hepatic", "Cardiac", "Pulmonary",
        "Arterial", "Venous", "Lymph", "Neural",
        # Lab company/service words
        "Quest", "Diagnostics", "Laboratory", "Laboratories",
        "National", "Reference", "Services", "Healthcare",
    })

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
        self._cache_lock = threading.Lock()

        # NER circuit breaker state
        self._ner_was_available: bool = False
        self._ner_failure_count: int = 0

        if use_ner:
            self._ner = self._try_init_ner()

        self._ner_was_available = self._ner is not None

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
        with self._cache_lock:
            if len(self._cache) >= self._CACHE_MAX_SIZE:
                # Evict oldest entry (first key in dict — insertion order)
                oldest = next(iter(self._cache))
                del self._cache[oldest]
            self._cache[key] = value

    @property
    def has_ner(self) -> bool:
        """Whether NER layer is active."""
        return self._ner is not None

    def _ner_call_safe(self, text: str) -> list:
        """Call NER with circuit breaker — disable after repeated failures.

        Returns detected entities or empty list on failure. After
        _NER_CIRCUIT_BREAKER_THRESHOLD consecutive failures, NER is
        disabled and a CRITICAL alert is logged.
        """
        if not self._ner:
            return []
        try:
            result = self._ner.detect(text)
            self._ner_failure_count = 0  # Reset on success
            return result
        except Exception as e:
            self._ner_failure_count += 1
            logger.warning("NER call failed (%d/%d): %s",
                           self._ner_failure_count,
                           _NER_CIRCUIT_BREAKER_THRESHOLD, e)
            if self._ner_failure_count >= _NER_CIRCUIT_BREAKER_THRESHOLD:
                logger.critical(
                    "NER circuit breaker tripped — disabling NER after %d "
                    "consecutive failures. Falling back to regex + heuristic.",
                    self._ner_failure_count,
                )
                self._ner = None
                try:
                    from healthbot.security.pii_alert import PiiAlertService
                    svc = PiiAlertService.get_instance()
                    svc.record(
                        category="NER_circuit_break",
                        destination="anonymizer",
                    )
                except (ImportError, OSError, RuntimeError):
                    pass
            return []

    @classmethod
    def _heuristic_name_scan(cls, text: str) -> list[str]:
        """Detect likely person names via Title Case word pairs.

        Fallback for when NER is unavailable. Filters out:
        - Medical eponyms (Graves, Hashimoto, etc.)
        - Known medical terms (from ner_layer.MEDICAL_TERMS)
        - Known ignore texts (from ner_layer.IGNORE_TEXTS)
        - Known two-word medical term pairs (e.g., "Diabetes Mellitus")
        - Pairs where BOTH words are common title/medical words

        Returns list of suspected name strings.
        """
        try:
            from healthbot.security.ner_layer import IGNORE_TEXTS, MEDICAL_TERMS
            medical_terms = MEDICAL_TERMS
            ignore_texts = IGNORE_TEXTS
        except ImportError:
            medical_terms = frozenset()
            ignore_texts: set[str] = set()

        suspects: list[str] = []
        for m in cls._CAPITALIZED_PAIR.finditer(text):
            first, second = m.group(1), m.group(2)
            full = f"{first} {second}"

            # Skip common Title Case header/document words
            if (first in cls._COMMON_TITLE_WORDS
                    or second in cls._COMMON_TITLE_WORDS):
                continue

            # Skip medical eponyms
            if first in cls._MEDICAL_EPONYMS or second in cls._MEDICAL_EPONYMS:
                continue

            # Skip known medical terms
            if (first.lower() in medical_terms
                    or second.lower() in medical_terms
                    or full.lower() in medical_terms):
                continue

            # Skip known ignore texts
            if first in ignore_texts or second in ignore_texts or full in ignore_texts:
                continue

            # Skip known two-word medical term pairs
            if full in cls._MEDICAL_TERM_PAIRS:
                continue

            # Skip if BOTH words are common title/medical words
            if (first in cls._COMMON_TITLE_WORDS
                    and second in cls._COMMON_TITLE_WORDS):
                continue

            suspects.append(full)

        return suspects

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
                        "Ollama canary: SSN in canary text not detected. "
                        "Ollama layer may have reduced accuracy.",
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
        with self._cache_lock:
            if cache_key in self._cache:
                return self._cache[cache_key]

        # Collect all PII spans: (start, end, tag)
        spans: list[tuple[int, int, str]] = []

        # Layer 1: NER (if available) — with circuit breaker
        for e in self._ner_call_safe(text):
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

        # Heuristic name detection (when NER unavailable)
        if not self.has_ner:
            for name in self._heuristic_name_scan(text):
                start = 0
                while True:
                    idx = text.find(name, start)
                    if idx < 0:
                        break
                    spans.append((idx, idx + len(name), "heuristic_name"))
                    start = idx + len(name)

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

        # Layer 1: NER (if available) — with circuit breaker
        for e in self._ner_call_safe(text):
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

        # Heuristic name detection (when NER unavailable)
        if not self.has_ner:
            for name in self._heuristic_name_scan(text):
                start = 0
                while True:
                    idx = text.find(name, start)
                    if idx < 0:
                        break
                    text_hash = hashlib.sha256(
                        name.encode(),
                    ).hexdigest()[:12]
                    pii_spans.append(PiiSpan(
                        start=idx, end=idx + len(name),
                        tag="heuristic_name",
                        layer="regex", confidence=0.7,
                        text_hash=text_hash,
                    ))
                    start = idx + len(name)

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

        # Layer 1: NER (if available) — with circuit breaker
        for e in self._ner_call_safe(text):
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

        # Heuristic name detection (when NER unavailable)
        if not self.has_ner:
            for name in self._heuristic_name_scan(text):
                start = 0
                while True:
                    idx = text.find(name, start)
                    if idx < 0:
                        break
                    text_hash = hashlib.sha256(
                        name.encode(),
                    ).hexdigest()[:12]
                    pii_spans.append(PiiSpan(
                        start=idx, end=idx + len(name),
                        tag="heuristic_name",
                        layer="regex", confidence=0.7,
                        text_hash=text_hash,
                    ))
                    start = idx + len(name)

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

        # NER check — with circuit breaker
        entities = self._ner_call_safe(stripped)
        if entities:
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
                # Ollama ran successfully and found nothing — score unchanged
            except Exception:
                # Ollama failed — cannot confirm text is clean via this layer.
                # Do NOT treat failure as "clean"; reduce confidence to reflect
                # incomplete coverage (avoids inflating score on Ollama failure).
                score -= 0.1
                logger.debug("Ollama score_redaction layer failed, reducing confidence")

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

        # Check NER — filter out field labels and short noise (circuit breaker)
        if self._ner:
            entities = self._ner_call_safe(stripped)
            real = [
                e for e in entities
                if e.text.strip() not in self._FIELD_LABELS
                and len(e.text.strip()) > 2
            ]
            if real:
                labels = {e.label for e in real}
                issues.append(f"NER: {', '.join(labels)}")
        else:
            # Heuristic fallback: detect unlabeled names when NER unavailable
            suspects = self._heuristic_name_scan(stripped)
            if suspects:
                issues.append(
                    f"heuristic_name: {', '.join(suspects[:5])}"
                )

        # Check regex — skip identity-specific patterns (id_* prefix).
        # The anonymize() step already handled identity patterns; re-checking
        # them here causes false positives on medical text (e.g., user's
        # last name matching a medical term like "White" in "white blood cells").
        matches = self._fw.scan(stripped)
        base_matches = [m for m in matches if not m.category.startswith("id_")]
        if base_matches:
            categories = {m.category for m in base_matches}
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
            # Log details for debugging false positives
            logger.warning(
                "assert_safe BLOCKED: issues=%s text_preview=%.80s",
                issues, stripped[:80],
            )

            # Trigger PII alert
            try:
                from healthbot.security.pii_alert import PiiAlertService
                svc = PiiAlertService.get_instance()
                for issue in issues:
                    svc.record(category=issue, destination="outbound")
            except (ImportError, OSError, RuntimeError):
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


def heuristic_name_scan(text: str) -> list[str]:
    """Standalone heuristic name detector for use as a callback.

    Thin wrapper around Anonymizer._heuristic_name_scan.
    Matches the Callable[[str], list[str]] signature expected by
    build_research_packet().
    """
    return Anonymizer._heuristic_name_scan(text)
