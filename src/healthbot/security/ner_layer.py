"""GLiNER-based Named Entity Recognition for PII detection.

Recommended dependency. If gliner is not installed, is_available() returns False
and the Anonymizer falls back to regex-only (reduced PII coverage).

All processing is local. No API calls. The model runs on-device via ONNX/PyTorch.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger("healthbot")

# Lazy import — gliner is a recommended dependency (not required)
_GLINER_AVAILABLE = False
try:
    from gliner import GLiNER  # type: ignore[import-untyped]

    _GLINER_AVAILABLE = True
except ImportError:
    pass

# Default model — good balance of speed and accuracy (~500MB)
DEFAULT_MODEL = "urchade/gliner_medium-v2.1"

# Entity labels we ask GLiNER to detect
NER_LABELS = [
    "person",
    "location",
    "organization",
    "email",
    "phone number",
    "social security number",
]

# Minimum confidence score to consider an entity as PII
MIN_CONFIDENCE = 0.4

# Medical terms that NER frequently misclassifies as person/location/org.
MEDICAL_TERMS: frozenset[str] = frozenset({
    # Vitamins & supplements (NER: "organization" or "person")
    "vitamin a", "vitamin b", "vitamin b6", "vitamin b12", "vitamin c",
    "vitamin d", "vitamin e", "vitamin k", "folate", "folic acid",
    "biotin", "niacin", "riboflavin", "thiamine", "cobalamin",
    "melatonin", "collagen", "creatine", "glutamine", "coq10",
    # Minerals (NER: "person")
    "iron", "calcium", "zinc", "selenium", "magnesium", "potassium",
    "sodium", "phosphorus", "copper", "manganese", "chromium",
    "iodine", "boron",
    # Lab tests (NER: "organization")
    "cbc", "cmp", "bmp", "tsh", "a1c", "hba1c", "ldl", "hdl",
    "alt", "ast", "alp", "ggt", "bun", "egfr", "psa", "crp",
    "esr", "ana", "rf", "bnp", "inr", "pth", "dhea", "igf",
    "shbg", "acth", "cortisol", "insulin", "ferritin", "transferrin",
    "hemoglobin", "hematocrit", "platelets", "fibrinogen",
    "triglycerides", "cholesterol", "creatinine", "albumin",
    "bilirubin", "globulin", "procalcitonin", "lipase", "amylase",
    "troponin", "homocysteine", "methylmalonic acid",
    # Conditions (NER: "organization" or "location")
    "anemia", "diabetes", "prediabetes", "hypothyroidism",
    "hyperthyroidism", "hypertension", "hypotension",
    "polycythemia", "thrombocytosis", "leukocytosis",
    "pots", "eds", "mcas", "celiac", "crohn", "ibs",
    "gerd", "nafld", "pcos", "lupus", "fibromyalgia",
    "osteoporosis", "osteopenia", "arthritis", "psoriasis",
    "eczema", "asthma", "copd", "apnea",
    # Body parts / organs (NER: "location")
    "thyroid", "liver", "kidney", "pancreas", "spleen",
    "adrenal", "pituitary", "prostate", "ovary", "uterus",
    # Medical terms (NER: various)
    "inflammation", "deficiency", "malabsorption", "resistance",
    "metabolic", "autoimmune", "chronic", "acute", "benign",
    "pathology", "etiology", "prognosis", "remission",
    # Genetic terms (NER: "person" or "organization")
    "jak2", "calr", "mpl", "mthfr", "brca", "apoe", "cyp2d6",
    "factor v leiden", "hla", "snp",
})

# Conversation labels and common terms that should never be flagged as PII
IGNORE_TEXTS: set[str] = {
    "User", "You", "I", "Assistant", "HealthBot",
    "user", "you", "assistant", "healthbot",
    # Wearable/service brands that appear in health data context
    "WHOOP", "Whoop", "Oura", "Fitbit", "Garmin", "Apple Health",
}

# Person entities starting with these words are references, not names
_REFERENCE_PREFIXES = re.compile(
    r"^(?:my|your|the|his|her|our|their|a|an)\s",
    re.IGNORECASE,
)

# Medical value spans — regex patterns that identify medical data.
# Any NER entity overlapping these spans is filtered out.
MEDICAL_VALUE_PATTERNS: list[re.Pattern[str]] = [
    # Lab results: "Glucose: 105 mg/dL", "HbA1c 5.7%"
    re.compile(
        r"\b(?:Glucose|Hemoglobin|HbA1c|A1c|Cholesterol|LDL|HDL|Triglycerides"
        r"|Creatinine|BUN|TSH|T3|T4|Insulin|Cortisol|Iron|Ferritin"
        r"|Calcium|Potassium|Sodium|Chloride|Magnesium|Phosphorus"
        r"|Albumin|Bilirubin|ALT|AST|ALP|GGT|Platelets|WBC|RBC"
        r"|Hematocrit|MCV|MCH|MCHC|RDW|MPV|CRP|ESR"
        r"|Vitamin\s*[ABDEK]\d*|B12|Folate|Zinc|Selenium"
        r"|PSA|ANA|RF|Anti.CCP|Troponin|BNP|D.Dimer"
        r"|Uric\s*Acid|Procalcitonin|Fibrinogen|Lipase|Amylase"
        r")\s*[:#]?\s*[\d.,]+\s*(?:%|mg/dL|g/dL|mmol/L|mIU/L|ng/mL|"
        r"pg/mL|mcg/dL|IU/L|U/L|cells/uL|K/uL|M/uL|fL|pg|g/L|"
        r"mmHg|bpm|ms|mEq/L|umol/L|/uL)?",
        re.IGNORECASE,
    ),
    # Wearable metrics: "HRV:42", "RHR:68", "Recovery:65"
    re.compile(
        r"\b(?:HRV|RHR|Recovery|Sleep|Strain|SpO2|Heart\s*Rate|Resp\s*Rate)"
        r"\s*[:#]?\s*[\d.]+",
        re.IGNORECASE,
    ),
    # Medication patterns: "Metformin 500mg", "Atorvastatin 20mg"
    re.compile(
        r"\b(?:Metformin|Atorvastatin|Lisinopril|Levothyroxine|Amlodipine"
        r"|Omeprazole|Losartan|Gabapentin|Hydrochlorothiazide|Simvastatin"
        r"|Pantoprazole|Montelukast|Escitalopram|Rosuvastatin|Bupropion"
        r"|Furosemide|Prednisone|Tamsulosin|Duloxetine|Sertraline"
        r"|Pravastatin|Carvedilol|Warfarin|Clopidogrel|Aspirin"
        r")\b",
        re.IGNORECASE,
    ),
    # Reference ranges: "(ref 70-100)", "(ref 4.0-5.6)"
    re.compile(r"\(ref\s+[\d.]+-[\d.]+\)", re.IGNORECASE),
    # Standalone medical conditions and terms (no numeric value required).
    # Catches NER spans like "Iron deficiency anemia" via keyword overlap.
    re.compile(
        r"\b(?:deficiency|insufficiency|resistance|syndrome|disorder"
        r"|anemia|diabetes|hypothyroid|hyperthyroid|hypertension"
        r"|polycythemia|thrombocytosis|inflammation|malabsorption"
        r"|autoimmune|remission|pathology)\b",
        re.IGNORECASE,
    ),
]


@dataclass
class NerEntity:
    """A detected named entity."""

    label: str
    text: str
    start: int
    end: int
    score: float


class NerLayer:
    """GLiNER-based NER for intelligent PII detection.

    Loads the model once at construction. Subsequent calls are fast (~100ms).
    All inference is local — no API calls.

    The GLiNER model (~500MB) is cached at the class level so multiple
    NerLayer instances (e.g. main anonymizer + sync anonymizer) share one
    loaded model without the memory/speed cost of reloading.
    """

    _model_cache: dict[str, object] = {}  # class-level cache: model_name -> GLiNER

    def __init__(self, model_name: str = DEFAULT_MODEL) -> None:
        if not _GLINER_AVAILABLE:
            raise RuntimeError("gliner is not installed. Run: pip install gliner")

        if model_name in NerLayer._model_cache:
            self._model = NerLayer._model_cache[model_name]
            logger.info("NER model '%s' reused from cache.", model_name)
        else:
            logger.info("Loading NER model '%s'...", model_name)
            self._model = GLiNER.from_pretrained(model_name)
            NerLayer._model_cache[model_name] = self._model
            logger.info("NER model loaded.")
        self._labels = list(NER_LABELS)
        self._known_names: set[str] = set()

    def set_known_names(self, names: set[str]) -> None:
        """Set known names that bypass MIN_CONFIDENCE threshold.

        Known names are always detected by _filter_medical regardless of
        score, and are also scanned via simple substring matching as a
        fallback when GLiNER doesn't flag them at all.
        """
        self._known_names = {n for n in names if n and len(n) >= 3}
        if self._known_names:
            logger.info("NER known names loaded: %d entries", len(self._known_names))

    # GLiNER truncates at ~384 tokens. Chunk longer texts with overlap.
    _CHUNK_SIZE = 300  # chars per chunk (conservative to stay within token limit)
    _CHUNK_OVERLAP = 50  # chars overlap to avoid splitting entities at boundaries

    def detect(self, text: str) -> list[NerEntity]:
        """Detect PII entities in text, filtering out medical false positives.

        Long texts are chunked with overlap to handle GLiNER's 384-token limit.
        """
        if len(text) <= self._CHUNK_SIZE:
            raw_entities = self._predict(text, offset=0)
        else:
            raw_entities = self._predict_chunked(text)

        return self._filter_medical(text, raw_entities)

    def _predict(self, text: str, offset: int = 0) -> list[NerEntity]:
        """Run NER on a single text chunk."""
        raw = self._model.predict_entities(text, self._labels, threshold=MIN_CONFIDENCE)
        entities = []
        for e in raw:
            entities.append(NerEntity(
                label=e["label"],
                text=e["text"],
                start=e["start"] + offset,
                end=e["end"] + offset,
                score=e["score"],
            ))
        return entities

    # Sentence boundary pattern — prefer splitting between sentences
    _SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")

    def _predict_chunked(self, text: str) -> list[NerEntity]:
        """Split text into overlapping chunks and merge NER results.

        Prefers sentence boundaries to avoid splitting entities across chunks.
        """
        all_entities: list[NerEntity] = []
        start = 0

        while start < len(text):
            end = min(start + self._CHUNK_SIZE, len(text))

            # Try to break at a sentence boundary first, then fall back
            # to newline/space to avoid splitting words or names
            if end < len(text):
                search_start = start + self._CHUNK_SIZE // 2
                best_break = -1

                # Priority 1: Sentence boundary (". Capital")
                for m in self._SENTENCE_BREAK.finditer(text, search_start, end):
                    best_break = m.start()

                # Priority 2: Newline
                if best_break < start:
                    brk = text.rfind("\n", search_start, end)
                    if brk > start:
                        best_break = brk + 1

                # Priority 3: Space
                if best_break < start:
                    brk = text.rfind(" ", search_start, end)
                    if brk > start:
                        best_break = brk + 1

                if best_break > start:
                    end = best_break

            chunk = text[start:end]
            chunk_entities = self._predict(chunk, offset=start)
            all_entities.extend(chunk_entities)

            # Stop if we've reached the end
            if end >= len(text):
                break

            start = end - self._CHUNK_OVERLAP

        # Deduplicate entities from overlapping regions
        return self._dedup_entities(all_entities)

    def redact(self, text: str) -> tuple[str, bool]:
        """Detect and redact PII entities. Returns (cleaned_text, had_pii)."""
        entities = self.detect(text)
        if not entities:
            return text, False

        # Sort by position descending so replacements don't shift indices
        entities.sort(key=lambda e: e.start, reverse=True)
        result = text
        for e in entities:
            tag = e.label.replace(" ", "_")
            result = result[: e.start] + f"[NER-{tag}]" + result[e.end :]

        return result, True

    @staticmethod
    def _dedup_entities(entities: list[NerEntity]) -> list[NerEntity]:
        """Remove duplicate entities from overlapping chunks.

        Overlap-ratio: if two entities with the same label overlap by >50%
        of the shorter one's length, merge (keep higher score, widest span).
        """
        if not entities:
            return []
        # Sort by start, then by end descending (longest first)
        entities.sort(key=lambda e: (e.start, -e.end))
        deduped: list[NerEntity] = [entities[0]]
        for e in entities[1:]:
            prev = deduped[-1]
            # Calculate overlap
            overlap = max(0, min(prev.end, e.end) - max(prev.start, e.start))
            shorter_len = min(prev.end - prev.start, e.end - e.start)
            overlap_ratio = overlap / shorter_len if shorter_len > 0 else 0

            if overlap_ratio > 0.5 and e.label == prev.label:
                # Merge: widest span, highest score
                merged_start = min(prev.start, e.start)
                merged_end = max(prev.end, e.end)
                winner = e if e.score > prev.score else prev
                deduped[-1] = NerEntity(
                    label=winner.label,
                    text=winner.text,
                    start=merged_start,
                    end=merged_end,
                    score=max(prev.score, e.score),
                )
            else:
                deduped.append(e)
        return deduped

    # Window size for context-aware confidence scoring
    _CONTEXT_WINDOW = 100

    def _filter_medical(self, text: str, entities: list[NerEntity]) -> list[NerEntity]:
        """Remove entities that overlap with medical values or are conversation labels.

        Entities matching known names are NEVER filtered out — identity-aware
        detection takes priority over medical false-positive suppression.

        For person/organization entities, applies context-aware confidence:
        if 3+ medical terms appear within a 100-char window, confidence is
        reduced by 30%. If it drops below MIN_CONFIDENCE, the entity is
        filtered out. This catches cases like "Iron deficiency anemia" where
        NER flags "Iron" as a person — the dense medical context signals
        it's not a real name.
        """
        # Build list of medical value spans
        medical_spans: list[tuple[int, int]] = []
        for pattern in MEDICAL_VALUE_PATTERNS:
            for m in pattern.finditer(text):
                medical_spans.append((m.start(), m.end()))

        # Pre-tokenize text_lower for context scoring
        text_lower = text.lower()

        filtered = []
        for e in entities:
            # Known names always pass — never filtered
            if self._is_known_name(e.text):
                filtered.append(e)
                continue

            # Skip entities whose text is a known medical term
            if e.text.strip().lower() in MEDICAL_TERMS:
                continue

            # Skip conversation labels
            if e.text.strip() in IGNORE_TEXTS:
                continue

            # Skip person entities that are references ("my sister"),
            # not actual names ("Sarah Johnson")
            if e.label == "person" and _REFERENCE_PREFIXES.match(e.text):
                continue

            # Skip entities overlapping medical value spans
            if any(e.start < ms_end and e.end > ms_start for ms_start, ms_end in medical_spans):
                continue

            # Context-aware confidence for person/organization entities:
            # dense medical context reduces confidence (likely a false positive)
            if e.label in ("person", "organization"):
                window_start = max(0, e.start - self._CONTEXT_WINDOW)
                window_end = min(len(text), e.end + self._CONTEXT_WINDOW)
                window = text_lower[window_start:window_end]
                med_count = sum(1 for term in MEDICAL_TERMS if term in window)
                if med_count >= 3:
                    adjusted = e.score * 0.7  # 30% reduction
                    if adjusted < MIN_CONFIDENCE:
                        continue

            filtered.append(e)

        return filtered

    def _is_known_name(self, entity_text: str) -> bool:
        """Check if entity text matches any known name (case-insensitive)."""
        if not self._known_names:
            return False
        text_lower = entity_text.strip().lower()
        return any(name.lower() == text_lower for name in self._known_names)

    @staticmethod
    def is_available() -> bool:
        """Check if GLiNER is installed and a model can be loaded."""
        return _GLINER_AVAILABLE
