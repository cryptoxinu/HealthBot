"""Triage red-flag detection.

Deterministic rule-based classification. No LLM involvement.
Critical value thresholds from standard clinical lab ranges.
Emergency keyword detection for immediate safety responses.
"""
from __future__ import annotations

import re

from healthbot.data.models import LabResult, TriageLevel
from healthbot.normalize.lab_normalizer import normalize_test_name

# Critical value thresholds (immediate clinical concern)
CRITICAL_VALUES: dict[str, dict[str, float]] = {
    "glucose": {"critical_low": 40, "critical_high": 500},
    "potassium": {"critical_low": 2.5, "critical_high": 6.5},
    "sodium": {"critical_low": 120, "critical_high": 160},
    "hemoglobin": {"critical_low": 5.0, "critical_high": 20.0},
    "platelets": {"critical_low": 20, "critical_high": 1000},
    "wbc": {"critical_low": 1.0, "critical_high": 50.0},
    "calcium": {"critical_low": 6.0, "critical_high": 14.0},
    "inr": {"critical_high": 5.0},
    "troponin": {"critical_high": 0.04},
    "creatinine": {"critical_high": 10.0},
    "hba1c": {"critical_high": 14.0},
    "bilirubin": {"critical_high": 15.0},
    "alt": {"critical_high": 1000},
    "ast": {"critical_high": 1000},
}

# Emergency keyword patterns (short-circuit to safety message)
EMERGENCY_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"\b(?:chest\s+pain|chest\s+tightness|heart\s+attack)\b", re.IGNORECASE),
        "If you are experiencing chest pain or tightness, call 911 or go to "
        "the nearest emergency room immediately. Do not wait.",
    ),
    (
        re.compile(
            r"\b(?:difficulty\s+breathing|can'?t\s+breathe|shortness\s+of\s+breath)\b",
            re.IGNORECASE,
        ),
        "If you are having difficulty breathing, call 911 or go to the "
        "nearest emergency room immediately.",
    ),
    (
        re.compile(
            r"\b(?:suicid\w*|kill\s+my\s*self|end\s+my\s+life|want\s+to\s+die|"
            r"self[- ]?harm|hurting\s+my\s*self)\b",
            re.IGNORECASE,
        ),
        "If you or someone you know is in crisis, please contact the "
        "988 Suicide & Crisis Lifeline: call or text 988. "
        "You can also chat at 988lifeline.org. You are not alone.",
    ),
    (
        re.compile(
            r"\b(?:stroke|sudden\s+numbness|face\s+drooping|slurred\s+speech)\b",
            re.IGNORECASE,
        ),
        "If you suspect a stroke (face drooping, arm weakness, speech "
        "difficulty), call 911 immediately. Time is critical.",
    ),
    (
        re.compile(r"\b(?:overdose|took\s+too\s+many|poisoning)\b", re.IGNORECASE),
        "If you suspect an overdose or poisoning, call 911 or Poison "
        "Control (1-800-222-1222) immediately.",
    ),
    (
        re.compile(
            r"\b(?:anaphylax\w*|severe\s+allergic\s+reaction|throat\s+(?:closing|swelling))\b",
            re.IGNORECASE,
        ),
        "If you are experiencing anaphylaxis or a severe allergic reaction "
        "(throat swelling, difficulty breathing), use an EpiPen if available "
        "and call 911 immediately.",
    ),
    (
        re.compile(r"\b(?:seizure|convuls\w*|having\s+a\s+fit)\b", re.IGNORECASE),
        "If someone is having a seizure, keep them safe from injury, do not "
        "restrain them, and call 911 if it lasts more than 5 minutes or they "
        "don't regain consciousness.",
    ),
    (
        re.compile(
            r"\b(?:hemorrhag\w*|uncontrolled\s+bleeding|severe\s+bleeding|"
            r"bleeding\s+(?:won'?t|doesn'?t|cannot|can'?t)\s+stop)\b",
            re.IGNORECASE,
        ),
        "If you are experiencing uncontrolled or severe bleeding, apply direct "
        "pressure and call 911 immediately.",
    ),
]


class TriageEngine:
    """Rule-based triage classification."""

    def classify(
        self,
        lab: LabResult,
        sex: str | None = None,
        age: int | None = None,
    ) -> TriageLevel:
        """Classify a lab result's triage level.

        Priority: CRITICAL > URGENT > WATCH > NORMAL.

        When sex/age are provided, uses demographic-adjusted reference
        ranges as fallback when the PDF didn't include ref ranges.
        """
        canonical = normalize_test_name(lab.test_name)
        value = lab.value

        # Must be numeric to classify — qualitative results use flag
        if not isinstance(value, (int, float)):
            try:
                value = float(str(value))
            except (ValueError, TypeError):
                # Qualitative result: flag "A" means abnormal (e.g.
                # JAK2 mutation "Detected" when reference is "Not Detected")
                if lab.flag and "A" in lab.flag.upper():
                    return TriageLevel.URGENT
                return TriageLevel.NORMAL

        # 1. Check critical value thresholds (population-wide, always)
        if canonical in CRITICAL_VALUES:
            thresholds = CRITICAL_VALUES[canonical]
            if "critical_low" in thresholds and value <= thresholds["critical_low"]:
                return TriageLevel.CRITICAL
            if "critical_high" in thresholds and value >= thresholds["critical_high"]:
                return TriageLevel.CRITICAL

        # 2. Use PDF reference ranges if present; otherwise fall back
        #    to demographic-adjusted ranges
        ref_low = lab.reference_low
        ref_high = lab.reference_high

        if ref_low is None and ref_high is None:
            from healthbot.reasoning.reference_ranges import get_range
            ref = get_range(canonical, sex=sex, age=age)
            if ref:
                ref_low = ref.get("low")
                ref_high = ref.get("high")

        # 3. Check against reference range
        if ref_low is not None and value < ref_low:
            return TriageLevel.URGENT
        if ref_high is not None and value > ref_high:
            return TriageLevel.URGENT

        # 4. Check within 10% of boundary (WATCH zone)
        if ref_high is not None and ref_high > 0:
            boundary_zone = ref_high * 0.10
            if value >= ref_high - boundary_zone:
                return TriageLevel.WATCH
        if ref_low is not None and ref_low > 0:
            boundary_zone = ref_low * 0.10
            if value <= ref_low + boundary_zone:
                return TriageLevel.WATCH

        return TriageLevel.NORMAL

    def classify_batch(
        self,
        labs: list[LabResult],
        sex: str | None = None,
        age: int | None = None,
    ) -> dict[TriageLevel, list[LabResult]]:
        """Classify multiple results, group by triage level."""
        groups: dict[TriageLevel, list[LabResult]] = {
            level: [] for level in TriageLevel
        }
        for lab in labs:
            level = self.classify(lab, sex=sex, age=age)
            lab.triage_level = level
            groups[level].append(lab)
        return groups

    def check_emergency_keywords(self, text: str) -> tuple[TriageLevel | None, str]:
        """Check for emergency keywords. Returns (level, message) or (None, "")."""
        for pattern, message in EMERGENCY_PATTERNS:
            if pattern.search(text):
                return TriageLevel.EMERGENCY, message
        return None, ""

    def get_triage_summary(self, labs: list[LabResult]) -> str:
        """Generate a text summary of triage findings."""
        groups = self.classify_batch(labs)
        lines = []

        for level in [TriageLevel.CRITICAL, TriageLevel.URGENT, TriageLevel.WATCH]:
            items = groups[level]
            if items:
                entries = []
                for lab in items:
                    ref = ""
                    if lab.reference_low is not None and lab.reference_high is not None:
                        ref = f" (ref {lab.reference_low}-{lab.reference_high})"
                    elif lab.reference_high is not None:
                        ref = f" (ref <{lab.reference_high})"
                    elif lab.reference_low is not None:
                        ref = f" (ref >{lab.reference_low})"
                    entries.append(f"{lab.test_name} {lab.value} {lab.unit}{ref}")
                lines.append(f"{level.value.upper()} ({len(items)}): {'; '.join(entries)}")

        normal_count = len(groups[TriageLevel.NORMAL])
        if normal_count:
            lines.append(f"NORMAL ({normal_count}): all within reference ranges")

        return "\n".join(lines) if lines else "No results to triage."

    def get_triage_flagged(self, labs: list[LabResult]) -> dict:
        """Return structured triage data for chat output."""
        groups = self.classify_batch(labs)
        return {
            "critical": groups[TriageLevel.CRITICAL],
            "urgent": groups[TriageLevel.URGENT],
            "watch": groups[TriageLevel.WATCH],
            "normal": groups[TriageLevel.NORMAL],
        }
