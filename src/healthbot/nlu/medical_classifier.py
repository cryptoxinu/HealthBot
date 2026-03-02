"""Deterministic medical relevance classifier.

Identifies messages that are medically relevant and should be
permanently archived in the medical journal. NO LLM calls.
"""
from __future__ import annotations

import re

# Patterns that indicate medical relevance (keep permanently)
_MEDICAL_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE) for p in [
        r"\b(?:doctor|physician|specialist|cardiologist|endocrinologist|"
        r"dermatologist|neurologist|rheumatologist|urologist|oncologist|"
        r"gastroenterologist|appointment|visit|diagnosed|prescription)\b",
        r"\b(?:symptoms?|pains?|aches?|nausea|dizzy|dizziness|fatigue|"
        r"headaches?|migraines?|cramps?|swelling|rash|numbness|tingling|"
        r"shortness of breath|chest pain|palpitations?)\b",
        r"\b(?:medication|medicine|drug|dose|dosage|tablet|"
        r"capsule|supplement|vitamin|probiotic|antibiotic|statin)\b"
        r"|(?:\d+\s*(?:mg|mcg)\b)",
        r"\b(?:lab|test|result|blood|urine|imaging|scan|mri|ct|"
        r"x-?ray|ultrasound|biopsy|ecg|ekg|bloodwork|panel)\b",
        r"\b(?:condition|disease|disorder|syndrome|deficiency|"
        r"infection|inflammation|chronic|acute)\b",
        r"\b(?:surgery|procedure|operation|injection|infusion|"
        r"transplant|dialysis|chemotherapy|radiation)\b",
        r"\b(?:allerg(?:y|ic|ies)|reaction|side effect|adverse|"
        r"intolerance|sensitivity|anaphylaxis)\b",
        r"\b(?:diet|exercise|sleep|weight|blood pressure|heart rate|"
        r"cholesterol|glucose|insulin|thyroid|hormone)\b",
        r"\b(?:family history\s+(?:of|includes?)|runs?\s+in\s+(?:my|the)\s+family|"
        r"hereditary|genetic|(?:my\s+)?(?:parent|mother|father|"
        r"sibling|brother|sister|grandparent)\s+(?:has|had|was|were|diagnosed))\b",
        r"\b(?:pregnant|pregnancy|fertility|menstrual|period|ovulation)\b",
        r"\b(?:ferritin|hemoglobin|hba1c|tsh|ldl|hdl|triglyceride|"
        r"creatinine|alt|ast|egfr|psa|vitamin[_ ]?d|vitamin[_ ]?b12|"
        r"iron|calcium|potassium|sodium|magnesium)\b",
        r"\b(?:anemia|diabetes|prediabetes|hypothyroid|hyperthyroid|"
        r"hypertension|pots|fibromyalgia|ibs|celiac|crohn)\b",
        r"\b(?:depression|anxiety|SSRI|SNRI|antidepressant|therapy|"
        r"counseling|bipolar|ADHD|OCD|PTSD|panic|mental\s+health)\b",
        r"\b(?:mammogram|colonoscopy|PSA|screening|preventive)\b",
    ]
]

# Patterns for non-medical messages (skip archiving)
_SKIP_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE) for p in [
        r"^(?:hi|hello|hey|thanks|thank you|ok|okay|bye|later|"
        r"good morning|good night|sure|yep|nope|got it|cool)\s*[!.?]*$",
        r"^(?:what can you do|how do you work|help|/\w+)\s*$",
    ]
]

# Category classification patterns
_CATEGORY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("symptom_report", re.compile(
        r"\b(?:symptom|pain|ache|nausea|dizzy|fatigue|headache|"
        r"feeling|hurts|cramp|swelling|rash|numbness|tingling|"
        r"shortness of breath|palpitation)\b", re.IGNORECASE,
    )),
    ("medication_change", re.compile(
        r"\b(?:start(?:ed|ing)|stop(?:ped|ping)|switch(?:ed|ing)|"
        r"chang(?:ed|ing)|prescrib|increas|decreas|dose|dosage|"
        r"medication|drug|supplement|mg|mcg)\b", re.IGNORECASE,
    )),
    ("doctor_visit", re.compile(
        r"\b(?:doctor|physician|specialist|appointment|visit|"
        r"told me|said (?:I|my)|diagnosed|referr)\b", re.IGNORECASE,
    )),
    ("lab_discussion", re.compile(
        r"\b(?:lab|test|result|blood|panel|came back|levels?|"
        r"range|normal|abnormal|high|low|flagged)\b", re.IGNORECASE,
    )),
    ("wearable_query", re.compile(
        r"\b(?:hrv|heart rate variability|rhr|resting heart rate|"
        r"sleep\s*(?:score|quality|duration|efficiency|latency)?|"
        r"recovery(?:\s*score)?|strain|whoop|oura|wearable|"
        r"steps|workout|readiness|deep sleep|rem\b|spo2|"
        r"skin temp|respiratory rate)\b", re.IGNORECASE,
    )),
]


def is_medically_relevant(text: str) -> bool:
    """Deterministic check — NO LLM. Returns True if message likely health-related."""
    if not text or len(text.strip()) < 3:
        return False

    # Skip obvious non-medical messages
    for pattern in _SKIP_PATTERNS:
        if pattern.match(text.strip()):
            return False

    # Check medical patterns
    for pattern in _MEDICAL_PATTERNS:
        if pattern.search(text):
            return True

    return False


def classify_medical_category(text: str) -> str:
    """Classify into: symptom_report, medication_change, doctor_visit, lab_discussion, general."""
    for category, pattern in _CATEGORY_PATTERNS:
        if category == "lab_discussion":
            # Require 2+ medical terms to classify as lab_discussion
            # to avoid false positives on common words like "high" or "low"
            matches = pattern.findall(text)
            if len(matches) >= 2:
                return category
        elif pattern.search(text):
            return category
    return "general"
