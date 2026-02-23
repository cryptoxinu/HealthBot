"""Deterministic data extraction from photo descriptions.

Takes the text description from the vision model (Stage 1) and
extracts structured data: medication info, lab values, etc.
All logic is deterministic (regex). No LLM calls.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger("healthbot")


@dataclass
class PhotoClassification:
    """Classification result for a photo description."""

    content_type: str   # "medication_bottle", "lab_printout", "general_health"
    confidence: float   # 0.0-1.0
    extracted_meds: list[ExtractedMedication] = field(default_factory=list)
    extracted_labs: list[ExtractedLabValue] = field(default_factory=list)


@dataclass
class ExtractedMedication:
    """A medication extracted from a photo description."""

    name: str
    dose: str = ""
    unit: str = ""
    form: str = ""     # "tablet", "capsule", "liquid"


@dataclass
class ExtractedLabValue:
    """A lab value extracted from a photo description."""

    test_name: str
    value: str
    unit: str = ""


# Patterns that indicate medication packaging
_MED_KEYWORDS = re.compile(
    r"\b(?:pill|tablet|capsule|bottle|medication|prescription|rx|"
    r"pharmacy|drug\s+facts|supplement|vitamin|dosage|"
    r"oral|mg|mcg|milligram|microgram)\b",
    re.IGNORECASE,
)

# Patterns that indicate lab results / medical documents
_LAB_KEYWORDS = re.compile(
    r"\b(?:lab\s+result|blood\s+test|blood\s+work|test\s+result|"
    r"reference\s+range|normal\s+range|specimen|"
    r"patient|accession|collected|reported|"
    r"chemistry|hematology|lipid\s+panel|metabolic\s+panel|"
    r"cbc|bmp|cmp|urinalysis)\b",
    re.IGNORECASE,
)

# Common drug names for extraction
_DRUG_NAMES = re.compile(
    r"\b("
    r"atorvastatin|rosuvastatin|simvastatin|pravastatin|"
    r"metformin|lisinopril|losartan|amlodipine|"
    r"levothyroxine|synthroid|"
    r"omeprazole|pantoprazole|esomeprazole|"
    r"metoprolol|atenolol|propranolol|carvedilol|"
    r"sertraline|fluoxetine|escitalopram|citalopram|"
    r"gabapentin|pregabalin|"
    r"prednisone|prednisolone|"
    r"amoxicillin|azithromycin|ciprofloxacin|"
    r"ibuprofen|acetaminophen|aspirin|naproxen|"
    r"insulin|glipizide|glimepiride|"
    r"warfarin|apixaban|rivaroxaban|"
    r"hydrochlorothiazide|furosemide|spironolactone|"
    r"montelukast|albuterol|"
    r"vitamin\s+d|vitamin\s+b12|vitamin\s+c|"
    r"fish\s+oil|omega-?3|iron|magnesium|zinc|calcium|folate|folic\s+acid"
    r")\b",
    re.IGNORECASE,
)

# Dose extraction: "500 mg", "50 mcg", "10 mg/mL"
_DOSE_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(mg|mcg|g|ml|iu|units?|mEq)\b",
    re.IGNORECASE,
)

# Lab value extraction: "Glucose 95 mg/dL", "TSH 2.5 mIU/L"
_LAB_VALUE_RE = re.compile(
    r"\b("
    r"glucose|hemoglobin|hematocrit|white\s+blood|red\s+blood|platelet|"
    r"sodium|potassium|chloride|bicarbonate|calcium|magnesium|phosphorus|"
    r"creatinine|bun|urea|gfr|egfr|"
    r"alt|ast|alp|ggt|bilirubin|albumin|"
    r"ldl|hdl|total\s+cholesterol|triglyceride|"
    r"tsh|free\s+t4|free\s+t3|t4|t3|"
    r"hba1c|a1c|hemoglobin\s+a1c|"
    r"iron|ferritin|tibc|transferrin|"
    r"vitamin\s+d|vitamin\s+b12|folate|"
    r"psa|crp|esr|bnp|troponin|"
    r"uric\s+acid|inr|ptt|"
    r"wbc|rbc|mcv|mch|mchc|rdw|mpv"
    r")\s*:?\s*"
    r"(\d+(?:\.\d+)?)\s*"
    r"([a-zA-Z/%]+(?:/[a-zA-Z]+)?)?",
    re.IGNORECASE,
)


def classify_photo(description: str) -> PhotoClassification:
    """Classify photo content from its text description.

    Args:
        description: Text description from the vision model.

    Returns:
        PhotoClassification with content type and extracted data.
    """
    med_matches = len(_MED_KEYWORDS.findall(description))
    lab_matches = len(_LAB_KEYWORDS.findall(description))

    # Classify based on keyword density
    if lab_matches >= 3:
        content_type = "lab_printout"
        confidence = min(1.0, lab_matches / 5)
    elif med_matches >= 2:
        content_type = "medication_bottle"
        confidence = min(1.0, med_matches / 4)
    else:
        content_type = "general_health"
        confidence = 0.5

    result = PhotoClassification(
        content_type=content_type,
        confidence=confidence,
    )

    if content_type == "medication_bottle":
        result.extracted_meds = extract_medications(description)
    elif content_type == "lab_printout":
        result.extracted_labs = extract_lab_values(description)

    return result


def extract_medications(description: str) -> list[ExtractedMedication]:
    """Extract medication names and doses from a description."""
    meds: list[ExtractedMedication] = []
    seen_names: set[str] = set()

    for match in _DRUG_NAMES.finditer(description):
        name = match.group(1).strip()
        name_lower = name.lower()
        if name_lower in seen_names:
            continue
        seen_names.add(name_lower)

        # Look for dose near the drug name (within 50 chars)
        start = max(0, match.start() - 20)
        end = min(len(description), match.end() + 50)
        context = description[start:end]

        dose = ""
        unit = ""
        dose_match = _DOSE_RE.search(context)
        if dose_match:
            dose = dose_match.group(1)
            unit = dose_match.group(2)

        # Detect form
        form = ""
        if re.search(r"\btablet\b", context, re.IGNORECASE):
            form = "tablet"
        elif re.search(r"\bcapsule\b", context, re.IGNORECASE):
            form = "capsule"
        elif re.search(r"\bliquid\b", context, re.IGNORECASE):
            form = "liquid"

        meds.append(ExtractedMedication(
            name=name.title(),
            dose=dose,
            unit=unit.lower() if unit else "",
            form=form,
        ))

    return meds


def extract_lab_values(description: str) -> list[ExtractedLabValue]:
    """Extract lab test values from a description."""
    labs: list[ExtractedLabValue] = []
    seen: set[str] = set()

    for match in _LAB_VALUE_RE.finditer(description):
        name = match.group(1).strip()
        value = match.group(2)
        unit = match.group(3) or ""

        name_lower = name.lower()
        if name_lower in seen:
            continue
        seen.add(name_lower)

        labs.append(ExtractedLabValue(
            test_name=name.title(),
            value=value,
            unit=unit,
        ))

    return labs


def format_extraction_summary(classification: PhotoClassification) -> str:
    """Format what was extracted from the photo for user confirmation."""
    if classification.content_type == "medication_bottle":
        if not classification.extracted_meds:
            return ""
        lines = ["Detected medication:"]
        for med in classification.extracted_meds:
            parts = [med.name]
            if med.dose:
                parts.append(f"{med.dose} {med.unit}".strip())
            if med.form:
                parts.append(f"({med.form})")
            lines.append(f"  - {' '.join(parts)}")
        lines.append(
            "\nSay 'store this' to add to your medication list.",
        )
        return "\n".join(lines)

    if classification.content_type == "lab_printout":
        if not classification.extracted_labs:
            return ""
        lines = [f"Detected {len(classification.extracted_labs)} lab values:"]
        for lab in classification.extracted_labs:
            unit_str = f" {lab.unit}" if lab.unit else ""
            lines.append(f"  - {lab.test_name}: {lab.value}{unit_str}")
        lines.append(
            "\nSay 'store these results' to save to your records.",
        )
        return "\n".join(lines)

    return ""
