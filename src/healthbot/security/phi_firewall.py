"""PHI (Protected Health Information) regex-based detection.

All patterns are deterministic regex. No LLM involvement.
Detects: SSN, MRN, phone numbers, email addresses, dates of birth,
labeled name patterns, and exact lab values in context.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


@dataclass
class PhiMatch:
    """A detected PHI occurrence."""

    category: str
    start: int
    end: int
    text: str


# Compiled regex patterns
PHI_PATTERNS: dict[str, re.Pattern[str]] = {
    # SSN: 3-2-4 format, excluding invalid area numbers (000, 666, 900-999)
    "ssn": re.compile(
        r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"
    ),
    "mrn": re.compile(
        r"\b(?:MRN|MR#|Medical\s*Record)\s*[:#]?\s*\d{6,12}\b", re.IGNORECASE
    ),
    "phone_us": re.compile(
        r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
    ),
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "dob_slash": re.compile(
        r"\b(?:0?[1-9]|1[0-2])[/](?:0?[1-9]|[12]\d|3[01])[/](?:19|20)\d{2}\b"
    ),
    "dob_labeled": re.compile(
        r"\b(?:DOB|Date\s+of\s+Birth|Born|D\.O\.B|birthday)\s*[:#]?\s*"
        r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
        re.IGNORECASE,
    ),
    # Labeled: "Patient: John Smith", "Name: Jane Doe", "patient: john smith"
    "name_labeled": re.compile(
        r"\b(?:Patient|Name|Pt|Patient\s+Name)\s*[:#]\s*"
        r"[A-Za-z][a-z]+(?:\s+[A-Za-z]\.?)?\s+[A-Za-z][a-z]+\b",
        re.IGNORECASE,
    ),
    # Labeled ALL-CAPS: "Patient: SMITH, JOHN A" or "Patient: JOHN SMITH"
    "name_labeled_caps": re.compile(
        r"\b(?:Patient|Name|Pt|Patient\s+Name)\s*[:#]\s*"
        r"[A-Z]{2,}(?:[,\s]+[A-Z]{2,})+(?:\s+[A-Z]\.?)?\b"
    ),
    # Self-introduction: "My name is John", "I'm John Smith", "I am Sarah Jones"
    # Includes common non-English intro patterns.
    # Intro phrases are case-insensitive via (?i:...), but the name part
    # requires Title Case to avoid matching "I am taking" or "I am allergic".
    "name_intro": re.compile(
        r"(?i:\b(?:my name is|I'm|I am"
        r"|me llamo|mi nombre es"        # Spanish
        r"|je m'appelle|je suis"         # French
        r"|ich bin|mein Name ist"        # German
        r"|mi chiamo|io sono"            # Italian
        r"|eu sou|meu nome [eé]"         # Portuguese
        r"))\s+"
        r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b",
    ),
    # Doctor/provider names: "Dr. Sarah Johnson", "Doctor Jane Doe"
    "doctor_name": re.compile(
        r"\b(?:Dr|Doctor|Prof|Professor)\.?\s+"
        r"[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+\b"
    ),
    # Doctor ALL-CAPS: "DR. SMITH", "DR WILSON"
    "doctor_name_caps": re.compile(
        r"\b(?:DR|DOCTOR|PROF|PROFESSOR)\.?\s+[A-Z]{2,}(?:\s+[A-Z]{2,})?\b"
    ),
    # Provider-labeled names: "Provider: Dr. Jane Doe", "PCP: Smith"
    "provider_labeled": re.compile(
        r"\b(?:Provider|Referring|Ordering|PCP|Physician)\s*[:#]\s*"
        r"(?:Dr\.?\s+)?[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s*[A-Z]?[a-z]*\b"
    ),
    # Provider ALL-CAPS: "Ordering Provider: WILSON, SARAH MD"
    "provider_labeled_caps": re.compile(
        r"\b(?:Provider|Referring|Ordering|PCP|Physician)\s*[:#]\s*"
        r"(?:DR\.?\s+)?[A-Z]{2,}(?:[,\s]+[A-Z]{2,})*(?:\s+(?:MD|DO|NP|PA|RN))?\b",
        re.IGNORECASE,
    ),
    # Account/Patient ID: "Account #: 12345678", "Patient ID: ABC12345"
    "account_id": re.compile(
        r"\b(?:Account|Acct|Patient\s*ID|PID|Encounter|Visit)\s*"
        r"(?:#\s*:?\s*|[:#]\s*)"
        r"[A-Z0-9][A-Z0-9-]{4,20}\b",
        re.IGNORECASE,
    ),
    "address": re.compile(
        r"\b\d{1,5}\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+"
        r"(?:St|Ave|Blvd|Dr|Ln|Rd|Way|Ct|Pl|Circle|Terr)\.?\b",
        re.IGNORECASE,
    ),
    # ZIP only with context (avoids false positives on 5-digit lab values)
    "zip_code": re.compile(
        r"\b(?:zip|ZIP|Zip)\s*[:#]?\s*\d{5}(?:-\d{4})?\b"
        r"|\b(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA"
        r"|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR"
        r"|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\s+\d{5}(?:-\d{4})?\b"
    ),
    # DOB with text month: "born on January 15, 1998", "birthday: March 3, 2000"
    "dob_text": re.compile(
        r"\b(?:born\s+(?:on\s+)?|birthday\s*[:#]?\s*)"
        r"(?:January|February|March|April|May|June|July|August"
        r"|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b",
        re.IGNORECASE,
    ),
    # DOB with ISO format: "born: 1998-01-15", "DOB: 1998-01-15"
    "dob_iso": re.compile(
        r"\b(?:DOB|Date\s+of\s+Birth|Born|D\.O\.B|birthday)\s*[:#]?\s*"
        r"(?:19|20)\d{2}[-/]\d{1,2}[-/]\d{1,2}\b",
        re.IGNORECASE,
    ),
    # Insurance/policy IDs: "Insurance ID: ABC12345678"
    "insurance_id": re.compile(
        r"\b(?:Insurance|Policy|Member|Subscriber|Group)"
        r"\s*(?:ID|#|Number|No)\s*[:#]?\s*[A-Z0-9][A-Z0-9-]{4,20}\b",
        re.IGNORECASE,
    ),
}


class PhiDetectedError(Exception):
    """Raised when PHI is detected in a context where it must not exist."""


# Lab report date labels — dates following these are clinical metadata, NOT PHI.
# Used to suppress false positives from the broad dob_slash pattern.
_SAFE_DATE_PREFIX = re.compile(
    r"(?:Date\s+)?(?:Collected|Received|Reported|Ordered|Printed|Released"
    r"|Verified|Resulted|Drawn|Accessioned|Entered|Reviewed|Approved"
    r"|Completed|Finalized|Specimen|of\s+Service|of\s+Report)\s*[:#]?\s*$",
    re.IGNORECASE,
)

# Lab ID prefixes — phone-like numbers preceded by these are lab identifiers, not phones.
_LAB_ID_PREFIX = re.compile(
    r"(?:Specimen|Accession|Lab|ID|#)\s*[:#]?\s*$",
    re.IGNORECASE,
)

# DOB context keywords — the standalone dob_slash pattern (MM/DD/YYYY) only
# matches when one of these keywords is within 60 chars of the date.
_DOB_CONTEXT = re.compile(
    r"\b(?:born|birthday|DOB|D\.O\.B|date\s+of\s+birth|age|"
    r"Patient|Name|Pt)\b",
    re.IGNORECASE,
)


class PhiFirewall:
    """Scans text for PHI patterns and provides redaction."""

    def __init__(self, extra_patterns: dict[str, re.Pattern[str]] | None = None) -> None:
        self._patterns = dict(PHI_PATTERNS)
        if extra_patterns:
            self._patterns.update(extra_patterns)

    def add_patterns(self, extra_patterns: dict[str, re.Pattern[str]]) -> None:
        """Add identity-specific patterns (name, DOB) to this firewall instance.

        Called at vault unlock after loading the identity profile.  Updates
        the live instance in-place so every component sharing this reference
        (HandlerCore, CleanSync, Claude conversation, log scrubber) sees
        the new patterns immediately.
        """
        self._patterns.update(extra_patterns)

    def clear_identity_patterns(self) -> None:
        """Remove identity-profile patterns added at vault unlock.

        All identity patterns from _compile_name_patterns() use the ``id_``
        prefix (e.g. ``id_full_name_forward``, ``id_full_name_last``).
        Base PHI patterns (ssn, phone, email, etc.) don't use this prefix.
        """
        self._patterns = {k: v for k, v in list(self._patterns.items())
                          if not k.startswith("id_")}

    def scan(self, text: str) -> list[PhiMatch]:
        """Return all PHI matches found in text."""
        # Normalize Unicode to catch homoglyph evasion (e.g. fullwidth digits)
        text = unicodedata.normalize("NFKC", text)
        matches: list[PhiMatch] = []
        for category, pattern in self._patterns.items():
            for m in pattern.finditer(text):
                # Skip dates that follow lab report labels (clinical metadata, not PHI)
                # Also require DOB context keywords nearby to reduce false positives
                if category == "dob_slash":
                    preceding = text[max(0, m.start() - 40):m.start()]
                    if _SAFE_DATE_PREFIX.search(preceding):
                        continue
                    window = text[max(0, m.start() - 60):min(len(text), m.end() + 60)]
                    if not _DOB_CONTEXT.search(window):
                        continue
                # Skip phone-like numbers preceded by lab ID labels
                if category == "phone_us":
                    preceding = text[max(0, m.start() - 30):m.start()]
                    if _LAB_ID_PREFIX.search(preceding):
                        continue
                matches.append(
                    PhiMatch(
                        category=category,
                        start=m.start(),
                        end=m.end(),
                        text=m.group(),
                    )
                )
        # Sort by position
        matches.sort(key=lambda m: m.start)
        return matches

    def contains_phi(self, text: str) -> bool:
        """Quick check: does text contain any PHI?"""
        # Normalize Unicode to catch homoglyph evasion (e.g. fullwidth digits)
        text = unicodedata.normalize("NFKC", text)
        for category, pattern in self._patterns.items():
            if category == "dob_slash":
                # Apply same safe-date-prefix + DOB context suppression as scan()
                for m in pattern.finditer(text):
                    preceding = text[max(0, m.start() - 40):m.start()]
                    if _SAFE_DATE_PREFIX.search(preceding):
                        continue
                    window = text[max(0, m.start() - 60):min(len(text), m.end() + 60)]
                    if _DOB_CONTEXT.search(window):
                        return True
            elif category == "phone_us":
                # Apply same lab-ID-prefix suppression as scan()
                for m in pattern.finditer(text):
                    preceding = text[max(0, m.start() - 30):m.start()]
                    if not _LAB_ID_PREFIX.search(preceding):
                        return True
            elif pattern.search(text):
                return True
        return False

    def redact(self, text: str) -> str:
        """Replace all PHI matches with [REDACTED-category]."""
        matches = self.scan(text)
        if not matches:
            return text
        # Process from end to start to preserve positions
        result = text
        for m in reversed(matches):
            replacement = f"[REDACTED-{m.category}]"
            result = result[: m.start] + replacement + result[m.end :]
        return result

    def assert_no_phi(self, text: str, context: str = "") -> None:
        """Raise PhiDetectedError if PHI is found."""
        matches = self.scan(text)
        if matches:
            categories = {m.category for m in matches}
            raise PhiDetectedError(
                f"PHI detected ({', '.join(categories)}) in {context or 'text'}. "
                f"Blocked to prevent data leakage."
            )
