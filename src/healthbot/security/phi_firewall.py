"""PHI (Protected Health Information) regex-based detection.

All patterns are deterministic regex. No LLM involvement.
Detects: SSN, MRN, phone numbers, email addresses, dates of birth,
labeled name patterns, and exact lab values in context.
"""
from __future__ import annotations

import re
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
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
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
    # Labeled: "Patient: John Smith", "Name: Jane Doe"
    "name_labeled": re.compile(
        r"\b(?:Patient|Name|Pt|Patient\s+Name)\s*[:#]\s*"
        r"[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+\b"
    ),
    # Labeled ALL-CAPS: "Patient: SMITH, JOHN A" or "Patient: JOHN SMITH"
    "name_labeled_caps": re.compile(
        r"\b(?:Patient|Name|Pt|Patient\s+Name)\s*[:#]\s*"
        r"[A-Z]{2,}(?:[,\s]+[A-Z]{2,})+(?:\s+[A-Z]\.?)?\b"
    ),
    # Self-introduction: "My name is John", "I'm John Smith", "I am Sarah Jones"
    "name_intro": re.compile(
        r"\b(?:my name is|I'm|I am)\s+"
        r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b",
        re.IGNORECASE,
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
        self._patterns = {k: v for k, v in self._patterns.items()
                          if not k.startswith("id_")}

    def scan(self, text: str) -> list[PhiMatch]:
        """Return all PHI matches found in text."""
        matches: list[PhiMatch] = []
        for category, pattern in self._patterns.items():
            for m in pattern.finditer(text):
                # Skip dates that follow lab report labels (clinical metadata, not PHI)
                if category == "dob_slash":
                    preceding = text[max(0, m.start() - 40):m.start()]
                    if _SAFE_DATE_PREFIX.search(preceding):
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
        for category, pattern in self._patterns.items():
            if category == "dob_slash":
                # Apply same safe-date-prefix suppression as scan()
                for m in pattern.finditer(text):
                    preceding = text[max(0, m.start() - 40):m.start()]
                    if not _SAFE_DATE_PREFIX.search(preceding):
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
