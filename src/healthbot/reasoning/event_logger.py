"""Free-form event/symptom logger with deterministic parsing.

No LLM for parsing. Regex + heuristics only.
Stores user-reported symptoms/events in the observations table.
"""
from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime

from healthbot.nlu.date_parse import parse_date

logger = logging.getLogger("healthbot")

# Symptom category patterns (checked in order)
_CATEGORY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("dizziness", re.compile(
        r"\b(?:dizz(?:y|iness)|vertigo|lightheaded|faint(?:ing)?|wooz(?:y|iness))\b",
        re.IGNORECASE,
    )),
    ("headache", re.compile(
        r"\b(?:headache|migraine|head\s+pain|head\s+ache)\b",
        re.IGNORECASE,
    )),
    ("fatigue", re.compile(
        r"\b(?:fatigue|tired|exhausted|lethar(?:gy|gic)|low\s+energy|worn\s+out)\b",
        re.IGNORECASE,
    )),
    ("pain", re.compile(
        r"\b(?:pain|ache|sore(?:ness)?|cramp(?:s|ing)?|stiff(?:ness)?|hurt(?:s|ing)?)\b",
        re.IGNORECASE,
    )),
    ("nausea", re.compile(
        r"\b(?:nausea|nauseous|vomit(?:ing)?|queasy|sick\s+to\s+my\s+stomach)\b",
        re.IGNORECASE,
    )),
    ("sleep", re.compile(
        r"\b(?:insomnia|can'?t\s+sleep|sleep(?:less|lessness)|wak(?:e|ing)\s+up|bad\s+sleep)\b",
        re.IGNORECASE,
    )),
    ("mood", re.compile(
        r"\b(?:anxi(?:ous|ety)|depress(?:ed|ion)|stress(?:ed)?|irritab(?:le|ility)|mood)\b",
        re.IGNORECASE,
    )),
    ("digestive", re.compile(
        r"\b(?:diarrhea|constipat(?:ed|ion)|bloat(?:ed|ing)|heartburn|stomach|nausea"
        r"|indigestion|gas|acid\s+reflux)\b",
        re.IGNORECASE,
    )),
    ("heart", re.compile(
        r"\b(?:palpitat(?:ions?)?|racing\s+heart|chest\s+tight(?:ness)?|heart\s+flutter)\b",
        re.IGNORECASE,
    )),
]

# Severity keywords
_SEVERITY_MAP: dict[str, str] = {
    "mild": "mild", "slight": "mild", "minor": "mild",
    "moderate": "moderate",
    "severe": "severe", "bad": "severe", "terrible": "severe",
    "worst": "severe", "intense": "severe", "extreme": "severe",
}

_SEVERITY_RE = re.compile(
    r"\b(" + "|".join(_SEVERITY_MAP.keys()) + r")\b",
    re.IGNORECASE,
)

# Strip command prefixes
_PREFIX_RE = re.compile(
    r"^\s*(?:log|note|record)\s+",
    re.IGNORECASE,
)


@dataclass
class ParsedEvent:
    """A parsed free-form health event."""

    raw_text: str
    cleaned_text: str
    symptom_category: str
    severity: str  # "mild", "moderate", "severe", or ""
    date_effective: date
    extra_notes: str = ""


class EventLogger:
    """Parse and store free-form symptom/event reports."""

    def __init__(self, db: object) -> None:
        self._db = db

    def parse(self, text: str) -> ParsedEvent:
        """Parse free-form text into a structured event.

        Strips command prefixes, detects symptom category, extracts
        date and severity. No LLM involvement.
        """
        cleaned = _PREFIX_RE.sub("", text).strip()

        # Detect category
        category = "general"
        for cat_name, pattern in _CATEGORY_PATTERNS:
            if pattern.search(cleaned):
                category = cat_name
                break

        # Extract severity
        severity = ""
        m = _SEVERITY_RE.search(cleaned)
        if m:
            severity = _SEVERITY_MAP[m.group(1).lower()]

        # Extract date
        parsed_date = parse_date(cleaned)
        if parsed_date is None:
            parsed_date = date.today()

        return ParsedEvent(
            raw_text=text,
            cleaned_text=cleaned,
            symptom_category=category,
            severity=severity,
            date_effective=parsed_date,
        )

    def store(self, event: ParsedEvent, user_id: int) -> str:
        """Store event in the observations table.

        Uses record_type='user_event' with encrypted data.
        Returns the observation ID.
        """
        obs_id = uuid.uuid4().hex
        aad = f"observations.encrypted_data.{obs_id}"

        event_data = {
            "raw_text": event.raw_text,
            "cleaned_text": event.cleaned_text,
            "symptom_category": event.symptom_category,
            "severity": event.severity,
            "date_effective": event.date_effective.isoformat(),
            "user_id": user_id,
        }

        enc_data = self._db._encrypt(event_data, aad)
        self._db.conn.execute(
            """INSERT INTO observations (obs_id, record_type, canonical_name,
               date_effective, triage_level, flag, source_doc_id, source_page,
               source_section, created_at, encrypted_data)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                obs_id,
                "user_event",
                event.symptom_category,
                event.date_effective.isoformat(),
                "normal",
                "",
                "",
                0,
                "",
                datetime.now(UTC).isoformat(),
                enc_data,
            ),
        )
        self._db.conn.commit()
        logger.info("Stored user event %s (category=%s)", obs_id, event.symptom_category)
        return obs_id

    def format_confirmation(self, event: ParsedEvent) -> str:
        """Format a human-readable confirmation of the logged event."""
        parts = [f"Logged: {event.cleaned_text}"]
        parts.append(f"Category: {event.symptom_category}")
        if event.severity:
            parts.append(f"Severity: {event.severity}")
        parts.append(f"Date: {event.date_effective.isoformat()}")
        return " | ".join(parts)
