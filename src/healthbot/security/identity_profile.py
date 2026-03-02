"""Encrypted identity profile for smarter PII detection.

Stores user's personal identifiers (name, email, DOB, family names) in
AES-256-GCM encrypted fields, then compiles them into regex patterns that
supercharge PhiFirewall and NER detection.

The identity data itself is stored in the RAW VAULT ONLY — never synced
to the Clean DB, never included in AI export, never exposed via MCP.
Only the compiled patterns (regex objects in memory) are used — they
contain no PII themselves.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger("healthbot")

# Months for DOB pattern generation
_MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


@dataclass
class DetectionResult:
    """A PII detection from identity-aware scanning."""

    field_key: str
    pattern_name: str
    matched_text: str
    start: int
    end: int


@dataclass
class IdentityProfile:
    """Manage user identity for enhanced PII detection.

    Wraps DB CRUD and compiles identity data into regex patterns
    for PhiFirewall and name sets for NER boosting.
    """

    db: object  # HealthDB instance
    _cache: dict[int, list[dict]] = field(default_factory=dict)

    def store_field(
        self, user_id: int, field_key: str, value: str, field_type: str,
    ) -> str:
        """Store an identity field (encrypted). Returns field ID."""
        field_id = self.db.upsert_identity_field(user_id, field_key, value, field_type)
        self._cache.pop(user_id, None)
        return field_id

    def get_all_fields(self, user_id: int) -> list[dict]:
        """Get all identity fields for a user (decrypted)."""
        if user_id not in self._cache:
            self._cache[user_id] = self.db.get_identity_fields(user_id)
        return self._cache[user_id]

    def delete_field(self, user_id: int, field_key: str) -> bool:
        """Delete a specific identity field."""
        self._cache.pop(user_id, None)
        return self.db.delete_identity_field(user_id, field_key)

    def delete_all(self, user_id: int) -> int:
        """Delete all identity fields for a user."""
        self._cache.pop(user_id, None)
        return self.db.delete_all_identity_fields(user_id)

    def compile_phi_patterns(self, user_id: int) -> dict[str, re.Pattern[str]]:
        """Compile identity data into regex patterns for PhiFirewall.

        Returns a dict mapping pattern names to compiled regex patterns.
        The patterns themselves contain no PII — they're regex objects
        that match the user's known identifiers.
        """
        fields = self.get_all_fields(user_id)
        if not fields:
            return {}

        patterns: dict[str, re.Pattern[str]] = {}

        for f in fields:
            value = f["value"].strip()
            ftype = f["type"]
            fkey = f["field_key"]

            if not value:
                continue

            if ftype == "name":
                patterns.update(self._compile_name_patterns(fkey, value))
            elif ftype == "email":
                patterns.update(self._compile_email_pattern(fkey, value))
            elif ftype == "dob":
                patterns.update(self._compile_dob_patterns(fkey, value))
            elif ftype == "custom":
                patterns.update(self._compile_custom_pattern(fkey, value))

        return patterns

    def compile_ner_known_names(self, user_id: int) -> set[str]:
        """Return a set of known names for NER layer boosting.

        These names bypass the MIN_CONFIDENCE threshold — any NER entity
        matching a known name is always detected regardless of score.
        """
        fields = self.get_all_fields(user_id)
        names: set[str] = set()

        for f in fields:
            value = f["value"].strip()
            if not value:
                continue

            if f["type"] == "name":
                names.add(value)
                # Add individual parts
                parts = value.split()
                for part in parts:
                    if len(part) >= 3:
                        names.add(part)
            elif f["field_key"].startswith("family:"):
                # Family names — each comma-separated value
                for name in value.split(","):
                    name = name.strip()
                    if name:
                        names.add(name)
                        parts = name.split()
                        for part in parts:
                            if len(part) >= 3:
                                names.add(part)

        return names

    def test_anonymization(
        self, user_id: int, text: str,
    ) -> list[DetectionResult]:
        """Test identity-enhanced detection against sample text.

        Returns list of matches found using the compiled patterns.
        Useful for the /identity_check command.
        """
        patterns = self.compile_phi_patterns(user_id)
        results: list[DetectionResult] = []

        for pname, pattern in patterns.items():
            for m in pattern.finditer(text):
                # Derive field_key from pattern name (e.g. "id_full_name_forward" -> "full_name")
                fkey = pname.replace("id_", "", 1).rsplit("_", 1)[0]
                # Clean up further — remove trailing pattern type hints
                for suffix in ("_forward", "_reversed", "_initial", "_first",
                               "_email", "_slash", "_iso", "_text", "_custom"):
                    fkey = fkey.removesuffix(suffix)
                results.append(DetectionResult(
                    field_key=fkey,
                    pattern_name=pname,
                    matched_text=m.group(),
                    start=m.start(),
                    end=m.end(),
                ))

        # Sort by position
        results.sort(key=lambda r: r.start)
        return results

    # --- Pattern compilation helpers ---

    @staticmethod
    def _compile_name_patterns(
        field_key: str, name: str,
    ) -> dict[str, re.Pattern[str]]:
        """Compile regex patterns for a person's name.

        Generates patterns for:
        - Forward: "John Smith"
        - Reversed: "Smith, John"
        - Initial: "J. Smith" / "J Smith"
        - First name alone (if 4+ chars)
        """
        patterns: dict[str, re.Pattern[str]] = {}
        parts = name.split()
        if not parts:
            return patterns

        escaped_parts = [re.escape(p) for p in parts]
        prefix = f"id_{field_key}"

        # Forward: "John Smith" (case-insensitive, word boundary)
        forward = r"\b" + r"\s+".join(escaped_parts) + r"\b"
        patterns[f"{prefix}_forward"] = re.compile(forward, re.IGNORECASE)

        if len(parts) >= 2:
            # Reversed: "Smith, John"
            last = escaped_parts[-1]
            firsts = r"\s+".join(escaped_parts[:-1])
            reversed_pat = r"\b" + last + r"(?:,\s*|\s+)" + firsts + r"\b"
            patterns[f"{prefix}_reversed"] = re.compile(reversed_pat, re.IGNORECASE)

            # Initial form: "J. Smith" — require period after single-char initial
            # to avoid bare "J" matching too broadly
            first_initial = re.escape(parts[0][0])
            initial_pat = r"\b" + first_initial + r"\.\s+" + last + r"\b"
            patterns[f"{prefix}_initial"] = re.compile(initial_pat, re.IGNORECASE)

        # First name alone (4+ chars to avoid false positives on short names)
        first = parts[0]
        if len(first) >= 4:
            first_pat = r"\b" + re.escape(first) + r"\b"
            patterns[f"{prefix}_first"] = re.compile(first_pat, re.IGNORECASE)

        # Last name alone (4+ chars, only for multi-part names)
        if len(parts) >= 2:
            last_raw = parts[-1]
            if len(last_raw) >= 4:
                last_pat = r"\b" + re.escape(last_raw) + r"\b"
                patterns[f"{prefix}_last"] = re.compile(last_pat, re.IGNORECASE)

        return patterns

    @staticmethod
    def _compile_email_pattern(
        field_key: str, email: str,
    ) -> dict[str, re.Pattern[str]]:
        """Compile exact-match pattern for an email address."""
        return {
            f"id_{field_key}_email": re.compile(
                re.escape(email), re.IGNORECASE,
            ),
        }

    @staticmethod
    def _compile_dob_patterns(
        field_key: str, dob: str,
    ) -> dict[str, re.Pattern[str]]:
        """Compile date patterns for all common DOB formats.

        Accepts ISO format (YYYY-MM-DD) and generates patterns for:
        - MM/DD/YYYY (with and without leading zeros)
        - YYYY-MM-DD
        - Month DD, YYYY (text month)
        """
        patterns: dict[str, re.Pattern[str]] = {}
        prefix = f"id_{field_key}"

        # Parse YYYY-MM-DD
        parts = dob.split("-")
        if len(parts) != 3:
            # Try MM/DD/YYYY
            slash_parts = dob.split("/")
            if len(slash_parts) == 3:
                month, day, year = slash_parts[0], slash_parts[1], slash_parts[2]
            else:
                return patterns
        else:
            year, month, day = parts[0], parts[1], parts[2]

        try:
            y = int(year)
            m = int(month)
            d = int(day)
        except ValueError:
            return patterns

        if not (1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31):
            return patterns

        # Slash formats: M/D/YYYY, MM/DD/YYYY, M/DD/YYYY, MM/D/YYYY
        m_variants = {str(m), f"{m:02d}"}
        d_variants = {str(d), f"{d:02d}"}
        slash_alts = []
        for mv in sorted(m_variants):
            for dv in sorted(d_variants):
                slash_alts.append(f"{mv}/{dv}/{y}")
                slash_alts.append(f"{mv}-{dv}-{y}")
        slash_pat = "|".join(re.escape(a) for a in slash_alts)
        patterns[f"{prefix}_slash"] = re.compile(r"\b(?:" + slash_pat + r")\b")

        # ISO: YYYY-MM-DD
        iso_pat = re.escape(f"{y}-{m:02d}-{d:02d}")
        patterns[f"{prefix}_iso"] = re.compile(r"\b" + iso_pat + r"\b")

        # Text month: "March 15, 1990" / "March 15 1990"
        month_name = _MONTHS[m - 1]
        text_pat = r"\b" + re.escape(month_name) + r"\s+" + str(d) + r",?\s+" + str(y) + r"\b"
        patterns[f"{prefix}_text"] = re.compile(text_pat, re.IGNORECASE)

        return patterns

    @staticmethod
    def _compile_custom_pattern(
        field_key: str, value: str,
    ) -> dict[str, re.Pattern[str]]:
        """Compile word-boundary match for custom PII values."""
        return {
            f"id_{field_key}_custom": re.compile(
                r"\b" + re.escape(value) + r"\b", re.IGNORECASE,
            ),
        }
