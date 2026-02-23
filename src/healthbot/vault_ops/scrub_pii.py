"""Vault PII scrubber — removes identifying information from existing records.

One-time migration: decrypts each record, blanks PII fields,
re-encrypts with the same AAD. Also scrubs LTM entries.
Idempotent: safe to run multiple times.

Run via: /scrub_pii Telegram command
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from healthbot.data.db import HealthDB
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")

# Regex to extract age from DOB facts
_AGE_RE = re.compile(r"\(age\s+(\d+)\)", re.IGNORECASE)
_DOB_DATE_RE = re.compile(
    r"(?:19|20)\d{2}[-/]\d{1,2}[-/]\d{1,2}"
    r"|(?:0[1-9]|1[0-2])[/]\d{1,2}[/]\d{2,4}"
)


@dataclass
class ScrubResult:
    """Summary of PII scrubbing operation."""

    observations_scrubbed: int = 0
    medications_scrubbed: int = 0
    ltm_entries_removed: int = 0
    ltm_entries_redacted: int = 0
    search_entries_scrubbed: int = 0
    errors: list[str] = field(default_factory=list)


class VaultPiiScrubber:
    """Scrub PII from existing encrypted vault data.

    Follows the decrypt -> modify -> re-encrypt pattern.
    AAD context strings are preserved exactly.
    """

    # Fields to blank in observations (encrypted_data)
    _OBS_PII_FIELDS = ("ordering_provider", "lab_name")

    # Fields to blank in medications (encrypted_data)
    _MED_PII_FIELDS = ("prescriber",)

    # LTM fact prefixes that are pure PII
    _LTM_NAME_PATTERNS = (
        "Name:",
        "name:",
        "Patient name:",
        "Patient Name:",
        "My name is",
        "my name is",
    )

    # LTM fact prefixes for DOB (convert to age-only)
    _LTM_DOB_PATTERNS = (
        "Date of birth:",
        "date of birth:",
        "DOB:",
        "dob:",
        "Birthday:",
        "birthday:",
        "Born:",
        "born:",
    )

    def __init__(
        self,
        db: HealthDB,
        phi_firewall: PhiFirewall | None = None,
    ) -> None:
        self._db = db
        self._fw = phi_firewall or PhiFirewall()

    def scrub_all(self, user_id: int = 0) -> ScrubResult:
        """Run full PII scrub on all data for a user.

        Idempotent: blanking already-blank fields is a no-op.
        """
        result = ScrubResult()
        self._scrub_observations(user_id, result)
        self._scrub_medications(user_id, result)
        self._scrub_ltm(user_id, result)
        self._scrub_search_index(user_id, result)

        logger.info(
            "PII scrub complete: %d obs, %d meds, %d LTM removed, %d LTM redacted, "
            "%d search entries",
            result.observations_scrubbed, result.medications_scrubbed,
            result.ltm_entries_removed, result.ltm_entries_redacted,
            result.search_entries_scrubbed,
        )
        return result

    def _scrub_observations(self, user_id: int, result: ScrubResult) -> None:
        """Decrypt observations, blank PII fields, re-encrypt."""
        rows = self._db.conn.execute(
            "SELECT obs_id, encrypted_data FROM observations WHERE user_id = ?",
            (user_id,),
        ).fetchall()

        for row in rows:
            obs_id = row["obs_id"]
            aad = f"observations.encrypted_data.{obs_id}"
            try:
                data = self._db._decrypt(row["encrypted_data"], aad)
                changed = False
                for field_name in self._OBS_PII_FIELDS:
                    if data.get(field_name):
                        data[field_name] = ""
                        changed = True
                if changed:
                    enc = self._db._encrypt(data, aad)
                    self._db.conn.execute(
                        "UPDATE observations SET encrypted_data = ? WHERE obs_id = ?",
                        (enc, obs_id),
                    )
                    result.observations_scrubbed += 1
            except Exception as e:
                result.errors.append(f"obs {obs_id}: {e}")

        if result.observations_scrubbed:
            self._db.conn.commit()

    def _scrub_medications(self, user_id: int, result: ScrubResult) -> None:
        """Decrypt medications, blank prescriber, re-encrypt."""
        rows = self._db.conn.execute(
            "SELECT med_id, encrypted_data FROM medications WHERE user_id = ?",
            (user_id,),
        ).fetchall()

        for row in rows:
            med_id = row["med_id"]
            aad = f"medications.encrypted_data.{med_id}"
            try:
                data = self._db._decrypt(row["encrypted_data"], aad)
                changed = False
                for field_name in self._MED_PII_FIELDS:
                    if data.get(field_name):
                        data[field_name] = ""
                        changed = True
                if changed:
                    enc = self._db._encrypt(data, aad)
                    self._db.conn.execute(
                        "UPDATE medications SET encrypted_data = ? WHERE med_id = ?",
                        (enc, med_id),
                    )
                    result.medications_scrubbed += 1
            except Exception as e:
                result.errors.append(f"med {med_id}: {e}")

        if result.medications_scrubbed:
            self._db.conn.commit()

    def _scrub_ltm(self, user_id: int, result: ScrubResult) -> None:
        """Remove Name entries, convert DOB to age-only, redact remaining PII."""
        rows = self._db.conn.execute(
            "SELECT id, encrypted_data FROM memory_ltm WHERE user_id = ?",
            (user_id,),
        ).fetchall()

        to_delete: list[str] = []
        to_update: list[tuple[str, str, str]] = []  # (id, new_fact, category)

        for row in rows:
            fact_id = row["id"]
            aad = f"memory_ltm.encrypted_data.{fact_id}"
            try:
                data = self._db._decrypt(row["encrypted_data"], aad)
                fact = data.get("fact", "")
                category = data.get("category", "")

                # Delete name entries entirely
                if any(fact.startswith(p) for p in self._LTM_NAME_PATTERNS):
                    to_delete.append(fact_id)
                    continue

                # Convert DOB to age-only
                if any(fact.startswith(p) for p in self._LTM_DOB_PATTERNS):
                    age_match = _AGE_RE.search(fact)
                    if age_match:
                        age = age_match.group(1)
                        to_update.append((fact_id, f"Age: {age}", category))
                    else:
                        # Can't extract age — delete the DOB fact
                        to_delete.append(fact_id)
                    continue

                # Check remaining facts for PII and redact
                if self._fw.contains_phi(fact):
                    redacted = self._fw.redact(fact)
                    if redacted != fact:
                        to_update.append((fact_id, redacted, category))

            except Exception as e:
                result.errors.append(f"ltm {fact_id}: {e}")

        # Apply deletions
        for fact_id in to_delete:
            self._db.conn.execute(
                "DELETE FROM memory_ltm WHERE id = ?", (fact_id,),
            )
            result.ltm_entries_removed += 1

        # Apply updates (re-encrypt with new fact text)
        for fact_id, new_fact, category in to_update:
            aad = f"memory_ltm.encrypted_data.{fact_id}"
            enc = self._db._encrypt(
                {"fact": new_fact, "category": category}, aad,
            )
            self._db.conn.execute(
                "UPDATE memory_ltm SET encrypted_data = ? WHERE id = ?",
                (enc, fact_id),
            )
            result.ltm_entries_redacted += 1

        if to_delete or to_update:
            self._db.conn.commit()

    def _scrub_search_index(self, user_id: int, result: ScrubResult) -> None:
        """Redact PII from search_index text_for_search entries."""
        try:
            rows = self._db.conn.execute(
                "SELECT doc_id, text_for_search FROM search_index"
            ).fetchall()
        except Exception:
            return  # Table may not exist

        updated = 0
        for row in rows:
            text = row["text_for_search"]
            if text and self._fw.contains_phi(text):
                self._db.conn.execute(
                    "UPDATE search_index SET text_for_search = ? WHERE doc_id = ?",
                    (self._fw.redact(text), row["doc_id"]),
                )
                updated += 1
        if updated:
            self._db.conn.commit()
        result.search_entries_scrubbed = updated
