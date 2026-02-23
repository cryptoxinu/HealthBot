"""Compact emergency medical card.

Pulls allergies, active medications, conditions, blood type, and
emergency contacts from the user's encrypted health data.
All logic is deterministic. No LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass
class EmergencyCard:
    """Emergency medical summary."""

    name: str = ""
    dob: str = ""
    blood_type: str = ""
    allergies: list[str] = field(default_factory=list)
    conditions: list[str] = field(default_factory=list)
    medications: list[str] = field(default_factory=list)
    emergency_contacts: list[str] = field(default_factory=list)
    generated_at: str = ""


class EmergencyCardBuilder:
    """Build an emergency medical card from stored health data."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def build(self, user_id: int) -> EmergencyCard:
        """Build the emergency card from all available data."""
        card = EmergencyCard(
            generated_at=datetime.now(UTC).strftime("%Y-%m-%d"),
        )

        # Pull all LTM facts
        try:
            facts = self._db.get_ltm_by_user(user_id)
        except Exception:
            facts = []

        for fact in facts:
            category = fact.get("_category", fact.get("category", ""))
            text = fact.get("fact", "")
            if not text:
                continue

            lower = text.lower()

            if category == "demographic":
                self._parse_demographic(card, text, lower)
            elif category == "allergy":
                card.allergies.append(text)
            elif category == "condition":
                card.conditions.append(text)
            elif category == "emergency_contact":
                card.emergency_contacts.append(text)
            else:
                # Check for allergy/blood type/emergency in uncategorized facts
                if "allerg" in lower:
                    card.allergies.append(text)
                elif "blood type" in lower or "blood group" in lower:
                    self._parse_blood_type(card, text)
                elif "emergency contact" in lower or "ICE:" in text:
                    card.emergency_contacts.append(text)

        # Conditions from condition extractor
        if not card.conditions:
            try:
                from healthbot.reasoning.condition_extractor import (
                    extract_conditions,
                )
                card.conditions = extract_conditions(self._db, user_id)
            except Exception:
                pass

        # Active medications
        try:
            meds = self._db.get_active_medications(user_id=user_id)
            for med in meds:
                name = med.get("name", "")
                dose = med.get("dose", "")
                unit = med.get("unit", "")
                freq = med.get("frequency", "")
                entry = f"{name} {dose} {unit} {freq}".strip()
                card.medications.append(entry)
        except Exception:
            pass

        # Demographics
        try:
            demographics = self._db.get_user_demographics(user_id)
            if demographics.get("dob") and not card.dob:
                card.dob = demographics["dob"]
        except Exception:
            pass

        return card

    def _parse_demographic(
        self, card: EmergencyCard, text: str, lower: str,
    ) -> None:
        """Extract demographic info from a LTM fact."""
        if "blood type" in lower or "blood group" in lower:
            self._parse_blood_type(card, text)
        elif "name:" in lower or "name is" in lower:
            # Extract name
            for prefix in ("name:", "name is", "my name is"):
                if prefix in lower:
                    card.name = text.split(prefix, 1)[-1].strip().strip(".")

    def _parse_blood_type(self, card: EmergencyCard, text: str) -> None:
        """Extract blood type from text."""
        import re
        # Look for blood type after contextual keywords
        bt = re.search(
            r"(?:blood\s+(?:type|group)\s*:?\s*)((?:AB|A|B|O)[+-]?)",
            text, re.IGNORECASE,
        )
        if bt:
            card.blood_type = bt.group(1).upper()
            return
        # Fallback: standalone blood type at word boundary
        bt2 = re.search(r"\b((?:AB|A|B|O)[+-])\b", text)
        if bt2:
            card.blood_type = bt2.group(1).upper()


def format_emergency_card(card: EmergencyCard) -> str:
    """Format emergency card for display."""
    lines = [
        "EMERGENCY MEDICAL CARD",
        "=" * 30,
    ]

    if card.name:
        lines.append(f"Name: {card.name}")
    if card.dob:
        lines.append(f"DOB: {card.dob}")
    if card.blood_type:
        lines.append(f"Blood Type: {card.blood_type}")

    lines.append("")

    # Allergies
    lines.append("ALLERGIES:")
    if card.allergies:
        for allergy in card.allergies:
            lines.append(f"  ! {allergy}")
    else:
        lines.append("  None on file")

    lines.append("")

    # Conditions
    lines.append("CONDITIONS:")
    if card.conditions:
        for condition in card.conditions:
            lines.append(f"  - {condition}")
    else:
        lines.append("  None on file")

    lines.append("")

    # Medications
    lines.append("MEDICATIONS:")
    if card.medications:
        for med in card.medications:
            lines.append(f"  - {med}")
    else:
        lines.append("  None on file")

    lines.append("")

    # Emergency contacts
    lines.append("EMERGENCY CONTACTS:")
    if card.emergency_contacts:
        for contact in card.emergency_contacts:
            lines.append(f"  - {contact}")
    else:
        lines.append("  None on file")

    lines.append("")
    lines.append("-" * 30)
    lines.append(f"Generated: {card.generated_at}")

    return "\n".join(lines)
