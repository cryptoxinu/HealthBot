"""Tests for emergency medical card (Phase T4)."""
from __future__ import annotations

import uuid
from datetime import date

from healthbot.data.models import Medication
from healthbot.export.emergency_card import (
    EmergencyCard,
    EmergencyCardBuilder,
    format_emergency_card,
)


class TestEmergencyCardBuilder:
    """Test building emergency cards."""

    def test_empty_card(self, db) -> None:
        """No data should produce empty card."""
        builder = EmergencyCardBuilder(db)
        card = builder.build(user_id=1)
        assert card.allergies == []
        assert card.medications == []
        assert card.generated_at

    def test_medications_from_db(self, db) -> None:
        """Active medications should appear on card."""
        builder = EmergencyCardBuilder(db)
        med = Medication(
            id=uuid.uuid4().hex,
            name="Metformin",
            dose="500",
            unit="mg",
            frequency="twice daily",
            start_date=date.today(),
        )
        db.insert_medication(med, user_id=1)

        card = builder.build(user_id=1)
        assert len(card.medications) == 1
        assert "Metformin" in card.medications[0]

    def test_allergies_from_ltm(self, db) -> None:
        """Allergies in LTM should appear on card."""
        builder = EmergencyCardBuilder(db)
        db.insert_ltm(1, "allergy", "Penicillin allergy")

        card = builder.build(user_id=1)
        assert len(card.allergies) == 1
        assert "Penicillin" in card.allergies[0]

    def test_conditions_from_ltm(self, db) -> None:
        """Conditions in LTM should appear on card."""
        builder = EmergencyCardBuilder(db)
        db.insert_ltm(1, "condition", "Type 2 Diabetes")

        card = builder.build(user_id=1)
        assert len(card.conditions) == 1
        assert "Diabetes" in card.conditions[0]

    def test_blood_type_from_ltm(self, db) -> None:
        """Blood type in LTM should appear on card."""
        builder = EmergencyCardBuilder(db)
        db.insert_ltm(1, "demographic", "Blood type: O+")

        card = builder.build(user_id=1)
        assert card.blood_type == "O+"

    def test_emergency_contact_from_ltm(self, db) -> None:
        """Emergency contacts in LTM should appear on card."""
        builder = EmergencyCardBuilder(db)
        db.insert_ltm(1, "emergency_contact", "Jane Doe 555-1234")

        card = builder.build(user_id=1)
        assert len(card.emergency_contacts) == 1
        assert "Jane Doe" in card.emergency_contacts[0]

    def test_dob_from_demographics(self, db) -> None:
        """DOB from demographics should appear on card."""
        builder = EmergencyCardBuilder(db)
        db.insert_ltm(1, "demographic", "DOB: 1985-03-15")

        card = builder.build(user_id=1)
        assert str(card.dob) == "1985-03-15"

    def test_allergy_from_uncategorized(self, db) -> None:
        """Allergy mentions in general facts should be detected."""
        builder = EmergencyCardBuilder(db)
        db.insert_ltm(1, "pattern", "Patient has allergy to shellfish")

        card = builder.build(user_id=1)
        assert len(card.allergies) == 1
        assert "shellfish" in card.allergies[0]


class TestFormatEmergencyCard:
    """Test emergency card formatting."""

    def test_format_empty(self) -> None:
        """Empty card should show 'None on file' sections."""
        card = EmergencyCard(generated_at="2025-01-15")
        text = format_emergency_card(card)
        assert "EMERGENCY MEDICAL CARD" in text
        assert "None on file" in text

    def test_format_full_card(self) -> None:
        """Full card should show all sections."""
        card = EmergencyCard(
            name="John Doe",
            dob="1985-03-15",
            blood_type="A+",
            allergies=["Penicillin", "Shellfish"],
            conditions=["Type 2 Diabetes", "Hypertension"],
            medications=["Metformin 500 mg twice daily"],
            emergency_contacts=["Jane Doe 555-1234"],
            generated_at="2025-01-15",
        )
        text = format_emergency_card(card)
        assert "John Doe" in text
        assert "1985-03-15" in text
        assert "A+" in text
        assert "Penicillin" in text
        assert "Shellfish" in text
        assert "Diabetes" in text
        assert "Metformin" in text
        assert "Jane Doe" in text
        assert "ALLERGIES:" in text
        assert "CONDITIONS:" in text
        assert "MEDICATIONS:" in text
        assert "EMERGENCY CONTACTS:" in text
