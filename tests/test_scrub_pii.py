"""Tests for vault PII scrubber."""
from __future__ import annotations

import json
import os
import sqlite3

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.vault_ops.scrub_pii import VaultPiiScrubber


class FakeDB:
    """Minimal in-memory DB for testing scrubber without full HealthDB."""

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._key = os.urandom(32)
        self.conn.executescript("""
            CREATE TABLE observations (
                obs_id TEXT PRIMARY KEY,
                user_id INTEGER DEFAULT 0,
                encrypted_data BLOB NOT NULL
            );
            CREATE TABLE medications (
                med_id TEXT PRIMARY KEY,
                user_id INTEGER DEFAULT 0,
                encrypted_data BLOB NOT NULL
            );
            CREATE TABLE memory_ltm (
                id TEXT PRIMARY KEY,
                user_id INTEGER DEFAULT 0,
                category TEXT DEFAULT '',
                encrypted_data BLOB NOT NULL
            );
        """)

    def _encrypt(self, data, aad: str) -> bytes:
        nonce = os.urandom(12)
        aesgcm = AESGCM(self._key)
        plaintext = json.dumps(data).encode()
        ct = aesgcm.encrypt(nonce, plaintext, aad.encode())
        return nonce + ct

    def _decrypt(self, blob: bytes, aad: str):
        nonce = blob[:12]
        ct = blob[12:]
        aesgcm = AESGCM(self._key)
        plaintext = aesgcm.decrypt(nonce, ct, aad.encode())
        return json.loads(plaintext)

    def insert_obs(self, obs_id: str, data: dict, user_id: int = 0):
        aad = f"observations.encrypted_data.{obs_id}"
        enc = self._encrypt(data, aad)
        self.conn.execute(
            "INSERT INTO observations (obs_id, user_id, encrypted_data) VALUES (?, ?, ?)",
            (obs_id, user_id, enc),
        )
        self.conn.commit()

    def insert_med(self, med_id: str, data: dict, user_id: int = 0):
        aad = f"medications.encrypted_data.{med_id}"
        enc = self._encrypt(data, aad)
        self.conn.execute(
            "INSERT INTO medications (med_id, user_id, encrypted_data) VALUES (?, ?, ?)",
            (med_id, user_id, enc),
        )
        self.conn.commit()

    def insert_ltm(self, fact_id: str, data: dict, user_id: int = 0):
        aad = f"memory_ltm.encrypted_data.{fact_id}"
        enc = self._encrypt(data, aad)
        cat = data.get("category", "")
        self.conn.execute(
            "INSERT INTO memory_ltm (id, user_id, category, encrypted_data) VALUES (?, ?, ?, ?)",
            (fact_id, user_id, cat, enc),
        )
        self.conn.commit()

    def get_obs_data(self, obs_id: str) -> dict:
        row = self.conn.execute(
            "SELECT encrypted_data FROM observations WHERE obs_id = ?", (obs_id,)
        ).fetchone()
        aad = f"observations.encrypted_data.{obs_id}"
        return self._decrypt(row["encrypted_data"], aad)

    def get_med_data(self, med_id: str) -> dict:
        row = self.conn.execute(
            "SELECT encrypted_data FROM medications WHERE med_id = ?", (med_id,)
        ).fetchone()
        aad = f"medications.encrypted_data.{med_id}"
        return self._decrypt(row["encrypted_data"], aad)

    def get_ltm_data(self, fact_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT encrypted_data FROM memory_ltm WHERE id = ?", (fact_id,)
        ).fetchone()
        if not row:
            return None
        aad = f"memory_ltm.encrypted_data.{fact_id}"
        return self._decrypt(row["encrypted_data"], aad)

    def ltm_exists(self, fact_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM memory_ltm WHERE id = ?", (fact_id,)
        ).fetchone()
        return row is not None


@pytest.fixture
def fake_db():
    return FakeDB()


@pytest.fixture
def scrubber(fake_db):
    return VaultPiiScrubber(fake_db)


class TestObservationScrubbing:

    def test_scrubs_ordering_provider(self, fake_db, scrubber):
        fake_db.insert_obs("obs-1", {
            "test_name": "Glucose",
            "value": 95,
            "ordering_provider": "Dr. Sarah Smith",
            "lab_name": "",
        })
        result = scrubber.scrub_all(user_id=0)
        assert result.observations_scrubbed == 1
        data = fake_db.get_obs_data("obs-1")
        assert data["ordering_provider"] == ""
        assert data["test_name"] == "Glucose"
        assert data["value"] == 95

    def test_scrubs_lab_name(self, fake_db, scrubber):
        fake_db.insert_obs("obs-2", {
            "test_name": "HbA1c",
            "value": 5.7,
            "ordering_provider": "",
            "lab_name": "Quest Diagnostics - Anytown",
        })
        result = scrubber.scrub_all(user_id=0)
        assert result.observations_scrubbed == 1
        data = fake_db.get_obs_data("obs-2")
        assert data["lab_name"] == ""

    def test_preserves_medical_data(self, fake_db, scrubber):
        fake_db.insert_obs("obs-3", {
            "test_name": "Hemoglobin",
            "canonical_name": "hemoglobin",
            "value": 14.5,
            "unit": "g/dL",
            "reference_low": 13.5,
            "reference_high": 17.5,
            "ordering_provider": "Dr. Jones",
            "lab_name": "LabCorp",
        })
        scrubber.scrub_all(user_id=0)
        data = fake_db.get_obs_data("obs-3")
        assert data["test_name"] == "Hemoglobin"
        assert data["value"] == 14.5
        assert data["unit"] == "g/dL"
        assert data["reference_low"] == 13.5
        assert data["reference_high"] == 17.5

    def test_no_change_if_already_blank(self, fake_db, scrubber):
        fake_db.insert_obs("obs-4", {
            "test_name": "Glucose",
            "value": 100,
            "ordering_provider": "",
            "lab_name": "",
        })
        result = scrubber.scrub_all(user_id=0)
        assert result.observations_scrubbed == 0


class TestMedicationScrubbing:

    def test_scrubs_prescriber(self, fake_db, scrubber):
        fake_db.insert_med("med-1", {
            "name": "Metformin",
            "dose": "500mg",
            "prescriber": "Dr. Jane Doe",
        })
        result = scrubber.scrub_all(user_id=0)
        assert result.medications_scrubbed == 1
        data = fake_db.get_med_data("med-1")
        assert data["prescriber"] == ""
        assert data["name"] == "Metformin"
        assert data["dose"] == "500mg"


class TestLtmScrubbing:

    def test_removes_name_entry(self, fake_db, scrubber):
        fake_db.insert_ltm("ltm-name", {
            "fact": "Name: Jane Doe",
            "category": "demographic",
        })
        result = scrubber.scrub_all(user_id=0)
        assert result.ltm_entries_removed == 1
        assert not fake_db.ltm_exists("ltm-name")

    def test_converts_dob_to_age(self, fake_db, scrubber):
        fake_db.insert_ltm("ltm-dob", {
            "fact": "Date of birth: 1995-03-15 (age 30)",
            "category": "demographic",
        })
        result = scrubber.scrub_all(user_id=0)
        assert result.ltm_entries_redacted == 1
        data = fake_db.get_ltm_data("ltm-dob")
        assert data["fact"] == "Age: 30"
        assert "1995" not in data["fact"]

    def test_deletes_dob_without_age(self, fake_db, scrubber):
        fake_db.insert_ltm("ltm-dob2", {
            "fact": "Date of birth: 1995-03-15",
            "category": "demographic",
        })
        result = scrubber.scrub_all(user_id=0)
        assert result.ltm_entries_removed == 1
        assert not fake_db.ltm_exists("ltm-dob2")

    def test_redacts_phi_in_fact(self, fake_db, scrubber):
        fake_db.insert_ltm("ltm-phi", {
            "fact": "Patient SSN: 123-45-6789",
            "category": "demographic",
        })
        result = scrubber.scrub_all(user_id=0)
        assert result.ltm_entries_redacted == 1
        data = fake_db.get_ltm_data("ltm-phi")
        assert "123-45-6789" not in data["fact"]

    def test_preserves_medical_facts(self, fake_db, scrubber):
        fake_db.insert_ltm("ltm-med", {
            "fact": "Known condition: Type 2 Diabetes",
            "category": "condition",
        })
        result = scrubber.scrub_all(user_id=0)
        assert result.ltm_entries_removed == 0
        assert result.ltm_entries_redacted == 0
        data = fake_db.get_ltm_data("ltm-med")
        assert data["fact"] == "Known condition: Type 2 Diabetes"


class TestIdempotency:

    def test_double_scrub_same_result(self, fake_db, scrubber):
        fake_db.insert_obs("obs-5", {
            "test_name": "Glucose",
            "value": 95,
            "ordering_provider": "Dr. Smith",
            "lab_name": "Quest",
        })
        fake_db.insert_ltm("ltm-x", {
            "fact": "Name: Jane Doe",
            "category": "demographic",
        })

        result1 = scrubber.scrub_all(user_id=0)
        assert result1.observations_scrubbed == 1
        assert result1.ltm_entries_removed == 1

        result2 = scrubber.scrub_all(user_id=0)
        assert result2.observations_scrubbed == 0
        assert result2.ltm_entries_removed == 0

    def test_preserves_aad_binding(self, fake_db, scrubber):
        """Verify data can still be decrypted after scrubbing."""
        fake_db.insert_obs("obs-6", {
            "test_name": "ALT",
            "value": 25,
            "ordering_provider": "Dr. X",
            "lab_name": "Lab Y",
        })
        scrubber.scrub_all(user_id=0)
        # Should be able to decrypt with the same AAD
        data = fake_db.get_obs_data("obs-6")
        assert data["test_name"] == "ALT"
        assert data["value"] == 25
