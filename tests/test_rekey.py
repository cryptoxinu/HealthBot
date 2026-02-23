"""Tests for vault re-keying (key rotation)."""
from __future__ import annotations

import json
import uuid
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult, Medication
from healthbot.security.key_manager import KeyManager
from healthbot.security.vault import Vault
from healthbot.vault_ops.rekey import RekeyError, VaultRekey

OLD_PASSPHRASE = "old-test-passphrase-rekey"
NEW_PASSPHRASE = "new-test-passphrase-rekey"


def _setup_vault(tmp_path: Path) -> tuple[Config, KeyManager, HealthDB, Vault]:
    """Create a fresh vault with test data."""
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir()
    config = Config(vault_home=vault_dir)
    config.ensure_dirs()

    km = KeyManager(config)
    km.setup(OLD_PASSPHRASE)

    vault_obj = Vault(config.blobs_dir, km)
    db = HealthDB(config, km)
    db.open()
    db.run_migrations()
    return config, km, db, vault_obj


class TestRekeyRoundtrip:
    """Test full re-encryption with new passphrase."""

    def test_rekey_roundtrip(self, tmp_path: Path) -> None:
        """Create vault, insert data, rekey, verify data still decrypts."""
        config, km, db, vault_obj = _setup_vault(tmp_path)

        # Insert a lab result
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=date(2024, 6, 15),
        )
        obs_id = db.insert_observation(lab)

        # Insert a medication
        med = Medication(
            id=uuid.uuid4().hex,
            name="Metformin",
            dose="500mg",
            frequency="twice daily",
            status="active",
        )
        db.insert_medication(med)

        # Store a blob
        blob_id = vault_obj.store_blob(b"test document content")

        db.close()

        # Perform rekey
        rekeyer = VaultRekey(config, km)
        backup_path = rekeyer.rotate(NEW_PASSPHRASE)
        assert backup_path.exists()

        # Lock and re-unlock with NEW passphrase
        km.lock()
        assert km.unlock(NEW_PASSPHRASE), "Should unlock with new passphrase"

        # Verify data is still accessible
        db2 = HealthDB(config, km)
        db2.open()

        obs = db2.get_observation(obs_id)
        assert obs is not None
        assert obs.get("test_name") == "Glucose"
        assert obs.get("value") == 95.0

        meds = db2.get_active_medications()
        assert len(meds) >= 1
        assert any(m.get("name") == "Metformin" for m in meds)

        # Verify blob
        data = vault_obj.retrieve_blob(blob_id)
        assert data == b"test document content"

        db2.close()

    def test_old_passphrase_no_longer_works(self, tmp_path: Path) -> None:
        """After rekey, the old passphrase should NOT unlock the vault."""
        config, km, db, vault_obj = _setup_vault(tmp_path)

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="HbA1c",
            canonical_name="hba1c",
            value=5.7,
            unit="%",
        )
        db.insert_observation(lab)
        db.close()

        rekeyer = VaultRekey(config, km)
        rekeyer.rotate(NEW_PASSPHRASE)
        km.lock()

        # Old passphrase should fail
        assert not km.unlock(OLD_PASSPHRASE), (
            "Old passphrase should no longer work"
        )

        # New passphrase should succeed
        assert km.unlock(NEW_PASSPHRASE)
        km.lock()


class TestRekeyBackup:
    """Test that rekey creates a safety backup."""

    def test_rekey_creates_backup(self, tmp_path: Path) -> None:
        """A backup file must exist after rekey."""
        config, km, db, vault_obj = _setup_vault(tmp_path)
        db.close()

        rekeyer = VaultRekey(config, km)
        backup_path = rekeyer.rotate(NEW_PASSPHRASE)

        assert backup_path.exists()
        assert backup_path.suffix == ".enc"
        assert backup_path.stat().st_size > 0


class TestRekeyManifest:
    """Test that manifest is updated correctly."""

    def test_rekey_updates_manifest(self, tmp_path: Path) -> None:
        """Manifest should contain new salt and verification data."""
        config, km, db, vault_obj = _setup_vault(tmp_path)
        db.close()

        # Read old manifest
        old_manifest = json.loads(config.manifest_path.read_text())
        old_salt = old_manifest["kdf"]["salt"]
        old_verify_ct = old_manifest["verify_ct"]

        rekeyer = VaultRekey(config, km)
        rekeyer.rotate(NEW_PASSPHRASE)

        # Read new manifest
        new_manifest = json.loads(config.manifest_path.read_text())
        new_salt = new_manifest["kdf"]["salt"]
        new_verify_ct = new_manifest["verify_ct"]

        assert new_salt != old_salt, "Salt should change after rekey"
        assert new_verify_ct != old_verify_ct, "Verify CT should change after rekey"
        assert "rekeyed_at" in new_manifest
        assert new_manifest["kdf"]["type"] == "argon2id"


class TestRekeyFailureRecovery:
    """Test that failures during rekey preserve the old key."""

    def test_rekey_failure_preserves_old_key(self, tmp_path: Path) -> None:
        """If re-encryption fails, old key must still work."""
        config, km, db, vault_obj = _setup_vault(tmp_path)

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Creatinine",
            canonical_name="creatinine",
            value=1.1,
            unit="mg/dL",
        )
        obs_id = db.insert_observation(lab)
        db.close()

        rekeyer = VaultRekey(config, km)

        # Mock blob preparation to fail (before DB is committed)
        with patch.object(
            rekeyer, "_prepare_blob_reencryption",
            side_effect=OSError("Disk full"),
        ):
            with pytest.raises(RekeyError, match="Disk full"):
                rekeyer.rotate(NEW_PASSPHRASE)

        # Old key should be restored and still work
        assert km.is_unlocked, "Key manager should still be unlocked with old key"
        old_key = km.get_key()
        assert old_key is not None

        # Data should still be accessible with old key
        db2 = HealthDB(config, km)
        db2.open()
        obs = db2.get_observation(obs_id)
        assert obs is not None
        assert obs.get("test_name") == "Creatinine"
        db2.close()

    def test_rekey_failure_during_db_reencrypt(self, tmp_path: Path) -> None:
        """If DB re-encryption fails, the transaction should be rolled back."""
        config, km, db, vault_obj = _setup_vault(tmp_path)

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=2.5,
            unit="mIU/L",
        )
        obs_id = db.insert_observation(lab)
        db.close()

        rekeyer = VaultRekey(config, km)

        # Mock DB re-encryption to fail
        with patch.object(
            rekeyer, "_reencrypt_database",
            side_effect=RuntimeError("Corruption"),
        ):
            with pytest.raises(RekeyError, match="Corruption"):
                rekeyer.rotate(NEW_PASSPHRASE)

        # Old key should be restored
        assert km.is_unlocked

        # Original data should still decrypt with old key
        db2 = HealthDB(config, km)
        db2.open()
        obs = db2.get_observation(obs_id)
        assert obs is not None
        assert obs.get("test_name") == "TSH"
        db2.close()


class TestRekeyEmptyVault:
    """Test rekey on vault with no data."""

    def test_rekey_empty_vault(self, tmp_path: Path) -> None:
        """Rekey should succeed on a vault with no encrypted data."""
        config, km, db, vault_obj = _setup_vault(tmp_path)
        db.close()

        rekeyer = VaultRekey(config, km)
        backup_path = rekeyer.rotate(NEW_PASSPHRASE)
        assert backup_path.exists()

        km.lock()
        assert km.unlock(NEW_PASSPHRASE)
        km.lock()
