"""Tests for portable vault functionality."""
from __future__ import annotations

import json
import shutil
import uuid
from datetime import date

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult
from healthbot.security.key_manager import KeyManager
from healthbot.security.vault import Vault
from healthbot.vault_ops.backup import VaultBackup

PASSPHRASE = "test-vault-portability-passphrase"


class TestPortableVaultRoundtrip:
    """Test that vault can be moved to a new location and still work."""

    def test_roundtrip(self, tmp_path):
        """Create vault, add data, copy to new location, verify works."""
        # 1. Create vault at location A
        vault_a = tmp_path / "vault_a"
        vault_a.mkdir()
        config_a = Config(vault_home=vault_a)
        config_a.ensure_dirs()

        km_a = KeyManager(config_a)
        km_a.setup(PASSPHRASE)

        vault = Vault(config_a.blobs_dir, km_a)
        db_a = HealthDB(config_a, km_a)
        db_a.open()
        db_a.run_migrations()

        # Insert a lab result
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=date(2024, 1, 15),
        )
        obs_id = db_a.insert_observation(lab)

        # Store a blob
        blob_id = vault.store_blob(b"test pdf content")
        db_a.close()
        km_a.lock()

        # 2. Copy vault bundle to location B (simulating new machine)
        vault_b = tmp_path / "vault_b"
        shutil.copytree(vault_a, vault_b)

        # 3. Open vault at location B
        config_b = Config(vault_home=vault_b)
        km_b = KeyManager(config_b)
        assert km_b.unlock(PASSPHRASE), "Should unlock with same passphrase"

        vault_b_obj = Vault(config_b.blobs_dir, km_b)
        db_b = HealthDB(config_b, km_b)
        db_b.open()

        # 4. Verify data is accessible
        obs = db_b.get_observation(obs_id)
        assert obs is not None
        assert obs.get("test_name") == "Glucose"
        assert obs.get("value") == 95.0

        # 5. Verify blob is accessible
        data = vault_b_obj.retrieve_blob(blob_id)
        assert data == b"test pdf content"

        db_b.close()
        km_b.lock()


class TestBackupRestore:
    """Test encrypted backup and restore."""

    def test_backup_produces_encrypted_file(self, config, key_manager, vault, db):
        """Backup archive must not contain plaintext."""
        # Store some data
        vault.store_blob(b"%PDF-1.4 fake pdf content")
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Hemoglobin",
            canonical_name="hemoglobin",
            value=14.5,
            unit="g/dL",
        )
        db.insert_observation(lab)

        # Create backup
        vb = VaultBackup(config, key_manager)
        backup_path = vb.create_backup()

        assert backup_path.exists()
        content = backup_path.read_bytes()
        # Must NOT contain plaintext PDF header
        assert b"%PDF" not in content, "Backup must not contain plaintext %PDF"
        # Must NOT contain plaintext test names
        assert b"Hemoglobin" not in content, "Backup must not contain plaintext data"


class TestManifestSecurity:
    """Test manifest contains no secrets."""

    def test_manifest_no_keys(self, config, key_manager):
        """manifest.json must not contain key material."""
        manifest_path = config.manifest_path
        assert manifest_path.exists()
        content = manifest_path.read_text()
        manifest = json.loads(content)

        # Check no key-like fields
        assert "master_key" not in manifest
        assert "key" not in manifest
        assert "password" not in manifest
        assert "passphrase" not in manifest
        assert "secret" not in manifest

        # Check only expected fields
        expected = {"schema_version", "vault_version", "kdf", "cipher",
                    "verify_nonce", "verify_ct", "created_at"}
        assert set(manifest.keys()) <= expected

        # KDF section should not have the derived key
        kdf = manifest["kdf"]
        assert "derived_key" not in kdf
        assert "key" not in kdf


class TestKeychainOptional:
    """Test that keychain is optional (passphrase-only unlock works)."""

    def test_passphrase_unlock_without_keychain(self, tmp_path):
        """Vault should work with passphrase alone, no keychain needed."""
        vault_dir = tmp_path / "no_keychain_vault"
        vault_dir.mkdir()
        config = Config(vault_home=vault_dir)
        config.ensure_dirs()

        km = KeyManager(config)
        km.setup("my-secure-passphrase")
        assert km.is_unlocked

        # Lock and re-unlock
        km.lock()
        assert not km.is_unlocked
        assert km.unlock("my-secure-passphrase")
        assert km.is_unlocked

        km.lock()
