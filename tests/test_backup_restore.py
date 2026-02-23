"""Tests for vault backup and restore."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from healthbot.config import Config
from healthbot.security.key_manager import KeyManager
from healthbot.vault_ops.backup import VaultBackup
from healthbot.vault_ops.restore import RestoreError, VaultRestore

TEST_PASSPHRASE = "test-passphrase-do-not-use-in-production"


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def backup_env(tmp_path: Path):
    """Set up a vault directory with some test data."""
    vault_home = tmp_path / ".healthbot"
    vault_home.mkdir()
    (vault_home / "db").mkdir()
    (vault_home / "vault").mkdir()
    (vault_home / "index").mkdir()
    (vault_home / "backups").mkdir()
    (vault_home / "config").mkdir()

    # Write some test data
    (vault_home / "db" / "health.db").write_text("fake-db-content")
    (vault_home / "config" / "app.json").write_text('{"test": true}')

    config = Config(vault_home=vault_home)
    km = KeyManager(config)
    km.setup(TEST_PASSPHRASE)  # Creates manifest.json with salt
    return config, km


# ── Backup Tests ─────────────────────────────────────────────────────

class TestVaultBackup:
    def test_create_backup_produces_file(self, backup_env):
        config, km = backup_env
        vb = VaultBackup(config, km)
        path = vb.create_backup()
        assert path.exists()
        assert path.suffix == ".enc"
        assert path.stat().st_size > 0

    def test_backup_file_format(self, backup_env):
        """Verify binary format: aad_len(4) || aad || nonce(12) || ciphertext."""
        config, km = backup_env
        vb = VaultBackup(config, km)
        path = vb.create_backup()
        raw = path.read_bytes()

        aad_len = int.from_bytes(raw[:4], "big")
        aad = raw[4 : 4 + aad_len]
        nonce = raw[4 + aad_len : 4 + aad_len + 12]
        ciphertext = raw[4 + aad_len + 12 :]

        aad_data = json.loads(aad)
        assert "backup_id" in aad_data
        assert aad_data["backup_id"].startswith("backup_")
        assert "kdf" in aad_data
        assert "salt" in aad_data["kdf"]
        assert len(nonce) == 12
        assert len(ciphertext) > 0

    def test_list_backups_empty(self, backup_env):
        config, km = backup_env
        vb = VaultBackup(config, km)
        assert vb.list_backups() == []

    def test_list_backups_after_create(self, backup_env):
        config, km = backup_env
        vb = VaultBackup(config, km)
        vb.create_backup()
        backups = vb.list_backups()
        assert len(backups) == 1
        assert "name" in backups[0]
        assert "size" in backups[0]
        assert backups[0]["size"] > 0

    def test_multiple_backups(self, backup_env):
        config, km = backup_env
        vb = VaultBackup(config, km)

        # Use deterministic time advancement instead of sleep
        t0 = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        t1 = t0 + timedelta(seconds=2)
        with patch("healthbot.vault_ops.backup.datetime") as mock_dt:
            mock_dt.now.return_value = t0
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            vb.create_backup()
            mock_dt.now.return_value = t1
            vb.create_backup()
        assert len(vb.list_backups()) == 2

    def test_zstd_fallback(self, backup_env):
        """Backup should succeed even without zstd (fallback to uncompressed)."""
        config, km = backup_env
        vb = VaultBackup(config, km)
        with patch("healthbot.vault_ops.backup.shutil.which", return_value=None):
            path = vb.create_backup()
        assert path.exists()
        assert path.stat().st_size > 0

    def test_backup_name_contains_timestamp(self, backup_env):
        config, km = backup_env
        vb = VaultBackup(config, km)
        path = vb.create_backup()
        assert path.name.startswith("vault_")
        assert path.name.endswith(".bak.enc")


# ── Retention Tests ──────────────────────────────────────────────────

class TestBackupRetention:
    def _create_backup_at(self, backups_dir: Path, dt: datetime) -> Path:
        """Create a fake backup file with a specific mtime."""
        ts = dt.strftime("%Y%m%dT%H%M%S")
        p = backups_dir / f"vault_{ts}.bak.enc"
        p.write_bytes(b"fake-backup-data")
        import os
        os.utime(p, (dt.timestamp(), dt.timestamp()))
        return p

    def test_no_pruning_when_under_limit(self, backup_env):
        config, km = backup_env
        config.backup_daily_retention = 7
        config.backup_weekly_retention = 4
        vb = VaultBackup(config, km)
        # Create 5 backups (under daily limit of 7)
        now = datetime(2025, 6, 15, 12, 0, tzinfo=UTC)
        for i in range(5):
            self._create_backup_at(config.backups_dir, now - timedelta(days=i))
        deleted = vb.cleanup_old_backups()
        assert deleted == 0
        assert len(list(config.backups_dir.glob("*.bak.enc"))) == 5

    def test_keeps_daily_limit(self, backup_env):
        config, km = backup_env
        config.backup_daily_retention = 3
        config.backup_weekly_retention = 0
        vb = VaultBackup(config, km)
        now = datetime(2025, 6, 15, 12, 0, tzinfo=UTC)
        for i in range(6):
            self._create_backup_at(config.backups_dir, now - timedelta(days=i))
        deleted = vb.cleanup_old_backups()
        assert deleted == 3
        remaining = sorted(config.backups_dir.glob("*.bak.enc"))
        assert len(remaining) == 3

    def test_keeps_weekly_from_older(self, backup_env):
        config, km = backup_env
        config.backup_daily_retention = 3
        config.backup_weekly_retention = 2
        vb = VaultBackup(config, km)
        now = datetime(2025, 6, 15, 12, 0, tzinfo=UTC)
        # 3 recent (daily keepers) + 4 older across different weeks
        for i in range(3):
            self._create_backup_at(config.backups_dir, now - timedelta(days=i))
        for i in range(4):
            self._create_backup_at(
                config.backups_dir, now - timedelta(weeks=i + 1, hours=i)
            )
        assert len(list(config.backups_dir.glob("*.bak.enc"))) == 7
        deleted = vb.cleanup_old_backups()
        remaining = list(config.backups_dir.glob("*.bak.enc"))
        # 3 daily + 2 weekly = 5 kept, 2 deleted
        assert len(remaining) == 5
        assert deleted == 2

    def test_cleanup_returns_zero_on_empty(self, backup_env):
        config, km = backup_env
        vb = VaultBackup(config, km)
        assert vb.cleanup_old_backups() == 0

    def test_same_week_keeps_one(self, backup_env):
        """Multiple backups in the same week beyond daily limit: keep one per week."""
        config, km = backup_env
        config.backup_daily_retention = 2
        config.backup_weekly_retention = 1
        vb = VaultBackup(config, km)
        now = datetime(2025, 6, 15, 12, 0, tzinfo=UTC)
        # 2 daily keepers
        self._create_backup_at(config.backups_dir, now)
        self._create_backup_at(config.backups_dir, now - timedelta(days=1))
        # 3 older, all in the same week
        for i in range(3):
            self._create_backup_at(
                config.backups_dir, now - timedelta(days=8, hours=i)
            )
        assert len(list(config.backups_dir.glob("*.bak.enc"))) == 5
        deleted = vb.cleanup_old_backups()
        # 2 daily + 1 weekly = 3 kept, 2 deleted
        assert len(list(config.backups_dir.glob("*.bak.enc"))) == 3
        assert deleted == 2


# ── Restore Tests ────────────────────────────────────────────────────

class TestVaultRestore:
    def test_round_trip(self, backup_env):
        """Backup then restore should produce the same data."""
        config, km = backup_env
        # Create backup
        vb = VaultBackup(config, km)
        backup_path = vb.create_backup()

        # Wipe vault data
        db_file = config.vault_home / "db" / "health.db"
        db_file.unlink()
        assert not db_file.exists()

        # manifest.json already exists from km.setup()
        vr = VaultRestore(config, km)
        vr.restore(backup_path, TEST_PASSPHRASE)
        assert db_file.exists()
        assert db_file.read_text() == "fake-db-content"

    def test_wrong_passphrase(self, backup_env):
        """Restore with wrong passphrase should raise RestoreError."""
        config, km = backup_env
        vb = VaultBackup(config, km)
        backup_path = vb.create_backup()

        # manifest.json already exists from km.setup()
        vr = VaultRestore(config, km)
        with pytest.raises(RestoreError, match="Decryption failed"):
            vr.restore(backup_path, "wrong-passphrase-here")

    def test_restore_fresh_machine(self, backup_env):
        """Restore on a fresh machine (no manifest) should work — KDF is in AAD."""
        config, km = backup_env
        vb = VaultBackup(config, km)
        backup_path = vb.create_backup()

        # Wipe vault data AND manifest (simulates a fresh machine)
        db_file = config.vault_home / "db" / "health.db"
        db_file.unlink()
        config.manifest_path.unlink()

        vr = VaultRestore(config, km)
        vr.restore(backup_path, TEST_PASSPHRASE)
        # DB restored, and manifest restored from inside the tar
        assert db_file.exists()
        assert db_file.read_text() == "fake-db-content"
        assert config.manifest_path.exists()

    def test_restore_zstd_fallback(self, backup_env):
        """Restore should handle uncompressed backups when zstd not available."""
        config, km = backup_env

        # Create backup without zstd
        vb = VaultBackup(config, km)
        with patch("healthbot.vault_ops.backup.shutil.which", return_value=None):
            backup_path = vb.create_backup()

        # Wipe and restore without zstd
        db_file = config.vault_home / "db" / "health.db"
        db_file.unlink()

        vr = VaultRestore(config, km)
        with patch("healthbot.vault_ops.restore.shutil.which", return_value=None):
            vr.restore(backup_path, TEST_PASSPHRASE)
        assert db_file.exists()

    def test_old_format_backup_falls_back_to_manifest(self, backup_env):
        """Old-format backups (plain string AAD) fall back to disk manifest."""
        import io
        import os
        import tarfile

        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        config, km = backup_env
        key = km.get_key()

        # Build an old-format backup manually (plain string AAD)
        tar_buf = io.BytesIO()
        with tarfile.open(fileobj=tar_buf, mode="w") as tar:
            for item in ["db", "vault", "index", "manifest.json", "config"]:
                path = config.vault_home / item
                if path.exists():
                    tar.add(str(path), arcname=item)
        tar_bytes = tar_buf.getvalue()

        nonce = os.urandom(12)
        aesgcm = AESGCM(key)
        aad = b"backup_20250101T000000"  # Old plain-string format
        encrypted = aesgcm.encrypt(nonce, tar_bytes, aad)

        aad_len = len(aad).to_bytes(4, "big")
        backup_path = config.backups_dir / "old_format.bak.enc"
        backup_path.write_bytes(aad_len + aad + nonce + encrypted)

        # Wipe DB, keep manifest (old format needs it on disk)
        db_file = config.vault_home / "db" / "health.db"
        db_file.unlink()

        vr = VaultRestore(config, km)
        vr.restore(backup_path, TEST_PASSPHRASE)
        assert db_file.exists()
        assert db_file.read_text() == "fake-db-content"

    def test_old_format_no_manifest_raises(self, backup_env):
        """Old-format backup without manifest on disk should raise RestoreError."""
        import io
        import os
        import tarfile

        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        config, km = backup_env
        key = km.get_key()

        # Build old-format backup
        tar_buf = io.BytesIO()
        with tarfile.open(fileobj=tar_buf, mode="w") as tar:
            for item in ["db", "vault"]:
                path = config.vault_home / item
                if path.exists():
                    tar.add(str(path), arcname=item)

        nonce = os.urandom(12)
        aesgcm = AESGCM(key)
        aad = b"backup_old"
        encrypted = aesgcm.encrypt(nonce, tar_buf.getvalue(), aad)

        aad_len = len(aad).to_bytes(4, "big")
        backup_path = config.backups_dir / "old_no_manifest.bak.enc"
        backup_path.write_bytes(aad_len + aad + nonce + encrypted)

        # Remove manifest from disk
        config.manifest_path.unlink()

        vr = VaultRestore(config, km)
        with pytest.raises(RestoreError, match="old format"):
            vr.restore(backup_path, TEST_PASSPHRASE)
