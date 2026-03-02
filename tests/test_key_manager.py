"""Tests for key derivation and session management."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from healthbot.security.key_manager import KeyManager, LockedError


def _mock_config(tmp_path):
    config = MagicMock()
    config.vault_home = tmp_path
    config.manifest_path = tmp_path / "manifest.json"
    config.session_timeout_seconds = 1800
    config.argon2_time_cost = 1
    config.argon2_memory_cost = 1024  # Low for tests
    config.argon2_parallelism = 1
    config.argon2_hash_len = 32
    config.argon2_salt_len = 16
    config.ensure_dirs = MagicMock()
    return config


class TestKeyDerivation:
    def test_derive_key_returns_bytearray(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        key = km.derive_key("passphrase", b"\x00" * 16)
        assert isinstance(key, bytearray)
        assert len(key) == 32

    def test_derive_key_deterministic(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        salt = b"\x01" * 16
        k1 = km.derive_key("passphrase", salt)
        k2 = km.derive_key("passphrase", salt)
        assert k1 == k2

    def test_different_passphrase_different_key(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        salt = b"\x01" * 16
        k1 = km.derive_key("pass1", salt)
        k2 = km.derive_key("pass2", salt)
        assert k1 != k2

    def test_different_salt_different_key(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        k1 = km.derive_key("pass", b"\x01" * 16)
        k2 = km.derive_key("pass", b"\x02" * 16)
        assert k1 != k2


class TestSetupAndUnlock:
    def test_setup_creates_manifest(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        km.setup("test_passphrase")
        assert config.manifest_path.exists()
        assert km.is_unlocked is True

    def test_unlock_with_correct_passphrase(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        km.setup("test_passphrase")
        km.lock()
        assert km.is_unlocked is False
        assert km.unlock("test_passphrase") is True
        assert km.is_unlocked is True

    def test_unlock_with_wrong_passphrase(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        km.setup("test_passphrase")
        km.lock()
        assert km.unlock("wrong_passphrase") is False
        assert km.is_unlocked is False

    def test_unlock_no_manifest(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        assert km.unlock("anything") is False


class TestLockAndSession:
    def test_lock_zeros_key(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        km.setup("test_passphrase")
        km.lock()
        assert km.is_unlocked is False

    def test_get_key_when_locked_raises(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        with pytest.raises(LockedError):
            km.get_key()

    def test_get_key_when_unlocked(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        km.setup("test_passphrase")
        key = km.get_key()
        assert isinstance(key, bytes)
        assert len(key) == 32

    def test_on_lock_callback(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        callback = MagicMock()
        km.set_on_lock(callback)
        km.setup("test_passphrase")
        km.lock()
        callback.assert_called_once()

    def test_on_lock_callback_error_handled(self, tmp_path):
        config = _mock_config(tmp_path)
        km = KeyManager(config)
        km.set_on_lock(MagicMock(side_effect=RuntimeError("boom")))
        km.setup("test_passphrase")
        km.lock()  # Should not raise
        assert km.is_unlocked is False

    def test_session_timeout(self, tmp_path):
        config = _mock_config(tmp_path)
        config.session_timeout_seconds = 0  # Immediate timeout
        km = KeyManager(config)
        km.setup("test_passphrase")
        time.sleep(0.01)
        assert km.is_unlocked is False

    def test_touch_refreshes_timeout(self, tmp_path):
        config = _mock_config(tmp_path)
        config.session_timeout_seconds = 1800
        km = KeyManager(config)
        km.setup("test_passphrase")
        km.touch()
        assert km.is_unlocked is True
