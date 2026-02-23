"""Tests for security/keychain.py — macOS Keychain operations (mocked)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from healthbot.security.keychain import KEYCHAIN_SERVICE, Keychain, KeychainError


@pytest.fixture
def keychain():
    return Keychain()


class TestStore:
    @patch("healthbot.security.keychain.subprocess.run")
    def test_store_success(self, mock_run, keychain):
        mock_run.return_value = MagicMock(returncode=0)
        keychain.store("test_account", "test_password")
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "add-generic-password" in cmd
        assert "-a" in cmd
        assert "test_account" in cmd
        assert "-w" in cmd
        assert "test_password" in cmd
        assert KEYCHAIN_SERVICE in cmd

    @patch("healthbot.security.keychain.subprocess.run")
    def test_store_failure_raises(self, mock_run, keychain):
        mock_run.return_value = MagicMock(returncode=1, stderr="error msg")
        with pytest.raises(KeychainError, match="Failed to store"):
            keychain.store("test_account", "test_password")


class TestRetrieve:
    @patch("healthbot.security.keychain.subprocess.run")
    def test_retrieve_success(self, mock_run, keychain):
        mock_run.return_value = MagicMock(returncode=0, stdout="my_secret\n")
        result = keychain.retrieve("test_account")
        assert result == "my_secret"

    @patch("healthbot.security.keychain.subprocess.run")
    def test_retrieve_not_found(self, mock_run, keychain):
        mock_run.return_value = MagicMock(returncode=44)
        result = keychain.retrieve("nonexistent")
        assert result is None


class TestDelete:
    @patch("healthbot.security.keychain.subprocess.run")
    def test_delete_success(self, mock_run, keychain):
        mock_run.return_value = MagicMock(returncode=0)
        result = keychain.delete("test_account")
        assert result is True

    @patch("healthbot.security.keychain.subprocess.run")
    def test_delete_not_found(self, mock_run, keychain):
        mock_run.return_value = MagicMock(returncode=44)
        result = keychain.delete("nonexistent")
        assert result is False
