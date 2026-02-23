"""Tests for password-protected encrypted export."""
from __future__ import annotations

from pathlib import Path

import pytest
from cryptography.exceptions import InvalidTag

from healthbot.config import Config
from healthbot.export.encrypted_export import EncryptedExport

_SALT_LEN = 16
_NONCE_LEN = 12


@pytest.fixture
def exporter(tmp_path: Path) -> EncryptedExport:
    vault = tmp_path / ".healthbot"
    vault.mkdir()
    (vault / "exports").mkdir()
    cfg = Config(vault_home=vault)
    return EncryptedExport(cfg)


class TestEncryptDecryptRoundtrip:
    """Core encrypt / decrypt cycle."""

    def test_encrypt_decrypt_roundtrip(self, exporter: EncryptedExport) -> None:
        plaintext = b"Doctor visit packet PDF bytes here"
        password = "strong-test-password-42!"
        encrypted = exporter.encrypt_for_sharing(plaintext, password)
        decrypted = EncryptedExport.decrypt_export(encrypted, password)
        assert decrypted == plaintext

    def test_large_payload_roundtrip(self, exporter: EncryptedExport) -> None:
        plaintext = b"A" * 1_000_000  # 1 MB
        password = "big-file-password"
        encrypted = exporter.encrypt_for_sharing(plaintext, password)
        decrypted = EncryptedExport.decrypt_export(encrypted, password)
        assert decrypted == plaintext


class TestWrongPassword:
    """Decryption with wrong password must fail."""

    def test_wrong_password_fails(self, exporter: EncryptedExport) -> None:
        plaintext = b"secret data"
        encrypted = exporter.encrypt_for_sharing(plaintext, "correct-password")
        with pytest.raises(InvalidTag):
            EncryptedExport.decrypt_export(encrypted, "wrong-password")


class TestOutputFormat:
    """Wire format: salt(16) + nonce(12) + ciphertext."""

    def test_output_format_salt_nonce_ct(self, exporter: EncryptedExport) -> None:
        plaintext = b"hello"
        encrypted = exporter.encrypt_for_sharing(plaintext, "pw")
        # Must be longer than salt + nonce + at least 1 byte ct + 16 tag
        assert len(encrypted) > _SALT_LEN + _NONCE_LEN + 16
        # The first 16 bytes are the salt, next 12 the nonce.
        salt = encrypted[:_SALT_LEN]
        nonce = encrypted[_SALT_LEN : _SALT_LEN + _NONCE_LEN]
        ct = encrypted[_SALT_LEN + _NONCE_LEN :]
        # All parts should be non-empty.
        assert len(salt) == _SALT_LEN
        assert len(nonce) == _NONCE_LEN
        assert len(ct) > 0

    def test_different_encryptions_differ(self, exporter: EncryptedExport) -> None:
        """Two encryptions of the same data should produce different output (random salt/nonce)."""
        plaintext = b"same content"
        pw = "same-password"
        a = exporter.encrypt_for_sharing(plaintext, pw)
        b = exporter.encrypt_for_sharing(plaintext, pw)
        assert a != b  # Random salt + nonce should make them different.


class TestSaveEncryptedExport:
    """Test saving to exports directory."""

    def test_save_encrypted_export(self, exporter: EncryptedExport) -> None:
        plaintext = b"PDF content here"
        password = "save-test"
        path = exporter.save_encrypted_export(plaintext, password, "packet_2024.enc")
        assert path.exists()
        assert path.name == "packet_2024.enc"
        # Should be decryptable from file.
        raw = path.read_bytes()
        decrypted = EncryptedExport.decrypt_export(raw, password)
        assert decrypted == plaintext
