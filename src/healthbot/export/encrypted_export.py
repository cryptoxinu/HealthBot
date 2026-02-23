"""Password-protected encrypted export for sharing.

Uses AES-256-GCM with a user-provided password via Argon2id KDF.
SEPARATE from vault key derivation — uses its own salt per export.
"""
from __future__ import annotations

import os
from pathlib import Path

from argon2.low_level import Type, hash_secret_raw
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.config import Config

# Wire format: salt (16) || nonce (12) || ciphertext+tag (variable)
_SALT_LEN = 16
_NONCE_LEN = 12
_AAD = b"doctorpacket_export"

# Argon2id parameters for export key derivation.
_ARGON2_TIME = 3
_ARGON2_MEMORY = 65536  # 64 KiB blocks -> 64 MB
_ARGON2_PARALLELISM = 4
_ARGON2_HASH_LEN = 32  # 256-bit key


def _derive_key(password: str, salt: bytes) -> bytes:
    """Derive a 256-bit AES key from *password* + *salt* via Argon2id."""
    return hash_secret_raw(
        secret=password.encode("utf-8"),
        salt=salt,
        time_cost=_ARGON2_TIME,
        memory_cost=_ARGON2_MEMORY,
        parallelism=_ARGON2_PARALLELISM,
        hash_len=_ARGON2_HASH_LEN,
        type=Type.ID,
    )


class EncryptedExport:
    """Encrypt / decrypt doctor packet exports with a user-chosen password."""

    def __init__(self, config: Config) -> None:
        self._config = config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def encrypt_for_sharing(self, data: bytes, password: str) -> bytes:
        """Encrypt *data* with *password*. Returns ``salt + nonce + ct``."""
        salt = os.urandom(_SALT_LEN)
        key = _derive_key(password, salt)
        nonce = os.urandom(_NONCE_LEN)
        ct = AESGCM(key).encrypt(nonce, data, _AAD)
        return salt + nonce + ct

    def save_encrypted_export(
        self, data: bytes, password: str, filename: str,
    ) -> Path:
        """Encrypt *data* and write to ``exports/<filename>``."""
        encrypted = self.encrypt_for_sharing(data, password)
        self._config.exports_dir.mkdir(parents=True, exist_ok=True)
        path = self._config.exports_dir / filename
        path.write_bytes(encrypted)
        return path

    @staticmethod
    def decrypt_export(encrypted_data: bytes, password: str) -> bytes:
        """Decrypt bytes previously produced by :meth:`encrypt_for_sharing`.

        Raises :class:`cryptography.exceptions.InvalidTag` on wrong password.
        """
        salt = encrypted_data[:_SALT_LEN]
        nonce = encrypted_data[_SALT_LEN : _SALT_LEN + _NONCE_LEN]
        ct = encrypted_data[_SALT_LEN + _NONCE_LEN :]
        key = _derive_key(password, salt)
        return AESGCM(key).decrypt(nonce, ct, _AAD)
