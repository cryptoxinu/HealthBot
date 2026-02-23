"""Key derivation and session management.

Uses Argon2id for key derivation from a user passphrase.
The master key lives only in memory and is zeroed on lock/timeout.
"""
from __future__ import annotations

import ctypes
import json
import logging
import os
import threading
import time
from collections.abc import Callable

from argon2.low_level import Type, hash_secret_raw

from healthbot.config import Config

logger = logging.getLogger("healthbot")


class LockedError(Exception):
    """Raised when an operation requires an unlocked vault."""


class KeyManager:
    """Manages key derivation, session lock/unlock, and key zeroing."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._master_key: bytearray | None = None
        self._last_activity: float = 0.0
        self._on_lock: Callable[[], None] | None = None
        self._lock = threading.Lock()

    def set_on_lock(self, callback: Callable[[], None] | None) -> None:
        """Register a callback that fires BEFORE the key is zeroed on lock.

        Used by Handlers to trigger memory consolidation on passive timeout.
        The callback runs while the key is still available (for DB access).
        """
        self._on_lock = callback

    @property
    def is_unlocked(self) -> bool:
        """Check if vault is currently unlocked and session is active.

        Called from the main event loop. Triggers lock cascade on timeout.
        """
        with self._lock:
            if self._master_key is None:
                return False
            if time.time() - self._last_activity > self._config.session_timeout_seconds:
                pass  # Fall through to lock() outside the lock
            else:
                return True
        # Timed out — trigger full lock cascade (outside self._lock to avoid
        # deadlock since lock() acquires self._lock internally)
        self.lock()
        return False

    def derive_key(self, passphrase: str, salt: bytes) -> bytes:
        """Derive 256-bit key from passphrase using Argon2id."""
        raw = hash_secret_raw(
            secret=passphrase.encode("utf-8"),
            salt=salt,
            time_cost=self._config.argon2_time_cost,
            memory_cost=self._config.argon2_memory_cost,
            parallelism=self._config.argon2_parallelism,
            hash_len=self._config.argon2_hash_len,
            type=Type.ID,
        )
        return raw

    def setup(self, passphrase: str) -> None:
        """First-time vault setup. Generate salt, store manifest."""
        salt = os.urandom(self._config.argon2_salt_len)
        key = self.derive_key(passphrase, salt)

        # Create a verification tag: encrypt a known plaintext
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        nonce = os.urandom(12)
        aesgcm = AESGCM(key)
        verify_ct = aesgcm.encrypt(nonce, b"HEALTHBOT_VERIFY", b"verify")

        manifest = {
            "schema_version": 1,
            "vault_version": "0.1.0",
            "kdf": {
                "type": "argon2id",
                "time_cost": self._config.argon2_time_cost,
                "memory_cost": self._config.argon2_memory_cost,
                "parallelism": self._config.argon2_parallelism,
                "hash_len": self._config.argon2_hash_len,
                "salt": salt.hex(),
            },
            "cipher": "AES-256-GCM",
            "verify_nonce": nonce.hex(),
            "verify_ct": verify_ct.hex(),
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        self._config.ensure_dirs()
        self._config.manifest_path.write_text(json.dumps(manifest, indent=2))

        with self._lock:
            self._master_key = bytearray(key)
            self._last_activity = time.time()

    def unlock(self, passphrase: str) -> bool:
        """Unlock the vault. Returns True on success."""
        manifest_path = self._config.manifest_path
        if not manifest_path.exists():
            return False

        manifest = json.loads(manifest_path.read_text())
        kdf = manifest["kdf"]
        salt = bytes.fromhex(kdf["salt"])

        key = self.derive_key(passphrase, salt)

        # Verify the key against stored verification tag
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        nonce = bytes.fromhex(manifest["verify_nonce"])
        verify_ct = bytes.fromhex(manifest["verify_ct"])
        aesgcm = AESGCM(key)
        try:
            plaintext = aesgcm.decrypt(nonce, verify_ct, b"verify")
            if plaintext != b"HEALTHBOT_VERIFY":
                return False
        except Exception:
            return False

        with self._lock:
            self._master_key = bytearray(key)
            self._last_activity = time.time()
        return True

    def verify_passphrase(self, passphrase: str) -> bool:
        """Verify a passphrase against the vault manifest without changing state.

        Used by /rekey to confirm the user knows the current passphrase
        before allowing re-encryption. Does NOT modify _master_key.
        """
        manifest_path = self._config.manifest_path
        if not manifest_path.exists():
            return False

        manifest = json.loads(manifest_path.read_text())
        kdf = manifest["kdf"]
        salt = bytes.fromhex(kdf["salt"])

        key = self.derive_key(passphrase, salt)

        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        nonce = bytes.fromhex(manifest["verify_nonce"])
        verify_ct = bytes.fromhex(manifest["verify_ct"])
        aesgcm = AESGCM(key)
        try:
            plaintext = aesgcm.decrypt(nonce, verify_ct, b"verify")
            return plaintext == b"HEALTHBOT_VERIFY"
        except Exception:
            return False

    def lock(self) -> None:
        """Fire on_lock callback, then zero the master key and clear session state.

        The callback runs BEFORE the key is zeroed so it can still access
        encrypted data (e.g. for memory consolidation).
        """
        if self._on_lock:
            try:
                self._on_lock()
            except Exception as e:
                logger.warning("on_lock callback failed: %s", e)
        with self._lock:
            if self._master_key is not None:
                self._zero_bytearray(self._master_key)
                self._master_key = None
            self._last_activity = 0.0

    def get_clean_key(self) -> bytes:
        """Derive a secondary key for the clean (anonymized) data store.

        Uses HKDF with 'healthbot-clean-v1' context for cryptographic
        separation from the master key.
        """
        from cryptography.hazmat.primitives.hashes import SHA256
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF

        master = self.get_key()
        hkdf = HKDF(
            algorithm=SHA256(),
            length=32,
            salt=None,
            info=b"healthbot-clean-v1",
        )
        return hkdf.derive(master)

    def get_key(self) -> bytes:
        """Return master key if session is active. Raises LockedError if not.

        Thread-safe. Does NOT trigger lock cascade on timeout — just raises
        LockedError. Does NOT reset the activity timer — only explicit user
        actions (via touch()) should extend the session. Background jobs
        (med reminders, incoming poll, etc.) must not keep the session alive.
        """
        with self._lock:
            if self._master_key is None:
                raise LockedError("Vault is locked. Send /unlock first.")
            if time.time() - self._last_activity > self._config.session_timeout_seconds:
                raise LockedError("Vault is locked. Send /unlock first.")
            return bytes(self._master_key)

    def touch(self) -> None:
        """Refresh the session timeout."""
        with self._lock:
            if self._master_key is not None:
                self._last_activity = time.time()

    def get_remaining_seconds(self) -> float:
        """Seconds until auto-lock. Negative if expired. -1 if locked."""
        with self._lock:
            if self._master_key is None:
                return -1.0
            elapsed = time.time() - self._last_activity
            return self._config.session_timeout_seconds - elapsed

    def _zero_bytearray(self, data: bytearray) -> None:
        """Overwrite bytearray contents with zeros. Best-effort for CPython."""
        n = len(data)
        if n == 0:
            return
        # bytearray is mutable, so we can overwrite in-place
        for i in range(n):
            data[i] = 0
        # Also try ctypes for the underlying buffer
        try:
            buf = (ctypes.c_char * n).from_buffer(data)
            ctypes.memset(buf, 0, n)
        except Exception:
            pass  # Best-effort
