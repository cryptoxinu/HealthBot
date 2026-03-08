"""Vault restore from encrypted backup."""
from __future__ import annotations

import io
import logging
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.config import Config
from healthbot.security.key_manager import KeyManager

logger = logging.getLogger("healthbot")


class RestoreError(Exception):
    """Raised when restore fails."""


class VaultRestore:
    """Restore vault from an encrypted backup."""

    def __init__(self, config: Config, key_manager: KeyManager) -> None:
        self._config = config
        self._km = key_manager

    def restore(self, backup_path: Path, passphrase: str) -> None:
        """Restore a vault from an encrypted backup.

        1. Read backup file
        2. Parse AAD + nonce + ciphertext
        3. Derive key from passphrase
        4. Decrypt
        5. Decompress with zstd
        6. Extract tar to vault directory
        """
        raw = backup_path.read_bytes()

        # Parse format: aad_len(4) || aad || nonce(12) || ciphertext
        aad_len = int.from_bytes(raw[:4], "big")
        aad = raw[4 : 4 + aad_len]
        nonce = raw[4 + aad_len : 4 + aad_len + 12]
        ciphertext = raw[4 + aad_len + 12 :]

        # Derive key — try KDF params from AAD first (new self-contained format),
        # fall back to manifest on disk (old backups)
        import json
        kdf_params = None
        try:
            aad_data = json.loads(aad)
            kdf_params = aad_data.get("kdf")
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass  # Old-format AAD (plain string like "backup_TIMESTAMP")

        if not kdf_params:
            manifest_path = self._config.manifest_path
            if manifest_path.exists():
                kdf_params = json.loads(manifest_path.read_text()).get("kdf")
            else:
                raise RestoreError(
                    "Backup uses old format and no manifest found on disk. "
                    "Run --setup first, or use a backup created with the new format."
                )

        salt = bytes.fromhex(kdf_params["salt"])
        key = self._km.derive_key(passphrase, salt)

        # Decrypt
        aesgcm = AESGCM(key)
        try:
            compressed = aesgcm.decrypt(nonce, ciphertext, aad)
        except Exception as e:
            raise RestoreError(f"Decryption failed (wrong passphrase?): {e}") from e

        # Decompress
        zstd_bin = shutil.which("zstd")
        if zstd_bin:
            try:
                _env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
                result = subprocess.run(
                    [zstd_bin, "--decompress", "-"],
                    input=compressed,
                    capture_output=True,
                    timeout=300,
                    env=_env,
                )
                if result.returncode == 0:
                    tar_bytes = result.stdout
                else:
                    tar_bytes = compressed  # Fallback: wasn't compressed
            except subprocess.TimeoutExpired:
                tar_bytes = compressed
        else:
            # Try treating as uncompressed tar; if it fails, zstd is needed
            if compressed[:4] == b"\x28\xb5\x2f\xfd":  # zstd magic bytes
                raise RestoreError(
                    "Backup is zstd-compressed but zstd is not installed. "
                    "Install: brew install zstd"
                )
            tar_bytes = compressed  # Not zstd-compressed

        # Extract to staging directory, then atomic swap (prevents
        # inconsistent vault state if extraction fails midway)
        self._config.ensure_dirs()
        vault_home = self._config.vault_home
        staging = vault_home.parent / f".healthbot_restore_{os.getpid()}"
        staging.mkdir(parents=True, exist_ok=True)
        try:
            with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
                tar.extractall(str(staging), filter="data")
            # Atomic swap: move current vault aside, move staging in
            old = vault_home.parent / f".healthbot_old_{os.getpid()}"
            vault_home.rename(old)
            try:
                staging.rename(vault_home)
            except BaseException:
                # Restore original vault if staging rename fails
                old.rename(vault_home)
                raise
            shutil.rmtree(old, ignore_errors=True)
            logger.info("Vault restored successfully from backup")
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
