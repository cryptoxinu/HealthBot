"""Encrypted blob storage.

Each blob is encrypted with AES-256-GCM using a random 12-byte nonce.
Storage format on disk: nonce (12 bytes) || ciphertext || tag (16 bytes)
AAD includes the blob UUID for authenticated binding.
"""
from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.security.key_manager import KeyManager


class Vault:
    """Manages encrypted blob storage on disk."""

    def __init__(self, blobs_dir: Path, key_manager: KeyManager) -> None:
        self._blobs_dir = blobs_dir
        self._km = key_manager

    def store_blob(self, data: bytes, blob_id: str | None = None) -> str:
        """Encrypt and store data. Returns UUID identifier."""
        if blob_id is None:
            blob_id = uuid.uuid4().hex
        key = self._km.get_key()
        nonce = os.urandom(12)
        aesgcm = AESGCM(key)
        aad = blob_id.encode("utf-8")
        ct = aesgcm.encrypt(nonce, data, aad)
        # Write nonce || ciphertext_with_tag (atomic: temp file + rename)
        out_path = self._blobs_dir / f"{blob_id}.enc"
        fd, tmp_path = tempfile.mkstemp(dir=self._blobs_dir, suffix=".tmp")
        closed = False
        try:
            os.write(fd, nonce + ct)
            os.close(fd)
            closed = True
            os.rename(tmp_path, out_path)
        except BaseException:
            if not closed:
                os.close(fd)
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        return blob_id

    def retrieve_blob(self, blob_id: str) -> bytes:
        """Load and decrypt a blob by UUID."""
        key = self._km.get_key()
        path = self._blobs_dir / f"{blob_id}.enc"
        raw = path.read_bytes()
        nonce = raw[:12]
        ct = raw[12:]
        aesgcm = AESGCM(key)
        aad = blob_id.encode("utf-8")
        return aesgcm.decrypt(nonce, ct, aad)

    def delete_blob(self, blob_id: str) -> None:
        """Delete a blob file."""
        path = self._blobs_dir / f"{blob_id}.enc"
        if path.exists():
            # Overwrite with random bytes before unlinking (best-effort secure delete)
            size = path.stat().st_size
            path.write_bytes(os.urandom(size))
            path.unlink()

    def list_blobs(self) -> list[str]:
        """List all blob UUIDs."""
        return [
            p.stem for p in self._blobs_dir.glob("*.enc")
        ]

    def blob_exists(self, blob_id: str) -> bool:
        """Check if a blob exists."""
        return (self._blobs_dir / f"{blob_id}.enc").exists()
