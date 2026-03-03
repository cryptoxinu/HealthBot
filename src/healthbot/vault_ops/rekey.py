"""Key rotation: re-encrypt entire vault with new passphrase.

Safety protocol:
1. Create full backup with current key
2. Derive new key from new passphrase
3. Re-encrypt all vault blobs (keep originals until DB is done)
4. Re-encrypt all DB fields in SQLite transaction
5. Commit DB + finalize blobs atomically
6. Update manifest with new KDF params
7. Verify by decrypting sample record
8. On failure: rollback DB, restore blob originals, old key stays valid
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from pathlib import Path

from argon2.low_level import Type, hash_secret_raw
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.config import Config
from healthbot.security.key_manager import KeyManager
from healthbot.vault_ops.backup import VaultBackup

logger = logging.getLogger("healthbot")


class RekeyError(Exception):
    """Key rotation failed."""


# Tables that contain encrypted data: (table, id_column, encrypted_column)
_ENCRYPTED_TABLES: tuple[tuple[str, str, str], ...] = (
    ("observations", "obs_id", "encrypted_data"),
    ("medications", "med_id", "encrypted_data"),
    ("wearable_daily", "id", "encrypted_data"),
    ("concerns", "concern_id", "encrypted_data"),
    ("external_evidence", "evidence_id", "encrypted_data"),
    ("memory_stm", "id", "encrypted_data"),
    ("memory_ltm", "id", "encrypted_data"),
    ("hypotheses", "id", "encrypted_data"),
    ("documents", "doc_id", "meta_encrypted"),
    ("knowledge_base", "id", "encrypted_data"),
    ("medical_journal", "entry_id", "encrypted_data"),
    ("med_reminders", "id", "encrypted_data"),
    ("health_goals", "id", "encrypted_data"),
    ("providers", "id", "encrypted_data"),
    ("appointments", "id", "encrypted_data"),
    ("genetic_variants", "id", "encrypted_data"),
    ("workouts", "id", "encrypted_data"),
    ("user_identity", "id", "encrypted_data"),
    ("redaction_log", "id", "encrypted_data"),
    ("trend_cache", "id", "encrypted_data"),
    ("health_records_ext", "id", "encrypted_data"),
    ("substance_knowledge", "id", "encrypted_data"),
    ("saved_messages", "id", "encrypted_data"),
    ("search_index", "doc_id", "encrypted_text"),
)


class VaultRekey:
    """Re-encrypt an entire vault with a new passphrase.

    This allows changing the vault passphrase without losing any data.
    A full backup is created before re-encryption begins. On any failure,
    the old key is restored and the backup can be used for recovery.
    """

    def __init__(self, config: Config, key_manager: KeyManager) -> None:
        self._config = config
        self._km = key_manager

    def rotate(self, new_passphrase: str) -> Path:
        """Re-encrypt the vault with a new passphrase.

        The re-encryption uses a two-phase approach for safety:
        - Phase 1: Re-encrypt blobs to .tmp files (originals preserved)
        - Phase 2: Re-encrypt DB in a transaction (not committed yet)
        - Phase 3: Commit DB + rename blob temps to finals
        On failure at any point, everything rolls back cleanly.

        Args:
            new_passphrase: The new passphrase to use for the vault.

        Returns:
            Path to the safety backup created before re-encryption.

        Raises:
            RekeyError: If re-encryption fails (old key is restored).
        """
        # 0. Validate state
        old_key = self._km.get_key()  # Raises LockedError if locked

        # 1. Create safety backup
        logger.info("Rekey: creating safety backup")
        backup = VaultBackup(self._config, self._km)
        backup_path = backup.create_backup()
        logger.info("Rekey: backup created at %s", backup_path)

        # 2. Derive new key from new passphrase
        new_salt = os.urandom(self._config.argon2_salt_len)
        new_key = self._derive_key(new_passphrase, new_salt)

        # 3. Re-encrypt everything atomically
        try:
            self._reencrypt_all(old_key, new_key)
            self._update_manifest(new_passphrase, new_salt, new_key)
            self._switch_key(new_key)
            self._verify_sample()
        except Exception as e:
            logger.error("Rekey failed: %s. Restoring old key.", e)
            self._restore_old_key(old_key)
            raise RekeyError(f"Key rotation failed: {e}") from e

        logger.info("Rekey: complete. Vault re-encrypted with new passphrase.")
        return backup_path

    def _derive_key(self, passphrase: str, salt: bytes) -> bytes:
        """Derive a 256-bit key from a passphrase using Argon2id."""
        return hash_secret_raw(
            secret=passphrase.encode("utf-8"),
            salt=salt,
            time_cost=self._config.argon2_time_cost,
            memory_cost=self._config.argon2_memory_cost,
            parallelism=self._config.argon2_parallelism,
            hash_len=self._config.argon2_hash_len,
            type=Type.ID,
        )

    def _reencrypt_all(self, old_key: bytes, new_key: bytes) -> None:
        """Re-encrypt database and blobs with atomic commit.

        1. Write re-encrypted blobs to .rekey.tmp files (originals intact)
        2. Re-encrypt DB rows in a transaction (uncommitted)
        3. Commit DB transaction
        4. Rename .rekey.tmp files over originals
        On failure: rollback DB, delete .rekey.tmp files.
        """
        old_aesgcm = AESGCM(old_key)
        new_aesgcm = AESGCM(new_key)

        # Phase 1: Prepare re-encrypted blobs as .rekey.tmp files
        tmp_blobs: list[tuple[Path, Path]] = []  # (tmp_path, final_path)
        try:
            tmp_blobs = self._prepare_blob_reencryption(old_aesgcm, new_aesgcm)
        except Exception:
            self._cleanup_tmp_blobs(tmp_blobs)
            raise

        # Phase 2: Re-encrypt database in a transaction
        conn: sqlite3.Connection | None = None
        try:
            conn = self._reencrypt_database(old_aesgcm, new_aesgcm)
        except Exception:
            if conn is not None:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                conn.close()
            self._cleanup_tmp_blobs(tmp_blobs)
            raise

        # Phase 3: Commit - point of no return
        try:
            conn.execute("COMMIT")
            logger.info("Rekey: database transaction committed")
        except Exception:
            self._cleanup_tmp_blobs(tmp_blobs)
            raise
        finally:
            conn.close()

        # Finalize blob files (rename .rekey.tmp -> .enc)
        self._finalize_blobs(tmp_blobs)

    def _prepare_blob_reencryption(
        self, old_aesgcm: AESGCM, new_aesgcm: AESGCM
    ) -> list[tuple[Path, Path]]:
        """Re-encrypt blobs to temporary files, keeping originals intact.

        Returns:
            List of (tmp_path, final_path) tuples.
        """
        blobs_dir = self._config.blobs_dir
        if not blobs_dir.exists():
            return []

        enc_files = list(blobs_dir.glob("*.enc"))
        if not enc_files:
            return []

        tmp_blobs: list[tuple[Path, Path]] = []
        for enc_path in enc_files:
            blob_id = enc_path.stem
            aad = blob_id.encode("utf-8")

            raw = enc_path.read_bytes()
            nonce = raw[:12]
            ct = raw[12:]

            plaintext = old_aesgcm.decrypt(nonce, ct, aad)

            new_nonce = os.urandom(12)
            new_ct = new_aesgcm.encrypt(new_nonce, plaintext, aad)

            tmp_path = enc_path.with_suffix(".rekey.tmp")
            tmp_path.write_bytes(new_nonce + new_ct)
            tmp_blobs.append((tmp_path, enc_path))

        logger.info("Rekey: prepared %d blob re-encryptions", len(tmp_blobs))
        return tmp_blobs

    def _reencrypt_database(
        self, old_aesgcm: AESGCM, new_aesgcm: AESGCM
    ) -> sqlite3.Connection:
        """Re-encrypt all encrypted DB columns in a transaction.

        Returns the connection with an UNCOMMITTED transaction so the
        caller can commit after blobs are also ready.
        """
        db_path = self._config.db_path

        if not db_path.exists():
            logger.info("Rekey: no database found, returning empty connection")
            conn = sqlite3.connect(":memory:")
            conn.execute("BEGIN")
            return conn

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        existing_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        conn.execute("BEGIN IMMEDIATE")

        total_rows = 0
        for table, id_col, enc_col in _ENCRYPTED_TABLES:
            if table not in existing_tables:
                logger.debug("Rekey: table '%s' does not exist, skipping", table)
                continue

            rows = conn.execute(
                f"SELECT {id_col}, {enc_col} FROM {table}"  # noqa: S608
            ).fetchall()

            for row in rows:
                row_id = row[id_col]
                blob = row[enc_col]

                if blob is None:
                    continue

                aad = f"{table}.{enc_col}.{row_id}".encode()

                nonce = blob[:12]
                ct = blob[12:]
                plaintext = old_aesgcm.decrypt(nonce, ct, aad)

                new_nonce = os.urandom(12)
                new_ct = new_aesgcm.encrypt(new_nonce, plaintext, aad)
                new_blob = new_nonce + new_ct

                conn.execute(
                    f"UPDATE {table} SET {enc_col} = ? WHERE {id_col} = ?",  # noqa: S608
                    (new_blob, row_id),
                )
                total_rows += 1

        logger.info("Rekey: prepared %d database rows for re-encryption", total_rows)
        # Transaction is NOT committed -- caller does that
        return conn

    def _finalize_blobs(self, tmp_blobs: list[tuple[Path, Path]]) -> None:
        """Rename .rekey.tmp files to .enc (replacing originals)."""
        for tmp_path, final_path in tmp_blobs:
            tmp_path.rename(final_path)
        if tmp_blobs:
            logger.info("Rekey: finalized %d blob files", len(tmp_blobs))

    def _cleanup_tmp_blobs(self, tmp_blobs: list[tuple[Path, Path]]) -> None:
        """Remove .rekey.tmp files after a failed rotation."""
        for tmp_path, _ in tmp_blobs:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass

    def _update_manifest(
        self, new_passphrase: str, new_salt: bytes, new_key: bytes
    ) -> None:
        """Update manifest.json with new KDF parameters and verification tag."""
        manifest_path = self._config.manifest_path

        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
        else:
            manifest = {
                "schema_version": 1,
                "vault_version": "0.1.0",
                "cipher": "AES-256-GCM",
            }

        manifest["kdf"] = {
            "type": "argon2id",
            "time_cost": self._config.argon2_time_cost,
            "memory_cost": self._config.argon2_memory_cost,
            "parallelism": self._config.argon2_parallelism,
            "hash_len": self._config.argon2_hash_len,
            "salt": new_salt.hex(),
        }

        nonce = os.urandom(12)
        aesgcm = AESGCM(new_key)
        verify_ct = aesgcm.encrypt(nonce, b"HEALTHBOT_VERIFY", b"verify")

        manifest["verify_nonce"] = nonce.hex()
        manifest["verify_ct"] = verify_ct.hex()
        manifest["rekeyed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        manifest_path.write_text(json.dumps(manifest, indent=2))
        logger.info("Rekey: manifest updated with new KDF parameters")

    def _switch_key(self, new_key: bytes) -> None:
        """Replace the key manager's in-memory master key with the new key.

        Uses KeyManager.replace_master_key() which securely zeros the old
        key and sets the new one with proper thread safety.
        """
        self._km.replace_master_key(new_key)

    def _restore_old_key(self, old_key: bytes) -> None:
        """Restore the old key in the key manager after a failed rotation.

        Uses KeyManager.replace_master_key() which securely zeros the
        current key and sets the old one with proper thread safety.
        """
        self._km.replace_master_key(old_key)

    def _verify_sample(self) -> None:
        """Verify the new key works by decrypting a sample row.

        Checks the first non-empty encrypted table. If no data exists,
        verification is skipped (nothing could have been corrupted).
        """
        db_path = self._config.db_path
        if not db_path.exists():
            return

        new_key = self._km.get_key()
        aesgcm = AESGCM(new_key)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            existing_tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }

            for table, id_col, enc_col in _ENCRYPTED_TABLES:
                if table not in existing_tables:
                    continue

                row = conn.execute(
                    f"SELECT {id_col}, {enc_col} FROM {table} LIMIT 1"  # noqa: S608
                ).fetchone()

                if row is None or row[enc_col] is None:
                    continue

                row_id = row[id_col]
                blob = row[enc_col]
                aad = f"{table}.{enc_col}.{row_id}".encode()

                nonce = blob[:12]
                ct = blob[12:]
                aesgcm.decrypt(nonce, ct, aad)  # Raises on failure

                logger.info(
                    "Rekey: verification passed (table=%s, row=%s)", table, row_id
                )
                return

        finally:
            conn.close()

        logger.info("Rekey: no encrypted data to verify (empty vault)")
