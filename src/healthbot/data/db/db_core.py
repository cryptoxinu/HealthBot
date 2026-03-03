"""Core database class — lifecycle, encryption, migrations.

Wraps sqlite3 with field-level AES-256-GCM encryption for sensitive data.
Each encrypted field uses AAD (Additional Authenticated Data) including
table name, column name, and row ID to prevent ciphertext swapping.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from datetime import UTC, date, datetime
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.config import Config
from healthbot.data.db_memory import MemoryMixin
from healthbot.data.models import (
    RecordType,
    TriageLevel,
)
from healthbot.data.schema import CREATE_TABLES, MIGRATIONS, SCHEMA_VERSION
from healthbot.security.key_manager import KeyManager

logger = logging.getLogger("healthbot")


def _serialize(obj: Any) -> str:
    """JSON-serialize with date/datetime support."""
    def default(o: Any) -> Any:
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        if isinstance(o, TriageLevel):
            return o.value
        if isinstance(o, RecordType):
            return o.value
        return str(o)
    return json.dumps(obj.__dict__ if hasattr(obj, "__dict__") else obj, default=default)


class HealthDBCore(MemoryMixin):
    """Encrypted health data database — core lifecycle and encryption."""

    def __init__(self, config: Config, key_manager: KeyManager) -> None:
        self._db_path = config.db_path
        self._km = key_manager
        self._conn: sqlite3.Connection | None = None
        self._local = threading.local()

    def open(self) -> None:
        """Open the database connection and ensure schema exists."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = self._make_conn()
        self._conn.executescript(CREATE_TABLES)
        # Set schema version if not set
        self._conn.execute(
            "INSERT OR IGNORE INTO vault_meta (key, value) VALUES (?, ?)",
            ("schema_version", str(SCHEMA_VERSION)),
        )
        self._conn.commit()
        self._owner_thread = threading.get_ident()

    def _make_conn(self) -> sqlite3.Connection:
        """Create a new SQLite connection with standard settings."""
        conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
        # Close any thread-local connections
        local_conn = getattr(self._local, "conn", None)
        if local_conn:
            local_conn.close()
            self._local.conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Database not open. Call open() first.")
        # If called from a different thread, return a thread-local connection
        if threading.get_ident() != getattr(self, "_owner_thread", 0):
            local_conn = getattr(self._local, "conn", None)
            if local_conn is None:
                self._local.conn = self._make_conn()
                self._local.migrations_checked = False
            if not getattr(self._local, "migrations_checked", False):
                self._local.migrations_checked = True
                self._local.conn.executescript(CREATE_TABLES)
                self.run_migrations()
            return self._local.conn
        return self._conn

    # --- Encryption helpers ---

    def _encrypt(self, data: Any, aad_context: str) -> bytes:
        """Encrypt a value with AES-256-GCM. AAD = context string."""
        key = self._km.get_key()
        nonce = os.urandom(12)
        aesgcm = AESGCM(key)
        plaintext = _serialize(data).encode("utf-8") if not isinstance(data, bytes) else data
        ct = aesgcm.encrypt(nonce, plaintext, aad_context.encode("utf-8"))
        return nonce + ct

    def _decrypt(self, blob: bytes, aad_context: str) -> Any:
        """Decrypt a field. Returns deserialized Python object."""
        if len(blob) < 28:  # 12-byte nonce + 16-byte AES-GCM tag minimum
            raise ValueError(
                f"Ciphertext too short: {len(blob)} bytes (need >= 28)"
            )
        key = self._km.get_key()
        nonce = blob[:12]
        ct = blob[12:]
        aesgcm = AESGCM(key)
        plaintext = aesgcm.decrypt(nonce, ct, aad_context.encode("utf-8"))
        return json.loads(plaintext.decode("utf-8"))

    def _now(self) -> str:
        return datetime.now(UTC).isoformat()

    # --- Schema version ---

    def get_schema_version(self) -> int:
        """Get current schema version."""
        row = self.conn.execute(
            "SELECT value FROM vault_meta WHERE key = 'schema_version'"
        ).fetchone()
        return int(row["value"]) if row else 0

    def run_migrations(self, dry_run: bool = False) -> int:
        """Run pending migrations. Returns number of migrations applied.

        If dry_run=True, only reports which migrations would run without
        executing them.
        """
        current = self.get_schema_version()
        applied = 0
        for version in sorted(MIGRATIONS.keys()):
            if version > current:
                if dry_run:
                    logger.info(
                        "Migration %d would run (%d statements)",
                        version, len(MIGRATIONS[version]),
                    )
                    applied += 1
                    continue
                try:
                    self.conn.execute("BEGIN")
                    for sql in MIGRATIONS[version]:
                        try:
                            self.conn.execute(sql)
                        except Exception as e:
                            if "duplicate column" in str(e).lower():
                                continue  # Column already in base schema
                            raise
                    self.conn.execute(
                        "UPDATE vault_meta SET value = ? WHERE key = 'schema_version'",
                        (str(version),),
                    )
                    self.conn.execute("COMMIT")
                except Exception:
                    self.conn.execute("ROLLBACK")
                    raise
                applied += 1

        # Data migrations — encrypt previously-plaintext fields.
        # Idempotent: each method checks if rows need migrating.
        # Requires vault key, so skip in dry_run mode.
        if not dry_run:
            try:
                self.migrate_document_filenames()
            except Exception as e:
                logger.warning("Document filename migration skipped: %s", e)
            try:
                self.migrate_search_index_encryption()
            except Exception as e:
                logger.warning("Search index encryption migration skipped: %s", e)

        return applied
