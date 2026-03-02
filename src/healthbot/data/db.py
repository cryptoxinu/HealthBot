"""Encrypted database operations.

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
import uuid
from datetime import UTC, date, datetime
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.config import Config
from healthbot.data.db_memory import MemoryMixin
from healthbot.data.models import (
    Document,
    ExternalEvidence,
    LabResult,
    Medication,
    RecordType,
    TriageLevel,
    VitalSign,
    WhoopDaily,
    Workout,
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


class HealthDB(MemoryMixin):
    """Encrypted health data database."""

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

    # --- Documents ---

    def insert_document(
        self, doc: Document, user_id: int = 0, commit: bool = True,
    ) -> str:
        """Insert a document record."""
        doc_id = doc.id or uuid.uuid4().hex
        aad = f"documents.meta_encrypted.{doc_id}"
        meta_enc = self._encrypt(doc.meta, aad) if doc.meta else None
        try:
            self.conn.execute(
                """INSERT INTO documents (doc_id, source, sha256, received_at,
                   mime_type, size_bytes, page_count, enc_blob_path, filename,
                   meta_encrypted, user_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (doc_id, doc.source, doc.sha256, self._now(),
                 doc.mime_type, doc.size_bytes, doc.page_count,
                 doc.enc_blob_path, doc.filename, meta_enc, user_id),
            )
        except Exception:
            # Fallback for pre-migration schema without user_id column
            self.conn.execute(
                """INSERT INTO documents (doc_id, source, sha256, received_at,
                   mime_type, size_bytes, page_count, enc_blob_path, filename,
                   meta_encrypted)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (doc_id, doc.source, doc.sha256, self._now(),
                 doc.mime_type, doc.size_bytes, doc.page_count,
                 doc.enc_blob_path, doc.filename, meta_enc),
            )
        if commit:
            self.conn.commit()
        return doc_id

    def document_exists_by_sha256(self, sha256: str) -> dict | None:
        """Check if a document with this SHA256 already exists."""
        row = self.conn.execute(
            "SELECT doc_id, filename, received_at, enc_blob_path"
            " FROM documents WHERE sha256 = ?",
            (sha256,),
        ).fetchone()
        return dict(row) if row else None

    def delete_document(self, doc_id: str) -> None:
        """Delete a document record by ID (e.g., on parse failure)."""
        self.conn.execute("DELETE FROM documents WHERE doc_id = ?", (doc_id,))
        self.conn.commit()

    def get_observation_keys_for_doc(self, source_doc_id: str) -> set[tuple[str, str | None]]:
        """Return (canonical_name, date_effective) pairs for a document's observations."""
        rows = self.conn.execute(
            "SELECT canonical_name, date_effective FROM observations"
            " WHERE source_doc_id = ?",
            (source_doc_id,),
        ).fetchall()
        return {(r["canonical_name"], r["date_effective"]) for r in rows}

    def get_observation_details_for_doc(
        self, source_doc_id: str,
    ) -> dict[tuple[str, str | None], dict]:
        """Return {(canonical_name, date_effective): {obs_id, value, ...}} for a document.

        Used during rescan to detect corrected lab values. The encrypted_data
        is decrypted to extract the original numeric value.
        """
        rows = self.conn.execute(
            "SELECT obs_id, canonical_name, date_effective, encrypted_data"
            " FROM observations WHERE source_doc_id = ?",
            (source_doc_id,),
        ).fetchall()
        result: dict[tuple[str, str | None], dict] = {}
        for r in rows:
            aad = f"observations.encrypted_data.{r['obs_id']}"
            try:
                data = self._decrypt(r["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for observations row %s: %s", r["obs_id"], e)
                data = {}
            key = (r["canonical_name"], r["date_effective"])
            result[key] = {
                "obs_id": r["obs_id"],
                "value": data.get("value"),
                "reference_low": data.get("reference_low"),
                "reference_high": data.get("reference_high"),
            }
        return result

    def update_observation_value(
        self,
        obs_id: str,
        obs: LabResult | VitalSign,
        user_id: int = 0,
        age_at_collection: int | None = None,
        commit: bool = True,
    ) -> None:
        """Update an existing observation's encrypted data and set corrected_at.

        Used when a corrected lab report provides a new value for the same
        (canonical_name, date_collected) key.
        """
        aad = f"observations.encrypted_data.{obs_id}"
        enc_data = self._encrypt(obs, aad)
        triage = "normal"
        flag = ""
        if isinstance(obs, LabResult):
            triage = obs.triage_level.value
            flag = obs.flag
        try:
            self.conn.execute(
                """UPDATE observations SET encrypted_data = ?, triage_level = ?,
                   flag = ?, corrected_at = ? WHERE obs_id = ?""",
                (enc_data, triage, flag, self._now(), obs_id),
            )
        except Exception:
            # corrected_at column may not exist yet (pre-migration)
            self.conn.execute(
                """UPDATE observations SET encrypted_data = ?, triage_level = ?,
                   flag = ? WHERE obs_id = ?""",
                (enc_data, triage, flag, obs_id),
            )
        if commit:
            self.conn.commit()

    def update_document_rescanned(
        self, doc_id: str, commit: bool = True,
    ) -> None:
        """Set last_rescanned timestamp on a document."""
        try:
            self.conn.execute(
                "UPDATE documents SET last_rescanned = ? WHERE doc_id = ?",
                (self._now(), doc_id),
            )
            if commit:
                self.conn.commit()
        except Exception:
            pass  # Column may not exist in pre-migration schema

    def list_documents(self, user_id: int | None = None) -> list[dict]:
        """Return all documents, optionally filtered by user_id."""
        sql = "SELECT * FROM documents"
        params: list[Any] = []
        if user_id is not None:
            sql += " WHERE user_id = ?"
            params.append(user_id)
        sql += " ORDER BY received_at DESC"
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def get_document_meta(self, doc_id: str) -> dict:
        """Decrypt and return the meta dict for a document."""
        row = self.conn.execute(
            "SELECT meta_encrypted FROM documents WHERE doc_id = ?",
            (doc_id,),
        ).fetchone()
        if not row or not row["meta_encrypted"]:
            return {}
        aad = f"documents.meta_encrypted.{doc_id}"
        try:
            return self._decrypt(row["meta_encrypted"], aad)
        except Exception:
            return {}

    def update_document_meta(
        self, doc_id: str, meta: dict, commit: bool = True,
    ) -> None:
        """Encrypt and update the meta dict for a document."""
        aad = f"documents.meta_encrypted.{doc_id}"
        meta_enc = self._encrypt(meta, aad)
        self.conn.execute(
            "UPDATE documents SET meta_encrypted = ? WHERE doc_id = ?",
            (meta_enc, doc_id),
        )
        if commit:
            self.conn.commit()

    def update_document_routing_status(
        self, doc_id: str, *, status: str = "done", error: str = "",
    ) -> None:
        """Update the routing_status and routing_error for a document."""
        try:
            self.conn.execute(
                "UPDATE documents SET routing_status = ?, routing_error = ? WHERE doc_id = ?",
                (status, error, doc_id),
            )
            self.conn.commit()
        except Exception:
            pass  # Columns may not exist in pre-migration schema

    def get_pending_routing_documents(self, user_id: int | None = None) -> list[dict]:
        """Return documents with routing_status = 'pending_routing'."""
        sql = "SELECT * FROM documents WHERE routing_status = 'pending_routing'"
        params: list[Any] = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        sql += " ORDER BY received_at ASC"
        try:
            rows = self.conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    # --- Health Records Ext (extensible storage) ---

    def insert_health_record_ext(
        self, user_id: int, data_type: str, label: str, data_dict: dict,
    ) -> str:
        """Insert or update an extensible health record.

        Encrypts data_dict with AAD `health_records_ext.encrypted_data.{id}`.
        Deduplicates on (user_id, data_type, label).
        Returns the record ID.
        """
        # Check for existing record with same (user_id, data_type, label)
        row = self.conn.execute(
            "SELECT id FROM health_records_ext WHERE user_id = ? AND data_type = ? AND label = ?",
            (user_id, data_type, label),
        ).fetchone()
        if row:
            rec_id = row["id"]
        else:
            rec_id = uuid.uuid4().hex

        aad = f"health_records_ext.encrypted_data.{rec_id}"
        enc = self._encrypt(data_dict, aad)
        now = self._now()

        self.conn.execute(
            """INSERT OR REPLACE INTO health_records_ext
               (id, user_id, data_type, label, created_at, updated_at, encrypted_data)
               VALUES (?, ?, ?, ?, COALESCE(
                   (SELECT created_at FROM health_records_ext WHERE id = ?), ?
               ), ?, ?)""",
            (rec_id, user_id, data_type, label, rec_id, now, now, enc),
        )
        self.conn.commit()
        return rec_id

    def get_health_records_ext(
        self, user_id: int, data_type: str | None = None, since: str | None = None,
    ) -> list[dict]:
        """Query extensible health records, decrypting each."""
        sql = "SELECT * FROM health_records_ext WHERE user_id = ?"
        params: list[Any] = [user_id]
        if data_type:
            sql += " AND data_type = ?"
            params.append(data_type)
        if since:
            sql += " AND updated_at >= ?"
            params.append(since)
        sql += " ORDER BY updated_at DESC"
        rows = self.conn.execute(sql, params).fetchall()

        results: list[dict] = []
        for r in rows:
            d = dict(r)
            if d.get("encrypted_data"):
                aad = f"health_records_ext.encrypted_data.{d['id']}"
                try:
                    d["data"] = self._decrypt(d.pop("encrypted_data"), aad)
                except Exception:
                    d["data"] = {}
                    d.pop("encrypted_data", None)
            else:
                d["data"] = {}
                d.pop("encrypted_data", None)
            results.append(d)
        return results

    # --- Substance Knowledge ---

    def insert_substance_knowledge(
        self,
        user_id: int,
        name: str,
        data: dict,
        quality_score: float = 0.0,
    ) -> str:
        """Insert a substance knowledge profile with AES-256-GCM encryption."""
        sk_id = uuid.uuid4().hex
        aad = f"substance_knowledge.encrypted_data.{sk_id}"
        enc_data = self._encrypt(data, aad)
        now = self._now()
        self.conn.execute(
            """INSERT INTO substance_knowledge
               (id, user_id, name, created_at, updated_at, quality_score,
                encrypted_data)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id, name) DO UPDATE SET
                 updated_at = excluded.updated_at,
                 quality_score = excluded.quality_score,
                 encrypted_data = excluded.encrypted_data""",
            (sk_id, user_id, name.lower(), now, now, quality_score, enc_data),
        )
        self.conn.commit()
        return sk_id

    def get_substance_knowledge(
        self, user_id: int, name: str,
    ) -> dict | None:
        """Get a substance knowledge profile by name."""
        row = self.conn.execute(
            "SELECT * FROM substance_knowledge WHERE user_id = ? AND name = ?",
            (user_id, name.lower()),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        aad = f"substance_knowledge.encrypted_data.{d['id']}"
        try:
            d["data"] = self._decrypt(d.pop("encrypted_data"), aad)
        except Exception:
            d["data"] = {}
            d.pop("encrypted_data", None)
        return d

    def get_all_substance_knowledge(self, user_id: int) -> list[dict]:
        """Get all substance knowledge profiles for a user."""
        rows = self.conn.execute(
            "SELECT * FROM substance_knowledge WHERE user_id = ? ORDER BY name",
            (user_id,),
        ).fetchall()
        results: list[dict] = []
        for row in rows:
            d = dict(row)
            aad = f"substance_knowledge.encrypted_data.{d['id']}"
            try:
                d["data"] = self._decrypt(d.pop("encrypted_data"), aad)
            except Exception:
                d["data"] = {}
                d.pop("encrypted_data", None)
            results.append(d)
        return results

    def update_substance_knowledge(
        self,
        user_id: int,
        name: str,
        data: dict,
        quality_score: float | None = None,
    ) -> bool:
        """Update an existing substance knowledge profile."""
        row = self.conn.execute(
            "SELECT id FROM substance_knowledge WHERE user_id = ? AND name = ?",
            (user_id, name.lower()),
        ).fetchone()
        if not row:
            return False
        sk_id = row["id"]
        aad = f"substance_knowledge.encrypted_data.{sk_id}"
        enc_data = self._encrypt(data, aad)
        sql = "UPDATE substance_knowledge SET encrypted_data = ?, updated_at = ?"
        params: list = [enc_data, self._now()]
        if quality_score is not None:
            sql += ", quality_score = ?"
            params.append(quality_score)
        sql += " WHERE id = ?"
        params.append(sk_id)
        self.conn.execute(sql, params)
        self.conn.commit()
        return True

    # --- Observations (lab results, vitals, etc.) ---

    def insert_observation(
        self,
        obs: LabResult | VitalSign,
        user_id: int = 0,
        age_at_collection: int | None = None,
        commit: bool = True,
    ) -> str:
        """Insert an observation (lab result, vital sign, etc.)."""
        obs_id = obs.id or uuid.uuid4().hex
        aad = f"observations.encrypted_data.{obs_id}"
        enc_data = self._encrypt(obs, aad)

        record_type = "lab_result"
        canonical_name = ""
        date_eff = None
        triage = "normal"
        flag = ""
        source_doc = ""
        source_page = 0
        source_section = ""

        source_lab = ""

        if isinstance(obs, LabResult):
            canonical_name = obs.canonical_name or obs.test_name.lower()
            date_eff = obs.date_collected.isoformat() if obs.date_collected else None
            triage = obs.triage_level.value
            flag = obs.flag
            source_doc = obs.source_blob_id
            source_page = obs.source_page
            source_section = obs.source_section
            source_lab = obs.lab_name or ""
        elif isinstance(obs, VitalSign):
            record_type = "vital_sign"
            canonical_name = obs.type
            date_eff = obs.timestamp.isoformat() if obs.timestamp else None
            source_doc = obs.source_blob_id

        try:
            self.conn.execute(
                """INSERT INTO observations (obs_id, record_type, canonical_name,
                   date_effective, triage_level, flag, source_doc_id, source_page,
                   source_section, created_at, encrypted_data, user_id,
                   age_at_collection, source_lab)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (obs_id, record_type, canonical_name, date_eff, triage, flag,
                 source_doc, source_page, source_section, self._now(), enc_data,
                 user_id, age_at_collection, source_lab),
            )
        except Exception:
            # Fallback for pre-migration schema without age_at_collection
            try:
                self.conn.execute(
                    """INSERT INTO observations (obs_id, record_type, canonical_name,
                       date_effective, triage_level, flag, source_doc_id, source_page,
                       source_section, created_at, encrypted_data, user_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (obs_id, record_type, canonical_name, date_eff, triage, flag,
                     source_doc, source_page, source_section, self._now(), enc_data,
                     user_id),
                )
            except Exception:
                # Fallback for pre-migration schema without user_id column
                self.conn.execute(
                    """INSERT INTO observations (obs_id, record_type, canonical_name,
                       date_effective, triage_level, flag, source_doc_id, source_page,
                       source_section, created_at, encrypted_data)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (obs_id, record_type, canonical_name, date_eff, triage, flag,
                     source_doc, source_page, source_section, self._now(), enc_data),
                )
        if commit:
            self.conn.commit()
        return obs_id

    def backfill_source_lab(self) -> int:
        """Populate source_lab from encrypted LabResult data for existing rows.

        Decrypts each observation, extracts lab_name, and stores it in the
        plaintext source_lab column. Returns number of rows updated.
        """
        rows = self.conn.execute(
            "SELECT obs_id, encrypted_data FROM observations "
            "WHERE record_type = 'lab_result' AND "
            "(source_lab IS NULL OR source_lab = '')",
        ).fetchall()
        updated = 0
        for r in rows:
            aad = f"observations.encrypted_data.{r['obs_id']}"
            try:
                data = self._decrypt(r["encrypted_data"], aad)
            except Exception:
                continue
            lab_name = data.get("lab_name", "")
            if lab_name:
                self.conn.execute(
                    "UPDATE observations SET source_lab = ? WHERE obs_id = ?",
                    (lab_name, r["obs_id"]),
                )
                updated += 1
        if updated:
            self.conn.commit()
        return updated

    def get_existing_observation_keys(
        self,
        record_type: str = "vital_sign",
        canonical_names: list[str] | None = None,
    ) -> set[tuple[str, str | None]]:
        """Return (canonical_name, date_effective) pairs for dedup checks.

        Loads all existing keys into a set for O(1) lookup during batch imports.
        """
        sql = "SELECT canonical_name, date_effective FROM observations WHERE record_type = ?"
        params: list[Any] = [record_type]
        if canonical_names:
            placeholders = ",".join("?" for _ in canonical_names)
            sql += f" AND canonical_name IN ({placeholders})"
            params.extend(canonical_names)
        rows = self.conn.execute(sql, params).fetchall()
        return {(r["canonical_name"], r["date_effective"]) for r in rows}

    def delete_observation(self, obs_id: str) -> bool:
        """Delete an observation by ID. Returns True if a row was deleted."""
        cursor = self.conn.execute(
            "DELETE FROM observations WHERE obs_id = ?", (obs_id,)
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def stamp_collection_date(self, source_doc_id: str, date_iso: str) -> int:
        """Set date_effective on undated observations for a given document.

        Returns the number of rows updated.
        """
        cursor = self.conn.execute(
            "UPDATE observations SET date_effective = ? "
            "WHERE source_doc_id = ? AND "
            "(date_effective IS NULL OR date_effective = '')",
            (date_iso, source_doc_id),
        )
        self.conn.commit()
        return cursor.rowcount

    # --- Workouts ---

    def insert_workout(self, wo: Workout, user_id: int = 0) -> str:
        """Insert a workout record with AES-256-GCM encryption."""
        wo_id = wo.id or uuid.uuid4().hex
        aad = f"workouts.encrypted_data.{wo_id}"
        enc_data = self._encrypt(wo, aad)
        start_date = wo.start_time.isoformat() if wo.start_time else ""
        try:
            self.conn.execute(
                """INSERT INTO workouts
                   (id, user_id, sport_type, start_date, source, created_at,
                    encrypted_data)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (wo_id, user_id, wo.sport_type, start_date,
                 wo.source, self._now(), enc_data),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return wo_id

    def get_existing_workout_keys(
        self, user_id: int = 0,
    ) -> set[tuple[str, str | None]]:
        """Return (sport_type, start_date) pairs for dedup checks."""
        rows = self.conn.execute(
            "SELECT sport_type, start_date FROM workouts WHERE user_id = ?",
            (user_id,),
        ).fetchall()
        return {(r["sport_type"], r["start_date"]) for r in rows}

    def query_workouts(
        self,
        sport_type: str | None = None,
        start_after: str | None = None,
        user_id: int | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Query and decrypt workouts with optional filters."""
        sql = "SELECT * FROM workouts WHERE 1=1"
        params: list[Any] = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        if sport_type:
            sql += " AND sport_type = ?"
            params.append(sport_type)
        if start_after:
            sql += " AND start_date >= ?"
            params.append(start_after)
        sql += " ORDER BY start_date DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(sql, params).fetchall()
        results: list[dict] = []
        for row in rows:
            aad = f"workouts.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
                data["_id"] = row["id"]
                data["_sport_type"] = row["sport_type"]
                data["_start_date"] = row["start_date"]
                results.append(data)
            except Exception as e:
                logger.warning("Decrypt failed for workouts row %s: %s", row["id"], e)
                continue
        return results

    def get_workout_summary(
        self,
        days: int = 30,
        user_id: int | None = None,
    ) -> dict:
        """Aggregate workout stats over a time period.

        Returns dict with:
            total_workouts, total_minutes, total_calories,
            by_sport: {sport: {count, minutes, calories}},
            streak_days: consecutive days with at least one workout.
        """
        from datetime import UTC, datetime, timedelta

        start_after = (
            datetime.now(UTC) - timedelta(days=days)
        ).strftime("%Y-%m-%d")
        rows = self.query_workouts(
            start_after=start_after, user_id=user_id, limit=500,
        )

        total_mins = 0.0
        total_cal = 0.0
        by_sport: dict[str, dict] = {}
        workout_dates: set[str] = set()

        for row in rows:
            sport = row.get("sport_type", row.get("_sport_type", "other"))
            dur = float(row.get("duration_minutes", 0) or 0)
            cal = float(row.get("calories_burned", 0) or 0)
            total_mins += dur
            total_cal += cal

            if sport not in by_sport:
                by_sport[sport] = {"count": 0, "minutes": 0.0, "calories": 0.0}
            by_sport[sport]["count"] += 1
            by_sport[sport]["minutes"] += dur
            by_sport[sport]["calories"] += cal

            dt = row.get("_start_date", "")[:10]
            if dt:
                workout_dates.add(dt)

        # Calculate streak: consecutive days ending today (or most recent)
        streak = 0
        if workout_dates:
            sorted_dates = sorted(workout_dates, reverse=True)
            today = datetime.now(UTC).strftime("%Y-%m-%d")
            # Start from today or most recent workout date
            check = today if today in workout_dates else sorted_dates[0]
            check_date = datetime.strptime(check, "%Y-%m-%d").date()
            for _ in range(len(sorted_dates)):
                if check_date.isoformat() in workout_dates:
                    streak += 1
                    check_date -= timedelta(days=1)
                else:
                    break

        return {
            "total_workouts": len(rows),
            "total_minutes": total_mins,
            "total_calories": total_cal,
            "by_sport": by_sport,
            "streak_days": streak,
        }

    def get_observation(self, obs_id: str) -> dict | None:
        """Retrieve and decrypt an observation by ID."""
        row = self.conn.execute(
            "SELECT * FROM observations WHERE obs_id = ?", (obs_id,)
        ).fetchone()
        if not row:
            return None
        aad = f"observations.encrypted_data.{obs_id}"
        data = self._decrypt(row["encrypted_data"], aad)
        data["_meta"] = {
            "record_type": row["record_type"],
            "date_effective": row["date_effective"],
            "triage_level": row["triage_level"],
        }
        return data

    def query_observations(
        self,
        record_type: str | None = None,
        canonical_name: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        triage_level: str | None = None,
        limit: int = 200,
        user_id: int | None = None,
        since: str | None = None,
    ) -> list[dict]:
        """Query observations by plaintext metadata, decrypt matching rows."""
        sql = "SELECT * FROM observations WHERE 1=1"
        params: list[Any] = []

        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        if record_type:
            sql += " AND record_type = ?"
            params.append(record_type)
        if canonical_name:
            sql += " AND canonical_name = ?"
            params.append(canonical_name)
        if start_date:
            sql += " AND date_effective >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date_effective <= ?"
            params.append(end_date)
        if triage_level:
            sql += " AND triage_level = ?"
            params.append(triage_level)
        if since:
            sql += " AND (created_at > ? OR COALESCE(corrected_at, '') > ?)"
            params.extend([since, since])

        sql += " ORDER BY date_effective DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            aad = f"observations.encrypted_data.{row['obs_id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
                data["_meta"] = {
                    "obs_id": row["obs_id"],
                    "record_type": row["record_type"],
                    "date_effective": row["date_effective"],
                    "triage_level": row["triage_level"],
                    "source_doc_id": row["source_doc_id"],
                    "source_page": row["source_page"],
                    "source_section": row["source_section"],
                }
                results.append(data)
            except Exception as e:
                logger.warning("Decrypt failed for observations row %s: %s", row["obs_id"], e)
                continue  # Skip corrupted records
        return results

    # --- Medications ---

    def insert_medication(self, med: Medication, user_id: int = 0) -> str:
        """Insert a medication record."""
        med_id = med.id or uuid.uuid4().hex
        aad = f"medications.encrypted_data.{med_id}"
        enc_data = self._encrypt(med, aad)
        try:
            self.conn.execute(
                """INSERT INTO medications (med_id, status, start_date, end_date,
                   source_doc_id, created_at, encrypted_data, user_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (med_id, med.status,
                 med.start_date.isoformat() if med.start_date else None,
                 med.end_date.isoformat() if med.end_date else None,
                 med.source_blob_id, self._now(), enc_data, user_id),
            )
        except Exception:
            # Fallback for pre-migration schema without user_id column
            self.conn.execute(
                """INSERT INTO medications (med_id, status, start_date, end_date,
                   source_doc_id, created_at, encrypted_data)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (med_id, med.status,
                 med.start_date.isoformat() if med.start_date else None,
                 med.end_date.isoformat() if med.end_date else None,
                 med.source_blob_id, self._now(), enc_data),
            )
        self.conn.commit()
        return med_id

    def get_active_medications(
        self, user_id: int | None = None, since: str | None = None,
    ) -> list[dict]:
        """Return all active medications (decrypted)."""
        sql = "SELECT * FROM medications WHERE status = 'active'"
        params: list[Any] = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        if since:
            sql += " AND created_at > ?"
            params.append(since)
        rows = self.conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            aad = f"medications.encrypted_data.{row['med_id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
                results.append(data)
            except Exception as exc:
                logger.warning(
                    "Skipping corrupt medication row %s: %s",
                    row["med_id"], exc,
                )
        return results

    # --- Medication Reminders ---

    def upsert_med_reminder(
        self, user_id: int, med_name: str, time: str, notes: str = "",
    ) -> str:
        """Insert or update a medication reminder (encrypted)."""
        data = {"med_name": med_name, "notes": notes}

        # Check if reminder already exists for this med
        existing = self.get_med_reminders(user_id)
        for r in existing:
            if r.get("med_name", "").lower() == med_name.lower():
                # Update existing — encrypt once with the existing row's AAD
                old_id = r.get("_id", "")
                old_aad = f"med_reminders.encrypted_data.{old_id}"
                enc_data = self._encrypt(data, old_aad)
                self.conn.execute(
                    "UPDATE med_reminders SET time = ?, encrypted_data = ? WHERE id = ?",
                    (time, enc_data, old_id),
                )
                self.conn.commit()
                return old_id

        # Insert new — encrypt once with the new row's AAD
        reminder_id = uuid.uuid4().hex
        aad = f"med_reminders.encrypted_data.{reminder_id}"
        enc_data = self._encrypt(data, aad)
        self.conn.execute(
            """INSERT INTO med_reminders (id, user_id, time, enabled, created_at, encrypted_data)
               VALUES (?, ?, ?, 1, ?, ?)""",
            (reminder_id, user_id, time, self._now(), enc_data),
        )
        self.conn.commit()
        return reminder_id

    def get_med_reminders(self, user_id: int) -> list[dict]:
        """Get all medication reminders for a user (decrypted)."""
        try:
            rows = self.conn.execute(
                "SELECT * FROM med_reminders WHERE user_id = ? AND enabled = 1",
                (user_id,),
            ).fetchall()
        except Exception:
            return []  # Table might not exist yet
        results = []
        for row in rows:
            aad = f"med_reminders.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
                data["_id"] = row["id"]
                data["_time"] = row["time"]
                data["_enabled"] = bool(row["enabled"])
                results.append(data)
            except Exception as e:
                logger.warning("Decrypt failed for med_reminders row %s: %s", row["id"], e)
                continue
        return results

    def disable_med_reminder(self, user_id: int, med_name: str) -> bool:
        """Disable a medication reminder by name. Returns True if found."""
        reminders = self.get_med_reminders(user_id)
        for r in reminders:
            if r.get("med_name", "").lower() == med_name.lower():
                self.conn.execute(
                    "UPDATE med_reminders SET enabled = 0 WHERE id = ?",
                    (r["_id"],),
                )
                self.conn.commit()
                return True
        return False

    def pause_med_reminder(
        self, user_id: int, med_name: str,
        paused_reason: str, resume_after: str,
    ) -> bool:
        """Pause a medication reminder with reason and resume date.

        Stores paused_reason and resume_after in the encrypted JSON blob.
        Returns True if a matching reminder was found and paused.
        """
        reminders = self.get_med_reminders(user_id)
        for r in reminders:
            if r.get("med_name", "").lower() == med_name.lower():
                rid = r["_id"]
                aad = f"med_reminders.encrypted_data.{rid}"
                data = {
                    "med_name": r.get("med_name", ""),
                    "notes": r.get("notes", ""),
                    "paused_reason": paused_reason,
                    "resume_after": resume_after,
                }
                enc_data = self._encrypt(data, aad)
                self.conn.execute(
                    "UPDATE med_reminders SET encrypted_data = ? WHERE id = ?",
                    (enc_data, rid),
                )
                self.conn.commit()
                return True
        return False

    def resume_med_reminder(self, user_id: int, med_name: str) -> bool:
        """Resume a paused medication reminder. Returns True if found and resumed."""
        reminders = self.get_med_reminders(user_id)
        for r in reminders:
            if (
                r.get("med_name", "").lower() == med_name.lower()
                and r.get("paused_reason")
            ):
                rid = r["_id"]
                aad = f"med_reminders.encrypted_data.{rid}"
                data = {
                    "med_name": r.get("med_name", ""),
                    "notes": r.get("notes", ""),
                }
                enc_data = self._encrypt(data, aad)
                self.conn.execute(
                    "UPDATE med_reminders SET encrypted_data = ? WHERE id = ?",
                    (enc_data, rid),
                )
                self.conn.commit()
                return True
        return False

    def get_paused_reminders(self, user_id: int) -> list[dict]:
        """Get all paused medication reminders (those with paused_reason set)."""
        all_reminders = self.get_med_reminders(user_id)
        return [r for r in all_reminders if r.get("paused_reason")]

    # --- Health goals ---

    def insert_health_goal(self, user_id: int, goal_data: dict) -> str:
        """Insert a health goal (encrypted)."""
        goal_id = uuid.uuid4().hex
        aad = f"health_goals.encrypted_data.{goal_id}"
        enc_data = self._encrypt(goal_data, aad)
        self.conn.execute(
            """INSERT INTO health_goals (id, user_id, created_at, encrypted_data)
               VALUES (?, ?, ?, ?)""",
            (goal_id, user_id, self._now(), enc_data),
        )
        self.conn.commit()
        return goal_id

    def get_health_goals(self, user_id: int) -> list[dict]:
        """Get all health goals for a user (decrypted)."""
        try:
            rows = self.conn.execute(
                "SELECT * FROM health_goals WHERE user_id = ?",
                (user_id,),
            ).fetchall()
        except Exception:
            return []  # Table might not exist yet
        results = []
        for row in rows:
            aad = f"health_goals.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
                data["_id"] = row["id"]
                data["_created_at"] = row["created_at"]
                results.append(data)
            except Exception as e:
                logger.warning("Decrypt failed for health_goals row %s: %s", row["id"], e)
                continue
        return results

    def delete_health_goal(self, goal_id: str) -> bool:
        """Delete a health goal by ID."""
        cursor = self.conn.execute(
            "DELETE FROM health_goals WHERE id = ?", (goal_id,),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    # --- Wearable daily ---

    def insert_wearable_daily(self, wd: WhoopDaily, user_id: int = 0) -> str:
        """Insert a wearable daily record."""
        wd_id = wd.id or uuid.uuid4().hex
        aad = f"wearable_daily.encrypted_data.{wd_id}"
        enc_data = self._encrypt(wd, aad)
        self.conn.execute(
            """INSERT OR REPLACE INTO wearable_daily (id, date, provider,
               created_at, encrypted_data, user_id)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (wd_id, wd.date.isoformat(), wd.provider, self._now(), enc_data, user_id),
        )
        self.conn.commit()
        return wd_id

    def query_wearable_daily(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        provider: str = "whoop",
        limit: int = 365,
        user_id: int | None = None,
        since: str | None = None,
    ) -> list[dict]:
        """Query wearable daily data."""
        sql = "SELECT * FROM wearable_daily WHERE provider = ?"
        params: list[Any] = [provider]
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)
        if since:
            sql += " AND created_at > ?"
            params.append(since)
        sql += " ORDER BY date DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            aad = f"wearable_daily.encrypted_data.{row['id']}"
            data = self._decrypt(row["encrypted_data"], aad)
            data["_id"] = row["id"]
            data["_date"] = row["date"]
            results.append(data)
        return results

    def query_wearable_stats(self, provider: str) -> dict | None:
        """Return aggregate stats for a wearable provider.

        Returns dict with keys: count, first_date, last_date.
        Returns None if no records found.
        """
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt, MIN(date) as first, MAX(date) as last "
            "FROM wearable_daily WHERE provider = ?",
            (provider,),
        ).fetchone()
        if not row or not row["cnt"]:
            return None
        return {
            "count": row["cnt"],
            "first_date": row["first"],
            "last_date": row["last"],
        }

    # --- External evidence ---

    def insert_external_evidence(self, ev: ExternalEvidence) -> str:
        """Insert external evidence from research."""
        ev_id = ev.id or uuid.uuid4().hex
        aad = f"external_evidence.encrypted_data.{ev_id}"
        enc_data = self._encrypt(ev, aad)
        self.conn.execute(
            """INSERT INTO external_evidence (evidence_id, source, query_hash,
               created_at, encrypted_data) VALUES (?, ?, ?, ?, ?)""",
            (ev_id, ev.source, ev.query_hash, self._now(), enc_data),
        )
        self.conn.commit()
        return ev_id

    # --- Search index helpers ---

    def upsert_search_text(
        self, doc_id: str, record_type: str,
        date_effective: str | None, text: str,
        commit: bool = True,
    ) -> None:
        """Update the search index text for a record."""
        self.conn.execute(
            """INSERT OR REPLACE INTO search_index
               (doc_id, record_type, date_effective, text_for_search)
               VALUES (?, ?, ?, ?)""",
            (doc_id, record_type, date_effective, text),
        )
        if commit:
            self.conn.commit()

    def get_all_search_texts(self) -> list[tuple[str, str, str]]:
        """Return (doc_id, record_type, text) for all indexed records."""
        rows = self.conn.execute(
            "SELECT doc_id, record_type, text_for_search FROM search_index"
        ).fetchall()
        return [(r["doc_id"], r["record_type"], r["text_for_search"]) for r in rows]

    # --- Providers ---

    def insert_provider(self, user_id: int, data: dict) -> str:
        """Insert a healthcare provider. Returns provider ID."""
        prov_id = uuid.uuid4().hex
        aad = f"providers.encrypted_data.{prov_id}"
        enc_data = self._encrypt(data, aad)
        self.conn.execute(
            """INSERT INTO providers (id, user_id, created_at, encrypted_data)
               VALUES (?, ?, ?, ?)""",
            (prov_id, user_id, self._now(), enc_data),
        )
        self.conn.commit()
        return prov_id

    def get_providers(self, user_id: int) -> list[dict]:
        """Get all providers for a user."""
        rows = self.conn.execute(
            "SELECT * FROM providers WHERE user_id = ? ORDER BY created_at",
            (user_id,),
        ).fetchall()
        results = []
        for row in rows:
            aad = f"providers.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for providers row %s: %s", row["id"], e)
                data = {}
            data["_id"] = row["id"]
            data["_created_at"] = row["created_at"]
            results.append(data)
        return results

    def delete_provider(self, provider_id: str) -> bool:
        """Delete a provider by ID."""
        cur = self.conn.execute(
            "DELETE FROM providers WHERE id = ?", (provider_id,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    # --- Appointments ---

    def insert_appointment(self, user_id: int, provider_id: str, data: dict) -> str:
        """Insert an appointment. Returns appointment ID."""
        appt_id = uuid.uuid4().hex
        aad = f"appointments.encrypted_data.{appt_id}"
        appt_date = data.get("date", "")
        enc_data = self._encrypt(data, aad)
        self.conn.execute(
            """INSERT INTO appointments
               (id, user_id, provider_id, appt_date, status, created_at, encrypted_data)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (appt_id, user_id, provider_id, appt_date, "scheduled",
             self._now(), enc_data),
        )
        self.conn.commit()
        return appt_id

    def get_appointments(
        self, user_id: int, status: str | None = None,
    ) -> list[dict]:
        """Get appointments for a user, optionally filtered by status."""
        sql = "SELECT * FROM appointments WHERE user_id = ?"
        params: list[Any] = [user_id]
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY appt_date ASC"
        rows = self.conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            aad = f"appointments.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for appointments row %s: %s", row["id"], e)
                data = {}
            data["_id"] = row["id"]
            data["_provider_id"] = row["provider_id"]
            data["_appt_date"] = row["appt_date"]
            data["_status"] = row["status"]
            data["_prep_sent"] = bool(row["prep_sent"])
            results.append(data)
        return results

    def get_upcoming_appointments(
        self, user_id: int, within_days: int = 2,
    ) -> list[dict]:
        """Get appointments within the next N days."""
        from datetime import timedelta
        today = date.today().isoformat()
        future = (date.today() + timedelta(days=within_days)).isoformat()
        sql = """SELECT * FROM appointments
                 WHERE user_id = ? AND appt_date >= ? AND appt_date <= ?
                 AND status = 'scheduled'
                 ORDER BY appt_date ASC"""
        rows = self.conn.execute(sql, (user_id, today, future)).fetchall()
        results = []
        for row in rows:
            aad = f"appointments.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for appointments row %s: %s", row["id"], e)
                data = {}
            data["_id"] = row["id"]
            data["_provider_id"] = row["provider_id"]
            data["_appt_date"] = row["appt_date"]
            data["_status"] = row["status"]
            data["_prep_sent"] = bool(row["prep_sent"])
            results.append(data)
        return results

    def mark_appointment_prep_sent(self, appt_id: str) -> None:
        """Mark appointment as having had prep sent."""
        self.conn.execute(
            "UPDATE appointments SET prep_sent = 1 WHERE id = ?",
            (appt_id,),
        )
        self.conn.commit()

    def update_appointment_status(self, appt_id: str, status: str) -> None:
        """Update appointment status (scheduled, completed, cancelled)."""
        self.conn.execute(
            "UPDATE appointments SET status = ? WHERE id = ?",
            (status, appt_id),
        )
        self.conn.commit()

    def delete_appointment(self, appt_id: str) -> bool:
        """Delete an appointment by ID."""
        cur = self.conn.execute(
            "DELETE FROM appointments WHERE id = ?", (appt_id,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    # --- Genetic variants ---

    def insert_genetic_variant(
        self, user_id: int, rsid: str, chromosome: str,
        position: int, variant_data: dict,
    ) -> str:
        """Insert a genetic variant. Returns variant ID.

        Upserts on (user_id, rsid) — updates if already exists.
        """
        vid = uuid.uuid4().hex
        aad = f"genetic_variants.encrypted_data.{vid}"
        enc_data = self._encrypt(variant_data, aad)
        try:
            self.conn.execute(
                """INSERT INTO genetic_variants
                   (id, user_id, rsid, chromosome, position, source,
                    created_at, encrypted_data)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (vid, user_id, rsid, chromosome, position,
                 variant_data.get("source", "tellmegen"),
                 self._now(), enc_data),
            )
        except sqlite3.IntegrityError:
            # Unique constraint on (user_id, rsid) — update existing
            row = self.conn.execute(
                "SELECT id FROM genetic_variants WHERE user_id = ? AND rsid = ?",
                (user_id, rsid),
            ).fetchone()
            if row:
                vid = row["id"]
                aad = f"genetic_variants.encrypted_data.{vid}"
                enc_data = self._encrypt(variant_data, aad)
                self.conn.execute(
                    "UPDATE genetic_variants SET encrypted_data = ? WHERE id = ?",
                    (enc_data, vid),
                )
        self.conn.commit()
        return vid

    def get_genetic_variants(
        self, user_id: int, rsids: list[str] | None = None,
    ) -> list[dict]:
        """Get genetic variants for a user, optionally filtered by rsid list."""
        if rsids:
            placeholders = ",".join("?" * len(rsids))
            rows = self.conn.execute(
                f"SELECT * FROM genetic_variants WHERE user_id = ? AND rsid IN ({placeholders})",
                [user_id, *rsids],
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM genetic_variants WHERE user_id = ? ORDER BY chromosome, position",
                (user_id,),
            ).fetchall()
        results = []
        for row in rows:
            aad = f"genetic_variants.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for genetic_variants row %s: %s", row["id"], e)
                data = {}
            data["_id"] = row["id"]
            data["_rsid"] = row["rsid"]
            data["_chromosome"] = row["chromosome"]
            data["_position"] = row["position"]
            data["_source"] = row["source"]
            results.append(data)
        return results

    def get_genetic_variant_count(self, user_id: int) -> int:
        """Count total variants stored for a user."""
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM genetic_variants WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return row["cnt"] if row else 0

    def delete_genetic_variants(self, user_id: int) -> int:
        """Delete all genetic variants for a user. Returns count deleted."""
        cur = self.conn.execute(
            "DELETE FROM genetic_variants WHERE user_id = ?", (user_id,),
        )
        self.conn.commit()
        return cur.rowcount

    # --- User identity (encrypted PII for smarter anonymization) ---

    def upsert_identity_field(
        self, user_id: int, field_key: str, value: str, field_type: str,
    ) -> str:
        """Insert or update an encrypted identity field.

        field_key: "full_name", "email", "dob", "family:0", "custom:0", etc.
        field_type: "name", "email", "dob", "custom"

        Returns field ID.
        """
        import sqlite3 as _sqlite3

        field_id = uuid.uuid4().hex
        aad = f"user_identity.encrypted_data.{field_id}"
        enc_data = self._encrypt({"value": value, "type": field_type}, aad)
        try:
            self.conn.execute(
                """INSERT INTO user_identity
                   (id, user_id, field_key, created_at, updated_at, encrypted_data)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (field_id, user_id, field_key, self._now(), self._now(), enc_data),
            )
        except _sqlite3.IntegrityError:
            # Unique constraint on (user_id, field_key) — update existing
            row = self.conn.execute(
                "SELECT id FROM user_identity WHERE user_id = ? AND field_key = ?",
                (user_id, field_key),
            ).fetchone()
            if row:
                field_id = row["id"]
                aad = f"user_identity.encrypted_data.{field_id}"
                enc_data = self._encrypt(
                    {"value": value, "type": field_type}, aad,
                )
                self.conn.execute(
                    "UPDATE user_identity SET encrypted_data = ?, updated_at = ? "
                    "WHERE id = ?",
                    (enc_data, self._now(), field_id),
                )
        self.conn.commit()
        return field_id

    def get_identity_fields(self, user_id: int) -> list[dict]:
        """Retrieve and decrypt all identity fields for a user."""
        rows = self.conn.execute(
            "SELECT id, field_key, encrypted_data FROM user_identity "
            "WHERE user_id = ? ORDER BY field_key",
            (user_id,),
        ).fetchall()
        results = []
        for row in rows:
            aad = f"user_identity.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for user_identity row %s: %s", row["id"], e)
                continue  # Skip corrupted entries
            results.append({
                "field_key": row["field_key"],
                "value": data.get("value", ""),
                "type": data.get("type", ""),
            })
        return results

    def delete_identity_field(self, user_id: int, field_key: str) -> bool:
        """Delete a specific identity field."""
        cur = self.conn.execute(
            "DELETE FROM user_identity WHERE user_id = ? AND field_key = ?",
            (user_id, field_key),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def delete_all_identity_fields(self, user_id: int) -> int:
        """Delete all identity fields for a user. Returns count deleted."""
        cur = self.conn.execute(
            "DELETE FROM user_identity WHERE user_id = ?", (user_id,),
        )
        self.conn.commit()
        return cur.rowcount

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
        return applied

    def store_redaction_log(
        self, entries: list[dict], destination: str,
    ) -> None:
        """Store encrypted redaction diff log entry.

        Only stores category + position (what was removed), NOT the
        original PII text. Encrypted in Tier 1 for audit trail.
        """
        import json
        import uuid
        from datetime import UTC, datetime

        if not entries:
            return

        log_id = str(uuid.uuid4())
        ts = datetime.now(UTC).isoformat()
        data = json.dumps(entries, ensure_ascii=False)
        encrypted = self._encrypt(data, f"redaction_log.data.{log_id}")

        try:
            self.conn.execute(
                "INSERT INTO redaction_log (id, destination, redaction_count, "
                "timestamp, encrypted_data) VALUES (?, ?, ?, ?, ?)",
                (log_id, destination, len(entries), ts, encrypted),
            )
            self.conn.commit()
        except Exception as e:
            if "no such table" not in str(e).lower():
                logger.warning("Failed to store redaction log: %s", e)

    # --- Saved Messages ---

    def save_message(
        self, user_id: int, text: str, context: str | None = None,
    ) -> str:
        """Save a bookmarked message. Returns the saved message ID."""
        msg_id = str(uuid.uuid4())
        # Extract preview: first sentence, truncated to 50 chars
        preview = text.split(". ")[0].split("\n")[0]
        if len(preview) > 50:
            preview = preview[:47] + "..."
        data = {"text": text, "preview": preview, "context": context}
        encrypted = self._encrypt(
            data, f"saved_messages.encrypted_data.{msg_id}",
        )
        self.conn.execute(
            "INSERT INTO saved_messages (id, user_id, saved_at, encrypted_data)"
            " VALUES (?, ?, ?, ?)",
            (msg_id, user_id, self._now(), encrypted),
        )
        self.conn.commit()
        return msg_id

    def get_saved_messages(
        self, user_id: int, limit: int = 100, offset: int = 0,
    ) -> list[dict]:
        """Fetch saved messages for a user, newest first."""
        rows = self.conn.execute(
            "SELECT id, saved_at, encrypted_data FROM saved_messages"
            " WHERE user_id = ? ORDER BY saved_at DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        ).fetchall()
        results = []
        for row in rows:
            try:
                data = self._decrypt(
                    row["encrypted_data"],
                    f"saved_messages.encrypted_data.{row['id']}",
                )
                data["id"] = row["id"]
                data["saved_at"] = row["saved_at"]
                results.append(data)
            except Exception as e:
                logger.warning("Failed to decrypt saved message %s: %s", row["id"], e)
        return results

    def count_saved_messages(self, user_id: int) -> int:
        """Count saved messages for a user."""
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM saved_messages WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return row["cnt"] if row else 0

    def delete_saved_message(self, user_id: int, msg_id: str) -> bool:
        """Delete a saved message. Returns True if deleted."""
        cur = self.conn.execute(
            "DELETE FROM saved_messages WHERE id = ? AND user_id = ?",
            (msg_id, user_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def delete_all_saved_messages(self, user_id: int) -> int:
        """Delete all saved messages for a user. Returns count deleted."""
        cur = self.conn.execute(
            "DELETE FROM saved_messages WHERE user_id = ?", (user_id,),
        )
        self.conn.commit()
        return cur.rowcount

    def search_saved_messages(self, user_id: int, query: str) -> list[dict]:
        """Search saved messages by text content (decrypts and filters in Python)."""
        all_msgs = self.get_saved_messages(user_id, limit=1000)
        q = query.lower()
        return [
            msg for msg in all_msgs
            if q in msg.get("text", "").lower()
            or q in (msg.get("context") or "").lower()
        ]

