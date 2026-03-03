"""Clean DB core — class definition, connection lifecycle, encryption, PII validation.

Tier 2 anonymized data store core. Contains the CleanDBCore base class with
connection management, encryption helpers, PII validation, and schema setup.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")


class PhiDetectedError(Exception):
    """Raised when PII is detected in data destined for the clean store."""


class EncryptionError(Exception):
    """Raised when encryption fails (e.g., no clean key available)."""


# ── Schema ──────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS clean_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clean_observations (
    obs_id TEXT PRIMARY KEY,
    record_type TEXT NOT NULL DEFAULT 'lab_result',
    canonical_name TEXT DEFAULT '',
    date_effective TEXT,
    triage_level TEXT DEFAULT 'normal',
    flag TEXT DEFAULT '',
    test_name TEXT DEFAULT '',
    value TEXT DEFAULT '',
    unit TEXT DEFAULT '',
    reference_low REAL,
    reference_high REAL,
    reference_text TEXT DEFAULT '',
    age_at_collection INTEGER,
    source_lab TEXT DEFAULT '',
    synced_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_clean_obs_date ON clean_observations(date_effective);
CREATE INDEX IF NOT EXISTS idx_clean_obs_name ON clean_observations(canonical_name);
CREATE INDEX IF NOT EXISTS idx_clean_obs_flag ON clean_observations(flag);
CREATE INDEX IF NOT EXISTS idx_clean_obs_lab ON clean_observations(source_lab);

CREATE TABLE IF NOT EXISTS clean_medications (
    med_id TEXT PRIMARY KEY,
    name TEXT DEFAULT '',
    dose TEXT DEFAULT '',
    unit TEXT DEFAULT '',
    frequency TEXT DEFAULT '',
    status TEXT DEFAULT 'active',
    start_date TEXT,
    end_date TEXT,
    synced_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_clean_meds_status ON clean_medications(status);

CREATE TABLE IF NOT EXISTS clean_wearable_daily (
    id TEXT PRIMARY KEY,
    date TEXT NOT NULL,
    provider TEXT DEFAULT 'whoop',
    hrv REAL,
    rhr REAL,
    resp_rate REAL,
    spo2 REAL,
    sleep_score REAL,
    recovery_score REAL,
    strain REAL,
    sleep_duration_min INTEGER,
    rem_min INTEGER,
    deep_min INTEGER,
    light_min INTEGER,
    calories REAL,
    sleep_latency_min REAL,
    wake_episodes INTEGER,
    sleep_efficiency_pct REAL,
    workout_sport_name TEXT,
    workout_avg_hr REAL,
    workout_max_hr REAL,
    skin_temp REAL,
    synced_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_clean_wearable_date ON clean_wearable_daily(date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_clean_wearable_date_provider
    ON clean_wearable_daily(date, provider);

CREATE TABLE IF NOT EXISTS clean_demographics (
    user_id INTEGER PRIMARY KEY,
    age INTEGER,
    sex TEXT DEFAULT '',
    ethnicity TEXT DEFAULT '',
    height_m REAL,
    weight_kg REAL,
    bmi REAL,
    synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clean_hypotheses (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    confidence REAL DEFAULT 0.0,
    evidence_for TEXT DEFAULT '[]',
    evidence_against TEXT DEFAULT '[]',
    missing_tests TEXT DEFAULT '[]',
    status TEXT DEFAULT 'active',
    synced_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_clean_hyp_status ON clean_hypotheses(status);

CREATE TABLE IF NOT EXISTS clean_health_context (
    id TEXT PRIMARY KEY,
    category TEXT DEFAULT '',
    fact BLOB NOT NULL,
    synced_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_clean_ctx_cat ON clean_health_context(category);

CREATE TABLE IF NOT EXISTS clean_workouts (
    id TEXT PRIMARY KEY,
    sport_type TEXT DEFAULT '',
    start_date TEXT DEFAULT '',
    source TEXT DEFAULT '',
    duration_minutes REAL,
    calories_burned REAL,
    avg_heart_rate REAL,
    max_heart_rate REAL,
    min_heart_rate REAL,
    distance_km REAL,
    synced_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clean_workouts_date ON clean_workouts(start_date);
CREATE INDEX IF NOT EXISTS idx_clean_workouts_sport ON clean_workouts(sport_type);

CREATE TABLE IF NOT EXISTS clean_genetic_variants (
    id TEXT PRIMARY KEY,
    rsid TEXT DEFAULT '',
    chromosome TEXT DEFAULT '',
    position INTEGER,
    source TEXT DEFAULT '',
    genotype TEXT DEFAULT '',
    risk_allele TEXT DEFAULT '',
    phenotype TEXT DEFAULT '',
    synced_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clean_genetics_rsid ON clean_genetic_variants(rsid);

CREATE TABLE IF NOT EXISTS clean_health_goals (
    id TEXT PRIMARY KEY,
    created_at TEXT DEFAULT '',
    goal_text TEXT NOT NULL,
    synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clean_med_reminders (
    id TEXT PRIMARY KEY,
    time TEXT DEFAULT '',
    enabled INTEGER DEFAULT 1,
    med_name TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clean_providers (
    id TEXT PRIMARY KEY,
    specialty TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clean_appointments (
    id TEXT PRIMARY KEY,
    provider_id TEXT DEFAULT '',
    appt_date TEXT DEFAULT '',
    status TEXT DEFAULT '',
    reason TEXT DEFAULT '',
    synced_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clean_appt_date ON clean_appointments(appt_date);
CREATE INDEX IF NOT EXISTS idx_clean_appt_status ON clean_appointments(status);

CREATE TABLE IF NOT EXISTS clean_user_memory (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    category TEXT DEFAULT '',
    confidence REAL DEFAULT 1.0,
    source TEXT DEFAULT 'claude_inferred',
    superseded_by TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    synced_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clean_mem_cat ON clean_user_memory(category);
CREATE INDEX IF NOT EXISTS idx_clean_mem_updated ON clean_user_memory(updated_at DESC);

CREATE TABLE IF NOT EXISTS clean_health_records_ext (
    id TEXT PRIMARY KEY,
    data_type TEXT NOT NULL,
    label TEXT NOT NULL,
    value TEXT,
    unit TEXT,
    date_effective TEXT,
    source TEXT,
    details TEXT,
    tags TEXT,
    synced_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clean_ext_type ON clean_health_records_ext(data_type);

CREATE TABLE IF NOT EXISTS clean_analysis_rules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    scope TEXT NOT NULL,
    rule TEXT NOT NULL,
    priority TEXT DEFAULT 'medium',
    active INTEGER DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clean_rules_active ON clean_analysis_rules(active);

CREATE TABLE IF NOT EXISTS clean_corrections (
    id TEXT PRIMARY KEY,
    original_claim TEXT NOT NULL,
    correction TEXT NOT NULL,
    source TEXT DEFAULT 'user',
    created_at TEXT NOT NULL,
    synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clean_system_improvements (
    id TEXT PRIMARY KEY,
    area TEXT DEFAULT '',
    suggestion TEXT NOT NULL,
    priority TEXT DEFAULT 'low',
    status TEXT DEFAULT 'open',
    created_at TEXT NOT NULL,
    synced_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clean_si_status ON clean_system_improvements(status);

CREATE TABLE IF NOT EXISTS clean_substance_knowledge (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    quality_score REAL DEFAULT 0.0,
    mechanism TEXT DEFAULT '',
    half_life TEXT DEFAULT '',
    cyp_interactions TEXT DEFAULT '',
    pathway_effects TEXT DEFAULT '',
    aliases TEXT DEFAULT '',
    clinical_summary TEXT DEFAULT '',
    research_sources TEXT DEFAULT '',
    synced_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clean_subknow_name ON clean_substance_knowledge(name);

CREATE TABLE IF NOT EXISTS clean_anon_cache (
    text_hash TEXT PRIMARY KEY,
    cleaned_text TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS memory_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    old_value TEXT DEFAULT '',
    new_value TEXT NOT NULL,
    source_type TEXT DEFAULT '',
    source_ref TEXT DEFAULT '',
    changed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_key ON memory_audit_log(key);
CREATE INDEX IF NOT EXISTS idx_audit_changed ON memory_audit_log(changed_at DESC);

CREATE TABLE IF NOT EXISTS schema_evolution_log (
    id TEXT PRIMARY KEY,
    data_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    changes_summary TEXT NOT NULL,
    files_modified TEXT NOT NULL DEFAULT '[]',
    ddl_executed TEXT NOT NULL DEFAULT '[]',
    migration_version INTEGER,
    status TEXT DEFAULT 'success',
    error_message TEXT DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_schema_evo_date ON schema_evolution_log(created_at DESC);
"""


class CleanDBCore:
    """Anonymized health data store — Tier 2 (core).

    Stores pre-anonymized copies of health data. No PII ever written.
    Uses HKDF-derived clean key for selected fields (health context text).
    Purely numeric data (wearables, lab values) stored in plaintext for
    query performance.
    """

    def __init__(self, db_path: Path, phi_firewall: PhiFirewall | None = None) -> None:
        self._path = db_path
        self._fw = phi_firewall or PhiFirewall()
        self._conn: sqlite3.Connection | None = None
        self._clean_key: bytearray | None = None
        self._in_transaction: bool = False

    def open(self, clean_key: bytes | None = None) -> None:
        """Open the clean database and create schema if needed."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._path), check_same_thread=False, isolation_level=None,
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.row_factory = sqlite3.Row
        # Dedup existing wearable rows before UNIQUE index creation
        try:
            self._conn.execute("BEGIN")
            self._conn.execute(
                """DELETE FROM clean_wearable_daily WHERE rowid NOT IN (
                    SELECT MIN(rowid) FROM clean_wearable_daily
                    GROUP BY date, provider
                )"""
            )
            self._conn.execute("COMMIT")
        except Exception:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
        # Add new wearable columns for existing DBs (no-op if column exists)
        # Each ALTER TABLE is wrapped in its own transaction
        for col, col_type in [
            ("calories", "REAL"), ("sleep_latency_min", "REAL"),
            ("wake_episodes", "INTEGER"), ("sleep_efficiency_pct", "REAL"),
            ("workout_sport_name", "TEXT"), ("workout_avg_hr", "REAL"),
            ("workout_max_hr", "REAL"), ("skin_temp", "REAL"),
        ]:
            try:
                self._conn.execute("BEGIN")
                self._conn.execute(
                    f"ALTER TABLE clean_wearable_daily ADD COLUMN {col} {col_type}"
                )
                self._conn.execute("COMMIT")
            except Exception:
                try:
                    self._conn.execute("ROLLBACK")
                except Exception:
                    pass
        # Add source_lab to existing clean_observations (no-op if exists)
        try:
            self._conn.execute("BEGIN")
            self._conn.execute(
                "ALTER TABLE clean_observations ADD COLUMN source_lab TEXT DEFAULT ''"
            )
            self._conn.execute("COMMIT")
        except Exception:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._clean_key = bytearray(clean_key) if clean_key else None

    def zero_clean_key(self) -> None:
        """Securely zero and discard the clean key from memory."""
        if self._clean_key is not None:
            self._clean_key[:] = b'\x00' * len(self._clean_key)
            self._clean_key = None

    def close(self) -> None:
        self.zero_clean_key()
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("CleanDB not opened. Call open() first.")
        return self._conn

    # ── Transaction helpers ──────────────────────────────

    def begin_transaction(self) -> None:
        """Begin an explicit transaction. Defers commits until commit()."""
        self.conn.execute("BEGIN IMMEDIATE")
        self._in_transaction = True

    def commit(self) -> None:
        """Commit the current transaction."""
        self.conn.commit()
        self._in_transaction = False

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.conn.rollback()
        self._in_transaction = False

    def _auto_commit(self) -> None:
        """Commit unless inside an explicit transaction."""
        if not self._in_transaction:
            self.conn.commit()

    # ── Metadata helpers ─────────────────────────────────

    def get_meta(self, key: str) -> str | None:
        """Read a value from the clean_meta key-value table."""
        row = self.conn.execute(
            "SELECT value FROM clean_meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        """Write a value to the clean_meta key-value table."""
        self.conn.execute(
            "INSERT OR REPLACE INTO clean_meta (key, value) VALUES (?, ?)",
            (key, value),
        )
        self._auto_commit()

    # ── Anonymization cache ────────────────────────────

    def get_anon_cache(self, text_hash: str) -> str | None:
        """Look up a cached anonymization result by SHA256 hash."""
        row = self.conn.execute(
            "SELECT cleaned_text FROM clean_anon_cache WHERE text_hash = ?",
            (text_hash,),
        ).fetchone()
        return row["cleaned_text"] if row else None

    def put_anon_cache(self, text_hash: str, cleaned_text: str) -> None:
        """Store an anonymization result keyed by SHA256 hash."""
        self.conn.execute(
            "INSERT OR REPLACE INTO clean_anon_cache"
            " (text_hash, cleaned_text, created_at) VALUES (?, ?, ?)",
            (text_hash, cleaned_text, self._now()),
        )
        self._auto_commit()

    # ── PII validation ──────────────────────────────────

    @staticmethod
    def _is_medical_false_positive(text: str, match) -> bool:
        """Check if an id_* match is a medical false positive.

        Returns True (safe to skip) when the matched text appears in a
        medical context — e.g. "white blood cells", "fisher exact test".
        Looks at ~50 chars of surrounding context for medical indicators.
        """
        # Medical/lab indicators in surrounding context
        medical_indicators = (
            "mg", "ml", "dl", "mmol", "cells", "count", "blood",
            "serum", "plasma", "urine", "level", "range", "test",
            "result", "lab", "panel", "ratio", "index", "score",
            "enzyme", "protein", "marker", "vitamin", "hormone",
            "supplement", "dose", "dosage", "mg/dl", "iu/l",
            "nmol", "pmol", "mcg", "\u00b5g", "ng", "pg",
        )
        start = max(0, match.start - 50)
        end = min(len(text), match.end + 50)
        context = text[start:end].lower()
        # Context-based medical false positive check: if surrounded by
        # medical terms, it's likely a medical term, not PII.
        for indicator in medical_indicators:
            if indicator in context:
                return True
        return False

    def _assert_no_phi(self, text: str, context: str = "") -> None:
        """Raise PhiDetectedError if text contains PII patterns.

        For identity-specific patterns (id_* prefix), applies a context-
        aware check instead of blindly skipping all of them. Matches in
        medical context (adjacent to lab terms, units, clinical vocab)
        are treated as false positives; others are flagged.
        """
        if not text:
            return
        matches = self._fw.scan(text)
        real_matches = []
        for m in matches:
            if m.category.startswith("id_"):
                if not self._is_medical_false_positive(text, m):
                    real_matches.append(m)
            else:
                real_matches.append(m)
        if real_matches:
            categories = {m.category for m in real_matches}
            logger.warning(
                "PII in clean store write (%s): categories=%s",
                context, categories,
            )
            raise PhiDetectedError(
                f"PII detected ({', '.join(categories)}) in {context}: blocked"
            )

    def _validate_text_fields(self, fields: dict[str, str], context: str) -> None:
        """Validate all text fields are PII-free."""
        for name, value in fields.items():
            if value:
                self._assert_no_phi(value, f"{context}.{name}")

    # ── Encryption helpers (for text fields) ────────────

    def _encrypt(self, data: str, aad: str) -> bytes:
        """Encrypt a text field with the clean key.

        Raises EncryptionError if no clean key is available — never falls
        back to storing plaintext, which would violate Tier 2 guarantees.
        """
        if not self._clean_key:
            raise EncryptionError(
                "Cannot encrypt: clean key not available. "
                "Refusing to store plaintext in clean DB."
            )
        nonce = os.urandom(12)
        aesgcm = AESGCM(self._clean_key)
        ct = aesgcm.encrypt(nonce, data.encode("utf-8"), aad.encode("utf-8"))
        return nonce + ct

    def _decrypt(self, blob: bytes, aad: str) -> str:
        """Decrypt a text field with the clean key."""
        if not self._clean_key:
            return blob.decode("utf-8")
        # AES-GCM needs 12-byte nonce + at least 16-byte auth tag
        if len(blob) < 28:
            raise ValueError(f"Corrupt encrypted blob ({len(blob)} bytes < 28 minimum)")
        nonce = blob[:12]
        ct = blob[12:]
        aesgcm = AESGCM(self._clean_key)
        return aesgcm.decrypt(nonce, ct, aad.encode("utf-8")).decode("utf-8")

    # ── Timestamp helper ────────────────────────────────

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    # Allowlist of valid table and column names for dynamic SQL
    _VALID_TABLES: set[str] = {
        "clean_observations", "clean_medications", "clean_wearable_daily",
        "clean_hypotheses", "clean_health_context",
        "clean_workouts", "clean_genetic_variants", "clean_health_goals",
        "clean_med_reminders", "clean_providers", "clean_appointments",
        "clean_health_records_ext", "clean_anon_cache",
        "clean_system_improvements", "clean_demographics",
        "clean_meta", "clean_user_memory", "clean_corrections",
        "clean_substance_knowledge", "clean_analysis_rules",
        "memory_audit_log", "schema_evolution_log",
    }
    _VALID_ID_COLUMNS: set[str] = {
        "id", "obs_id", "med_id", "hyp_id", "ctx_id", "goal_id",
        "reminder_id", "provider_id", "appointment_id", "variant_id",
    }

    def delete_stale(self, table: str, id_column: str, valid_ids: set[str] | None) -> int:
        """Delete records whose IDs are no longer in the raw vault.

        Args:
            table: Table name (internal constant, never user-supplied).
            id_column: Primary key column name.
            valid_ids: Set of IDs from the raw vault, or None to skip
                       (None = query failed, don't delete anything).

        Returns count of deleted rows.
        """
        if valid_ids is None:
            return 0

        # Validate table and column names against allowlist
        if table not in self._VALID_TABLES:
            raise ValueError(f"Invalid table name: {table}")
        if id_column not in self._VALID_ID_COLUMNS:
            raise ValueError(f"Invalid column name: {id_column}")

        # Fetch all IDs currently in the clean table
        rows = self.conn.execute(
            f"SELECT {id_column} FROM {table}",  # noqa: S608
        ).fetchall()
        stale_ids = [r[0] for r in rows if r[0] not in valid_ids]
        if not stale_ids:
            return 0

        # Delete in batches (SQLite parameter limit is ~999)
        deleted = 0
        for i in range(0, len(stale_ids), 500):
            batch = stale_ids[i:i + 500]
            placeholders = ",".join("?" for _ in batch)
            cursor = self.conn.execute(
                f"DELETE FROM {table} WHERE {id_column} IN ({placeholders})",  # noqa: S608
                batch,
            )
            deleted += cursor.rowcount
        if deleted:
            self._auto_commit()
            logger.info("Deleted %d stale records from %s", deleted, table)
        return deleted

    def count_rows(self, table: str) -> int:
        """Count rows in a clean DB table.

        Validates the table name against an allowlist to prevent SQL injection.
        """
        if table not in self._VALID_TABLES:
            raise ValueError(f"Invalid table name: {table}")
        try:
            row = self.conn.execute(
                f"SELECT COUNT(*) as cnt FROM {table}",  # noqa: S608
            ).fetchone()
            return row["cnt"] if row else 0
        except Exception:
            return 0

    def get_latest_sync(self) -> str | None:
        """Return the latest synced_at timestamp across all tables."""
        tables = [
            "clean_observations", "clean_medications", "clean_wearable_daily",
            "clean_hypotheses", "clean_health_context",
            "clean_workouts", "clean_genetic_variants", "clean_health_goals",
            "clean_med_reminders", "clean_providers", "clean_appointments",
            "clean_health_records_ext",
        ]
        timestamps = []
        for table in tables:
            row = self.conn.execute(
                f"SELECT MAX(synced_at) as ts FROM {table}",  # noqa: S608
            ).fetchone()
            if row and row["ts"]:
                timestamps.append(row["ts"])
        return max(timestamps) if timestamps else None
