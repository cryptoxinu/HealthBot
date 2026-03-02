"""Clean DB — Tier 2 anonymized data store.

Separate SQLite database for pre-anonymized health data. No PII ever written.
Accessible to Claude Code, OpenClaw, or any AI via MCP server.

Encryption uses HKDF-derived "clean key" from master key, NOT the master
key directly. This provides cryptographic separation between tiers.

Every text field is validated by PhiFirewall before write — if PII is
detected, the write is rejected with PhiDetectedError.
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
"""


class CleanDB:
    """Anonymized health data store — Tier 2.

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

    def _assert_no_phi(self, text: str, context: str = "") -> None:
        """Raise PhiDetectedError if text contains PII."""
        if text and self._fw.contains_phi(text):
            raise PhiDetectedError(
                f"PII detected in clean store write ({context}): blocked"
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

    # ── Upsert methods ──────────────────────────────────

    def upsert_observation(
        self,
        obs_id: str,
        *,
        record_type: str = "lab_result",
        canonical_name: str = "",
        date_effective: str = "",
        triage_level: str = "normal",
        flag: str = "",
        test_name: str = "",
        value: str = "",
        unit: str = "",
        reference_low: float | None = None,
        reference_high: float | None = None,
        reference_text: str = "",
        age_at_collection: int | None = None,
        source_lab: str = "",
    ) -> None:
        self._validate_text_fields(
            {"test_name": test_name, "canonical_name": canonical_name,
             "value": value, "unit": unit, "reference_text": reference_text},
            f"observation.{obs_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_observations
               (obs_id, record_type, canonical_name, date_effective, triage_level,
                flag, test_name, value, unit, reference_low, reference_high,
                reference_text, age_at_collection, source_lab, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (obs_id, record_type, canonical_name, date_effective, triage_level,
             flag, test_name, value, unit, reference_low, reference_high,
             reference_text, age_at_collection, source_lab, self._now()),
        )
        self._auto_commit()

    def upsert_medication(
        self,
        med_id: str,
        *,
        name: str = "",
        dose: str = "",
        unit: str = "",
        frequency: str = "",
        status: str = "active",
        start_date: str = "",
        end_date: str = "",
    ) -> None:
        self._validate_text_fields(
            {"name": name, "dose": dose, "frequency": frequency},
            f"medication.{med_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_medications
               (med_id, name, dose, unit, frequency, status, start_date, end_date, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (med_id, name, dose, unit, frequency, status, start_date, end_date,
             self._now()),
        )
        self._auto_commit()

    def upsert_wearable(
        self,
        wearable_id: str,
        *,
        date: str,
        provider: str = "whoop",
        hrv: float | None = None,
        rhr: float | None = None,
        resp_rate: float | None = None,
        spo2: float | None = None,
        sleep_score: float | None = None,
        recovery_score: float | None = None,
        strain: float | None = None,
        sleep_duration_min: int | None = None,
        rem_min: int | None = None,
        deep_min: int | None = None,
        light_min: int | None = None,
        calories: float | None = None,
        sleep_latency_min: float | None = None,
        wake_episodes: int | None = None,
        sleep_efficiency_pct: float | None = None,
        workout_sport_name: str | None = None,
        workout_avg_hr: float | None = None,
        workout_max_hr: float | None = None,
        skin_temp: float | None = None,
    ) -> None:
        # Wearable data is purely numeric (+ sport name from API presets) — no PII
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_wearable_daily
               (id, date, provider, hrv, rhr, resp_rate, spo2, sleep_score,
                recovery_score, strain, sleep_duration_min, rem_min, deep_min,
                light_min, calories, sleep_latency_min, wake_episodes,
                sleep_efficiency_pct, workout_sport_name, workout_avg_hr,
                workout_max_hr, skin_temp, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (wearable_id, date, provider, hrv, rhr, resp_rate, spo2,
             sleep_score, recovery_score, strain, sleep_duration_min,
             rem_min, deep_min, light_min, calories, sleep_latency_min,
             wake_episodes, sleep_efficiency_pct, workout_sport_name,
             workout_avg_hr, workout_max_hr, skin_temp, self._now()),
        )
        self._auto_commit()

    def upsert_demographics(
        self,
        user_id: int,
        *,
        age: int | None = None,
        sex: str = "",
        ethnicity: str = "",
        height_m: float | None = None,
        weight_kg: float | None = None,
        bmi: float | None = None,
    ) -> None:
        # Demographics are not PII when stripped of name/DOB/address
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_demographics
               (user_id, age, sex, ethnicity, height_m, weight_kg, bmi, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, age, sex, ethnicity, height_m, weight_kg, bmi, self._now()),
        )
        self._auto_commit()

    def upsert_hypothesis(
        self,
        hyp_id: str,
        *,
        title: str,
        confidence: float = 0.0,
        evidence_for: str = "[]",
        evidence_against: str = "[]",
        missing_tests: str = "[]",
        status: str = "active",
    ) -> None:
        self._validate_text_fields(
            {"title": title, "evidence_for": evidence_for,
             "evidence_against": evidence_against,
             "missing_tests": missing_tests},
            f"hypothesis.{hyp_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_hypotheses
               (id, title, confidence, evidence_for, evidence_against,
                missing_tests, status, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (hyp_id, title, confidence, evidence_for, evidence_against,
             missing_tests, status, self._now()),
        )
        self._auto_commit()

    def upsert_health_context(
        self,
        ctx_id: str,
        *,
        category: str = "",
        fact: str,
    ) -> None:
        self._assert_no_phi(fact, f"health_context.{ctx_id}")
        # Encrypt the fact text (may contain borderline data)
        encrypted = self._encrypt(fact, f"clean_health_context.fact.{ctx_id}")
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_health_context
               (id, category, fact, synced_at)
               VALUES (?, ?, ?, ?)""",
            (ctx_id, category, encrypted, self._now()),
        )
        self._auto_commit()

    def upsert_workout(
        self,
        workout_id: str,
        *,
        sport_type: str = "",
        start_date: str = "",
        source: str = "",
        duration_minutes: float | None = None,
        calories_burned: float | None = None,
        avg_heart_rate: float | None = None,
        max_heart_rate: float | None = None,
        min_heart_rate: float | None = None,
        distance_km: float | None = None,
    ) -> None:
        # Workouts are purely numeric + API preset sport names — no PII
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_workouts
               (id, sport_type, start_date, source, duration_minutes,
                calories_burned, avg_heart_rate, max_heart_rate,
                min_heart_rate, distance_km, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (workout_id, sport_type, start_date, source, duration_minutes,
             calories_burned, avg_heart_rate, max_heart_rate,
             min_heart_rate, distance_km, self._now()),
        )
        self._auto_commit()

    def upsert_genetic_variant(
        self,
        variant_id: str,
        *,
        rsid: str = "",
        chromosome: str = "",
        position: int | None = None,
        source: str = "",
        genotype: str = "",
        risk_allele: str = "",
        phenotype: str = "",
    ) -> None:
        # rsIDs are public scientific identifiers — no PII
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_genetic_variants
               (id, rsid, chromosome, position, source, genotype,
                risk_allele, phenotype, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (variant_id, rsid, chromosome, position, source, genotype,
             risk_allele, phenotype, self._now()),
        )
        self._auto_commit()

    def upsert_health_goal(
        self,
        goal_id: str,
        *,
        created_at: str = "",
        goal_text: str,
    ) -> None:
        self._assert_no_phi(goal_text, f"health_goal.{goal_id}")
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_health_goals
               (id, created_at, goal_text, synced_at)
               VALUES (?, ?, ?, ?)""",
            (goal_id, created_at, goal_text, self._now()),
        )
        self._auto_commit()

    def upsert_med_reminder(
        self,
        reminder_id: str,
        *,
        time: str = "",
        enabled: bool = True,
        med_name: str = "",
        notes: str = "",
    ) -> None:
        self._validate_text_fields(
            {"med_name": med_name, "notes": notes},
            f"med_reminder.{reminder_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_med_reminders
               (id, time, enabled, med_name, notes, synced_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (reminder_id, time, int(enabled), med_name, notes, self._now()),
        )
        self._auto_commit()

    def upsert_provider(
        self,
        provider_id: str,
        *,
        specialty: str = "",
        notes: str = "",
    ) -> None:
        self._validate_text_fields(
            {"specialty": specialty, "notes": notes},
            f"provider.{provider_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_providers
               (id, specialty, notes, synced_at)
               VALUES (?, ?, ?, ?)""",
            (provider_id, specialty, notes, self._now()),
        )
        self._auto_commit()

    def upsert_appointment(
        self,
        appt_id: str,
        *,
        provider_id: str = "",
        appt_date: str = "",
        status: str = "",
        reason: str = "",
    ) -> None:
        self._assert_no_phi(reason, f"appointment.{appt_id}")
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_appointments
               (id, provider_id, appt_date, status, reason, synced_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (appt_id, provider_id, appt_date, status, reason, self._now()),
        )
        self._auto_commit()

    # ── Query methods ───────────────────────────────────

    def get_lab_results(
        self,
        *,
        test_name: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        flag: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        sql = "SELECT * FROM clean_observations WHERE record_type = 'lab_result'"
        params: list = []
        if test_name:
            sql += " AND (canonical_name LIKE ? OR test_name LIKE ?)"
            pat = f"%{test_name}%"
            params.extend([pat, pat])
        if start_date:
            sql += " AND date_effective >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date_effective <= ?"
            params.append(end_date)
        if flag:
            sql += " AND flag = ?"
            params.append(flag)
        sql += " ORDER BY date_effective DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def _get_latest_per_test(self, *, limit: int = 200) -> list[dict]:
        """Return the most recent result for each unique test.

        Uses a SQL window function to deduplicate by canonical_name,
        ensuring qualitative tests (JAK2, CALR, HBsAg, etc.) are always
        visible even when newer numeric panels have many more rows.
        """
        sql = """
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY canonical_name
                    ORDER BY date_effective DESC
                ) AS rn
                FROM clean_observations
                WHERE record_type = 'lab_result'
            ) WHERE rn = 1
            ORDER BY date_effective DESC
            LIMIT ?
        """
        rows = self.conn.execute(sql, (limit,)).fetchall()
        # Strip the synthetic rn column from results
        return [{k: v for k, v in dict(r).items() if k != "rn"} for r in rows]

    def get_medications(self, status: str = "active") -> list[dict]:
        if status == "all":
            rows = self.conn.execute(
                "SELECT * FROM clean_medications ORDER BY start_date DESC",
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM clean_medications WHERE status = ? ORDER BY start_date DESC",
                (status,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_wearable_data(
        self,
        *,
        days: int = 7,
        provider: str = "whoop",
    ) -> list[dict]:
        rows = self.conn.execute(
            """SELECT * FROM clean_wearable_daily
               WHERE provider = ?
               ORDER BY date DESC LIMIT ?""",
            (provider, days),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_wearable_daily(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        provider: str = "whoop",
        limit: int = 365,
        user_id: int | None = None,
    ) -> list[dict]:
        """Query wearable data — same interface as HealthDB for duck typing.

        Wearable data is purely numeric (no PII), so reasoning modules
        can read directly from Clean DB without decrypt overhead.
        """
        sql = "SELECT * FROM clean_wearable_daily WHERE provider = ?"
        params: list = [provider]
        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)
        sql += " ORDER BY date DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def get_demographics(self, user_id: int | None = None) -> dict | None:
        if user_id is not None:
            row = self.conn.execute(
                "SELECT * FROM clean_demographics WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT * FROM clean_demographics LIMIT 1",
            ).fetchone()
        return dict(row) if row else None

    def get_hypotheses(self, status: str = "active") -> list[dict]:
        rows = self.conn.execute(
            """SELECT * FROM clean_hypotheses WHERE status = ?
               ORDER BY confidence DESC""",
            (status,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_health_context(self, category: str | None = None) -> list[dict]:
        if category:
            rows = self.conn.execute(
                "SELECT * FROM clean_health_context WHERE category = ?",
                (category,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM clean_health_context",
            ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            if isinstance(d["fact"], bytes):
                d["fact"] = self._decrypt(d["fact"], f"clean_health_context.fact.{d['id']}")
            results.append(d)
        return results

    def get_workouts(
        self,
        *,
        sport_type: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        if sport_type:
            rows = self.conn.execute(
                """SELECT * FROM clean_workouts WHERE sport_type = ?
                   ORDER BY start_date DESC LIMIT ?""",
                (sport_type, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM clean_workouts ORDER BY start_date DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_genetic_variants(
        self,
        *,
        rsid: str | None = None,
        limit: int = 200,
    ) -> list[dict]:
        if rsid:
            rows = self.conn.execute(
                "SELECT * FROM clean_genetic_variants WHERE rsid = ? LIMIT ?",
                (rsid, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT * FROM clean_genetic_variants
                   ORDER BY chromosome, position LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_health_goals(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM clean_health_goals ORDER BY created_at DESC",
        ).fetchall()
        return [dict(r) for r in rows]

    def get_med_reminders(self, enabled_only: bool = True) -> list[dict]:
        if enabled_only:
            rows = self.conn.execute(
                "SELECT * FROM clean_med_reminders WHERE enabled = 1 ORDER BY time",
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM clean_med_reminders ORDER BY time",
            ).fetchall()
        return [dict(r) for r in rows]

    def get_providers(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM clean_providers ORDER BY specialty",
        ).fetchall()
        return [dict(r) for r in rows]

    def get_appointments(
        self,
        *,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        if status:
            rows = self.conn.execute(
                """SELECT * FROM clean_appointments WHERE status = ?
                   ORDER BY appt_date DESC LIMIT ?""",
                (status, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM clean_appointments ORDER BY appt_date DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def search(self, query: str, limit: int = 20) -> list[dict]:
        """Search across all clean data by keyword."""
        pat = f"%{query}%"
        results: list[dict] = []

        # Search observations
        rows = self.conn.execute(
            """SELECT 'lab' as source, obs_id as id, test_name, value, unit,
                      date_effective as date, flag
               FROM clean_observations
               WHERE test_name LIKE ? OR canonical_name LIKE ? OR value LIKE ?
               LIMIT ?""",
            (pat, pat, pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        # Search medications
        rows = self.conn.execute(
            """SELECT 'medication' as source, med_id as id, name, dose, frequency,
                      start_date as date, status
               FROM clean_medications
               WHERE name LIKE ? OR dose LIKE ?
               LIMIT ?""",
            (pat, pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        # Search workouts
        rows = self.conn.execute(
            """SELECT 'workout' as source, id, sport_type, start_date as date,
                      duration_minutes, calories_burned
               FROM clean_workouts
               WHERE sport_type LIKE ?
               LIMIT ?""",
            (pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        # Search genetic variants
        rows = self.conn.execute(
            """SELECT 'genetic' as source, id, rsid, genotype, phenotype,
                      '' as date
               FROM clean_genetic_variants
               WHERE rsid LIKE ? OR phenotype LIKE ?
               LIMIT ?""",
            (pat, pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        # Search health goals
        rows = self.conn.execute(
            """SELECT 'goal' as source, id, goal_text, created_at as date
               FROM clean_health_goals
               WHERE goal_text LIKE ?
               LIMIT ?""",
            (pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        # Search hypotheses
        rows = self.conn.execute(
            """SELECT 'hypothesis' as source, id, title, confidence, status,
                      '' as date
               FROM clean_hypotheses
               WHERE title LIKE ? OR evidence_for LIKE ?
               LIMIT ?""",
            (pat, pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        return results[:limit]

    def get_health_summary_markdown(self) -> str:
        """Generate a comprehensive health summary as Markdown."""
        sections = self.get_health_summary_sections()
        # Join all full-detail sections (skip summaries, they're for compact mode)
        order = [
            "header", "demographics", "labs", "medications",
            "wearable_detail", "workouts", "hypotheses", "health_context",
            "genetics", "goals", "med_reminders", "providers",
            "appointments", "health_records_ext", "analysis_rules",
            "user_memory",
        ]
        parts = [sections[k] for k in order if sections.get(k)]
        return "\n".join(parts)

    def get_health_summary_sections(self) -> dict[str, str]:
        """Return health summary as named sections for query-aware selection.

        Keys returned (empty string if no data):
          header, demographics, labs, labs_summary, medications,
          wearable_detail, wearable_summary, hypotheses,
          health_context, user_memory
        """
        sections: dict[str, str] = {}

        # Header
        sections["header"] = (
            "# Health Data Summary (Anonymized)\n\n"
            f"> Generated: {datetime.now(UTC).strftime('%Y-%m-%d')}\n"
            "> All PII has been stripped. This data is safe for AI analysis.\n"
        )

        # Demographics
        sections["demographics"] = self._build_demographics_section()

        # Labs (full + summary)
        labs = self._get_latest_per_test(limit=200)
        sections["labs"] = self._build_labs_section(labs)
        sections["labs_summary"] = self._build_labs_summary(labs)

        # Medications
        sections["medications"] = self._build_medications_section()

        # Wearable data (full detail + compact summary)
        detail_parts: list[str] = []
        summary_parts: list[str] = []
        for prov, label in [("whoop", "WHOOP"), ("oura", "Oura")]:
            all_data = self.get_wearable_data(days=365, provider=prov)
            if not all_data:
                continue
            detail_parts.append(self._build_wearable_detail(all_data, label))
            summary_parts.append(self._build_wearable_summary(all_data, label))
        sections["wearable_detail"] = "\n".join(detail_parts)
        sections["wearable_summary"] = "\n".join(summary_parts)

        # Workouts
        sections["workouts"] = self._build_workouts_section()

        # Hypotheses
        sections["hypotheses"] = self._build_hypotheses_section()

        # Health context
        sections["health_context"] = self._build_health_context_section()

        # Genetics
        sections["genetics"] = self._build_genetics_section()

        # Goals
        sections["goals"] = self._build_goals_section()

        # Med reminders
        sections["med_reminders"] = self._build_med_reminders_section()

        # Providers
        sections["providers"] = self._build_providers_section()

        # Appointments
        sections["appointments"] = self._build_appointments_section()

        # Extended health records
        sections["health_records_ext"] = self._build_health_records_ext_section()

        # Analysis rules
        sections["analysis_rules"] = self._build_analysis_rules_section()

        # User memory
        sections["user_memory"] = self._build_user_memory_section()

        return sections

    # ── Section builders for get_health_summary_sections() ────

    def _build_demographics_section(self) -> str:
        demo = self.get_demographics()
        if not demo:
            return ""
        parts: list[str] = ["## Demographics\n"]
        if demo.get("age"):
            parts.append(f"- **Age**: {demo['age']}")
        if demo.get("sex"):
            parts.append(f"- **Sex**: {demo['sex']}")
        if demo.get("ethnicity"):
            parts.append(f"- **Ethnicity**: {demo['ethnicity']}")
        if demo.get("height_m"):
            inches = demo["height_m"] * 39.3701
            feet = int(inches // 12)
            rem = int(round(inches % 12))
            parts.append(f"- **Height**: {feet}'{rem}\" ({demo['height_m']:.2f} m)")
        if demo.get("weight_kg"):
            lbs = demo["weight_kg"] * 2.20462
            parts.append(f"- **Weight**: {lbs:.0f} lbs ({demo['weight_kg']:.1f} kg)")
        if demo.get("bmi"):
            parts.append(f"- **BMI**: {demo['bmi']:.1f}")
        parts.append("")
        return "\n".join(parts)

    def _build_labs_section(self, labs: list[dict]) -> str:
        if not labs:
            return ""
        parts: list[str] = ["## Recent Lab Results\n"]
        has_lab = any(lab.get("source_lab") for lab in labs)
        if has_lab:
            parts.append("| Date | Test | Value | Unit | Reference | Flag | Lab |")
            parts.append("|------|------|-------|------|-----------|------|-----|")
        else:
            parts.append("| Date | Test | Value | Unit | Reference | Flag |")
            parts.append("|------|------|-------|------|-----------|------|")
        for lab in labs:
            ref = ""
            if lab.get("reference_low") is not None and lab.get("reference_high") is not None:
                ref = f"{lab['reference_low']}-{lab['reference_high']}"
            elif lab.get("reference_text"):
                ref = lab["reference_text"]
            row = (
                f"| {lab.get('date_effective', '')} "
                f"| {lab.get('test_name') or lab.get('canonical_name', '')} "
                f"| {lab.get('value', '')} "
                f"| {lab.get('unit', '')} "
                f"| {ref} "
                f"| {lab.get('flag', '')} "
            )
            if has_lab:
                row += f"| {lab.get('source_lab', '')} |"
            else:
                row += "|"
            parts.append(row)
        parts.append("")
        return "\n".join(parts)

    def _build_labs_summary(self, labs: list[dict]) -> str:
        """Compact labs: flagged results + total count."""
        if not labs:
            return ""
        flagged = [
            lab for lab in labs
            if lab.get("flag") and lab["flag"].upper() not in ("", "NORMAL")
        ]
        parts: list[str] = [f"## Lab Results ({len(labs)} tests on file)\n"]
        if flagged:
            parts.append("Flagged results:")
            for lab in flagged[:15]:
                name = lab.get("test_name") or lab.get("canonical_name", "")
                parts.append(
                    f"- {name}: {lab.get('value', '')} {lab.get('unit', '')} "
                    f"[{lab.get('flag', '')}] ({lab.get('date_effective', '')})"
                )
        else:
            parts.append("No flagged results.")
        parts.append("")
        return "\n".join(parts)

    def _build_medications_section(self) -> str:
        meds = self.get_medications()
        if not meds:
            return ""
        parts: list[str] = [
            "## Active Medications\n",
            "| Medication | Dose | Frequency |",
            "|------------|------|-----------|",
        ]
        for med in meds:
            dose = med.get("dose", "")
            if med.get("unit"):
                dose = f"{dose} {med['unit']}"
            parts.append(f"| {med.get('name', '')} | {dose} | {med.get('frequency', '')} |")
        parts.append("")
        return "\n".join(parts)

    @staticmethod
    def _build_wearable_detail(all_data: list[dict], label: str) -> str:
        """Full wearable section: monthly averages + 14-day daily detail."""
        from collections import defaultdict

        dates = [d.get("date", "") for d in all_data if d.get("date")]
        first_date = min(dates) if dates else "?"
        last_date = max(dates) if dates else "?"
        parts: list[str] = [
            f"## {label} Data — {len(all_data)} records"
            f" ({first_date} to {last_date})\n",
        ]

        # Monthly averages
        months: dict[str, list[dict]] = defaultdict(list)
        for day in all_data:
            date_str = day.get("date", "")
            if date_str and len(date_str) >= 7:
                months[date_str[:7]].append(day)

        def _month_avg(rows: list[dict], field: str) -> str:
            vals = [d[field] for d in rows if d.get(field) is not None]
            return f"{sum(vals) / len(vals):.0f}" if vals else "-"

        if months:
            parts.append("### Monthly Averages\n")
            parts.append(
                "| Month | Days | Avg HRV | Avg RHR"
                " | Avg Sleep | Avg Recovery | Avg Strain |"
            )
            parts.append(
                "|-------|------|---------|--------"
                "|-----------|--------------|------------|"
            )
            for month_key in sorted(months.keys()):
                m = months[month_key]
                parts.append(
                    f"| {month_key} | {len(m)}"
                    f" | {_month_avg(m, 'hrv')}"
                    f" | {_month_avg(m, 'rhr')}"
                    f" | {_month_avg(m, 'sleep_score')}"
                    f" | {_month_avg(m, 'recovery_score')}"
                    f" | {_month_avg(m, 'strain')} |"
                )
            parts.append("")

        # Recent 14 days daily detail
        recent = all_data[:14] if len(all_data) >= 14 else all_data
        parts.append("### Recent Daily Detail\n")
        parts.append(
            "| Date | HRV | RHR | Sleep | Recovery"
            " | Strain | Deep | REM | SpO2 | Workout |"
        )
        parts.append(
            "|------|-----|-----|-------|----------"
            "|--------|------|-----|------|---------|"
        )
        for day in recent:
            hrv = f"{day['hrv']:.0f}" if day.get("hrv") is not None else "-"
            rhr = f"{day['rhr']:.0f}" if day.get("rhr") is not None else "-"
            slp = f"{day['sleep_score']:.0f}" if day.get("sleep_score") is not None else "-"
            rec_val = day.get("recovery_score")
            rec = f"{rec_val:.0f}%" if rec_val is not None else "-"
            strain = f"{day['strain']:.1f}" if day.get("strain") is not None else "-"
            deep = f"{day['deep_min']}m" if day.get("deep_min") is not None else "-"
            rem_val = f"{day['rem_min']}m" if day.get("rem_min") is not None else "-"
            spo2 = f"{day['spo2']:.0f}%" if day.get("spo2") is not None else "-"
            wk = day.get("workout_sport_name") or "-"
            parts.append(
                f"| {day.get('date', '')} | {hrv} | {rhr} | {slp}"
                f" | {rec} | {strain} | {deep} | {rem_val} | {spo2} | {wk} |"
            )
        parts.append("")
        return "\n".join(parts)

    @staticmethod
    def _build_wearable_summary(all_data: list[dict], label: str) -> str:
        """Compact wearable: 14-day averages in one line per provider."""
        recent = all_data[:14] if len(all_data) >= 14 else all_data
        if not recent:
            return ""

        def _avg(field: str) -> str:
            vals = [d[field] for d in recent if d.get(field) is not None]
            return f"{sum(vals) / len(vals):.0f}" if vals else "-"

        return (
            f"{label} ({len(recent)}d avg): "
            f"HRV {_avg('hrv')} | RHR {_avg('rhr')} | "
            f"Recovery {_avg('recovery_score')}% | "
            f"Sleep {_avg('sleep_score')} | "
            f"Strain {_avg('strain')}"
        )

    def _build_hypotheses_section(self) -> str:
        hyps = self.get_hypotheses()
        if not hyps:
            return ""
        parts: list[str] = ["## Health Hypotheses\n"]
        for h in hyps:
            parts.append(f"### {h['title']} (confidence: {h['confidence']:.0%})")
            parts.append(f"- Evidence for: {h.get('evidence_for', '[]')}")
            parts.append(f"- Evidence against: {h.get('evidence_against', '[]')}")
            parts.append(f"- Missing tests: {h.get('missing_tests', '[]')}")
            parts.append("")
        return "\n".join(parts)

    def _build_health_context_section(self) -> str:
        context = self.get_health_context()
        if not context:
            return ""
        parts: list[str] = ["## Health Context\n"]
        for ctx in context:
            prefix = f"[{ctx['category']}] " if ctx.get("category") else ""
            parts.append(f"- {prefix}{ctx['fact']}")
        parts.append("")
        return "\n".join(parts)

    def _build_workouts_section(self) -> str:
        workouts = self.get_workouts(limit=50)
        if not workouts:
            return ""
        parts: list[str] = [
            "## Recent Workouts\n",
            "| Date | Sport | Duration | Calories | Avg HR | Distance |",
            "|------|-------|----------|----------|--------|----------|",
        ]
        for w in workouts:
            dur = f"{w['duration_minutes']:.0f}m" if w.get("duration_minutes") else "-"
            cal = f"{w['calories_burned']:.0f}" if w.get("calories_burned") else "-"
            hr = f"{w['avg_heart_rate']:.0f}" if w.get("avg_heart_rate") else "-"
            dist = f"{w['distance_km']:.1f}km" if w.get("distance_km") else "-"
            parts.append(
                f"| {w.get('start_date', '')[:10]} "
                f"| {w.get('sport_type', '')} "
                f"| {dur} | {cal} | {hr} | {dist} |"
            )
        parts.append("")
        return "\n".join(parts)

    def _build_genetics_section(self) -> str:
        variants = self.get_genetic_variants(limit=200)
        if not variants:
            return ""
        parts: list[str] = [
            "## Genetic Variants\n",
            "| rsID | Genotype | Phenotype |",
            "|------|----------|-----------|",
        ]
        for v in variants:
            parts.append(
                f"| {v.get('rsid', '')} "
                f"| {v.get('genotype', '')} "
                f"| {v.get('phenotype', '')} |"
            )
        parts.append("")
        return "\n".join(parts)

    def _build_goals_section(self) -> str:
        goals = self.get_health_goals()
        if not goals:
            return ""
        parts: list[str] = ["## Health Goals\n"]
        for g in goals:
            parts.append(f"- {g['goal_text']}")
        parts.append("")
        return "\n".join(parts)

    def _build_med_reminders_section(self) -> str:
        reminders = self.get_med_reminders()
        if not reminders:
            return ""
        parts: list[str] = [
            "## Medication Reminders\n",
            "| Time | Medication | Notes |",
            "|------|------------|-------|",
        ]
        for r in reminders:
            parts.append(
                f"| {r.get('time', '')} "
                f"| {r.get('med_name', '')} "
                f"| {r.get('notes', '')} |"
            )
        parts.append("")
        return "\n".join(parts)

    def _build_providers_section(self) -> str:
        providers = self.get_providers()
        if not providers:
            return ""
        parts: list[str] = ["## Healthcare Providers\n"]
        for p in providers:
            line = f"- **{p.get('specialty', 'Unknown')}**"
            if p.get("notes"):
                line += f" — {p['notes']}"
            parts.append(line)
        parts.append("")
        return "\n".join(parts)

    def _build_appointments_section(self) -> str:
        appts = self.get_appointments(limit=20)
        if not appts:
            return ""
        parts: list[str] = [
            "## Appointments\n",
            "| Date | Status | Reason |",
            "|------|--------|--------|",
        ]
        for a in appts:
            parts.append(
                f"| {a.get('appt_date', '')} "
                f"| {a.get('status', '')} "
                f"| {a.get('reason', '')} |"
            )
        parts.append("")
        return "\n".join(parts)

    def _build_health_records_ext_section(self) -> str:
        try:
            records = self.get_health_records_ext()
        except Exception:
            return ""
        if not records:
            return ""
        by_type: dict[str, list[dict]] = {}
        for r in records:
            by_type.setdefault(r.get("data_type", "other"), []).append(r)
        parts: list[str] = ["## Additional Health Records\n"]
        for dtype in sorted(by_type.keys()):
            parts.append(f"### {dtype.replace('_', ' ').title()}")
            for r in by_type[dtype]:
                line = f"- {r.get('label', '')}"
                if r.get("value"):
                    line += f": {r['value']}"
                if r.get("unit"):
                    line += f" {r['unit']}"
                if r.get("date_effective"):
                    line += f" ({r['date_effective']})"
                parts.append(line)
        parts.append("")
        return "\n".join(parts)

    def _build_analysis_rules_section(self) -> str:
        try:
            rules = self.get_active_analysis_rules()
        except Exception:
            return ""
        if not rules:
            return ""
        parts: list[str] = ["## Active Analysis Rules\n"]
        for r in rules:
            priority = r.get("priority", "medium").upper()
            parts.append(f"- [{priority}] **{r.get('name', '')}** (scope: {r.get('scope', '')})")
            parts.append(f"  {r.get('rule', '')}")
        parts.append("")
        return "\n".join(parts)

    def _build_user_memory_section(self) -> str:
        try:
            memories = self.get_user_memory()
        except Exception:
            return ""  # Table may not exist in old DBs
        if not memories:
            return ""
        parts: list[str] = ["## User Memory\n"]
        by_cat: dict[str, list[dict]] = {}
        for mem in memories:
            by_cat.setdefault(mem.get("category", "general"), []).append(mem)
        for cat in sorted(by_cat.keys()):
            parts.append(f"### {cat.replace('_', ' ').title()}")
            for mem in by_cat[cat]:
                conf = mem.get("confidence", 1.0)
                marker = "" if conf >= 0.9 else f" (~{conf:.0%} confidence)"
                parts.append(f"- {mem['key']}: {mem['value']}{marker}")
        parts.append("")
        return "\n".join(parts)

    # ── Health records ext methods ────────────────────────

    def upsert_health_record_ext(
        self,
        record_id: str,
        *,
        data_type: str,
        label: str,
        value: str = "",
        unit: str = "",
        date_effective: str = "",
        source: str = "",
        details: str = "",
        tags: str = "",
    ) -> None:
        self._validate_text_fields(
            {"label": label, "value": value, "source": source,
             "details": details, "tags": tags},
            f"health_record_ext.{record_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_health_records_ext
               (id, data_type, label, value, unit, date_effective, source,
                details, tags, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (record_id, data_type, label, value, unit, date_effective,
             source, details, tags, self._now()),
        )
        self._auto_commit()

    def get_health_records_ext(
        self, data_type: str | None = None, limit: int = 200,
    ) -> list[dict]:
        if data_type:
            rows = self.conn.execute(
                """SELECT * FROM clean_health_records_ext
                   WHERE data_type = ? ORDER BY date_effective DESC LIMIT ?""",
                (data_type, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT * FROM clean_health_records_ext
                   ORDER BY data_type, date_effective DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Substance knowledge methods ───────────────────

    def upsert_substance_knowledge(
        self,
        substance_id: str,
        *,
        name: str,
        quality_score: float = 0.0,
        mechanism: str = "",
        half_life: str = "",
        cyp_interactions: str = "",
        pathway_effects: str = "",
        aliases: str = "",
        clinical_summary: str = "",
        research_sources: str = "",
    ) -> None:
        self._validate_text_fields(
            {"name": name, "mechanism": mechanism, "half_life": half_life,
             "clinical_summary": clinical_summary},
            f"substance_knowledge.{substance_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_substance_knowledge
               (id, name, quality_score, mechanism, half_life, cyp_interactions,
                pathway_effects, aliases, clinical_summary, research_sources,
                synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (substance_id, name.lower(), quality_score, mechanism, half_life,
             cyp_interactions, pathway_effects, aliases, clinical_summary,
             research_sources, self._now()),
        )
        self._auto_commit()

    def get_substance_knowledge(self, name: str) -> dict | None:
        """Get substance knowledge profile by name."""
        row = self.conn.execute(
            "SELECT * FROM clean_substance_knowledge WHERE name = ?",
            (name.lower(),),
        ).fetchone()
        return dict(row) if row else None

    def get_all_substance_knowledge(self) -> list[dict]:
        """Get all substance knowledge profiles."""
        rows = self.conn.execute(
            "SELECT * FROM clean_substance_knowledge ORDER BY name",
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Analysis rule methods ──────────────────────────

    def upsert_analysis_rule(
        self,
        name: str,
        scope: str,
        rule: str,
        priority: str = "medium",
        active: bool = True,
    ) -> str:
        """PII-validated upsert of an analysis rule. Returns rule ID."""
        self._validate_text_fields(
            {"name": name, "scope": scope, "rule": rule},
            f"analysis_rule.{name}",
        )
        import uuid as _uuid
        now = self._now()
        # Check for existing by name
        row = self.conn.execute(
            "SELECT id FROM clean_analysis_rules WHERE name = ?", (name,),
        ).fetchone()
        rule_id = row["id"] if row else _uuid.uuid4().hex
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_analysis_rules
               (id, name, scope, rule, priority, active, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?,
                COALESCE((SELECT created_at FROM clean_analysis_rules WHERE id = ?), ?),
                ?)""",
            (rule_id, name, scope, rule, priority, int(active),
             rule_id, now, now),
        )
        self._auto_commit()
        return rule_id

    def get_active_analysis_rules(self) -> list[dict]:
        rows = self.conn.execute(
            """SELECT * FROM clean_analysis_rules WHERE active = 1
               ORDER BY CASE priority
                   WHEN 'high' THEN 1
                   WHEN 'medium' THEN 2
                   WHEN 'low' THEN 3
                   ELSE 4
               END, updated_at DESC""",
        ).fetchall()
        return [dict(r) for r in rows]

    def deactivate_analysis_rule(self, name: str) -> bool:
        cursor = self.conn.execute(
            """UPDATE clean_analysis_rules SET active = 0, updated_at = ?
               WHERE name = ? AND active = 1""",
            (self._now(), name),
        )
        self._auto_commit()
        return cursor.rowcount > 0

    # ── User memory methods ──────────────────────────────

    def upsert_user_memory(
        self,
        key: str,
        value: str,
        category: str = "",
        confidence: float = 1.0,
        source: str = "claude_inferred",
    ) -> None:
        """PII-validated upsert of a user memory entry."""
        self._validate_text_fields(
            {"key": key, "value": value, "category": category},
            f"user_memory.{key}",
        )
        now = self._now()
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_user_memory
               (key, value, category, confidence, source, superseded_by,
                created_at, updated_at, synced_at)
               VALUES (?, ?, ?, ?, ?,
                COALESCE((SELECT superseded_by FROM clean_user_memory WHERE key = ?), ''),
                COALESCE((SELECT created_at FROM clean_user_memory WHERE key = ?), ?),
                ?, ?)""",
            (key, value, category, confidence, source, key, key, now, now, now),
        )
        self._auto_commit()

    def get_user_memory(self, category: str | None = None) -> list[dict]:
        """Return active memory entries (where superseded_by is empty)."""
        if category:
            rows = self.conn.execute(
                """SELECT * FROM clean_user_memory
                   WHERE superseded_by = '' AND category = ?
                   ORDER BY updated_at DESC LIMIT 200""",
                (category,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT * FROM clean_user_memory
                   WHERE superseded_by = ''
                   ORDER BY updated_at DESC LIMIT 200""",
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_user_memory(self, key: str) -> bool:
        """Delete a single user memory entry. Returns True if deleted."""
        cursor = self.conn.execute(
            "DELETE FROM clean_user_memory WHERE key = ?", (key,),
        )
        self._auto_commit()
        return cursor.rowcount > 0

    def clear_all_user_memory(self) -> int:
        """Delete all user memory entries. Returns count deleted."""
        cursor = self.conn.execute("DELETE FROM clean_user_memory")
        self._auto_commit()
        return cursor.rowcount

    def mark_memory_superseded(self, old_key: str, new_key: str) -> None:
        """Mark an old memory entry as superseded by a new key."""
        self.conn.execute(
            """UPDATE clean_user_memory SET superseded_by = ?, updated_at = ?
               WHERE key = ? AND superseded_by = ''""",
            (new_key, self._now(), old_key),
        )
        self._auto_commit()

    # ── Exact-fact lookup (patient constants) ────────────

    _FACT_CATEGORIES: frozenset[str] = frozenset({
        "allergy", "medication", "demographic", "baseline_metric",
        "medical_context", "supplement", "preference",
        "lifestyle", "goal",
    })

    def get_facts(self, category: str | None = None) -> dict[str, str]:
        """Return high-confidence user-stated facts as a key→value dict.

        Only returns active (non-superseded), user-stated memories with
        confidence >= 0.9. These are deterministic constants that Claude
        should never contradict.

        Args:
            category: Optional category filter. Must be one of the
                      recognized fact categories if provided.

        Returns:
            Dict mapping fact key to value string.
        """
        if category and category not in self._FACT_CATEGORIES:
            return {}
        if category:
            rows = self.conn.execute(
                """SELECT key, value FROM clean_user_memory
                   WHERE superseded_by = ''
                     AND confidence >= 0.9
                     AND source = 'user_stated'
                     AND category = ?
                   ORDER BY key""",
                (category,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT key, value, category FROM clean_user_memory
                   WHERE superseded_by = ''
                     AND confidence >= 0.9
                     AND source = 'user_stated'
                   ORDER BY category, key""",
            ).fetchall()
        return {r["key"]: r["value"] for r in rows}

    # ── Memory audit log ─────────────────────────────────

    def log_memory_change(
        self,
        key: str,
        old_value: str,
        new_value: str,
        source_type: str = "",
        source_ref: str = "",
    ) -> None:
        """Record a memory write event in the audit log."""
        self.conn.execute(
            """INSERT INTO memory_audit_log
               (key, old_value, new_value, source_type, source_ref, changed_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (key, old_value, new_value, source_type, source_ref, self._now()),
        )
        self._auto_commit()

    def get_memory_audit_log(self, limit: int = 50) -> list[dict]:
        """Return recent memory audit entries, newest first."""
        rows = self.conn.execute(
            """SELECT * FROM memory_audit_log
               ORDER BY changed_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Correction + system improvement methods ────────

    def insert_correction(
        self,
        correction_id: str,
        original_claim: str,
        correction: str,
        source: str = "user",
    ) -> None:
        """PII-validated insert of a correction entry."""
        self._validate_text_fields(
            {"original_claim": original_claim, "correction": correction},
            f"correction.{correction_id}",
        )
        now = self._now()
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_corrections
               (id, original_claim, correction, source, created_at, synced_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (correction_id, original_claim, correction, source, now, now),
        )
        self._auto_commit()

    def insert_system_improvement(
        self,
        area: str = "",
        suggestion: str = "",
        priority: str = "low",
    ) -> str:
        """PII-validated insert of a system improvement suggestion.

        Returns the generated improvement ID.
        """
        self._validate_text_fields(
            {"area": area, "suggestion": suggestion},
            "system_improvement",
        )
        import uuid
        imp_id = uuid.uuid4().hex
        now = self._now()
        self.conn.execute(
            """INSERT INTO clean_system_improvements
               (id, area, suggestion, priority, status, created_at, synced_at)
               VALUES (?, ?, ?, ?, 'open', ?, ?)""",
            (imp_id, area, suggestion, priority, now, now),
        )
        self._auto_commit()
        return imp_id

    def get_corrections(self, limit: int = 50) -> list[dict]:
        """Return recent corrections ordered by creation date."""
        rows = self.conn.execute(
            """SELECT * FROM clean_corrections
               ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_system_improvements(
        self, status: str | None = None, limit: int = 50,
    ) -> list[dict]:
        """Return system improvement suggestions, optionally filtered by status."""
        if status:
            rows = self.conn.execute(
                """SELECT * FROM clean_system_improvements
                   WHERE status = ? ORDER BY created_at DESC LIMIT ?""",
                (status, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT * FROM clean_system_improvements
                   ORDER BY created_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def update_system_improvement_status(
        self, improvement_id: str, status: str,
    ) -> bool:
        """Update the status of a system improvement. Returns True if updated."""
        cursor = self.conn.execute(
            """UPDATE clean_system_improvements SET status = ?, synced_at = ?
               WHERE id = ?""",
            (status, self._now(), improvement_id),
        )
        self._auto_commit()
        return cursor.rowcount > 0

    # Allowlist of valid table and column names for dynamic SQL
    _VALID_TABLES: set[str] = {
        "clean_observations", "clean_medications", "clean_wearable_daily",
        "clean_hypotheses", "clean_health_context",
        "clean_workouts", "clean_genetic_variants", "clean_health_goals",
        "clean_med_reminders", "clean_providers", "clean_appointments",
        "clean_health_records_ext", "clean_anon_cache",
        "clean_system_improvements", "clean_demographics",
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
        """Count rows in a clean DB table."""
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
