"""SQLite database schema and migrations.

Uses standard sqlite3 with field-level AES-256-GCM encryption.
Sensitive fields are encrypted; metadata columns remain plaintext
for indexing and querying.
"""
from __future__ import annotations

SCHEMA_VERSION = 1

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS vault_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS documents (
    doc_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    received_at TEXT NOT NULL,
    mime_type TEXT DEFAULT '',
    size_bytes INTEGER DEFAULT 0,
    page_count INTEGER DEFAULT 0,
    enc_blob_path TEXT DEFAULT '',
    filename TEXT DEFAULT '',
    meta_encrypted BLOB
);

CREATE TABLE IF NOT EXISTS observations (
    obs_id TEXT PRIMARY KEY,
    record_type TEXT NOT NULL DEFAULT 'lab_result',
    canonical_name TEXT DEFAULT '',
    date_effective TEXT,
    triage_level TEXT DEFAULT 'normal',
    flag TEXT DEFAULT '',
    source_doc_id TEXT DEFAULT '',
    source_page INTEGER DEFAULT 0,
    source_section TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    encrypted_data BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_obs_type ON observations(record_type);
CREATE INDEX IF NOT EXISTS idx_obs_date ON observations(date_effective);
CREATE INDEX IF NOT EXISTS idx_obs_triage ON observations(triage_level);
CREATE INDEX IF NOT EXISTS idx_obs_name ON observations(canonical_name);
CREATE INDEX IF NOT EXISTS idx_obs_type_name_date
  ON observations(record_type, canonical_name, date_effective DESC);

CREATE TABLE IF NOT EXISTS medications (
    med_id TEXT PRIMARY KEY,
    status TEXT DEFAULT 'active',
    start_date TEXT,
    end_date TEXT,
    source_doc_id TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    encrypted_data BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_meds_status ON medications(status);

CREATE TABLE IF NOT EXISTS wearable_daily (
    id TEXT PRIMARY KEY,
    date TEXT NOT NULL,
    provider TEXT NOT NULL DEFAULT 'whoop',
    created_at TEXT NOT NULL,
    encrypted_data BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wearable_date ON wearable_daily(date);

CREATE TABLE IF NOT EXISTS concerns (
    concern_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    severity TEXT DEFAULT 'watch',
    status TEXT DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    encrypted_data BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS external_evidence (
    evidence_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    query_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    encrypted_data BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_documents_sha256 ON documents(sha256);

CREATE INDEX IF NOT EXISTS idx_evidence_hash ON external_evidence(query_hash);

CREATE TABLE IF NOT EXISTS search_index (
    doc_id TEXT PRIMARY KEY,
    record_type TEXT NOT NULL,
    date_effective TEXT,
    text_for_search TEXT
);
"""

MIGRATIONS: dict[int, list[str]] = {
    2: [
        # Short-term conversation memory
        """CREATE TABLE IF NOT EXISTS memory_stm (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            created_at TEXT NOT NULL,
            consolidated INTEGER DEFAULT 0,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_stm_user ON memory_stm(user_id, created_at)",
        # Long-term medical profile facts
        """CREATE TABLE IF NOT EXISTS memory_ltm (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            source TEXT DEFAULT '',
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_ltm_user ON memory_ltm(user_id, category)",
        # Hypotheses being tracked
        """CREATE TABLE IF NOT EXISTS hypotheses (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            status TEXT DEFAULT 'active',
            confidence REAL DEFAULT 0.0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_hyp_user ON hypotheses(user_id, status)",
    ],
    3: [
        """CREATE TABLE IF NOT EXISTS alert_log (
            dedup_key TEXT PRIMARY KEY,
            alert_type TEXT NOT NULL,
            sent_at TEXT NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_alert_sent ON alert_log(sent_at)",
    ],
    4: [
        "ALTER TABLE external_evidence ADD COLUMN expires_at TEXT DEFAULT ''",
    ],
    5: [
        # Multi-user support: add user_id to core tables
        "ALTER TABLE observations ADD COLUMN user_id INTEGER DEFAULT 0",
        "CREATE INDEX IF NOT EXISTS idx_obs_user ON observations(user_id)",
        "ALTER TABLE medications ADD COLUMN user_id INTEGER DEFAULT 0",
        "CREATE INDEX IF NOT EXISTS idx_meds_user ON medications(user_id)",
        "ALTER TABLE wearable_daily ADD COLUMN user_id INTEGER DEFAULT 0",
        "CREATE INDEX IF NOT EXISTS idx_wearable_user ON wearable_daily(user_id)",
        "ALTER TABLE documents ADD COLUMN user_id INTEGER DEFAULT 0",
    ],
    6: [
        "ALTER TABLE documents ADD COLUMN filename TEXT DEFAULT ''",
    ],
    7: [
        "CREATE INDEX IF NOT EXISTS idx_documents_sha256 ON documents(sha256)",
    ],
    8: [
        "ALTER TABLE observations ADD COLUMN age_at_collection INTEGER",
        "ALTER TABLE observations ADD COLUMN last_reanalyzed TEXT DEFAULT ''",
    ],
    9: [
        # Permanent knowledge base — research findings + user corrections
        """CREATE TABLE IF NOT EXISTS knowledge_base (
            id TEXT PRIMARY KEY,
            topic TEXT NOT NULL,
            finding TEXT NOT NULL,
            source TEXT NOT NULL,
            relevance_score REAL DEFAULT 0.5,
            user_confirmed INTEGER DEFAULT 0,
            category TEXT DEFAULT 'research',
            created_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_kb_topic ON knowledge_base(topic)",
        "CREATE INDEX IF NOT EXISTS idx_kb_category ON knowledge_base(category)",
    ],
    10: [
        # Permanent medical journal — health-relevant messages never deleted
        """CREATE TABLE IF NOT EXISTS medical_journal (
            entry_id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            speaker TEXT NOT NULL,
            category TEXT DEFAULT '',
            source TEXT DEFAULT 'conversation',
            created_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_journal_user ON medical_journal(user_id, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_journal_category ON medical_journal(category)",
    ],
    11: [
        "ALTER TABLE documents ADD COLUMN last_rescanned TEXT DEFAULT ''",
    ],
    12: [
        # Medication reminders
        """CREATE TABLE IF NOT EXISTS med_reminders (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            time TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_reminder_user ON med_reminders(user_id)",
    ],
    13: [
        # Health goals (Phase S3)
        """CREATE TABLE IF NOT EXISTS health_goals (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_goals_user ON health_goals(user_id)",
    ],
    14: [
        # Providers and appointments (Phase T1)
        """CREATE TABLE IF NOT EXISTS providers (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_provider_user ON providers(user_id)",
        """CREATE TABLE IF NOT EXISTS appointments (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            provider_id TEXT NOT NULL,
            appt_date TEXT NOT NULL,
            status TEXT DEFAULT 'scheduled',
            prep_sent INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_appt_user ON appointments(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_appt_date ON appointments(appt_date)",
        "CREATE INDEX IF NOT EXISTS idx_appt_provider ON appointments(provider_id)",
    ],
    15: [
        # Phase Q: de-identification tracking
        "ALTER TABLE observations ADD COLUMN deidentified INTEGER DEFAULT 0",
        "ALTER TABLE medications ADD COLUMN deidentified INTEGER DEFAULT 0",
    ],
    16: [
        # Genetic variants (TellMeGen, 23andMe, etc.)
        """CREATE TABLE IF NOT EXISTS genetic_variants (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            rsid TEXT NOT NULL,
            chromosome TEXT DEFAULT '',
            position INTEGER DEFAULT 0,
            source TEXT DEFAULT 'tellmegen',
            created_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_genetic_user ON genetic_variants(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_genetic_rsid ON genetic_variants(rsid)",
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_genetic_user_rsid
           ON genetic_variants(user_id, rsid)""",
    ],
    17: [
        # Workouts from Apple Health, WHOOP, etc.
        """CREATE TABLE IF NOT EXISTS workouts (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            sport_type TEXT NOT NULL,
            start_date TEXT NOT NULL,
            source TEXT DEFAULT 'apple_health',
            created_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_workouts_user ON workouts(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_workouts_date ON workouts(start_date)",
        "CREATE INDEX IF NOT EXISTS idx_workouts_sport ON workouts(sport_type)",
    ],
    18: [
        # Encrypted identity profile for smarter PII detection.
        # RAW VAULT ONLY — never synced to clean DB, never in AI export.
        """CREATE TABLE IF NOT EXISTS user_identity (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            field_key TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_identity_user ON user_identity(user_id)",
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_identity_user_key
           ON user_identity(user_id, field_key)""",
    ],
    19: [
        # Corrected lab report tracking — stores when an observation was
        # updated by a corrected/resent lab report.
        "ALTER TABLE observations ADD COLUMN corrected_at TEXT DEFAULT ''",
    ],
    20: [
        # Redaction diff log — stores what PII was stripped from outbound data.
        # Only the diff (category + position), NOT the original text.
        # Encrypted in Tier 1 for audit trail.
        """CREATE TABLE IF NOT EXISTS redaction_log (
            id TEXT PRIMARY KEY,
            destination TEXT NOT NULL,
            redaction_count INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_redaction_dest ON redaction_log(destination)",
        "CREATE INDEX IF NOT EXISTS idx_redaction_ts ON redaction_log(timestamp)",
    ],
    21: [
        # Trend cache — caches computed trends per test to avoid recomputation
        """CREATE TABLE IF NOT EXISTS trend_cache (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            canonical_name TEXT NOT NULL,
            slope REAL,
            r_squared REAL,
            direction TEXT,
            pct_change REAL,
            data_points INTEGER,
            first_date TEXT,
            last_date TEXT,
            first_value REAL,
            last_value REAL,
            computed_at TEXT NOT NULL,
            encrypted_data BLOB NOT NULL
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_trend_cache_user_test
           ON trend_cache(user_id, canonical_name)""",
    ],
    22: [
        # Deduplicate wearable rows: keep earliest per date+provider, drop rest
        """DELETE FROM wearable_daily WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM wearable_daily GROUP BY date, provider
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_wearable_date_provider
           ON wearable_daily(date, provider)""",
    ],
    23: [
        # Lab source brand (LabCorp, Quest, etc.) — medical metadata, not PII.
        # Stored as plaintext for querying + lab-specific standardization.
        "ALTER TABLE observations ADD COLUMN source_lab TEXT DEFAULT ''",
    ],
    24: [
        # Extensible health records — stores data types without dedicated tables
        # (allergies, imaging, procedures, immunizations, psych notes, etc.)
        """CREATE TABLE IF NOT EXISTS health_records_ext (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            data_type TEXT NOT NULL,
            label TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            encrypted_data BLOB
        )""",
        """CREATE INDEX IF NOT EXISTS idx_health_ext_user_type
           ON health_records_ext(user_id, data_type)""",
        # Document routing status — tracks documents pending Claude CLI processing
        "ALTER TABLE documents ADD COLUMN routing_status TEXT DEFAULT 'done'",
        "ALTER TABLE documents ADD COLUMN routing_error TEXT DEFAULT ''",
    ],
    25: [
        # Substance knowledge profiles — structured research data per substance
        # (mechanism, CYP-450, pathways, dosing, side effects, etc.)
        """CREATE TABLE IF NOT EXISTS substance_knowledge (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL DEFAULT 0,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            quality_score REAL DEFAULT 0.0,
            encrypted_data BLOB NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_subknow_name ON substance_knowledge(name)",
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_subknow_user_name
           ON substance_knowledge(user_id, name)""",
    ],
}
