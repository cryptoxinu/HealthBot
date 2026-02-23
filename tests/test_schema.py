"""Tests for database schema and migrations."""
from __future__ import annotations

import sqlite3

from healthbot.data.schema import CREATE_TABLES, MIGRATIONS, SCHEMA_VERSION


class TestSchema:
    def test_schema_version_is_positive(self):
        assert SCHEMA_VERSION >= 1

    def test_create_tables_sql_valid(self):
        """CREATE_TABLES SQL should execute without error."""
        conn = sqlite3.connect(":memory:")
        conn.executescript(CREATE_TABLES)
        # Check core tables exist
        tables = {
            row[0] for row in
            conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "vault_meta" in tables
        assert "documents" in tables
        assert "observations" in tables
        assert "medications" in tables
        assert "wearable_daily" in tables
        assert "concerns" in tables
        assert "external_evidence" in tables
        assert "search_index" in tables
        conn.close()

    def test_observations_indexes(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(CREATE_TABLES)
        indexes = {
            row[0] for row in
            conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        }
        assert "idx_obs_type" in indexes
        assert "idx_obs_date" in indexes
        assert "idx_obs_triage" in indexes
        assert "idx_obs_name" in indexes
        conn.close()

    def test_migrations_dict_structure(self):
        assert isinstance(MIGRATIONS, dict)
        for version, sqls in MIGRATIONS.items():
            assert isinstance(version, int)
            assert isinstance(sqls, list)
            assert all(isinstance(s, str) for s in sqls)

    def test_migration_versions_ascending(self):
        versions = sorted(MIGRATIONS.keys())
        assert versions == list(range(versions[0], versions[-1] + 1))

    def test_migration_v2_creates_memory_tables(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(CREATE_TABLES)
        for sql in MIGRATIONS[2]:
            conn.execute(sql)
        tables = {
            row[0] for row in
            conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "memory_stm" in tables
        assert "memory_ltm" in tables
        assert "hypotheses" in tables
        conn.close()

    def test_migration_v3_creates_alert_log(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(CREATE_TABLES)
        for v in [2, 3]:
            for sql in MIGRATIONS[v]:
                conn.execute(sql)
        tables = {
            row[0] for row in
            conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "alert_log" in tables
        conn.close()

    def test_migration_v5_adds_user_id_columns(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(CREATE_TABLES)
        for v in sorted(MIGRATIONS.keys()):
            for sql in MIGRATIONS[v]:
                try:
                    conn.execute(sql)
                except sqlite3.OperationalError as e:
                    if "duplicate column" in str(e).lower():
                        continue
                    raise
        # Check user_id column exists on observations
        cols = [
            row[1] for row in conn.execute("PRAGMA table_info(observations)").fetchall()
        ]
        assert "user_id" in cols
        conn.close()

    def test_idempotent_create(self):
        """Running CREATE_TABLES twice should not error (IF NOT EXISTS)."""
        conn = sqlite3.connect(":memory:")
        conn.executescript(CREATE_TABLES)
        conn.executescript(CREATE_TABLES)
        conn.close()
