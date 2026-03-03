"""Tests for schema evolution pipeline (Part B)."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from healthbot.research.schema_evolution_prompt import build_evolution_prompt


class TestBuildEvolutionPrompt:
    """Test the prompt builder for schema evolution."""

    def test_includes_data_type(self):
        prompt = build_evolution_prompt(
            data_type="imaging_report",
            fields=[{"name": "modality", "type": "TEXT"}],
            reason="Need dedicated table",
        )
        assert "imaging_report" in prompt

    def test_includes_fields(self):
        prompt = build_evolution_prompt(
            data_type="imaging_report",
            fields=[
                {"name": "modality", "type": "TEXT", "index": True},
                {"name": "findings", "type": "TEXT", "pii_check": True},
            ],
            reason="Need dedicated table",
        )
        assert "modality" in prompt
        assert "findings" in prompt

    def test_includes_reason(self):
        prompt = build_evolution_prompt(
            data_type="imaging_report",
            fields=[{"name": "modality", "type": "TEXT"}],
            reason="Multiple imaging reports need queryable storage",
        )
        assert "Multiple imaging reports need queryable storage" in prompt

    def test_includes_sample_data(self):
        prompt = build_evolution_prompt(
            data_type="imaging_report",
            fields=[{"name": "modality", "type": "TEXT"}],
            reason="test",
            sample_data={"modality": "MRI", "body_part": "spine"},
        )
        assert "MRI" in prompt
        assert "spine" in prompt

    def test_no_sample_data(self):
        prompt = build_evolution_prompt(
            data_type="imaging_report",
            fields=[{"name": "modality", "type": "TEXT"}],
            reason="test",
            sample_data=None,
        )
        assert "None provided" in prompt

    def test_includes_pattern_references(self):
        prompt = build_evolution_prompt(
            data_type="test_type",
            fields=[{"name": "col", "type": "TEXT"}],
            reason="test",
        )
        assert "schema.py" in prompt
        assert "clean_sync_workers_ext.py" in prompt
        assert "INSERT OR REPLACE" in prompt


class TestSchemaEvolutionResult:
    """Test SchemaEvolutionResult dataclass."""

    def test_success_result(self):
        from healthbot.research.claude_cli_client import SchemaEvolutionResult

        result = SchemaEvolutionResult(
            success=True,
            data_type="imaging_report",
            files_modified=["data/schema.py", "data/db/imaging_report.py"],
            ddl_executed=["CREATE TABLE imaging_report (...)"],
            migration_version=15,
            summary="Created imaging_report table",
        )
        assert result.success is True
        assert len(result.files_modified) == 2
        assert result.error == ""

    def test_failure_result(self):
        from healthbot.research.claude_cli_client import SchemaEvolutionResult

        result = SchemaEvolutionResult(
            success=False,
            data_type="imaging_report",
            error="CLI not found",
        )
        assert result.success is False
        assert result.error == "CLI not found"
        assert result.files_modified == []


class TestParseEvolutionResult:
    """Test parsing Claude's output into SchemaEvolutionResult."""

    def test_parses_file_paths(self):
        from healthbot.research.claude_cli_client import ClaudeCLIResearchClient

        response = (
            "I created data/db/imaging_report.py and modified data/schema.py.\n"
            "Also edited data/clean_db/imaging_report.py for the clean mixin.\n"
            "```sql\nCREATE TABLE imaging_report (id TEXT PRIMARY KEY)\n```\n"
            "Migration version = 15\n"
            "Success — all done."
        )
        result = ClaudeCLIResearchClient._parse_evolution_result(
            "imaging_report", response,
        )
        assert result.success is True
        assert "data/db/imaging_report.py" in result.files_modified
        assert "data/schema.py" in result.files_modified
        assert result.migration_version == 15
        assert len(result.ddl_executed) == 1

    def test_empty_response(self):
        from healthbot.research.claude_cli_client import ClaudeCLIResearchClient

        result = ClaudeCLIResearchClient._parse_evolution_result(
            "test_type", "",
        )
        assert result.success is False


class TestSchemaEvolutionLog:
    """Test schema evolution audit log in Clean DB."""

    def setup_method(self):
        """Create a temporary clean DB for testing."""
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db_path = Path(self.tmp.name)

    def teardown_method(self):
        try:
            self.db_path.unlink()
        except OSError:
            pass

    def _make_db(self):
        """Create a minimal clean DB with schema evolution table."""
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""
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
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_schema_evo_date
            ON schema_evolution_log(created_at DESC)
        """)
        conn.commit()
        return conn

    def test_log_and_retrieve(self):
        import json

        conn = self._make_db()
        # Insert a log entry
        conn.execute(
            """INSERT INTO schema_evolution_log
               (id, data_type, reason, changes_summary, files_modified,
                ddl_executed, migration_version, status, error_message, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "test123", "imaging_report", "Complex imaging data",
                "Created table + mixin",
                json.dumps(["data/schema.py", "data/db/imaging.py"]),
                json.dumps(["CREATE TABLE imaging_report (...)"]),
                15, "success", "", "2026-03-03T12:00:00",
            ),
        )
        conn.commit()

        # Retrieve
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM schema_evolution_log ORDER BY created_at DESC LIMIT 10"
        ).fetchall()
        assert len(rows) == 1
        d = dict(rows[0])
        assert d["data_type"] == "imaging_report"
        assert d["status"] == "success"
        files = json.loads(d["files_modified"])
        assert "data/schema.py" in files
        conn.close()


class TestBlockPatternSchemaEvolve:
    """Test SCHEMA_EVOLVE in block pattern regex."""

    def test_pattern_matches_schema_evolve(self):
        from healthbot.llm.claude_conversation import _BLOCK_PATTERN
        text = 'SCHEMA_EVOLVE: {"data_type": "imaging", "fields": [], "reason": "test"}'
        match = _BLOCK_PATTERN.search(text)
        assert match is not None
        assert match.group(1) == "SCHEMA_EVOLVE"
