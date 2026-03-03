"""Security tests for schema evolution pipeline hardening.

Tests PHI gates on inputs/outputs, audit log validation, filesystem
containment, DDL validation, and generated code invariant checks.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from healthbot.research.schema_evolution_validator import (
    validate_ddl,
    validate_generated_files,
)

# ── Helpers ─────────────────────────────────────────────


def _make_firewall(phi_texts: set[str] | None = None):
    """Create a mock PhiFirewall that detects specified texts as PHI."""
    fw = MagicMock()
    phi_texts = phi_texts or set()

    def contains_phi(text):
        return any(t in text for t in phi_texts)

    def redact(text):
        result = text
        for t in phi_texts:
            result = result.replace(t, "[REDACTED]")
        return result

    fw.contains_phi = MagicMock(side_effect=contains_phi)
    fw.redact = MagicMock(side_effect=redact)
    return fw


def _make_config():
    cfg = MagicMock()
    cfg.claude_cli_path = None
    cfg.claude_cli_timeout = 60
    return cfg


def _make_client(firewall=None, cli_path="/usr/bin/claude"):
    """Create a ClaudeCLIResearchClient with mocked dependencies."""
    from healthbot.research.claude_cli_client import ClaudeCLIResearchClient

    fw = firewall or _make_firewall()
    cfg = _make_config()

    with patch.object(ClaudeCLIResearchClient, "_load_api_key", return_value=None):
        with patch(
            "healthbot.research.claude_cli_client.resolve_cli",
            return_value=Path(cli_path),
        ):
            client = ClaudeCLIResearchClient(cfg, fw)
    return client


# ── Test PHI gates on evolution inputs ──────────────────


class TestEvolutionPHIGates:
    """Test PHI detection and blocking on evolve_schema() inputs."""

    def test_blocks_phi_in_data_type(self):
        """SSN in data_type → success=False, evolution blocked."""
        fw = _make_firewall({"123-45-6789"})
        client = _make_client(firewall=fw)

        result = client.evolve_schema(
            data_type="patient_123-45-6789",
            fields=[{"name": "col", "type": "TEXT"}],
            reason="test",
        )

        assert result.success is False
        assert "PHI" in result.error

    @patch("subprocess.run")
    def test_redacts_phi_in_reason(self, mock_run):
        """Name in reason → prompt has it redacted."""
        fw = _make_firewall({"John Smith"})
        client = _make_client(firewall=fw)

        # Mock successful CLI response
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Created table test_type. Success.",
            stderr="",
        )

        with patch(
            "healthbot.research.claude_cli_client._CLAUDE_SEMAPHORE"
        ) as mock_sem:
            mock_sem.acquire.return_value = True
            client.evolve_schema(
                data_type="test_type",
                fields=[{"name": "col", "type": "TEXT"}],
                reason="Requested by John Smith for tracking",
            )

        # The prompt passed to subprocess should have the name redacted
        call_kwargs = mock_run.call_args
        prompt_input = call_kwargs.kwargs.get("input") or call_kwargs[1].get("input", "")
        if not prompt_input and call_kwargs.args:
            # Try positional
            pass
        # Check that John Smith was redacted from the prompt
        assert "John Smith" not in prompt_input

    @patch("subprocess.run")
    def test_drops_phi_sample_data(self, mock_run):
        """Email in sample_data → sample becomes None in prompt."""
        fw = _make_firewall({"test@example.com"})
        client = _make_client(firewall=fw)

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Created table test_type. Success.",
            stderr="",
        )

        with patch(
            "healthbot.research.claude_cli_client._CLAUDE_SEMAPHORE"
        ) as mock_sem:
            mock_sem.acquire.return_value = True
            client.evolve_schema(
                data_type="test_type",
                fields=[{"name": "col", "type": "TEXT"}],
                reason="test",
                sample_data={"email": "test@example.com", "value": 42},
            )

        # The prompt should contain "None provided" instead of the email
        call_kwargs = mock_run.call_args
        prompt_input = call_kwargs.kwargs.get("input") or call_kwargs[1].get("input", "")
        assert "test@example.com" not in prompt_input
        assert "None provided" in prompt_input

    @patch("subprocess.run")
    def test_redacts_phi_in_response(self, mock_run):
        """SSN in CLI stdout → parsed result has [REDACTED]."""
        fw = _make_firewall({"999-88-7777"})
        client = _make_client(firewall=fw)

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Created table. Patient SSN 999-88-7777 found. Success.",
            stderr="",
        )

        with patch(
            "healthbot.research.claude_cli_client._CLAUDE_SEMAPHORE"
        ) as mock_sem:
            mock_sem.acquire.return_value = True
            result = client.evolve_schema(
                data_type="test_type",
                fields=[{"name": "col", "type": "TEXT"}],
                reason="test",
            )

        # SSN should have been redacted in the summary
        assert "999-88-7777" not in result.summary

    @patch("subprocess.run")
    def test_redacts_phi_in_field_descriptions(self, mock_run):
        """Phone in field description → redacted before prompt."""
        fw = _make_firewall({"555-123-4567"})
        client = _make_client(firewall=fw)

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Created table. Success.",
            stderr="",
        )

        fields = [
            {"name": "phone", "type": "TEXT", "description": "Contact: 555-123-4567"},
        ]

        with patch(
            "healthbot.research.claude_cli_client._CLAUDE_SEMAPHORE"
        ) as mock_sem:
            mock_sem.acquire.return_value = True
            client.evolve_schema(
                data_type="test_type",
                fields=fields,
                reason="test",
            )

        # The field description should have been redacted
        assert "555-123-4567" not in fields[0]["description"]
        assert "[REDACTED]" in fields[0]["description"]


# ── Test audit log PHI validation ───────────────────────


class TestAuditLogPHIValidation:
    """Test that log_schema_evolution() validates PII."""

    def setup_method(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db_path = Path(self.tmp.name)

    def teardown_method(self):
        try:
            self.db_path.unlink()
        except OSError:
            pass

    def _make_clean_db(self, phi_texts=None):
        """Create a minimal CleanDB-like object with _validate_text_fields."""
        from healthbot.data.clean_db.memory import MemoryMixin

        # Create a mock that has the MemoryMixin methods + required infra
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
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
        conn.commit()

        from healthbot.security.phi_firewall import PhiFirewall
        fw = PhiFirewall()

        # Create a mock that behaves like CleanDB
        db = MagicMock()
        db.conn = conn
        db._now = MagicMock(return_value="2026-03-03T12:00:00")
        db._auto_commit = MagicMock(side_effect=lambda: conn.commit())
        db._phi_firewall = fw

        # Wire in _validate_text_fields and _assert_no_phi
        def _assert_no_phi(text, context):
            if fw.contains_phi(text):
                from healthbot.data.clean_db.db_core import PhiDetectedError
                raise PhiDetectedError(f"PII detected in {context}: blocked")

        def _validate_text_fields(fields, context):
            for name, value in fields.items():
                if value:
                    _assert_no_phi(value, f"{context}.{name}")

        db._assert_no_phi = _assert_no_phi
        db._validate_text_fields = _validate_text_fields

        # Bind log_schema_evolution from MemoryMixin
        import types
        db.log_schema_evolution = types.MethodType(MemoryMixin.log_schema_evolution, db)

        return db

    def test_log_schema_evolution_validates_phi(self):
        """SSN in reason → PhiDetectedError raised."""
        db = self._make_clean_db()

        from healthbot.data.clean_db.db_core import PhiDetectedError

        with pytest.raises(PhiDetectedError):
            db.log_schema_evolution(
                data_type="test_type",
                reason="Patient SSN: 123-45-6789",
                changes_summary="Created table",
                files_modified=[],
                ddl_executed=[],
                migration_version=None,
                status="success",
            )

    def test_log_schema_evolution_clean_data_passes(self):
        """Clean text → INSERT succeeds."""
        db = self._make_clean_db()

        evo_id = db.log_schema_evolution(
            data_type="imaging_report",
            reason="Complex imaging data needs dedicated table",
            changes_summary="Created imaging_report table with 5 columns",
            files_modified=["data/schema.py"],
            ddl_executed=["CREATE TABLE imaging_report (...)"],
            migration_version=15,
            status="success",
        )

        assert evo_id  # Returns a non-empty ID
        # Verify it was actually written
        row = db.conn.execute(
            "SELECT * FROM schema_evolution_log WHERE id = ?", (evo_id,)
        ).fetchone()
        assert row is not None


# ── Test filesystem containment ─────────────────────────


class TestEvolutionContainment:
    """Test that evolution subprocess is properly sandboxed."""

    def test_evolution_tool_flags_no_bash(self):
        """_EVOLUTION_TOOL_FLAGS must NOT contain Bash."""
        from healthbot.llm.claude_client import _EVOLUTION_TOOL_FLAGS

        flags_str = " ".join(_EVOLUTION_TOOL_FLAGS)
        assert "Bash" not in flags_str

    @patch("subprocess.run")
    def test_deny_dir_in_subprocess_args(self, mock_run):
        """Verify --deny-dir ~/.healthbot in subprocess args."""
        fw = _make_firewall()
        client = _make_client(firewall=fw)

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Success.",
            stderr="",
        )

        with patch(
            "healthbot.research.claude_cli_client._CLAUDE_SEMAPHORE"
        ) as mock_sem:
            mock_sem.acquire.return_value = True
            client.evolve_schema(
                data_type="test_type",
                fields=[{"name": "col", "type": "TEXT"}],
                reason="test",
            )

        # Check subprocess.run was called with --deny-dir
        call_args = mock_run.call_args
        cmd_list = call_args[0][0] if call_args[0] else call_args.kwargs.get("args", [])

        assert "--deny-dir" in cmd_list
        deny_idx = cmd_list.index("--deny-dir")
        vault_dir = cmd_list[deny_idx + 1]
        assert ".healthbot" in vault_dir


# ── Test DDL validation ─────────────────────────────────


class TestDDLValidation:
    """Test DDL validation blocks destructive statements."""

    def test_blocks_drop_table(self):
        errors = validate_ddl(["DROP TABLE observations"])
        assert any("DROP TABLE" in e for e in errors)

    def test_blocks_delete_from(self):
        errors = validate_ddl(["DELETE FROM observations WHERE id = 1"])
        assert any("DELETE FROM" in e for e in errors)

    def test_requires_if_not_exists(self):
        errors = validate_ddl(["CREATE TABLE foo (id TEXT PRIMARY KEY)"])
        assert any("IF NOT EXISTS" in e for e in errors)

    def test_allows_clean_migration(self):
        errors = validate_ddl([
            "CREATE TABLE IF NOT EXISTS imaging_report (id TEXT PRIMARY KEY, data TEXT)",
            "CREATE INDEX IF NOT EXISTS idx_imaging_date ON imaging_report(created_at DESC)",
        ])
        assert errors == []


# ── Test generated code validation ──────────────────────


class TestGeneratedCodeValidation:
    """Test static analysis of generated code files."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()

    def teardown_method(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_file(self, rel_path: str, content: str) -> str:
        full = Path(self.tmpdir) / rel_path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content, encoding="utf-8")
        return rel_path

    def test_flags_raw_vault_mixin_without_encrypt(self):
        """Raw vault mixin with INSERT but no _encrypt() → error."""
        code = (
            "class ImagingMixin:\n"
            "    def insert(self):\n"
            '        self.conn.execute("INSERT INTO foo VALUES (?)", (data,))\n'
        )
        rel = self._write_file("src/healthbot/data/db/imaging_report.py", code)
        errors = validate_generated_files([rel], self.tmpdir)
        assert any("_encrypt()" in e for e in errors)

    def test_flags_clean_db_mixin_without_validate(self):
        """Clean DB mixin with INSERT but no _validate_text_fields() → error."""
        code = (
            "class CleanImagingMixin:\n"
            "    def upsert(self):\n"
            '        self.conn.execute('
            '"INSERT OR REPLACE INTO clean_imaging VALUES (?)", (data,))\n'
        )
        rel = self._write_file(
            "src/healthbot/data/clean_db/imaging_report.py", code,
        )
        errors = validate_generated_files([rel], self.tmpdir)
        assert any("_validate_text_fields()" in e for e in errors)

    def test_flags_sync_worker_without_anonymize(self):
        """Sync worker with upsert_ call but no anonymize() → error."""
        code = (
            "def sync_imaging(db, clean_db):\n"
            "    clean_db.upsert_imaging(record_id, data)\n"
        )
        rel = self._write_file(
            "src/healthbot/data/clean_sync_imaging.py", code,
        )
        errors = validate_generated_files([rel], self.tmpdir)
        assert any("anonymize()" in e for e in errors)

    def test_passes_valid_generated_code(self):
        """Files with all required patterns → no errors."""
        vault_code = (
            "class ImagingMixin:\n"
            "    def insert(self):\n"
            "        ct = self._encrypt(data, aad)\n"
            '        self.conn.execute("INSERT INTO foo VALUES (?)", (ct,))\n'
        )
        self._write_file("src/healthbot/data/db/imaging_report.py", vault_code)

        clean_code = (
            "class CleanImagingMixin:\n"
            "    def upsert(self):\n"
            '        self._validate_text_fields({"col": val}, "ctx")\n'
            '        self.conn.execute('
            '"INSERT OR REPLACE INTO clean_imaging VALUES (?)", (val,))\n'
        )
        self._write_file(
            "src/healthbot/data/clean_db/imaging_report.py", clean_code,
        )

        sync_code = (
            "def sync_imaging(db, clean_db, anonymizer):\n"
            "    text = anonymize(raw_text)\n"
            "    clean_db.upsert_imaging(record_id, text)\n"
        )
        self._write_file("src/healthbot/data/clean_sync_imaging.py", sync_code)

        errors = validate_generated_files(
            [
                "src/healthbot/data/db/imaging_report.py",
                "src/healthbot/data/clean_db/imaging_report.py",
                "src/healthbot/data/clean_sync_imaging.py",
            ],
            self.tmpdir,
        )
        assert errors == []
