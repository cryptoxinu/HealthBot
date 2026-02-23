"""Tests for vault security audit checks."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from healthbot.config import Config
from healthbot.security.audit import AuditFinding, AuditReport, VaultAuditor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_vault(tmp_path: Path) -> Path:
    """Create a minimal vault directory tree and return vault_home."""
    vault = tmp_path / ".healthbot"
    vault.mkdir()
    (vault / "db").mkdir()
    (vault / "vault").mkdir()
    (vault / "logs").mkdir()
    (vault / "exports").mkdir()
    (vault / "config").mkdir()
    return vault


def _make_db_with_encrypted_row(vault: Path) -> None:
    """Create a tiny DB with a properly-encrypted-looking blob (random 64 bytes)."""
    import os
    db_path = vault / "db" / "health.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE IF NOT EXISTS observations "
        "(obs_id TEXT PRIMARY KEY, encrypted_data BLOB NOT NULL)"
    )
    # 64 random bytes — cannot decode as UTF-8 JSON, passes size check.
    conn.execute(
        "INSERT INTO observations VALUES (?, ?)",
        ("obs1", os.urandom(64)),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCleanVault:
    """A vault with no problems should pass all checks."""

    def test_clean_vault_passes(self, tmp_path: Path) -> None:
        vault = _make_vault(tmp_path)
        _make_db_with_encrypted_row(vault)
        # Add a safe log file
        (vault / "logs" / "healthbot.log").write_text("2024-01-01 INFO started\n")
        # Add an encrypted export
        (vault / "exports" / "packet.enc").write_bytes(b"\x00" * 40)
        # Add a .enc blob
        (vault / "vault" / "abc123.enc").write_bytes(b"\x00" * 40)

        cfg = Config(vault_home=vault)
        report = VaultAuditor(cfg).run_all()
        assert report.passed, report.format()
        assert report.fail_count == 0
        assert report.warn_count == 0


class TestPlaintextDetection:
    """Check 1: plaintext health data files."""

    def test_plaintext_pdf_detected(self, tmp_path: Path) -> None:
        vault = _make_vault(tmp_path)
        # Drop a raw PDF outside of encrypted storage.
        (vault / "report.pdf").write_bytes(b"%PDF-1.4 fake pdf content")

        cfg = Config(vault_home=vault)
        report = VaultAuditor(cfg).run_all()
        plaintext_findings = [
            f for f in report.findings if f.check_name == "plaintext_scan"
        ]
        assert any(f.status == "FAIL" for f in plaintext_findings)


class TestDbEncryption:
    """Check 3: DB field encryption."""

    def test_unencrypted_db_field_detected(self, tmp_path: Path) -> None:
        vault = _make_vault(tmp_path)
        db_path = vault / "db" / "health.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS observations "
            "(obs_id TEXT PRIMARY KEY, encrypted_data BLOB NOT NULL)"
        )
        # Store plaintext JSON — should be caught.
        conn.execute(
            "INSERT INTO observations VALUES (?, ?)",
            ("obs_bad", b'{"test_name":"glucose","value":"250"}'),
        )
        conn.commit()
        conn.close()

        cfg = Config(vault_home=vault)
        report = VaultAuditor(cfg).run_all()
        db_findings = [f for f in report.findings if f.check_name == "db_encryption"]
        assert any(f.status == "FAIL" for f in db_findings)


class TestLogScrubbing:
    """Check 2: PHI in log files."""

    def test_phi_in_logs_detected(self, tmp_path: Path) -> None:
        vault = _make_vault(tmp_path)
        # Write a log line containing an SSN.
        (vault / "logs" / "healthbot.log").write_text(
            "2024-01-01 INFO processing SSN: 123-45-6789\n"
        )

        cfg = Config(vault_home=vault)
        report = VaultAuditor(cfg).run_all()
        log_findings = [f for f in report.findings if f.check_name == "log_scrub"]
        assert any(f.status == "FAIL" for f in log_findings)


class TestUnexpectedFiles:
    """Check 5: unexpected file extensions."""

    def test_unexpected_file_warns(self, tmp_path: Path) -> None:
        vault = _make_vault(tmp_path)
        (vault / "notes.txt").write_text("hello")

        cfg = Config(vault_home=vault)
        report = VaultAuditor(cfg).run_all()
        uf_findings = [f for f in report.findings if f.check_name == "unexpected_files"]
        assert any(f.status == "WARN" for f in uf_findings)


class TestExportsDirectory:
    """Check 6: exports must be encrypted."""

    def test_clean_exports_pass(self, tmp_path: Path) -> None:
        vault = _make_vault(tmp_path)
        (vault / "exports" / "packet.enc").write_bytes(b"\x00" * 40)

        cfg = Config(vault_home=vault)
        report = VaultAuditor(cfg).run_all()
        export_findings = [f for f in report.findings if f.check_name == "exports_clean"]
        assert all(f.status == "PASS" for f in export_findings)

    def test_plaintext_in_exports_fails(self, tmp_path: Path) -> None:
        vault = _make_vault(tmp_path)
        (vault / "exports" / "report.pdf").write_bytes(b"%PDF-1.4")

        cfg = Config(vault_home=vault)
        report = VaultAuditor(cfg).run_all()
        export_findings = [f for f in report.findings if f.check_name == "exports_clean"]
        assert any(f.status == "FAIL" for f in export_findings)


class TestAuditReportFormat:
    """Test the human-readable format() output."""

    def test_format_contains_overall(self) -> None:
        report = AuditReport(findings=[
            AuditFinding("test_check", "PASS", "All good"),
        ])
        text = report.format()
        assert "OVERALL: PASS" in text
        assert "[+] test_check: PASS" in text

    def test_format_shows_fail(self) -> None:
        report = AuditReport(findings=[
            AuditFinding("bad_check", "FAIL", "Something wrong", file_path="foo/bar"),
        ])
        text = report.format()
        assert "OVERALL: FAIL" in text
        assert "[X] bad_check: FAIL" in text
        assert "File: foo/bar" in text
