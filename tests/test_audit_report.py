"""Tests for audit report dataclasses and constants."""
from __future__ import annotations

from healthbot.security.audit_report import (
    ALLOWED_EXTENSIONS,
    ENCRYPTED_COLUMN,
    ENCRYPTED_TABLES,
    PLAINTEXT_PATTERNS,
    SAFE_PLAINTEXT,
    AuditFinding,
    AuditReport,
)


class TestAuditFinding:
    def test_basic_fields(self):
        f = AuditFinding(check_name="test", status="PASS", details="ok")
        assert f.check_name == "test"
        assert f.status == "PASS"
        assert f.file_path is None

    def test_with_file_path(self):
        f = AuditFinding(
            check_name="scan", status="FAIL",
            details="bad", file_path="vault/test.pdf",
        )
        assert f.file_path == "vault/test.pdf"


class TestAuditReport:
    def test_empty_report_passes(self):
        r = AuditReport()
        assert r.passed is True
        assert r.fail_count == 0
        assert r.warn_count == 0

    def test_pass_findings_still_pass(self):
        r = AuditReport()
        r.findings.append(AuditFinding("a", "PASS", "ok"))
        r.findings.append(AuditFinding("b", "PASS", "ok"))
        assert r.passed is True
        assert r.fail_count == 0

    def test_fail_finding_fails_report(self):
        r = AuditReport()
        r.findings.append(AuditFinding("a", "PASS", "ok"))
        r.findings.append(AuditFinding("b", "FAIL", "bad"))
        assert r.passed is False
        assert r.fail_count == 1

    def test_warn_counts(self):
        r = AuditReport()
        r.findings.append(AuditFinding("a", "WARN", "hmm"))
        r.findings.append(AuditFinding("b", "WARN", "hmm"))
        assert r.warn_count == 2
        assert r.passed is False  # WARNs also cause passed=False (only PASS passes)

    def test_format_output(self):
        r = AuditReport()
        r.findings.append(AuditFinding("scan", "PASS", "All clean"))
        r.findings.append(AuditFinding("check", "FAIL", "Problem", "bad.txt"))
        text = r.format()
        assert "PASS" in text
        assert "FAIL" in text
        assert "bad.txt" in text

    def test_format_empty_report(self):
        r = AuditReport()
        text = r.format()
        assert isinstance(text, str)


class TestConstants:
    def test_safe_plaintext_is_set(self):
        assert isinstance(SAFE_PLAINTEXT, (set, frozenset))
        assert "app.json" in SAFE_PLAINTEXT

    def test_allowed_extensions_is_set(self):
        assert isinstance(ALLOWED_EXTENSIONS, (set, frozenset))
        assert ".enc" in ALLOWED_EXTENSIONS
        assert ".db" in ALLOWED_EXTENSIONS

    def test_encrypted_tables_is_list(self):
        assert isinstance(ENCRYPTED_TABLES, (list, tuple))
        assert "observations" in ENCRYPTED_TABLES

    def test_encrypted_column_is_dict(self):
        assert isinstance(ENCRYPTED_COLUMN, dict)

    def test_plaintext_patterns_are_compiled_regex(self):
        assert len(PLAINTEXT_PATTERNS) > 0
        for p in PLAINTEXT_PATTERNS:
            assert hasattr(p, "search")
