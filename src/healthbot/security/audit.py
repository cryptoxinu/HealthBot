"""Vault security audit.

Scans vault directory for plaintext PHI, verifies DB field encryption,
checks log scrubbing, detects unexpected files.
All checks are deterministic. Does NOT require vault unlock.
"""
from __future__ import annotations

import sqlite3

from healthbot.config import Config
from healthbot.security.audit_report import (
    ALLOWED_EXTENSIONS,
    ENCRYPTED_COLUMN,
    ENCRYPTED_TABLES,
    PLAINTEXT_PATTERNS,
    SAFE_PLAINTEXT,
    SAFE_PLAINTEXT_PREFIX,
    AuditFinding,
    AuditReport,
)
from healthbot.security.phi_firewall import PhiFirewall

# Re-export for external consumers
__all__ = ["AuditFinding", "AuditReport", "VaultAuditor"]


class VaultAuditor:
    """Runs deterministic security checks against the vault directory."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._firewall = PhiFirewall()

    def run_all(self) -> AuditReport:
        """Execute every audit check and return the report."""
        report = AuditReport()
        report.findings.extend(self._check_vault_plaintext_files())
        report.findings.extend(self._check_log_scrubbing())
        report.findings.extend(self._check_db_encryption())
        report.findings.extend(self._check_blob_format())
        report.findings.extend(self._check_unexpected_files())
        report.findings.extend(self._check_exports_directory())
        return report

    # ------------------------------------------------------------------
    # Check 1: Plaintext health data in vault dir
    # ------------------------------------------------------------------

    def _check_vault_plaintext_files(self) -> list[AuditFinding]:
        """Walk vault dir; flag non-encrypted files containing health data."""
        findings: list[AuditFinding] = []
        vault_dir = self._config.vault_home

        if not vault_dir.exists():
            findings.append(AuditFinding(
                check_name="plaintext_scan",
                status="PASS",
                details="Vault directory does not exist (nothing to scan)",
            ))
            return findings

        problem_found = False
        for path in vault_dir.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix in {".enc", ".db", ".db-wal", ".db-shm"}:
                continue
            if path.name in SAFE_PLAINTEXT or path.name.startswith(SAFE_PLAINTEXT_PREFIX):
                continue

            try:
                chunk = path.read_bytes()[:8192]
            except OSError:
                continue

            if chunk[:4] == b"%PDF":
                findings.append(AuditFinding(
                    check_name="plaintext_scan",
                    status="FAIL",
                    details="Raw PDF found outside encrypted blob storage",
                    file_path=str(path.relative_to(vault_dir)),
                ))
                problem_found = True
                continue

            try:
                text = chunk.decode("utf-8", errors="ignore")
            except Exception:
                continue

            for pattern in PLAINTEXT_PATTERNS:
                if pattern.search(text):
                    findings.append(AuditFinding(
                        check_name="plaintext_scan",
                        status="FAIL",
                        details="Plaintext health data structure detected",
                        file_path=str(path.relative_to(vault_dir)),
                    ))
                    problem_found = True
                    break

        if not problem_found:
            findings.append(AuditFinding(
                check_name="plaintext_scan",
                status="PASS",
                details="No plaintext health data found in vault directory",
            ))
        return findings

    # ------------------------------------------------------------------
    # Check 2: PHI in log files
    # ------------------------------------------------------------------

    def _check_log_scrubbing(self) -> list[AuditFinding]:
        """Scan healthbot.log* files for PHI using PhiFirewall."""
        findings: list[AuditFinding] = []
        log_dir = self._config.log_dir

        if not log_dir.exists():
            findings.append(AuditFinding(
                check_name="log_scrub",
                status="PASS",
                details="Log directory does not exist (nothing to scan)",
            ))
            return findings

        problem_found = False
        for path in log_dir.iterdir():
            if not path.is_file():
                continue
            if not path.name.startswith("healthbot.log"):
                continue
            try:
                content = path.read_text(errors="replace")
            except OSError:
                continue
            if self._firewall.contains_phi(content):
                findings.append(AuditFinding(
                    check_name="log_scrub",
                    status="FAIL",
                    details="PHI detected in log file",
                    file_path=str(path.relative_to(self._config.vault_home)),
                ))
                problem_found = True

        if not problem_found:
            findings.append(AuditFinding(
                check_name="log_scrub",
                status="PASS",
                details="No PHI found in log files",
            ))
        return findings

    # ------------------------------------------------------------------
    # Check 3: DB encryption validation
    # ------------------------------------------------------------------

    def _check_db_encryption(self) -> list[AuditFinding]:
        """Sample rows from encrypted tables; detect plaintext JSON."""
        findings: list[AuditFinding] = []
        db_path = self._config.db_path

        if not db_path.exists():
            findings.append(AuditFinding(
                check_name="db_encryption",
                status="PASS",
                details="Database does not exist (nothing to audit)",
            ))
            return findings

        problem_found = False
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        except sqlite3.OperationalError:
            findings.append(AuditFinding(
                check_name="db_encryption",
                status="WARN",
                details="Could not open database in read-only mode",
            ))
            return findings

        try:
            for table in ENCRYPTED_TABLES:
                col = ENCRYPTED_COLUMN.get(table, "encrypted_data")
                try:
                    rows = conn.execute(
                        f"SELECT {col} FROM {table} LIMIT 50"  # noqa: S608
                    ).fetchall()
                except sqlite3.OperationalError:
                    continue

                for (blob,) in rows:
                    if blob is None:
                        continue
                    if not isinstance(blob, (bytes, memoryview)):
                        findings.append(AuditFinding(
                            check_name="db_encryption",
                            status="FAIL",
                            details=f"Non-blob value in {table}.{col}",
                        ))
                        problem_found = True
                        continue
                    raw = bytes(blob)
                    if len(raw) < 28:
                        findings.append(AuditFinding(
                            check_name="db_encryption",
                            status="FAIL",
                            details=f"Blob too small for AES-GCM in {table}.{col} "
                                    f"({len(raw)} bytes < 28 minimum)",
                        ))
                        problem_found = True
                        continue
                    try:
                        text = raw.decode("utf-8")
                        if text.lstrip()[:1] in {"{", "[", '"'}:
                            findings.append(AuditFinding(
                                check_name="db_encryption",
                                status="FAIL",
                                details=f"Plaintext JSON detected in {table}.{col}",
                            ))
                            problem_found = True
                    except UnicodeDecodeError:
                        pass  # Binary blob -- expected for encrypted data.
        finally:
            conn.close()

        if not problem_found:
            findings.append(AuditFinding(
                check_name="db_encryption",
                status="PASS",
                details="All sampled DB fields appear properly encrypted",
            ))
        return findings

    # ------------------------------------------------------------------
    # Check 4: Blob file format validation
    # ------------------------------------------------------------------

    def _check_blob_format(self) -> list[AuditFinding]:
        """Check .enc files for suspicious magic bytes (raw PDF, ZIP)."""
        findings: list[AuditFinding] = []
        blobs_dir = self._config.blobs_dir

        if not blobs_dir.exists():
            findings.append(AuditFinding(
                check_name="blob_format",
                status="PASS",
                details="Blob directory does not exist",
            ))
            return findings

        problem_found = False
        for path in blobs_dir.glob("*.enc"):
            try:
                header = path.read_bytes()[:8]
            except OSError:
                continue
            if header[:5] == b"%PDF-":
                findings.append(AuditFinding(
                    check_name="blob_format",
                    status="FAIL",
                    details="Unencrypted PDF stored as .enc blob",
                    file_path=str(path.relative_to(self._config.vault_home)),
                ))
                problem_found = True
            elif header[:4] == b"PK\x03\x04":
                findings.append(AuditFinding(
                    check_name="blob_format",
                    status="FAIL",
                    details="Unencrypted ZIP stored as .enc blob",
                    file_path=str(path.relative_to(self._config.vault_home)),
                ))
                problem_found = True

        if not problem_found:
            findings.append(AuditFinding(
                check_name="blob_format",
                status="PASS",
                details="All .enc blobs have expected encrypted format",
            ))
        return findings

    # ------------------------------------------------------------------
    # Check 5: Unexpected files
    # ------------------------------------------------------------------

    def _check_unexpected_files(self) -> list[AuditFinding]:
        """Warn about files with extensions not in the allowlist."""
        findings: list[AuditFinding] = []
        vault_dir = self._config.vault_home

        if not vault_dir.exists():
            findings.append(AuditFinding(
                check_name="unexpected_files",
                status="PASS",
                details="Vault directory does not exist",
            ))
            return findings

        problem_found = False
        for path in vault_dir.rglob("*"):
            if not path.is_file():
                continue
            suffixes = "".join(path.suffixes)
            if suffixes in ALLOWED_EXTENSIONS or path.suffix in ALLOWED_EXTENSIONS:
                continue
            if path.name in SAFE_PLAINTEXT:
                continue
            findings.append(AuditFinding(
                check_name="unexpected_files",
                status="WARN",
                details=f"Unexpected file type: {path.name}",
                file_path=str(path.relative_to(vault_dir)),
            ))
            problem_found = True

        if not problem_found:
            findings.append(AuditFinding(
                check_name="unexpected_files",
                status="PASS",
                details="No unexpected file types found in vault",
            ))
        return findings

    # ------------------------------------------------------------------
    # Check 6: Exports directory
    # ------------------------------------------------------------------

    def _check_exports_directory(self) -> list[AuditFinding]:
        """FAIL if exports/ contains non-.enc files."""
        findings: list[AuditFinding] = []
        exports_dir = self._config.exports_dir

        if not exports_dir.exists():
            findings.append(AuditFinding(
                check_name="exports_clean",
                status="PASS",
                details="Exports directory does not exist",
            ))
            return findings

        problem_found = False
        for path in exports_dir.iterdir():
            if not path.is_file():
                continue
            if path.suffix != ".enc":
                findings.append(AuditFinding(
                    check_name="exports_clean",
                    status="FAIL",
                    details=f"Non-encrypted file in exports: {path.name}",
                    file_path=str(path.relative_to(self._config.vault_home)),
                ))
                problem_found = True

        if not problem_found:
            findings.append(AuditFinding(
                check_name="exports_clean",
                status="PASS",
                details="Exports directory contains only encrypted files",
            ))
        return findings
