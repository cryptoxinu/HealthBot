"""Audit report dataclasses and constants for vault security audit."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# Allowlisted plaintext files that are safe in the vault directory.
SAFE_PLAINTEXT: set[str] = {"manifest.json", "app.json"}
SAFE_PLAINTEXT_PREFIX: str = "healthbot.log"

# Extensions considered normal in the vault tree.
ALLOWED_EXTENSIONS: set[str] = {".enc", ".db", ".db-wal", ".db-shm", ".json", ".log", ".bak.enc"}

# Tables whose rows contain an ``encrypted_data`` BLOB column that must be
# properly AES-256-GCM formatted (minimum: 12-byte nonce + 16-byte tag = 28).
ENCRYPTED_TABLES: list[str] = [
    "observations",
    "medications",
    "wearable_daily",
    "memory_stm",
    "memory_ltm",
    "hypotheses",
    "documents",
]

# Column that holds the encrypted blob in each table.  ``documents`` uses a
# different name (``meta_encrypted``), everything else uses ``encrypted_data``.
ENCRYPTED_COLUMN: dict[str, str] = {
    "documents": "meta_encrypted",
}

# Patterns that indicate plaintext health data in a file.
PLAINTEXT_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"test_name.*value.*unit", re.IGNORECASE),
    re.compile(r"date_collected.*reference", re.IGNORECASE),
]


@dataclass
class AuditFinding:
    """A single audit check result."""

    check_name: str
    status: str  # "PASS", "FAIL", "WARN"
    details: str
    file_path: str | None = None


@dataclass
class AuditReport:
    """Aggregated audit results."""

    findings: list[AuditFinding] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(f.status == "PASS" for f in self.findings)

    @property
    def fail_count(self) -> int:
        return sum(1 for f in self.findings if f.status == "FAIL")

    @property
    def warn_count(self) -> int:
        return sum(1 for f in self.findings if f.status == "WARN")

    def format(self) -> str:
        """Human-readable audit report."""
        lines: list[str] = [
            "SECURITY AUDIT REPORT",
            "=" * 40,
            "",
        ]
        for f in self.findings:
            icon = "[+]" if f.status == "PASS" else "[X]" if f.status == "FAIL" else "[!]"
            lines.append(f"{icon} {f.check_name}: {f.status}")
            lines.append(f"    {f.details}")
            if f.file_path:
                lines.append(f"    File: {f.file_path}")
        total = len(self.findings)
        passed = total - self.fail_count - self.warn_count
        lines.append("")
        lines.append("-" * 40)
        lines.append(
            f"Result: {passed}/{total} passed, "
            f"{self.fail_count} failed, {self.warn_count} warnings"
        )
        lines.append(f"OVERALL: {'PASS' if self.passed else 'FAIL'}")
        return "\n".join(lines)
