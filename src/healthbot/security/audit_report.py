"""Audit report dataclasses and constants for vault security audit."""
from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger("healthbot")

# Allowlisted plaintext files that are safe in the vault directory.
SAFE_PLAINTEXT: set[str] = {"manifest.json", "app.json"}
SAFE_PLAINTEXT_PREFIX: str = "healthbot.log"

# Extensions considered normal in the vault tree.
ALLOWED_EXTENSIONS: set[str] = {".enc", ".db", ".db-wal", ".db-shm", ".json", ".log", ".bak.enc"}

# Fallback list of encrypted tables — used when introspection fails.
_FALLBACK_ENCRYPTED_TABLES: list[str] = [
    "observations",
    "medications",
    "wearable_daily",
    "memory_stm",
    "memory_ltm",
    "hypotheses",
    "documents",
]

# Default export (for backward compatibility). Will be overridden at
# runtime by discover_encrypted_tables() when a DB path is available.
ENCRYPTED_TABLES: list[str] = list(_FALLBACK_ENCRYPTED_TABLES)

# Column that holds the encrypted blob in each table.  ``documents`` uses a
# different name (``meta_encrypted``), everything else uses ``encrypted_data``.
ENCRYPTED_COLUMN: dict[str, str] = {
    "documents": "meta_encrypted",
}


def discover_encrypted_tables(db_path: Path) -> tuple[list[str], dict[str, str]]:
    """Auto-discover tables with encrypted columns via PRAGMA introspection.

    Returns (table_list, column_map) where column_map maps table names
    to their encrypted column name when it differs from ``encrypted_data``.
    Falls back to the hardcoded list if introspection fails.
    """
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        logger.debug("discover_encrypted_tables: cannot open %s", db_path)
        return list(_FALLBACK_ENCRYPTED_TABLES), dict(ENCRYPTED_COLUMN)

    tables: list[str] = []
    col_map: dict[str, str] = {}
    try:
        # Get all table names
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        for (table_name,) in rows:
            try:
                cols = conn.execute(
                    f"PRAGMA table_info({table_name})"  # noqa: S608
                ).fetchall()
            except sqlite3.OperationalError:
                continue
            col_names = [c[1] for c in cols]
            if "encrypted_data" in col_names:
                tables.append(table_name)
            elif "meta_encrypted" in col_names:
                tables.append(table_name)
                col_map[table_name] = "meta_encrypted"
            elif "encrypted_text" in col_names:
                tables.append(table_name)
                col_map[table_name] = "encrypted_text"
    except Exception as exc:
        logger.debug("discover_encrypted_tables introspection failed: %s", exc)
        return list(_FALLBACK_ENCRYPTED_TABLES), dict(ENCRYPTED_COLUMN)
    finally:
        conn.close()

    if not tables:
        return list(_FALLBACK_ENCRYPTED_TABLES), dict(ENCRYPTED_COLUMN)
    return tables, col_map

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
