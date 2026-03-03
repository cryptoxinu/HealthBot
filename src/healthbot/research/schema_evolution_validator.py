"""Validation for schema evolution DDL and generated code.

Ensures generated migrations don't contain destructive DDL and that
generated code follows encryption, PII validation, and anonymization patterns.
"""
from __future__ import annotations

import re
from pathlib import Path

# Destructive DDL patterns that MUST be blocked
_DESTRUCTIVE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bDROP\s+TABLE\b", re.IGNORECASE), "DROP TABLE"),
    (re.compile(r"\bDROP\s+INDEX\b", re.IGNORECASE), "DROP INDEX"),
    (re.compile(r"\bDELETE\s+FROM\b", re.IGNORECASE), "DELETE FROM"),
    (re.compile(r"\bTRUNCATE\b", re.IGNORECASE), "TRUNCATE"),
    (re.compile(r"\bUPDATE\s+\S+\s+SET\b", re.IGNORECASE), "UPDATE ... SET"),
    (re.compile(r"\bALTER\s+TABLE\s+\S+\s+DROP\b", re.IGNORECASE), "ALTER TABLE ... DROP"),
    (re.compile(r"\bALTER\s+TABLE\s+\S+\s+RENAME\b", re.IGNORECASE), "ALTER TABLE ... RENAME"),
]

# Allowlisted DDL patterns (must match one of these to pass)
_ALLOWED_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bCREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\b", re.IGNORECASE),
    re.compile(r"\bCREATE\s+INDEX\s+IF\s+NOT\s+EXISTS\b", re.IGNORECASE),
    re.compile(r"\bCREATE\s+UNIQUE\s+INDEX\s+IF\s+NOT\s+EXISTS\b", re.IGNORECASE),
    re.compile(r"\bALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN\b", re.IGNORECASE),
]

# Idempotency: CREATE statements must include IF NOT EXISTS
_CREATE_WITHOUT_IF_NOT_EXISTS = re.compile(
    r"\bCREATE\s+(?:TABLE|INDEX|UNIQUE\s+INDEX)\s+(?!IF\s+NOT\s+EXISTS)\b",
    re.IGNORECASE,
)


def validate_ddl(ddl_statements: list[str]) -> list[str]:
    """Validate DDL statements from schema evolution.

    Returns a list of error messages (empty = OK).
    Blocks destructive DDL, requires allowlisted patterns, and
    enforces idempotency (IF NOT EXISTS on all CREATE statements).
    """
    errors: list[str] = []

    for stmt in ddl_statements:
        stmt_stripped = stmt.strip()
        if not stmt_stripped:
            continue

        # Check for destructive patterns
        for pattern, label in _DESTRUCTIVE_PATTERNS:
            if pattern.search(stmt_stripped):
                errors.append(f"Destructive DDL blocked: {label} in: {stmt_stripped[:100]}")

        # Check that statement matches at least one allowed pattern
        if not any(p.search(stmt_stripped) for p in _ALLOWED_PATTERNS):
            errors.append(
                f"DDL not in allowlist: {stmt_stripped[:100]}"
            )

        # Enforce idempotency on CREATE statements
        if _CREATE_WITHOUT_IF_NOT_EXISTS.search(stmt_stripped):
            errors.append(
                f"CREATE without IF NOT EXISTS: {stmt_stripped[:100]}"
            )

    return errors


def validate_generated_files(
    files_modified: list[str], project_dir: str,
) -> list[str]:
    """Validate generated code follows required encryption/PII patterns.

    Checks:
    - Raw vault mixins must use _encrypt() with AAD format
    - Clean DB mixins must use _validate_text_fields() before INSERT
    - Sync workers must call anonymize() before upsert_
    """
    errors: list[str] = []
    root = Path(project_dir)

    for rel_path in files_modified:
        full_path = root / rel_path
        if not full_path.exists() or not full_path.suffix == ".py":
            continue

        try:
            content = full_path.read_text(encoding="utf-8")
        except OSError:
            continue

        # Raw vault mixins (data/db/*.py)
        if _is_raw_vault_mixin(rel_path):
            if _has_insert(content) and not _has_encrypt(content):
                errors.append(
                    f"Raw vault mixin {rel_path} has INSERT but no _encrypt() call"
                )

        # Clean DB mixins (data/clean_db/*.py)
        if _is_clean_db_mixin(rel_path):
            if _has_insert(content) and not _has_validate_text_fields(content):
                errors.append(
                    f"Clean DB mixin {rel_path} has INSERT but no _validate_text_fields() call"
                )

        # Sync workers (clean_sync*.py)
        if _is_sync_worker(rel_path):
            if _has_upsert(content) and not _has_anonymize(content):
                errors.append(
                    f"Sync worker {rel_path} has upsert_ call but no anonymize() call"
                )

    return errors


def _is_raw_vault_mixin(path: str) -> bool:
    """Check if path is a raw vault DB mixin."""
    normalized = path.replace("\\", "/")
    return "data/db/" in normalized and normalized.endswith(".py")


def _is_clean_db_mixin(path: str) -> bool:
    """Check if path is a clean DB mixin."""
    normalized = path.replace("\\", "/")
    return "data/clean_db/" in normalized and normalized.endswith(".py")


def _is_sync_worker(path: str) -> bool:
    """Check if path is a sync worker file."""
    normalized = path.replace("\\", "/")
    basename = Path(normalized).name
    return basename.startswith("clean_sync") and basename.endswith(".py")


def _has_insert(content: str) -> bool:
    return bool(re.search(r"\bINSERT\b", content, re.IGNORECASE))


def _has_encrypt(content: str) -> bool:
    return "self._encrypt(" in content


def _has_validate_text_fields(content: str) -> bool:
    return "_validate_text_fields(" in content


def _has_upsert(content: str) -> bool:
    return bool(re.search(r"\bupsert_", content))


def _has_anonymize(content: str) -> bool:
    return "anonymize(" in content
