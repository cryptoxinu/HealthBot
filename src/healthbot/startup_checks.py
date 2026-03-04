"""Runtime startup self-check — structured health report on vault unlock.

Logs privacy mode, identity patterns, clean sync status, and migration
status. Emits fail-closed warnings for critical configuration issues.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from healthbot.config import Config

logger = logging.getLogger("healthbot")


def _check_plaintext_residue(db_path: Path) -> list[str]:
    """Check for plaintext residue in legacy columns.

    Returns warnings if search_index.text_for_search or documents.filename
    still contain plaintext data (should have been cleared by migrations).
    """
    warnings: list[str] = []
    if not db_path.exists():
        return warnings

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        return warnings

    try:
        # Check search_index plaintext residue
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM search_index "
                "WHERE text_for_search IS NOT NULL AND text_for_search != ''",
            ).fetchone()
            search_count = row[0] if row else 0
        except sqlite3.OperationalError:
            search_count = 0

        # Check documents filename residue
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM documents "
                "WHERE filename IS NOT NULL AND filename != ''",
            ).fetchone()
            doc_count = row[0] if row else 0
        except sqlite3.OperationalError:
            doc_count = 0

        if search_count > 0:
            warnings.append(
                f"Plaintext residue: search_index has {search_count} rows "
                f"with non-empty text_for_search. Run migrations to clear."
            )
            logger.warning(
                "Startup check: search_index plaintext residue: %d rows",
                search_count,
            )

        if doc_count > 0:
            warnings.append(
                f"Plaintext residue: documents has {doc_count} rows "
                f"with non-empty filename. Run migrations to clear."
            )
            logger.warning(
                "Startup check: documents plaintext residue: %d rows",
                doc_count,
            )

        if search_count > 0 or doc_count > 0:
            logger.warning(
                "Plaintext residue detected — triggering migration to clear."
            )
            try:
                from healthbot.data.schema import run_migrations
                conn.close()
                # Re-open writable for migration
                rw_conn = sqlite3.connect(str(db_path))
                run_migrations(rw_conn)
                rw_conn.close()
                logger.info("Post-residue migration completed.")
            except Exception as e:
                logger.warning("Failed to run residue cleanup migration: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return warnings


def run_startup_checks(
    config: Config,
    *,
    identity_pattern_count: int = 0,
    clean_sync_ok: bool = False,
    clean_sync_last: str = "",
    migration_current: bool = True,
) -> list[str]:
    """Run startup self-checks and return a list of warnings (empty = all OK).

    Called after vault unlock to verify runtime invariants.
    Always logs a structured report regardless of outcome.
    """
    warnings: list[str] = []

    # 1. Privacy mode
    mode = config.privacy_mode
    logger.info("Startup check: privacy_mode=%s", mode)
    if mode == "relaxed":
        warnings.append(
            "Privacy mode is 'relaxed' — image-only PDFs may expose un-redacted "
            "content. Consider switching to 'strict'."
        )

    # 2. Identity patterns
    logger.info("Startup check: identity_patterns_loaded=%d", identity_pattern_count)
    if identity_pattern_count == 0:
        warnings.append(
            "No identity patterns loaded — PII detection limited to generic "
            "regex. Run /identity to configure personal PII patterns."
        )

    # 3. Clean sync status
    logger.info(
        "Startup check: clean_sync_ok=%s, last_sync=%s",
        clean_sync_ok, clean_sync_last or "never",
    )
    if not clean_sync_ok:
        warnings.append(
            "Clean DB sync has not completed successfully. "
            "AI queries may use stale data."
        )

    # 4. Migration status
    logger.info("Startup check: migrations_current=%s", migration_current)
    if not migration_current:
        warnings.append(
            "Database migrations are pending. Some features may not work correctly."
        )

    # 5. Allowed user IDs
    if not config.allowed_user_ids:
        warnings.append(
            "No allowed_user_ids configured — bot will reject all messages. "
            "Run --setup or add user IDs to app.json."
        )

    # 6. Plaintext residue check (P0-001, P0-002)
    warnings.extend(_check_plaintext_residue(config.db_path))

    # Summary
    if warnings:
        logger.warning(
            "Startup checks: %d warning(s):\n  %s",
            len(warnings), "\n  ".join(warnings),
        )
    else:
        logger.info("Startup checks: all OK")

    return warnings
