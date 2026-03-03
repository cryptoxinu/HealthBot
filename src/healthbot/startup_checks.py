"""Runtime startup self-check — structured health report on vault unlock.

Logs privacy mode, identity patterns, clean sync status, and migration
status. Emits fail-closed warnings for critical configuration issues.
"""
from __future__ import annotations

import logging

from healthbot.config import Config

logger = logging.getLogger("healthbot")


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

    # Summary
    if warnings:
        logger.warning(
            "Startup checks: %d warning(s):\n  %s",
            len(warnings), "\n  ".join(warnings),
        )
    else:
        logger.info("Startup checks: all OK")

    return warnings
