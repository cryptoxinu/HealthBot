"""Overdue notification pause state management.

Stores pause state as a simple JSON file in the config directory.
Does NOT require vault unlock to check (unlike DB-based approaches).
Persists across bot restarts.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

logger = logging.getLogger("healthbot")

_PAUSE_FILE = "overdue_pause.json"

_DURATION_PATTERN = re.compile(
    r"(\d+)\s*(hours?|hrs|hr|h|days?|d|weeks?|wks|wk|w|months?|mons|mon|m)"
    r"(?:\b|(?=\s|$))",
    re.IGNORECASE,
)


def _pause_path(config: object) -> Path:
    """Return path to the pause state file."""
    return config.vault_home / "config" / _PAUSE_FILE


def is_overdue_paused(config: object) -> bool:
    """Check if overdue notifications are currently paused."""
    path = _pause_path(config)
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text())
        paused_until = data.get("paused_until")
        if paused_until is None:
            return False
        deadline = datetime.fromisoformat(paused_until)
        if datetime.now(UTC) < deadline:
            return True
        # Expired — clean up
        path.unlink(missing_ok=True)
        return False
    except Exception as e:
        logger.debug("Overdue pause check failed: %s", e)
        return False


def get_pause_until(config: object) -> datetime | None:
    """Return the pause deadline, or None if not paused."""
    path = _pause_path(config)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        paused_until = data.get("paused_until")
        if paused_until is None:
            return None
        deadline = datetime.fromisoformat(paused_until)
        if datetime.now(UTC) < deadline:
            return deadline
        path.unlink(missing_ok=True)
        return None
    except Exception:
        return None


def pause_overdue(config: object, duration: timedelta) -> datetime:
    """Pause overdue notifications for the given duration. Returns deadline."""
    deadline = datetime.now(UTC) + duration
    path = _pause_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"paused_until": deadline.isoformat()}))
    return deadline


def unpause_overdue(config: object) -> bool:
    """Unpause overdue notifications. Returns True if was paused."""
    path = _pause_path(config)
    if path.exists():
        path.unlink()
        return True
    return False


def parse_duration(text: str) -> timedelta | None:
    """Parse a natural language duration into a timedelta.

    Supports: hours, days, weeks, months (approximated as 30 days).
    Returns None if no valid duration found.
    """
    m = _DURATION_PATTERN.search(text)
    if not m:
        return None
    amount = int(m.group(1))
    if amount <= 0:
        return None
    unit = m.group(2).lower().rstrip("s")
    if unit in ("hour", "hr", "h"):
        return timedelta(hours=amount)
    if unit in ("day", "d"):
        return timedelta(days=amount)
    if unit in ("week", "wk", "w"):
        return timedelta(weeks=amount)
    if unit in ("month", "mon", "m"):
        return timedelta(days=amount * 30)
    return None  # pragma: no cover
