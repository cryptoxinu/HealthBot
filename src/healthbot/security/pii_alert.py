"""Real-time PII leak alerting service.

Logs every PII detection event and provides a notification callback
for Telegram push alerts. No PHI is stored — only category + timestamp.
"""
from __future__ import annotations

import json
import logging
import threading
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger("healthbot")


@dataclass(frozen=True)
class PiiAlert:
    """A single PII detection event (no PHI stored)."""

    category: str          # e.g., "name", "SSN", "email", "NER-person"
    destination: str       # Where the PII was heading: "claude_cli", "mcp", "clean_db", "research"
    timestamp: str         # ISO 8601
    blocked: bool = True   # Always True — we never let PII through


@dataclass
class PiiAlertStats:
    """Cumulative alert statistics."""

    total_alerts: int = 0
    by_category: dict[str, int] = field(default_factory=dict)
    by_destination: dict[str, int] = field(default_factory=dict)
    last_alert: str = ""  # ISO timestamp of most recent alert


# Type for the optional Telegram notification callback
NotifyCallback = Callable[[str], None]


class PiiAlertService:
    """Track and report PII leak detections.

    Thread-safe. Singleton per process via get_instance().
    """

    _instance: PiiAlertService | None = None
    _lock = threading.Lock()

    def __init__(self, log_dir: Path | None = None) -> None:
        self._alerts: deque[PiiAlert] = deque(maxlen=1000)
        self._stats = PiiAlertStats()
        self._notify_cb: NotifyCallback | None = None
        self._data_lock = threading.Lock()
        self._log_path: Path | None = None

        if log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
            self._log_path = log_dir / "pii_alerts.log"

    @classmethod
    def get_instance(cls, log_dir: Path | None = None) -> PiiAlertService:
        """Get or create the singleton instance."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(log_dir=log_dir)
            return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton (for testing)."""
        with cls._lock:
            cls._instance = None

    def set_notify_callback(self, cb: NotifyCallback) -> None:
        """Set a callback for push notifications (e.g., Telegram)."""
        self._notify_cb = cb

    def record(self, category: str, destination: str) -> None:
        """Record a PII detection event.

        Args:
            category: Type of PII detected (e.g., "name", "SSN", "NER-person")
            destination: Where it was heading (e.g., "claude_cli", "mcp", "clean_db")
        """
        alert = PiiAlert(
            category=category,
            destination=destination,
            timestamp=datetime.now(UTC).isoformat(),
        )

        with self._data_lock:
            self._alerts.append(alert)
            self._stats.total_alerts += 1
            self._stats.by_category[category] = (
                self._stats.by_category.get(category, 0) + 1
            )
            self._stats.by_destination[destination] = (
                self._stats.by_destination.get(destination, 0) + 1
            )
            self._stats.last_alert = alert.timestamp

        # Log to file (scrubbed — no actual PII content)
        if self._log_path:
            try:
                with open(self._log_path, "a") as f:
                    f.write(json.dumps(asdict(alert)) + "\n")
            except Exception as e:
                logger.warning("Failed to write PII alert log: %s", e)

        # Push notification
        msg = (
            f"PII ALERT: [{category}] detected in outbound [{destination}]. "
            f"Blocked."
        )
        logger.warning(msg)

        if self._notify_cb:
            try:
                self._notify_cb(msg)
            except Exception as e:
                logger.warning("PII alert notification failed: %s", e)

    def get_stats(self) -> PiiAlertStats:
        """Get cumulative alert statistics."""
        with self._data_lock:
            return PiiAlertStats(
                total_alerts=self._stats.total_alerts,
                by_category=dict(self._stats.by_category),
                by_destination=dict(self._stats.by_destination),
                last_alert=self._stats.last_alert,
            )

    def get_recent(self, limit: int = 20) -> list[PiiAlert]:
        """Get recent alerts (newest first)."""
        with self._data_lock:
            return list(reversed(list(self._alerts)))[:limit]

    def format_report(self) -> str:
        """Format a human-readable alert report."""
        stats = self.get_stats()

        if stats.total_alerts == 0:
            return "PII Alert Report\n\nNo PII leaks detected. All clear."

        lines = [
            "PII Alert Report\n",
            f"Total alerts: {stats.total_alerts}",
            f"Last alert: {stats.last_alert}\n",
            "By category:",
        ]
        for cat, count in sorted(
            stats.by_category.items(), key=lambda x: -x[1],
        ):
            lines.append(f"  {cat}: {count}")

        lines.append("\nBy destination:")
        for dest, count in sorted(
            stats.by_destination.items(), key=lambda x: -x[1],
        ):
            lines.append(f"  {dest}: {count}")

        recent = self.get_recent(5)
        if recent:
            lines.append("\nRecent alerts:")
            for a in recent:
                lines.append(f"  [{a.timestamp[:19]}] {a.category} -> {a.destination}")

        return "\n".join(lines)
