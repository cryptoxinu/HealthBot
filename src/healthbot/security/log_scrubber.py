"""PHI-safe logging.

All log messages pass through PhiFirewall.redact() before being written.
Uses standard Python logging with a custom Filter.
"""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from healthbot.security.phi_firewall import PhiFirewall


class PhiScrubFilter(logging.Filter):
    """Logging filter that redacts PHI from all log records."""

    def __init__(self, firewall: PhiFirewall) -> None:
        super().__init__()
        self._firewall = firewall

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact PHI from the message. Always returns True (keeps record).

        Pre-formats the message with its args so that all text (including
        non-string args like dicts/lists that may contain PII) is scrubbed.
        """
        if record.args:
            try:
                record.msg = self._firewall.redact(str(record.msg % record.args))
            except (TypeError, ValueError):
                # Fallback: scrub msg and string args individually
                record.msg = self._firewall.redact(str(record.msg))
                if isinstance(record.args, tuple):
                    record.args = tuple(
                        self._firewall.redact(str(a)) if isinstance(a, str) else a
                        for a in record.args
                    )
                    return True
            record.args = None
        else:
            record.msg = self._firewall.redact(str(record.msg))
        return True


def setup_logging(log_dir: Path, firewall: PhiFirewall) -> logging.Logger:
    """Configure application logger with PHI scrubbing and file rotation."""
    logger = logging.getLogger("healthbot")
    logger.setLevel(logging.INFO)

    # Clear existing handlers
    logger.handlers.clear()

    # PHI scrub filter
    phi_filter = PhiScrubFilter(firewall)

    # File handler with rotation
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_dir / "healthbot.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
    )
    file_handler.setLevel(logging.INFO)
    file_handler.addFilter(phi_filter)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    logger.addHandler(file_handler)

    # Console handler (also scrubbed)
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    console.addFilter(phi_filter)
    console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(console)

    return logger
