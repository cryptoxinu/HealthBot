"""Background job scheduling for proactive alerts.

Uses python-telegram-bot's JobQueue (APScheduler-backed).
Three triggers:
1. On vault unlock -- immediate check
2. Periodic (every 4 hours while unlocked)
3. Incoming folder poll (every 60 seconds while unlocked)

This package splits the monolithic AlertScheduler into focused sub-modules
using mixin classes. The final AlertScheduler composes all mixins and is
re-exported here so that ``from healthbot.bot.scheduler import AlertScheduler``
continues to work unchanged.
"""
from __future__ import annotations

import logging

from healthbot.bot.formatters import paginate  # noqa: F401
from healthbot.bot.scheduler.alert_dispatch import AlertDispatchMixin
from healthbot.bot.scheduler.backup_jobs import BackupJobsMixin
from healthbot.bot.scheduler.health_checks import HealthChecksMixin
from healthbot.bot.scheduler.ingestion_jobs import IngestionJobsMixin
from healthbot.bot.scheduler.memory_jobs import MemoryJobsMixin
from healthbot.bot.scheduler.report_jobs import ReportJobsMixin
from healthbot.bot.scheduler.research_jobs import ResearchJobsMixin
from healthbot.bot.scheduler.scheduler_core import (
    _SPECIALTY_LABS,
    APPLE_HEALTH_POLL_INTERVAL,
    AUTH_HEALTH_CHECK_INTERVAL,
    DAILY_BACKUP_INTERVAL,
    DAILY_WEARABLE_SYNC_INTERVAL,
    DEFAULT_CONSOLIDATION_INTERVAL,
    INCOMING_POLL_INTERVAL,
    PERIODIC_INTERVAL,
    RESEARCH_INTERVAL,
    TIMEOUT_CHECK_INTERVAL,
    WEARABLE_GAP_CHECK_INTERVAL,
    WEARABLE_GAP_THRESHOLD_DAYS,
    AlertSchedulerCore,
)
from healthbot.bot.scheduler.sync_jobs import SyncJobsMixin
from healthbot.bot.scheduler.unlock_jobs import UnlockJobsMixin
from healthbot.config import Config  # noqa: F401
from healthbot.data.db import HealthDB  # noqa: F401
from healthbot.reasoning.watcher import (  # noqa: F401
    Alert,
    HealthWatcher,
)
from healthbot.security.key_manager import KeyManager  # noqa: F401

logger = logging.getLogger("healthbot")


class AlertScheduler(
    UnlockJobsMixin,
    HealthChecksMixin,
    IngestionJobsMixin,
    SyncJobsMixin,
    AlertDispatchMixin,
    ResearchJobsMixin,
    ReportJobsMixin,
    BackupJobsMixin,
    MemoryJobsMixin,
    AlertSchedulerCore,
):
    """Manages background jobs for proactive health alerts."""


__all__ = [
    "AlertScheduler",
    "PERIODIC_INTERVAL",
    "INCOMING_POLL_INTERVAL",
    "DEFAULT_CONSOLIDATION_INTERVAL",
    "TIMEOUT_CHECK_INTERVAL",
    "DAILY_BACKUP_INTERVAL",
    "RESEARCH_INTERVAL",
    "WEARABLE_GAP_CHECK_INTERVAL",
    "AUTH_HEALTH_CHECK_INTERVAL",
    "WEARABLE_GAP_THRESHOLD_DAYS",
    "APPLE_HEALTH_POLL_INTERVAL",
    "DAILY_WEARABLE_SYNC_INTERVAL",
    "_SPECIALTY_LABS",
]
