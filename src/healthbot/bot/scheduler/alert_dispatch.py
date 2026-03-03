"""Alert sending, dedup, and medication reminder methods."""
from __future__ import annotations

import logging

from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate
from healthbot.reasoning.watcher import Alert

logger = logging.getLogger("healthbot")


class AlertDispatchMixin:
    """Mixin for alert dispatch, dedup, and medication reminders."""

    async def _send_alerts(self, alerts: list[Alert], bot: object) -> None:
        """Send non-duplicate alerts. Respects overdue pause."""
        from healthbot.bot.overdue_pause import is_overdue_paused

        overdue_paused = is_overdue_paused(self._config)
        sent_overdue = False
        for alert in alerts:
            if self._has_sent_key(alert.dedup_key):
                continue
            if alert.alert_type == "overdue" and overdue_paused:
                continue
            self._record_sent_key(alert.dedup_key)
            icon = {"urgent": "!", "watch": "~", "info": ""}.get(alert.severity, "")
            msg = f"{icon} {alert.title}\n{alert.body}"
            for page in paginate(msg):
                await self._tracked_send(bot, page)
            if alert.alert_type == "overdue":
                sent_overdue = True
        # Single tip after all overdue alerts (not per-alert)
        if sent_overdue:
            await self._tracked_send(
                bot,
                "Tip: Say 'pause notifications for 2 weeks' to snooze overdue alerts.",
            )

    async def _check_med_reminders(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Check and send medication reminders. Skips if locked."""
        if not self._km.is_unlocked:
            return

        try:
            from healthbot.reasoning.med_reminders import (
                format_reminder,
                get_due_reminders,
            )

            db = self._get_db()
            user_id = self._primary_user_id
            due = get_due_reminders(db, user_id)
            for reminder in due:
                # Dedup: only send each reminder once per minute
                dedup = f"med_reminder_{reminder.med_name}_{reminder.time}"
                if self._has_sent_key(dedup):
                    continue
                self._record_sent_key(dedup)
                msg = format_reminder(reminder)
                await self._tracked_send(context.bot, msg)
        except Exception as e:
            logger.debug("Med reminder check: %s", e)

    async def _check_reminder_resumes(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Resume paused reminders whose retest window has opened. Daily check."""
        if not self._km.is_unlocked:
            return

        try:
            from healthbot.reasoning.med_reminders import check_reminder_resumes

            db = self._get_db()
            user_id = self._primary_user_id
            messages = check_reminder_resumes(db, user_id)
            for msg in messages:
                await self._tracked_send(context.bot, msg)
        except Exception as e:
            logger.debug("Reminder resume check: %s", e)
