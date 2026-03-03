"""Tests for the alert scheduler (background job management)."""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.bot.scheduler import AlertScheduler
from healthbot.config import Config
from healthbot.reasoning.watcher import Alert
from healthbot.security.key_manager import KeyManager


def _make_alert(dedup_key: str = "abc123", severity: str = "watch") -> Alert:
    return Alert(
        alert_type="overdue",
        title="Overdue: LDL",
        body="You're overdue for LDL recheck.",
        severity=severity,
        dedup_key=dedup_key,
    )


class TestAlertScheduler:
    """Unit tests for AlertScheduler."""

    def _make_scheduler(
        self, *, locked: bool = False, chat_id: int = 12345
    ) -> tuple[AlertScheduler, MagicMock, MagicMock]:
        config = MagicMock(spec=Config)
        config.incoming_dir = MagicMock()
        config.vault_home = Path(tempfile.mkdtemp())
        config.clean_db_path = Path(tempfile.mkdtemp()) / "clean.db"
        config.allowed_user_ids = [chat_id]
        config.auto_ai_export = False
        config.apple_health_export_path = ""
        config.weekly_report_day = ""
        config.weekly_report_time = "20:00"
        config.monthly_report_day = 0
        config.monthly_report_time = "20:00"
        km = MagicMock(spec=KeyManager)
        km.is_unlocked = not locked
        scheduler = AlertScheduler(config, km, chat_id)
        bot = AsyncMock()
        return scheduler, km, bot

    @pytest.mark.asyncio
    async def test_periodic_skips_when_locked(self) -> None:
        """Periodic check should silently skip if vault is locked."""
        scheduler, km, _ = self._make_scheduler(locked=True)
        context = MagicMock()
        context.bot = AsyncMock()

        with patch("healthbot.bot.scheduler.HealthWatcher") as mock_watcher:
            await scheduler._periodic_check(context)
            mock_watcher.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_unlock_sends_briefing(self) -> None:
        """run_on_unlock should send welcome briefing (quiet unlock)."""
        scheduler, km, bot = self._make_scheduler(locked=False)

        with patch.object(scheduler, "_get_db", return_value=MagicMock()):
            with patch.object(
                scheduler, "_build_welcome_briefing", return_value="Welcome back."
            ):
                await scheduler.run_on_unlock(bot)

        # Only the briefing, no watcher alerts
        assert bot.send_message.call_count == 1

    @pytest.mark.asyncio
    async def test_on_unlock_skips_when_locked(self) -> None:
        """run_on_unlock should do nothing if vault is locked."""
        scheduler, km, bot = self._make_scheduler(locked=True)

        with patch("healthbot.bot.scheduler.HealthWatcher") as mock_cls:
            await scheduler.run_on_unlock(bot)
            mock_cls.assert_not_called()

        bot.send_message.assert_not_called()

    def test_on_lock_clears_state(self) -> None:
        """on_lock should clear sent keys and close the database."""
        scheduler, _, _ = self._make_scheduler()
        scheduler._sent_keys["key1"] = 1.0
        scheduler._sent_keys["key2"] = 2.0
        mock_db = MagicMock()
        scheduler._db = mock_db

        scheduler.on_lock()

        assert len(scheduler._sent_keys) == 0
        mock_db.close.assert_called_once()
        assert scheduler._db is None

    @pytest.mark.asyncio
    async def test_dedup_prevents_repeat_alerts(self) -> None:
        """Same dedup_key should not be sent twice."""
        scheduler, _, bot = self._make_scheduler(locked=False)
        alert = _make_alert(dedup_key="same_key")
        alerts = [alert, alert]

        await scheduler._send_alerts(alerts, bot)

        # Only one alert + 1 overdue tip message
        assert bot.send_message.call_count == 2

    @pytest.mark.asyncio
    async def test_different_dedup_keys_both_sent(self) -> None:
        """Alerts with different dedup keys should both be sent."""
        scheduler, _, bot = self._make_scheduler(locked=False)
        alert1 = _make_alert(dedup_key="key_a")
        alert2 = _make_alert(dedup_key="key_b")

        await scheduler._send_alerts([alert1, alert2], bot)

        # 2 alerts + 1 overdue tip
        assert bot.send_message.call_count == 3

    @pytest.mark.asyncio
    async def test_alert_message_formatting(self) -> None:
        """Alert messages should include severity icon and body."""
        scheduler, _, bot = self._make_scheduler(locked=False)
        alert = _make_alert(severity="urgent")

        await scheduler._send_alerts([alert], bot)

        # First call is the alert, second is the tip
        first_call = bot.send_message.call_args_list[0]
        sent_text = first_call.kwargs["text"]
        assert "!" in sent_text
        assert "Overdue: LDL" in sent_text
        assert "overdue for LDL recheck" in sent_text

    def test_register_jobs_with_none_queue(self) -> None:
        """register_jobs should handle None job_queue gracefully."""
        scheduler, _, _ = self._make_scheduler()
        # Should not raise
        scheduler.register_jobs(None)

    def test_register_jobs_with_queue(self) -> None:
        """register_jobs should register fourteen repeating jobs."""
        scheduler, _, _ = self._make_scheduler()
        job_queue = MagicMock()

        scheduler.register_jobs(job_queue)

        assert job_queue.run_repeating.call_count == 14
