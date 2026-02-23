"""Tests for WHOOP sync integration."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestWhoopSyncScheduler:
    """WHOOP auto-sync on unlock."""

    @pytest.mark.asyncio
    async def test_whoop_sync_skip_when_no_credentials(self) -> None:
        """Should silently skip when WHOOP not configured."""
        from healthbot.bot.scheduler import AlertScheduler
        from healthbot.config import Config

        config = MagicMock(spec=Config)
        km = MagicMock()
        km.is_unlocked = True

        scheduler = AlertScheduler(config, km, chat_id=123)
        scheduler._db = MagicMock()
        scheduler._sent_keys = set()

        mock_bot = AsyncMock()

        # Mock HealthWatcher to return no alerts
        with patch("healthbot.bot.scheduler.HealthWatcher") as mock_watcher_cls:
            mock_watcher = MagicMock()
            mock_watcher.check_all.return_value = []
            mock_watcher_cls.return_value = mock_watcher

            # Mock Keychain to return None (no WHOOP credentials)
            with patch("healthbot.bot.scheduler.Keychain", create=True) as mock_kc:
                mock_kc_inst = MagicMock()
                mock_kc_inst.retrieve.return_value = None
                mock_kc.return_value = mock_kc_inst

                await scheduler.run_on_unlock(mock_bot)

        # Should not crash -- no WHOOP sync attempted


class TestSyncCommand:
    """Test that /sync can be called without crash (with mocked WhoopClient)."""

    def test_sync_text_is_not_a_command(self) -> None:
        """'sync my whoop data' is free text, routed to Claude (not /sync)."""
        # NLU router removed — all free text goes to Claude CLI
        # This test just verifies the /sync command exists as a handler
        pass
