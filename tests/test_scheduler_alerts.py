"""Tests for AlertScheduler."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.config import Config
from healthbot.reasoning.watcher import Alert


class TestSendAlerts:
    """Alert deduplication."""

    @pytest.mark.asyncio
    async def test_deduplicates_by_key(self, tmp_path) -> None:
        from healthbot.bot.scheduler import AlertScheduler

        config = MagicMock(spec=Config)
        config.vault_home = tmp_path
        km = MagicMock()
        km.is_unlocked = True

        scheduler = AlertScheduler(config, km, chat_id=123)
        bot = AsyncMock()

        alerts = [
            Alert("overdue", "TSH Overdue", "TSH last checked 2yr ago",
                  "watch", "overdue_tsh"),
            Alert("overdue", "TSH Overdue", "TSH last checked 2yr ago",
                  "watch", "overdue_tsh"),  # duplicate
        ]

        await scheduler._send_alerts(alerts, bot)

        # 1 alert (dedup) + 1 overdue tip
        assert bot.send_message.call_count == 2

    @pytest.mark.asyncio
    async def test_sends_different_alerts(self, tmp_path) -> None:
        from healthbot.bot.scheduler import AlertScheduler

        config = MagicMock(spec=Config)
        config.vault_home = tmp_path
        km = MagicMock()

        scheduler = AlertScheduler(config, km, chat_id=123)
        bot = AsyncMock()

        alerts = [
            Alert("overdue", "TSH", "body1", "watch", "key1"),
            Alert("trend", "Glucose", "body2", "info", "key2"),
        ]

        await scheduler._send_alerts(alerts, bot)
        # 2 alerts + 1 overdue tip (overdue alert triggers tip)
        assert bot.send_message.call_count == 3

    @pytest.mark.asyncio
    async def test_overdue_alerts_suppressed_when_paused(self, tmp_path) -> None:
        import json
        from datetime import UTC, datetime, timedelta

        from healthbot.bot.scheduler import AlertScheduler

        config = MagicMock(spec=Config)
        config.vault_home = tmp_path
        km = MagicMock()

        scheduler = AlertScheduler(config, km, chat_id=123)
        bot = AsyncMock()

        # Create pause file
        pause_dir = tmp_path / "config"
        pause_dir.mkdir(exist_ok=True)
        deadline = (datetime.now(UTC) + timedelta(days=7)).isoformat()
        (pause_dir / "overdue_pause.json").write_text(
            json.dumps({"paused_until": deadline})
        )

        alerts = [
            Alert("overdue", "TSH Overdue", "body", "watch", "key_overdue"),
            Alert("trend", "Glucose Rising", "body", "urgent", "key_trend"),
        ]

        await scheduler._send_alerts(alerts, bot)

        # Only trend alert should be sent (overdue suppressed)
        assert bot.send_message.call_count == 1
        call_text = bot.send_message.call_args[1]["text"]
        assert "Glucose" in call_text


    @pytest.mark.asyncio
    async def test_no_tip_when_only_trend_alerts(self, tmp_path) -> None:
        """Tip should NOT be sent when there are no overdue alerts."""
        from healthbot.bot.scheduler import AlertScheduler

        config = MagicMock(spec=Config)
        config.vault_home = tmp_path
        km = MagicMock()

        scheduler = AlertScheduler(config, km, chat_id=123)
        bot = AsyncMock()

        alerts = [
            Alert("trend", "Glucose Rising", "body1", "urgent", "key1"),
            Alert("trend", "LDL Dropping", "body2", "watch", "key2"),
        ]

        await scheduler._send_alerts(alerts, bot)

        # 2 alerts, no tip (no overdue alerts)
        assert bot.send_message.call_count == 2
        for call in bot.send_message.call_args_list:
            assert "pause notifications" not in call[1]["text"]

    @pytest.mark.asyncio
    async def test_no_tip_when_all_overdue_paused(self, tmp_path) -> None:
        """Tip should NOT be sent when overdue alerts are paused (none sent)."""
        import json
        from datetime import UTC, datetime, timedelta

        from healthbot.bot.scheduler import AlertScheduler

        config = MagicMock(spec=Config)
        config.vault_home = tmp_path
        km = MagicMock()

        scheduler = AlertScheduler(config, km, chat_id=123)
        bot = AsyncMock()

        # Create pause file
        pause_dir = tmp_path / "config"
        pause_dir.mkdir(exist_ok=True)
        deadline = (datetime.now(UTC) + timedelta(days=7)).isoformat()
        (pause_dir / "overdue_pause.json").write_text(
            json.dumps({"paused_until": deadline})
        )

        alerts = [
            Alert("overdue", "TSH", "body", "watch", "key1"),
        ]

        await scheduler._send_alerts(alerts, bot)

        # No messages at all — overdue suppressed, no tip
        assert bot.send_message.call_count == 0


class TestWelcomeBriefing:
    """Tests for _build_welcome_briefing pause state."""

    def test_briefing_shows_paused_until(self, tmp_path) -> None:
        """Welcome briefing should show pause deadline when paused."""
        import json
        from datetime import UTC, datetime, timedelta

        from healthbot.bot.scheduler import AlertScheduler

        config = MagicMock(spec=Config)
        config.vault_home = tmp_path
        config.allowed_user_ids = [123]
        km = MagicMock()
        km.is_unlocked = True

        scheduler = AlertScheduler(config, km, chat_id=123)

        # Create pause file
        pause_dir = tmp_path / "config"
        pause_dir.mkdir(exist_ok=True)
        deadline = datetime.now(UTC) + timedelta(days=14)
        (pause_dir / "overdue_pause.json").write_text(
            json.dumps({"paused_until": deadline.isoformat()})
        )

        with patch.object(scheduler, "_get_db", return_value=MagicMock()):
            briefing = scheduler._build_welcome_briefing()

        assert "paused until" in briefing.lower()
        # Briefing displays in local timezone, so compare local date
        local_deadline = deadline.astimezone()
        assert local_deadline.strftime("%b %d, %Y") in briefing


class TestIngestIncoming:
    """PDF ingestion should move file and send message."""

    @pytest.mark.asyncio
    async def test_ingest_moves_to_processed(self, tmp_path) -> None:
        from healthbot.bot.scheduler import AlertScheduler

        config = MagicMock(spec=Config)
        config.incoming_dir = tmp_path
        config.blobs_dir = tmp_path / "blobs"
        config.ollama_model = "test"
        config.ollama_url = "http://localhost:11434"
        config.ollama_timeout = 30
        config.allowed_user_ids = [123]
        km = MagicMock()
        km.is_unlocked = True

        scheduler = AlertScheduler(config, km, chat_id=123)

        # Create a fake PDF
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"%PDF-1.4 fake")

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.lab_results = []
        mock_result.triage_summary = ""

        bot = AsyncMock()

        with patch(
            "healthbot.ingest.telegram_pdf_ingest.TelegramPdfIngest"
        ) as mock_ingest_cls, \
             patch(
                 "healthbot.bot.scheduler.HealthWatcher"
             ) as mock_watcher, \
             patch("healthbot.ingest.lab_pdf_parser.LabPdfParser"), \
             patch("healthbot.security.pdf_safety.PdfSafety"), \
             patch("healthbot.reasoning.triage.TriageEngine"), \
             patch("healthbot.security.vault.Vault"), \
             patch.object(scheduler, "_get_db", return_value=MagicMock()):
            mock_ingest_cls.return_value.ingest.return_value = mock_result
            mock_watcher.return_value.check_all.return_value = []

            await scheduler._ingest_incoming(pdf_path, bot)

        # PDF should be moved
        assert not pdf_path.exists()
        assert (tmp_path / "processed" / "test.pdf").exists()
