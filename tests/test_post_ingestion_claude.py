"""Tests for post-ingestion Claude analysis triggers."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


class TestPostIngestionClaude:
    """Verify Claude analysis is triggered after data ingestion."""

    @pytest.mark.asyncio
    async def test_post_ingestion_lab_triggers_claude(self) -> None:
        """After lab PDF ingestion, Claude should be called with analysis prompt."""
        from healthbot.bot.message_router import MessageRouter
        from healthbot.config import Config

        config = MagicMock(spec=Config)
        km = MagicMock()
        km.is_unlocked = True

        router = MessageRouter(config, km, MagicMock(), MagicMock())

        # Mock Claude conversation manager
        mock_claude = MagicMock()
        mock_claude.handle_message.return_value = ("Analysis complete.", [])
        router._get_claude = lambda: mock_claude

        # Create mock update
        update = MagicMock()
        update.effective_chat = AsyncMock()
        update.effective_chat.send_action = AsyncMock()
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()

        # Simulate post-ingestion analysis
        lab_results = [
            {"test_name": "Glucose", "value": 110, "unit": "mg/dL", "flag": "HIGH"},
            {"test_name": "HbA1c", "value": 5.8, "unit": "%", "flag": ""},
        ]

        await router._post_ingestion_analysis(update, 123, lab_results)

        # Verify Claude was called
        mock_claude.handle_message.assert_called_once()
        call_args = mock_claude.handle_message.call_args[0][0]
        assert "New lab results just arrived" in call_args
        assert "Glucose" in call_args

    @pytest.mark.asyncio
    async def test_post_ingestion_skips_when_no_claude(self) -> None:
        """Post-ingestion should skip gracefully when Claude unavailable."""
        from healthbot.bot.message_router import MessageRouter
        from healthbot.config import Config

        config = MagicMock(spec=Config)
        km = MagicMock()
        router = MessageRouter(config, km, MagicMock(), MagicMock())
        router._get_claude = lambda: None

        update = MagicMock()
        update.message = AsyncMock()

        # Should not crash
        await router._post_ingestion_analysis(update, 123, [])

    @pytest.mark.asyncio
    async def test_post_ingestion_apple_health_triggers_claude(self) -> None:
        """After Apple Health import, Claude should be called."""
        from healthbot.bot.message_router import MessageRouter
        from healthbot.config import Config

        config = MagicMock(spec=Config)
        km = MagicMock()
        router = MessageRouter(config, km, MagicMock(), MagicMock())

        mock_claude = MagicMock()
        mock_claude.handle_message.return_value = ("Analysis.", [])
        router._get_claude = lambda: mock_claude

        update = MagicMock()
        update.effective_chat = AsyncMock()
        update.effective_chat.send_action = AsyncMock()
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()

        await router._post_ingestion_health_analysis(
            update, 123, 50, {"heart_rate": 30, "steps": 20},
        )

        mock_claude.handle_message.assert_called_once()
        call_args = mock_claude.handle_message.call_args[0][0]
        assert "Apple Health" in call_args
        assert "50 records" in call_args

    @pytest.mark.asyncio
    async def test_post_ingestion_genetic_triggers_claude(self) -> None:
        """After genetic data upload, Claude should be called."""
        from healthbot.bot.message_router import MessageRouter
        from healthbot.config import Config

        config = MagicMock(spec=Config)
        km = MagicMock()
        router = MessageRouter(config, km, MagicMock(), MagicMock())

        mock_claude = MagicMock()
        mock_claude.handle_message.return_value = ("Risk analysis.", [])
        router._get_claude = lambda: mock_claude

        update = MagicMock()
        update.effective_chat = AsyncMock()
        update.effective_chat.send_action = AsyncMock()
        update.message = AsyncMock()
        update.message.reply_text = AsyncMock()

        await router._post_ingestion_genetic_analysis(
            update, 123, 650000, "TellMeGen",
        )

        mock_claude.handle_message.assert_called_once()
        call_args = mock_claude.handle_message.call_args[0][0]
        assert "genetic data" in call_args.lower()
        assert "650,000" in call_args
