"""Tests for healthbot.bot.handlers_onboard — /onboard command."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from healthbot.bot.handlers_onboard import OnboardHandlers
from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.security.key_manager import KeyManager


def _make_handlers(
    config: Config, key_manager: KeyManager, db: HealthDB
) -> OnboardHandlers:
    db.run_migrations()
    return OnboardHandlers(
        config=config,
        key_manager=key_manager,
        get_db=lambda: db,
        check_auth=lambda update: True,
    )


def _mock_update(user_id: int = 123, text: str = "") -> MagicMock:
    update = MagicMock()
    update.effective_user.id = user_id
    update.message.text = text
    update.message.reply_text = AsyncMock()
    return update


def _mock_context() -> MagicMock:
    ctx = MagicMock()
    ctx.args = []
    return ctx


class TestOnboard:
    @pytest.mark.asyncio
    async def test_onboard_requires_unlock(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        key_manager.lock()
        handlers = _make_handlers(config, key_manager, db)
        update = _mock_update()
        await handlers.onboard(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_onboard_requires_auth(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = OnboardHandlers(
            config=config,
            key_manager=key_manager,
            get_db=lambda: db,
            check_auth=lambda update: False,
        )
        update = _mock_update()
        await handlers.onboard(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "unauthorized" in reply.lower()

    @pytest.mark.asyncio
    async def test_onboard_starts_interview(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager, db)
        update = _mock_update(user_id=42)
        await handlers.onboard(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "health profile" in reply.lower()

    @pytest.mark.asyncio
    async def test_is_active_before_start(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager, db)
        assert not handlers.is_active(123)


class TestHandleAnswer:
    @pytest.mark.asyncio
    async def test_handle_answer_when_not_active(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager, db)
        update = _mock_update(text="some answer")
        result = await handlers.handle_answer(update, _mock_context())
        assert result is False

    @pytest.mark.asyncio
    async def test_handle_answer_during_session(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager, db)
        # Start onboarding first
        update = _mock_update(user_id=42)
        await handlers.onboard(update, _mock_context())

        # Now answer
        answer_update = _mock_update(user_id=42, text="35")
        result = await handlers.handle_answer(answer_update, _mock_context())
        assert result is True
        answer_update.message.reply_text.assert_called()


class TestOnVaultLock:
    @pytest.mark.asyncio
    async def test_clears_sessions(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager, db)
        # Start a session
        update = _mock_update(user_id=42)
        await handlers.onboard(update, _mock_context())
        assert handlers.is_active(42)

        # Lock
        handlers.on_vault_lock()
        assert not handlers.is_active(42)
        assert handlers._engine is None
