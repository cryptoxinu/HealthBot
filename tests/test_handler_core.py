"""Tests for healthbot.bot.handler_core — HandlerCore shared state."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from healthbot.bot.handler_core import HandlerCore
from healthbot.config import Config
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall


def _make_core(config: Config, key_manager: KeyManager) -> HandlerCore:
    fw = PhiFirewall()
    return HandlerCore(config, key_manager, fw)


class TestGetDb:
    def test_lazy_creates_db(self, config: Config, key_manager: KeyManager) -> None:
        core = _make_core(config, key_manager)
        assert core._db is None
        db = core._get_db()
        assert db is not None
        assert core._db is db

    def test_reuses_existing_db(self, config: Config, key_manager: KeyManager) -> None:
        core = _make_core(config, key_manager)
        db1 = core._get_db()
        db2 = core._get_db()
        assert db1 is db2

    def test_recreates_after_close(self, config: Config, key_manager: KeyManager) -> None:
        core = _make_core(config, key_manager)
        db1 = core._get_db()
        db1.close()
        db2 = core._get_db()
        assert db2 is not db1


class TestCheckAuth:
    def test_allows_when_no_allowlist(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = []
        core = _make_core(config, key_manager)
        update = MagicMock()
        update.effective_user.id = 999
        assert core._check_auth(update) is True

    def test_allows_listed_user(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = [123]
        core = _make_core(config, key_manager)
        update = MagicMock()
        update.effective_user.id = 123
        assert core._check_auth(update) is True

    def test_blocks_unlisted_user(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = [123]
        core = _make_core(config, key_manager)
        update = MagicMock()
        update.effective_user.id = 999
        assert core._check_auth(update) is False

    def test_blocks_when_no_effective_user(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = [123]
        core = _make_core(config, key_manager)
        update = MagicMock()
        update.effective_user = None
        assert core._check_auth(update) is False


class TestOnVaultLock:
    def test_clears_claude_conversation(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        core = _make_core(config, key_manager)
        mock_conv = MagicMock()
        core._claude_conversation = mock_conv
        core._on_vault_lock()
        assert core._claude_conversation is None
        mock_conv.save_state.assert_called_once()
        mock_conv.clear.assert_called_once()

    def test_lock_calls_scheduler_on_lock(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        core = _make_core(config, key_manager)
        core._scheduler = MagicMock()
        core._on_vault_lock()
        core._scheduler.on_lock.assert_called_once()

    def test_lock_closes_db(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        core = _make_core(config, key_manager)
        mock_db = MagicMock()
        core._db = mock_db
        core._on_vault_lock()
        mock_db.close.assert_called_once()
        assert core._db is None

    def test_lock_clears_error_buffer(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        core = _make_core(config, key_manager)
        core._error_buffer.append(MagicMock())
        core._on_vault_lock()
        assert len(core._error_buffer) == 0


class TestHandleMessage:
    @pytest.mark.asyncio
    async def test_delegates_to_router(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        core = _make_core(config, key_manager)
        core._router.handle_message = AsyncMock()
        update = MagicMock()
        context = MagicMock()
        await core.handle_message(update, context)
        core._router.handle_message.assert_called_once_with(update, context)
