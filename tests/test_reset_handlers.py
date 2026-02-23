"""Tests for healthbot.bot.handlers_reset — /reset and /delete commands."""
from __future__ import annotations

import time
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.bot.handlers_reset import ResetHandlers
from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult, TriageLevel
from healthbot.security.key_manager import KeyManager
from healthbot.security.vault import Vault


def _mock_update(user_id: int = 123, text: str = "") -> MagicMock:
    update = MagicMock()
    update.effective_user.id = user_id
    update.effective_chat = AsyncMock()
    update.effective_chat.id = 456
    update.message.text = text
    update.message.reply_text = AsyncMock()
    update.get_bot.return_value = AsyncMock()
    return update


def _mock_context(args: list[str] | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.args = args or []
    return ctx


def _insert_lab(db: HealthDB) -> str:
    lab = LabResult(
        id="", test_name="glucose", canonical_name="glucose",
        value=100.0, unit="mg/dL",
        date_collected=date(2025, 1, 15),
        triage_level=TriageLevel.NORMAL,
    )
    return db.insert_observation(lab)


@pytest.fixture
def reset_handlers(
    config: Config, key_manager: KeyManager, db: HealthDB, vault: Vault
) -> ResetHandlers:
    db.run_migrations()
    return ResetHandlers(
        config=config,
        key_manager=key_manager,
        get_db=lambda: db,
        get_vault=lambda: vault,
        check_auth=lambda update: True,
    )


class TestResetCommand:
    @pytest.mark.asyncio
    async def test_reset_requires_unlock(
        self, reset_handlers: ResetHandlers, key_manager: KeyManager
    ) -> None:
        key_manager.lock()
        update = _mock_update()
        await reset_handlers.reset(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_reset_empty_vault(self, reset_handlers: ResetHandlers) -> None:
        update = _mock_update()
        await reset_handlers.reset(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "empty" in reply.lower()

    @pytest.mark.asyncio
    async def test_reset_shows_counts(
        self, reset_handlers: ResetHandlers, db: HealthDB
    ) -> None:
        _insert_lab(db)
        update = _mock_update()
        await reset_handlers.reset(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "labs" in reply.lower()
        assert "YES" in reply
        assert reset_handlers.is_awaiting_confirm(123)

    @pytest.mark.asyncio
    async def test_reset_confirm_yes_deletes(
        self, reset_handlers: ResetHandlers, db: HealthDB
    ) -> None:
        _insert_lab(db)
        # Trigger reset prompt
        update = _mock_update()
        await reset_handlers.reset(update, _mock_context())

        # Confirm with YES
        confirm_update = _mock_update(text="YES")
        with patch("healthbot.vault_ops.backup.VaultBackup") as mock_backup:
            mock_backup.return_value.create_backup.return_value = MagicMock(name="backup.enc")
            handled = await reset_handlers.handle_confirm(confirm_update, _mock_context())

        assert handled is True
        assert not reset_handlers.is_awaiting_confirm(123)
        # Verify data is gone
        count = db.conn.execute("SELECT COUNT(*) AS n FROM observations").fetchone()["n"]
        assert count == 0

    @pytest.mark.asyncio
    async def test_reset_confirm_no_cancels(
        self, reset_handlers: ResetHandlers, db: HealthDB
    ) -> None:
        _insert_lab(db)
        update = _mock_update()
        await reset_handlers.reset(update, _mock_context())

        confirm_update = _mock_update(text="no")
        handled = await reset_handlers.handle_confirm(confirm_update, _mock_context())
        assert handled is True
        reply = confirm_update.message.reply_text.call_args[0][0]
        assert "cancelled" in reply.lower()
        # Data should still exist
        count = db.conn.execute("SELECT COUNT(*) AS n FROM observations").fetchone()["n"]
        assert count == 1

    @pytest.mark.asyncio
    async def test_reset_confirm_expires(
        self, reset_handlers: ResetHandlers, db: HealthDB
    ) -> None:
        _insert_lab(db)
        update = _mock_update()
        await reset_handlers.reset(update, _mock_context())

        # Expire the confirmation
        reset_handlers._pending[123]["expires"] = time.time() - 1
        assert not reset_handlers.is_awaiting_confirm(123)


class TestDeleteCommand:
    @pytest.mark.asyncio
    async def test_delete_no_args_shows_usage(
        self, reset_handlers: ResetHandlers
    ) -> None:
        update = _mock_update()
        await reset_handlers.delete(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "usage" in reply.lower()

    @pytest.mark.asyncio
    async def test_delete_invalid_category(
        self, reset_handlers: ResetHandlers
    ) -> None:
        update = _mock_update()
        await reset_handlers.delete(update, _mock_context(["bogus"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "unknown" in reply.lower()

    @pytest.mark.asyncio
    async def test_delete_labs_asks_confirm(
        self, reset_handlers: ResetHandlers, db: HealthDB
    ) -> None:
        _insert_lab(db)
        update = _mock_update()
        await reset_handlers.delete(update, _mock_context(["labs"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "YES" in reply
        assert reset_handlers.is_awaiting_confirm(123)

    @pytest.mark.asyncio
    async def test_delete_labs_confirm_yes(
        self, reset_handlers: ResetHandlers, db: HealthDB
    ) -> None:
        _insert_lab(db)
        update = _mock_update()
        await reset_handlers.delete(update, _mock_context(["labs"]))

        confirm = _mock_update(text="YES")
        handled = await reset_handlers.handle_confirm(confirm, _mock_context())
        assert handled is True
        count = db.conn.execute("SELECT COUNT(*) AS n FROM observations").fetchone()["n"]
        assert count == 0

    @pytest.mark.asyncio
    async def test_delete_empty_category(
        self, reset_handlers: ResetHandlers
    ) -> None:
        update = _mock_update()
        await reset_handlers.delete(update, _mock_context(["labs"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "no" in reply.lower()

    @pytest.mark.asyncio
    async def test_delete_requires_unlock(
        self, reset_handlers: ResetHandlers, key_manager: KeyManager
    ) -> None:
        key_manager.lock()
        update = _mock_update()
        await reset_handlers.delete(update, _mock_context(["labs"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()
