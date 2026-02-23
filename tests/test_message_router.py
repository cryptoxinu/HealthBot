"""Tests for bot/message_router.py — routing logic."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from healthbot.bot.message_router import MessageRouter
from healthbot.config import Config
from healthbot.security.key_manager import KeyManager


@pytest.fixture
def router(config: Config, key_manager: KeyManager, db) -> MessageRouter:
    return MessageRouter(
        config=config,
        key_manager=key_manager,
        get_db=lambda: db,

        check_auth=lambda update: True,
    )


def _mock_update(text: str = "", user_id: int = 123, has_doc: bool = False):
    update = MagicMock()
    update.effective_user.id = user_id
    update.effective_chat.id = 456
    update.effective_chat.send_message = AsyncMock()
    update.message.text = text
    update.message.message_id = 100
    update.message.reply_text = AsyncMock()
    update.message.delete = AsyncMock()
    update.message.photo = None  # Explicitly unset photo
    if has_doc:
        update.message.document = MagicMock()
        update.message.document.file_name = "results.pdf"
        update.message.document.file_id = "file_123"
    else:
        update.message.document = None
    return update


def _mock_context():
    ctx = MagicMock()
    ctx.args = []
    ctx.bot = MagicMock()
    ctx.bot.get_file = AsyncMock()
    return ctx


class TestPassphraseHandling:
    @pytest.mark.asyncio
    async def test_awaiting_passphrase_unlocks(self, router):
        router._awaiting_passphrase.add(123)
        router._km.lock()

        update = _mock_update(text="test-passphrase-do-not-use-in-production")
        ctx = _mock_context()

        await router.handle_message(update, ctx)

        # Should have deleted the message
        update.message.delete.assert_called_once()
        # Should have sent unlock confirmation (may be followed by onboard prompt)
        update.effective_chat.send_message.assert_called()
        msgs = [
            call[0][0].lower()
            for call in update.effective_chat.send_message.call_args_list
        ]
        assert any("unlocked" in m for m in msgs)
        # No longer awaiting passphrase
        assert 123 not in router._awaiting_passphrase

    @pytest.mark.asyncio
    async def test_wrong_passphrase_rejects(self, router):
        router._awaiting_passphrase.add(123)
        router._km.lock()

        update = _mock_update(text="wrong-pass")
        ctx = _mock_context()

        await router.handle_message(update, ctx)

        msg = update.effective_chat.send_message.call_args[0][0]
        assert "invalid" in msg.lower()

    @pytest.mark.asyncio
    async def test_on_unlock_callback_called(self, router):
        callback = AsyncMock()
        router.set_on_unlock(callback)
        router._awaiting_passphrase.add(123)
        router._km.lock()

        update = _mock_update(text="test-passphrase-do-not-use-in-production")
        ctx = _mock_context()

        await router.handle_message(update, ctx)

        callback.assert_called_once()


class TestDocumentHandling:
    @pytest.mark.asyncio
    async def test_locked_vault_rejects_document(self, router):
        router._km.lock()
        update = _mock_update(has_doc=True)
        ctx = _mock_context()

        await router.handle_message(update, ctx)

        update.message.reply_text.assert_called()
        msg = update.message.reply_text.call_args[0][0]
        assert "locked" in msg.lower()

    @pytest.mark.asyncio
    async def test_non_pdf_rejected(self, router):
        update = _mock_update(has_doc=True)
        update.message.document.file_name = "data.docx"
        ctx = _mock_context()

        await router.handle_message(update, ctx)

        update.message.reply_text.assert_called()
        msg = update.message.reply_text.call_args[0][0]
        assert "pdf" in msg.lower()


class TestFreeTextRouting:
    @pytest.mark.asyncio
    async def test_locked_vault_sends_unlock_prompt(self, router):
        router._km.lock()
        update = _mock_update(text="How is my glucose?")
        ctx = _mock_context()

        await router.handle_message(update, ctx)

        update.message.reply_text.assert_called()
        msg = update.message.reply_text.call_args[0][0]
        assert "locked" in msg.lower()

    @pytest.mark.asyncio
    async def test_no_claude_shows_install_message(self, router):
        update = _mock_update(text="Tell me about my health")
        ctx = _mock_context()

        await router.handle_message(update, ctx)

        update.message.reply_text.assert_called()
        msg = update.message.reply_text.call_args[0][0]
        assert "claude" in msg.lower()

    @pytest.mark.asyncio
    async def test_auth_check_failure_returns_early(self, config, key_manager, db):
        router = MessageRouter(
            config=config,
            key_manager=key_manager,
            get_db=lambda: db,
            check_auth=lambda update: False,
        )
        update = _mock_update(text="test")
        ctx = _mock_context()

        await router.handle_message(update, ctx)

        # No reply should be sent
        update.message.reply_text.assert_not_called()
        update.effective_chat.send_message.assert_not_called()


class TestOnVaultLock:
    def test_clears_passphrase_state(self, router):
        router._awaiting_passphrase.add(123)
        router.on_vault_lock()
        assert len(router._awaiting_passphrase) == 0

    def test_empty_state_doesnt_crash(self, router):
        router.on_vault_lock()  # Should not raise


class TestResetConfirmInterception:
    @pytest.mark.asyncio
    async def test_routes_to_reset_handler(self, router):
        mock_reset = MagicMock()
        mock_reset.is_awaiting_confirm.return_value = True
        mock_reset.handle_confirm = AsyncMock(return_value=True)
        router._reset_handlers = mock_reset

        update = _mock_update(text="YES")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_reset.handle_confirm.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_when_not_awaiting(self, router):
        mock_reset = MagicMock()
        mock_reset.is_awaiting_confirm.return_value = False
        router._reset_handlers = mock_reset

        update = _mock_update(text="YES")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_reset.handle_confirm.assert_not_called()


class TestOnboardInterception:
    @pytest.mark.asyncio
    async def test_routes_to_onboard_handler(self, router):
        mock_onboard = MagicMock()
        mock_onboard.is_active.return_value = True
        mock_onboard.handle_answer = AsyncMock(return_value=True)
        router._onboard_handlers = mock_onboard

        update = _mock_update(text="John Doe")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_onboard.handle_answer.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_when_not_active(self, router):
        mock_onboard = MagicMock()
        mock_onboard.is_active.return_value = False
        router._onboard_handlers = mock_onboard

        update = _mock_update(text="answer")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_onboard.handle_answer.assert_not_called()


class TestPhotoRouting:
    @pytest.mark.asyncio
    async def test_locked_vault_rejects_photo(self, router):
        router._km.lock()
        update = _mock_update()
        update.message.photo = [MagicMock()]
        update.message.document = None
        ctx = _mock_context()

        await router.handle_message(update, ctx)

        update.message.reply_text.assert_called()
        msg = update.message.reply_text.call_args[0][0]
        assert "locked" in msg.lower()


class TestVaultLockOnboard:
    def test_on_vault_lock_calls_onboard(self, router):
        mock_onboard = MagicMock()
        router._onboard_handlers = mock_onboard
        router.on_vault_lock()
        mock_onboard.on_vault_lock.assert_called_once()


class TestAwaitingPassphrase:
    def test_exposed_set(self, router):
        assert isinstance(router.awaiting_passphrase, set)
        router.awaiting_passphrase.add(999)
        assert 999 in router._awaiting_passphrase


class TestPauseOverdueRouting:
    @pytest.mark.asyncio
    async def test_pause_notifications_routes(self, router):
        mock_session = MagicMock()
        mock_session.pause_overdue = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="pause notifications for 2 weeks")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.pause_overdue.assert_called_once()

    @pytest.mark.asyncio
    async def test_snooze_alerts_routes(self, router):
        mock_session = MagicMock()
        mock_session.pause_overdue = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="snooze overdue alerts for 3 days")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.pause_overdue.assert_called_once()

    @pytest.mark.asyncio
    async def test_pause_them_for_routes(self, router):
        mock_session = MagicMock()
        mock_session.pause_overdue = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="pause them for 1 month")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.pause_overdue.assert_called_once()
        # Check duration_text is "1 month"
        _, _, duration_text = mock_session.pause_overdue.call_args[0]
        assert "1 month" in duration_text

    @pytest.mark.asyncio
    async def test_pause_no_duration_defaults(self, router):
        mock_session = MagicMock()
        mock_session.pause_overdue = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="mute notifications")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.pause_overdue.assert_called_once()
        _, _, duration_text = mock_session.pause_overdue.call_args[0]
        assert duration_text is None

    @pytest.mark.asyncio
    async def test_pause_requires_unlock(self, router):
        router._km.lock()
        mock_session = MagicMock()
        mock_session.pause_overdue = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="pause notifications for 2 weeks")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.pause_overdue.assert_not_called()


class TestUnpauseOverdueRouting:
    @pytest.mark.asyncio
    async def test_unpause_notifications_routes(self, router):
        mock_session = MagicMock()
        mock_session.unpause_overdue = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="unpause notifications")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.unpause_overdue.assert_called_once()

    @pytest.mark.asyncio
    async def test_resume_them_routes(self, router):
        mock_session = MagicMock()
        mock_session.unpause_overdue = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="resume them")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.unpause_overdue.assert_called_once()

    @pytest.mark.asyncio
    async def test_unmute_it_routes(self, router):
        mock_session = MagicMock()
        mock_session.unpause_overdue = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="unmute it")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.unpause_overdue.assert_called_once()

    @pytest.mark.asyncio
    async def test_unpause_requires_unlock(self, router):
        router._km.lock()
        mock_session = MagicMock()
        mock_session.unpause_overdue = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="unpause notifications")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.unpause_overdue.assert_not_called()


class TestRestartRouting:
    @pytest.mark.asyncio
    async def test_restart_bot_routes(self, router):
        mock_session = MagicMock()
        mock_session.restart = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="restart bot")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.restart.assert_called_once()

    @pytest.mark.asyncio
    async def test_reboot_routes(self, router):
        mock_session = MagicMock()
        mock_session.restart = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="reboot")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.restart.assert_called_once()

    @pytest.mark.asyncio
    async def test_restart_no_unlock_required(self, router):
        """Restart should work even when vault is locked."""
        router._km.lock()
        mock_session = MagicMock()
        mock_session.restart = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="restart bot")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.restart.assert_called_once()

    @pytest.mark.asyncio
    async def test_restart_false_positive_rejected(self, router):
        """'restart my sleep schedule' should NOT trigger restart."""
        mock_session = MagicMock()
        mock_session.restart = AsyncMock()
        mock_session.is_awaiting_claude_auth = MagicMock(return_value=False)
        mock_session.is_awaiting_rekey = MagicMock(return_value=False)
        router._session_handlers = mock_session

        update = _mock_update(text="restart my sleep schedule")
        ctx = _mock_context()
        await router.handle_message(update, ctx)
        mock_session.restart.assert_not_called()
