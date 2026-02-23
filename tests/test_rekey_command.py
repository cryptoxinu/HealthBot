"""Tests for /rekey command handler — two-step interactive flow."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.config import Config


class TestRekeyCommand:
    """Tests for the two-step rekey handler."""

    @pytest.mark.asyncio
    async def test_rekey_starts_flow(self) -> None:
        """'/rekey' begins the two-step flow, asking for current passphrase."""
        from healthbot.bot.handlers_session import SessionHandlers

        core = MagicMock()
        core._km.is_unlocked = True
        handlers = SessionHandlers(core)

        update = MagicMock()
        update.effective_user.id = 1
        update.message.reply_text = AsyncMock()
        context = MagicMock()
        context.args = []

        await handlers.rekey(update, context)

        call_text = update.message.reply_text.call_args[0][0]
        assert "current passphrase" in call_text.lower()
        assert handlers._rekey_awaiting[1] == 1

    @pytest.mark.asyncio
    async def test_rekey_step1_wrong_pass_aborts(self) -> None:
        """Wrong current passphrase in step 1 aborts the flow."""
        from healthbot.bot.handlers_session import SessionHandlers

        core = MagicMock()
        core._km.is_unlocked = True
        core._km.verify_passphrase.return_value = False
        handlers = SessionHandlers(core)
        handlers._rekey_awaiting[1] = 1

        update = MagicMock()
        update.effective_user.id = 1
        update.message.text = "wrong-pass"
        update.message.delete = AsyncMock()
        update.message.reply_text = AsyncMock()
        update.effective_chat.send_message = AsyncMock()
        context = MagicMock()

        result = await handlers.handle_rekey_input(update, context)

        assert result is True
        update.message.delete.assert_called_once()
        call_text = update.effective_chat.send_message.call_args[0][0]
        assert "incorrect" in call_text.lower()
        assert 1 not in handlers._rekey_awaiting

    @pytest.mark.asyncio
    async def test_rekey_step1_correct_advances(self) -> None:
        """Correct current passphrase advances to step 2."""
        from healthbot.bot.handlers_session import SessionHandlers

        core = MagicMock()
        core._km.is_unlocked = True
        core._km.verify_passphrase.return_value = True
        handlers = SessionHandlers(core)
        handlers._rekey_awaiting[1] = 1

        update = MagicMock()
        update.effective_user.id = 1
        update.message.text = "correct-pass"
        update.message.delete = AsyncMock()
        update.message.reply_text = AsyncMock()
        update.effective_chat.send_message = AsyncMock()
        context = MagicMock()

        result = await handlers.handle_rekey_input(update, context)

        assert result is True
        update.message.delete.assert_called_once()
        assert handlers._rekey_awaiting[1] == 2
        call_text = update.effective_chat.send_message.call_args[0][0]
        assert "new passphrase" in call_text.lower()

    @pytest.mark.asyncio
    async def test_rekey_step2_calls_rotate(self, tmp_path: Path) -> None:
        """Step 2 calls VaultRekey.rotate with the new passphrase."""
        from healthbot.bot.handlers_session import SessionHandlers

        core = MagicMock()
        core._km.is_unlocked = True
        core._config = MagicMock(spec=Config)
        handlers = SessionHandlers(core)
        handlers._rekey_awaiting[1] = 2

        update = MagicMock()
        update.effective_user.id = 1
        update.message.text = "new-secret-pass"
        update.message.delete = AsyncMock()
        update.message.reply_text = AsyncMock()
        update.effective_chat = AsyncMock()
        context = MagicMock()

        backup_path = tmp_path / "backup_20250615.enc"
        backup_path.touch()

        with patch("healthbot.vault_ops.rekey.VaultRekey") as mock_cls:
            mock_cls.return_value.rotate.return_value = backup_path
            result = await handlers.handle_rekey_input(update, context)

        assert result is True
        update.message.delete.assert_called_once()
        mock_cls.return_value.rotate.assert_called_once_with("new-secret-pass")
        assert 1 not in handlers._rekey_awaiting

    @pytest.mark.asyncio
    async def test_cancel_during_rekey_aborts(self) -> None:
        """Sending /cancel during the flow aborts."""
        from healthbot.bot.handlers_session import SessionHandlers

        core = MagicMock()
        core._km.is_unlocked = True
        handlers = SessionHandlers(core)
        handlers._rekey_awaiting[1] = 1

        update = MagicMock()
        update.effective_user.id = 1
        update.message.text = "/cancel"
        update.message.delete = AsyncMock()
        update.message.reply_text = AsyncMock()
        context = MagicMock()

        result = await handlers.handle_rekey_input(update, context)

        assert result is True
        assert 1 not in handlers._rekey_awaiting
        call_text = update.message.reply_text.call_args[0][0]
        assert "cancel" in call_text.lower()

    def test_is_awaiting_rekey(self) -> None:
        """is_awaiting_rekey correctly reports state."""
        from healthbot.bot.handlers_session import SessionHandlers

        core = MagicMock()
        handlers = SessionHandlers(core)
        assert not handlers.is_awaiting_rekey(1)
        handlers._rekey_awaiting[1] = 1
        assert handlers.is_awaiting_rekey(1)

    def test_vault_lock_clears_rekey(self) -> None:
        """Vault lock clears rekey state."""
        from healthbot.bot.handlers_session import SessionHandlers

        core = MagicMock()
        handlers = SessionHandlers(core)
        handlers._rekey_awaiting[1] = 2
        handlers._rekey_awaiting.clear()
        assert 1 not in handlers._rekey_awaiting
