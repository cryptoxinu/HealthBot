"""Tests for healthbot.bot.handlers_session — session lifecycle commands."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.handlers_session import SessionHandlers
from healthbot.config import Config
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall


def _make_handlers(config: Config, key_manager: KeyManager) -> SessionHandlers:
    core = HandlerCore(config, key_manager, PhiFirewall())
    return SessionHandlers(core)


def _mock_update(user_id: int = 123) -> MagicMock:
    update = MagicMock()
    update.effective_user.id = user_id
    update.effective_chat.id = 456
    update.effective_chat.send_message = AsyncMock()
    update.effective_chat.send_action = AsyncMock()
    update.message.reply_text = AsyncMock()
    update.message.delete = AsyncMock()
    update.message.text = ""
    return update


def _mock_context(args: list[str] | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.args = args or []
    ctx.bot = AsyncMock()
    return ctx


class TestStart:
    @pytest.mark.asyncio
    async def test_start_shows_welcome(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = []
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.start(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        # Vault is unlocked (fixture runs setup), so returns returning-unlocked variant
        assert "Welcome back" in reply
        assert "unlocked" in reply.lower()

    @pytest.mark.asyncio
    async def test_start_first_time(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        """First-time user (no manifest) sees setup instructions."""
        config.allowed_user_ids = []
        key_manager.lock()
        # Remove manifest to simulate first-time user
        if config.manifest_path.exists():
            config.manifest_path.unlink()
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.start(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "HealthBot" in reply
        assert "/unlock" in reply

    @pytest.mark.asyncio
    async def test_start_locked_shows_unlock(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        """Returning user with locked vault sees unlock prompt."""
        config.allowed_user_ids = []
        key_manager.lock()
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.start(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()
        assert "/unlock" in reply

    @pytest.mark.asyncio
    async def test_start_blocked_for_unauthorized(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = [999]
        handlers = _make_handlers(config, key_manager)
        update = _mock_update(user_id=123)
        await handlers.start(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "unauthorized" in reply.lower()

    @pytest.mark.asyncio
    async def test_start_unlocked_shows_quick_actions(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = []
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.start(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "health question" in reply.lower()


class TestUnlock:
    @pytest.mark.asyncio
    async def test_unlock_prompts_passphrase(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = []
        handlers = _make_handlers(config, key_manager)
        update = _mock_update(user_id=42)
        await handlers.unlock(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "passphrase" in reply.lower()
        assert 42 in handlers._core._router.awaiting_passphrase


class TestLock:
    @pytest.mark.asyncio
    async def test_lock_locks_vault(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        assert key_manager.is_unlocked
        update = _mock_update()
        await handlers.lock(update, _mock_context())
        assert not key_manager.is_unlocked
        # Lock now sends via chat.send_message (after chat wipe)
        sent = update.effective_chat.send_message.call_args[0][0]
        assert "locked" in sent.lower()

    @pytest.mark.asyncio
    async def test_lock_closes_db(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        handlers._core._db = MagicMock()
        update = _mock_update()
        await handlers.lock(update, _mock_context())
        assert handlers._core._db is None


class TestFeedback:
    @pytest.mark.asyncio
    async def test_feedback_no_args_shows_usage(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = []
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.feedback(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "usage" in reply.lower()

    @pytest.mark.asyncio
    async def test_feedback_captures(
        self, config: Config, key_manager: KeyManager, tmp_path
    ) -> None:
        config.allowed_user_ids = []
        handlers = _make_handlers(config, key_manager)
        handlers._core._last_user_input = "test question"
        handlers._core._last_bot_response = "test response"
        update = _mock_update()
        eval_dir = tmp_path / ".healthbot" / "eval"
        with patch("pathlib.Path.home", return_value=tmp_path):
            await handlers.feedback(update, _mock_context(["wrong", "info"]))
        # Verify JSONL file was written
        jsonl = eval_dir / "failing_cases.jsonl"
        assert jsonl.exists()
        import json
        entry = json.loads(jsonl.read_text().strip())
        assert entry["user_feedback"] == "wrong info"
        assert entry["input"] == "test question"
        assert entry["bot_response"] == "test response"
        reply = update.message.reply_text.call_args[0][0]
        assert "captured" in reply.lower()


class TestBackup:
    @pytest.mark.asyncio
    async def test_backup_creates_backup(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        mock_path = MagicMock()
        mock_path.name = "backup_20250615.enc"
        with patch("healthbot.vault_ops.backup.VaultBackup") as mock_cls:
            mock_cls.return_value.create_backup.return_value = mock_path
            await handlers.backup(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("backup" in t.lower() for t in texts)

    @pytest.mark.asyncio
    async def test_backup_requires_unlock(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        key_manager.lock()
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.backup(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()


class TestAudit:
    @pytest.mark.asyncio
    async def test_audit_runs(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = []
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        mock_report = MagicMock()
        mock_report.format.return_value = "All clear"
        with patch("healthbot.security.audit.VaultAuditor") as mock_cls:
            mock_cls.return_value.run_all.return_value = mock_report
            await handlers.audit(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("clear" in t.lower() or "audit" in t.lower() for t in texts)

    @pytest.mark.asyncio
    async def test_audit_includes_integrity_when_unlocked(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        config.allowed_user_ids = []
        key_manager._is_unlocked = True
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        mock_report = MagicMock()
        mock_report.format.return_value = "Security OK"
        mock_integrity = MagicMock()
        mock_integrity.format_report.return_value = "Integrity: PASS\n  No issues found."
        with (
            patch("healthbot.security.audit.VaultAuditor") as mock_auditor,
            patch("healthbot.vault_ops.integrity_check.IntegrityChecker") as mock_checker,
        ):
            mock_auditor.return_value.run_all.return_value = mock_report
            mock_checker.return_value.check_all.return_value = MagicMock()
            mock_checker.return_value.format_report.return_value = (
                "Integrity Check: PASS\n  No issues found."
            )
            await handlers.audit(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("integrity" in t.lower() for t in texts)
