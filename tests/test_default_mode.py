"""Tests for Claude CLI as default conversation engine."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.config import Config


class TestClaudeCLIDefault:
    """Verify Claude CLI is always the conversation engine."""

    def test_handler_core_init_has_no_mode(self):
        from healthbot.bot.handler_core import HandlerCore
        from healthbot.security.key_manager import KeyManager
        from healthbot.security.phi_firewall import PhiFirewall

        config = Config()
        km = MagicMock(spec=KeyManager)
        fw = MagicMock(spec=PhiFirewall)
        core = HandlerCore(config, km, fw)
        assert not hasattr(core, "_mode")

    def test_handler_core_has_claude_conversation(self):
        from healthbot.bot.handler_core import HandlerCore
        from healthbot.security.key_manager import KeyManager
        from healthbot.security.phi_firewall import PhiFirewall

        config = Config()
        km = MagicMock(spec=KeyManager)
        fw = MagicMock(spec=PhiFirewall)
        core = HandlerCore(config, km, fw)
        assert core._claude_conversation is None

    def test_lock_clears_claude_conversation(self):
        from healthbot.bot.handler_core import HandlerCore
        from healthbot.security.key_manager import KeyManager
        from healthbot.security.phi_firewall import PhiFirewall

        config = Config()
        km = MagicMock(spec=KeyManager)
        fw = MagicMock(spec=PhiFirewall)
        core = HandlerCore(config, km, fw)
        mock_conv = MagicMock()
        core._claude_conversation = mock_conv
        core._on_vault_lock()
        assert core._claude_conversation is None
        mock_conv.save_state.assert_called_once()
        mock_conv.clear.assert_called_once()

    def test_config_has_no_default_mode(self):
        config = Config()
        assert not hasattr(config, "default_mode")
