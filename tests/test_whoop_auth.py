"""Tests for /whoop_auth OAuth flow and oauth_callback server."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.handlers_data import DataHandlers
from healthbot.bot.oauth_callback import wait_for_oauth_callback
from healthbot.config import Config
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall


def _make_handlers(config: Config, key_manager: KeyManager) -> DataHandlers:
    core = HandlerCore(config, key_manager, PhiFirewall())
    return DataHandlers(core)


def _mock_update(user_id: int = 123) -> MagicMock:
    update = MagicMock()
    update.effective_user.id = user_id
    update.effective_chat.send_action = AsyncMock()
    update.effective_chat.send_message = AsyncMock()
    update.message.reply_text = AsyncMock()
    return update


def _mock_context() -> MagicMock:
    ctx = MagicMock()
    ctx.args = []
    return ctx


def _keychain_with_whoop() -> MagicMock:
    kc = MagicMock()
    kc.retrieve.side_effect = lambda k: {
        "whoop_client_id": "243b6009-5d35-4f44-8b19-df5167cf3852",
        "whoop_client_secret": "abcdef1234567890abcdef1234567890ab",
    }.get(k)
    return kc


# ── OAuth callback server ──────────────────────────────────────


class TestOAuthCallback:
    @pytest.mark.asyncio
    async def test_captures_code_and_state(self):
        async def _send_callback():
            await asyncio.sleep(0.1)
            reader, writer = await asyncio.open_connection("127.0.0.1", 18765)
            writer.write(
                b"GET /callback?code=abc123&state=xyz789 HTTP/1.1\r\n"
                b"Host: localhost\r\n\r\n"
            )
            await writer.drain()
            await reader.read(4096)
            writer.close()

        task = asyncio.create_task(_send_callback())
        result = await wait_for_oauth_callback(port=18765, timeout=5)
        await task

        assert result["code"] == "abc123"
        assert result["state"] == "xyz789"
        assert result["error"] is None

    @pytest.mark.asyncio
    async def test_captures_error(self):
        async def _send_callback():
            await asyncio.sleep(0.1)
            reader, writer = await asyncio.open_connection("127.0.0.1", 18766)
            writer.write(
                b"GET /callback?error=access_denied HTTP/1.1\r\n"
                b"Host: localhost\r\n\r\n"
            )
            await writer.drain()
            await reader.read(4096)
            writer.close()

        task = asyncio.create_task(_send_callback())
        result = await wait_for_oauth_callback(port=18766, timeout=5)
        await task

        assert result["error"] == "access_denied"
        assert result["code"] is None

    @pytest.mark.asyncio
    async def test_timeout(self):
        with pytest.raises(asyncio.TimeoutError):
            await wait_for_oauth_callback(port=18767, timeout=0.2)


# ── /whoop_auth handler ────────────────────────────────────────


class TestWhoopAuth:
    @pytest.mark.asyncio
    async def test_requires_unlock(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        key_manager.lock()
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.whoop_auth(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_missing_client_id_starts_setup(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = MagicMock()
        kc.retrieve.return_value = None
        with patch("healthbot.security.keychain.Keychain", return_value=kc):
            await handlers.whoop_auth(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "connect your whoop" in reply.lower()
        assert "client id" in reply.lower()
        assert handlers.is_awaiting_setup(123)

    @pytest.mark.asyncio
    async def test_sends_auth_url(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = _keychain_with_whoop()

        with (
            patch("healthbot.security.keychain.Keychain", return_value=kc),
            patch("healthbot.security.vault.Vault"),
            patch(
                "healthbot.bot.oauth_callback.wait_for_oauth_callback",
                new_callable=AsyncMock,
                side_effect=asyncio.TimeoutError,
            ),
        ):
            await handlers.whoop_auth(update, _mock_context())

        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("api.prod.whoop.com" in t for t in texts)
        assert any("timed out" in t.lower() for t in texts)
        # Should auto-clear credentials and enter setup flow
        assert any("client id" in t.lower() for t in texts)
        assert handlers.is_awaiting_setup(123)

    @pytest.mark.asyncio
    async def test_port_in_use(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = _keychain_with_whoop()

        with (
            patch("healthbot.security.keychain.Keychain", return_value=kc),
            patch("healthbot.security.vault.Vault"),
            patch(
                "healthbot.bot.oauth_callback.wait_for_oauth_callback",
                new_callable=AsyncMock,
                side_effect=OSError("Address already in use"),
            ),
        ):
            await handlers.whoop_auth(update, _mock_context())

        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("port 8765" in t for t in texts)

    @pytest.mark.asyncio
    async def test_authorization_denied(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = _keychain_with_whoop()

        with (
            patch("healthbot.security.keychain.Keychain", return_value=kc),
            patch("healthbot.security.vault.Vault"),
            patch(
                "healthbot.bot.oauth_callback.wait_for_oauth_callback",
                new_callable=AsyncMock,
                return_value={"code": None, "state": None, "error": "access_denied"},
            ),
        ):
            await handlers.whoop_auth(update, _mock_context())

        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("denied" in t.lower() for t in texts)

    @pytest.mark.asyncio
    async def test_state_mismatch(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = _keychain_with_whoop()

        with (
            patch("healthbot.security.keychain.Keychain", return_value=kc),
            patch("healthbot.security.vault.Vault"),
            patch(
                "healthbot.bot.oauth_callback.wait_for_oauth_callback",
                new_callable=AsyncMock,
                return_value={"code": "abc", "state": "wrong_state", "error": None},
            ),
        ):
            await handlers.whoop_auth(update, _mock_context())

        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("state" in t.lower() or "csrf" in t.lower() for t in texts)

    @pytest.mark.asyncio
    async def test_successful_flow(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = _keychain_with_whoop()

        # Capture the state from the auth URL to simulate a valid callback
        captured_state = {}

        def _fake_get_auth_url(redirect_uri):
            state = "test_state_abc"
            captured_state["state"] = state
            return f"https://api.prod.whoop.com/oauth/oauth2/auth?state={state}", state

        mock_client = MagicMock()
        mock_client.get_authorization_url.side_effect = _fake_get_auth_url
        mock_client.exchange_code = AsyncMock()

        with (
            patch("healthbot.security.keychain.Keychain", return_value=kc),
            patch("healthbot.security.vault.Vault"),
            patch(
                "healthbot.importers.whoop_client.WhoopClient",
                return_value=mock_client,
            ),
            patch(
                "healthbot.bot.oauth_callback.wait_for_oauth_callback",
                new_callable=AsyncMock,
                return_value={
                    "code": "auth_code_123",
                    "state": "test_state_abc",
                    "error": None,
                },
            ),
        ):
            await handlers.whoop_auth(update, _mock_context())

        mock_client.exchange_code.assert_awaited_once_with(
            "auth_code_123", "http://localhost:8765/callback"
        )
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("connected" in t.lower() or "success" in t.lower() for t in texts)

    @pytest.mark.asyncio
    async def test_reset_clears_credentials(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = _keychain_with_whoop()
        ctx = _mock_context()
        ctx.args = ["reset"]

        with patch("healthbot.security.keychain.Keychain", return_value=kc):
            await handlers.whoop_auth(update, ctx)

        kc.delete.assert_any_call("whoop_client_id")
        kc.delete.assert_any_call("whoop_client_secret")
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("cleared" in t.lower() for t in texts)

    @pytest.mark.asyncio
    async def test_corrupted_credential_auto_clears(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = MagicMock()
        kc.retrieve.side_effect = lambda k: {
            "whoop_client_id": "This is the client ID 243b6009-5d35-4f44",
        }.get(k)

        with patch("healthbot.security.keychain.Keychain", return_value=kc):
            await handlers.whoop_auth(update, _mock_context())

        # Should auto-clear the corrupted credential
        kc.delete.assert_any_call("whoop_client_id")
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("corrupted" in t.lower() for t in texts)
        # Should enter setup flow
        assert any("client id" in t.lower() for t in texts)


# ── /oura_auth handler ─────────────────────────────────────────


class TestOuraAuth:
    @pytest.mark.asyncio
    async def test_requires_unlock(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        key_manager.lock()
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.oura_auth(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_missing_client_id_starts_setup(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = MagicMock()
        kc.retrieve.return_value = None
        with patch("healthbot.security.keychain.Keychain", return_value=kc):
            await handlers.oura_auth(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "connect your oura" in reply.lower()
        assert "client id" in reply.lower()
        assert handlers.is_awaiting_setup(123)

    @pytest.mark.asyncio
    async def test_successful_flow(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = MagicMock()
        kc.retrieve.side_effect = lambda k: {
            "oura_client_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "oura_client_secret": "abcdef1234567890abcdef1234567890ab",
        }.get(k)

        mock_client = MagicMock()
        mock_client.get_authorization_url.return_value = (
            "https://cloud.ouraring.com/oauth/authorize?state=oura_state",
            "oura_state",
        )
        mock_client.exchange_code = AsyncMock()

        with (
            patch("healthbot.security.keychain.Keychain", return_value=kc),
            patch("healthbot.security.vault.Vault"),
            patch(
                "healthbot.importers.oura_client.OuraClient",
                return_value=mock_client,
            ),
            patch(
                "healthbot.bot.oauth_callback.wait_for_oauth_callback",
                new_callable=AsyncMock,
                return_value={
                    "code": "oura_code_123",
                    "state": "oura_state",
                    "error": None,
                },
            ),
        ):
            await handlers.oura_auth(update, _mock_context())

        mock_client.exchange_code.assert_awaited_once_with(
            "oura_code_123", "http://localhost:8765/callback"
        )
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("connected" in t.lower() or "success" in t.lower() for t in texts)


# ── Natural language routing ────────────────────────────────────


class TestNaturalLanguagePatterns:
    def test_whoop_pattern_matches(self):
        from healthbot.bot.message_router import _WHOOP_AUTH_PATTERN

        assert _WHOOP_AUTH_PATTERN.search("connect my whoop")
        assert _WHOOP_AUTH_PATTERN.search("link whoop")
        assert _WHOOP_AUTH_PATTERN.search("I want to set up WHOOP")
        assert _WHOOP_AUTH_PATTERN.search("authorize whoop")
        assert not _WHOOP_AUTH_PATTERN.search("what is my whoop recovery?")
        assert not _WHOOP_AUTH_PATTERN.search("show me whoop data")

    def test_oura_pattern_matches(self):
        from healthbot.bot.message_router import _OURA_AUTH_PATTERN

        assert _OURA_AUTH_PATTERN.search("connect my oura")
        assert _OURA_AUTH_PATTERN.search("link oura")
        assert _OURA_AUTH_PATTERN.search("pair my Oura ring")
        assert _OURA_AUTH_PATTERN.search("set up oura")
        assert not _OURA_AUTH_PATTERN.search("show oura sleep data")
        assert not _OURA_AUTH_PATTERN.search("my oura ring says")


# ── In-Telegram credential setup flow ─────────────────────────


class TestCredentialSetup:
    @pytest.mark.asyncio
    async def test_setup_collects_client_id(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = MagicMock()
        kc.retrieve.return_value = None

        # Step 1: trigger auth with no credentials → starts setup
        with patch("healthbot.security.keychain.Keychain", return_value=kc):
            await handlers.whoop_auth(update, _mock_context())
        assert handlers.is_awaiting_setup(123)

        # Step 2: send client_id (message gets deleted, reply via chat.send_message)
        update2 = _mock_update()
        update2.message.text = "my_whoop_client_id"
        handled = await handlers.handle_setup_input(update2, _mock_context())
        assert handled is True
        update2.message.delete.assert_called_once()
        reply = update2.effective_chat.send_message.call_args[0][0]
        assert "client secret" in reply.lower()
        assert handlers.is_awaiting_setup(123)  # Still in flow

    @pytest.mark.asyncio
    async def test_setup_completes_and_triggers_auth(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = MagicMock()
        kc.retrieve.return_value = None
        kc.store = MagicMock()

        # Step 1: trigger auth → starts setup
        with patch("healthbot.security.keychain.Keychain", return_value=kc):
            await handlers.whoop_auth(update, _mock_context())

        # Step 2: send client_id
        update2 = _mock_update()
        update2.message.text = "test_client_id"
        await handlers.handle_setup_input(update2, _mock_context())

        # Step 3: send client_secret → stores both and auto-triggers auth
        update3 = _mock_update()
        update3.message.text = "test_client_secret"
        update3.message.delete = AsyncMock()

        # After storing, the re-triggered whoop_auth will find credentials
        kc_with_creds = _keychain_with_whoop()
        kc_with_creds.store = MagicMock()

        with (
            patch("healthbot.security.keychain.Keychain", return_value=kc_with_creds),
            patch("healthbot.security.vault.Vault"),
            patch(
                "healthbot.bot.oauth_callback.wait_for_oauth_callback",
                new_callable=AsyncMock,
                side_effect=TimeoutError,
            ),
        ):
            handled = await handlers.handle_setup_input(update3, _mock_context())

        assert handled is True
        update3.message.delete.assert_awaited_once()  # Secret deleted
        # Timeout auto-clears creds and re-enters setup flow
        assert handlers.is_awaiting_setup(123)

    @pytest.mark.asyncio
    async def test_setup_secret_message_deleted(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        """Verify the client secret message is deleted for security."""
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = MagicMock()
        kc.retrieve.return_value = None
        kc.store = MagicMock()

        with patch("healthbot.security.keychain.Keychain", return_value=kc):
            await handlers.whoop_auth(update, _mock_context())

        # Send client_id
        update2 = _mock_update()
        update2.message.text = "my_id"
        await handlers.handle_setup_input(update2, _mock_context())

        # Send client_secret
        update3 = _mock_update()
        update3.message.text = "my_secret"
        update3.message.delete = AsyncMock()

        kc_after = _keychain_with_whoop()
        kc_after.store = MagicMock()
        with (
            patch("healthbot.security.keychain.Keychain", return_value=kc_after),
            patch("healthbot.security.vault.Vault"),
            patch(
                "healthbot.bot.oauth_callback.wait_for_oauth_callback",
                new_callable=AsyncMock,
                side_effect=TimeoutError,
            ),
        ):
            await handlers.handle_setup_input(update3, _mock_context())

        update3.message.delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_vault_lock_clears_setup_state(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        """Vault lock must clear any in-progress credential setup."""
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        kc = MagicMock()
        kc.retrieve.return_value = None

        with patch("healthbot.security.keychain.Keychain", return_value=kc):
            await handlers.whoop_auth(update, _mock_context())
        assert handlers.is_awaiting_setup(123)

        # Simulate vault lock
        handlers._setup_state.clear()
        assert not handlers.is_awaiting_setup(123)

    def test_not_awaiting_by_default(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        assert not handlers.is_awaiting_setup(123)

    @pytest.mark.asyncio
    async def test_handle_setup_returns_false_if_not_in_setup(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        update.message.text = "random text"
        result = await handlers.handle_setup_input(update, _mock_context())
        assert result is False
