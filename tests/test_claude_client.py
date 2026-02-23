"""Tests for llm/claude_client.py — CLI subprocess with privacy isolation."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from healthbot.llm.claude_client import (
    _CLI_ERROR_RESPONSE,
    _PRIVACY_FLAGS,
    _TIMEOUT_RESPONSE,
    ClaudeClient,
    CLIAuthError,
    CLINotFoundError,
)


class TestClaudeClientInit:
    @patch("healthbot.llm.claude_client.resolve_cli", return_value=None)
    def test_cli_not_found_returns_none(self, mock_resolve):
        client = ClaudeClient(cli_path="/nonexistent/path/claude")
        assert not client.is_available()

    @patch("healthbot.llm.claude_client.resolve_cli", return_value=None)
    def test_no_claude_on_path(self, mock_resolve):
        client = ClaudeClient()
        assert not client.is_available()

    def test_claude_found_at_explicit_path(self, tmp_path):
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        assert client.is_available()


class TestSend:
    @pytest.fixture
    def client(self, tmp_path) -> ClaudeClient:
        cli = tmp_path / "claude"
        cli.touch()
        return ClaudeClient(cli_path=cli, timeout=30)

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_send_success(self, mock_run, client):
        mock_run.return_value = MagicMock(
            returncode=0, stdout="Research result here\n", stderr=""
        )
        result = client.send("What is glucose?")
        assert result == "Research result here"

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_send_with_system_prompt(self, mock_run, client):
        mock_run.return_value = MagicMock(
            returncode=0, stdout="response", stderr=""
        )
        client.send("query", system="You are a researcher.")
        call_kwargs = mock_run.call_args
        input_text = call_kwargs.kwargs.get("input", "")
        assert "You are a researcher." in input_text
        assert "query" in input_text

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_privacy_flags_in_command(self, mock_run, client):
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
        client.send("test")
        cmd = mock_run.call_args[0][0]
        for flag in _PRIVACY_FLAGS:
            assert flag in cmd

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_tool_flags_in_command(self, mock_run, client):
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
        client.send("test")
        cmd = mock_run.call_args[0][0]
        assert "WebSearch,WebFetch" in cmd

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_input_via_stdin_not_args(self, mock_run, client):
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
        client.send("sensitive health query")
        call_kwargs = mock_run.call_args
        input_text = call_kwargs.kwargs.get("input", "")
        assert "sensitive health query" in input_text
        cmd = mock_run.call_args[0][0]
        assert "sensitive health query" not in " ".join(cmd)

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_env_minimal(self, mock_run, client):
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
        client.send("test")
        call_kwargs = mock_run.call_args
        env = call_kwargs.kwargs.get("env", {})
        assert set(env.keys()) == {"PATH", "HOME", "USER"}

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_timeout_handling(self, mock_run, client):
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="claude", timeout=30)
        result = client.send("slow query")
        assert result == _TIMEOUT_RESPONSE

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_error_returncode(self, mock_run, client):
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="CLI error")
        result = client.send("test")
        assert "CLI error" in result
        assert "claude_auth" in result.lower()

    @patch("healthbot.llm.claude_client.resolve_cli", return_value=None)
    def test_send_without_cli_raises(self, mock_resolve):
        client = ClaudeClient(cli_path="/nonexistent")
        with pytest.raises(CLINotFoundError):
            client.send("test")


class TestAPIKeySupport:
    """Tests for API key passthrough to subprocess env."""

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_api_key_in_env(self, mock_run, tmp_path):
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli, api_key="sk-ant-test-key")
        mock_run.return_value = MagicMock(
            returncode=0, stdout="ok", stderr="",
        )
        client.send("test")
        env = mock_run.call_args.kwargs.get("env", {})
        assert env["ANTHROPIC_API_KEY"] == "sk-ant-test-key"
        assert "PATH" in env
        assert "HOME" in env

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_no_api_key_not_in_env(self, mock_run, tmp_path):
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        mock_run.return_value = MagicMock(
            returncode=0, stdout="ok", stderr="",
        )
        client.send("test")
        env = mock_run.call_args.kwargs.get("env", {})
        assert "ANTHROPIC_API_KEY" not in env
        assert set(env.keys()) == {"PATH", "HOME", "USER"}


class TestAuthErrorDetection:
    """Tests for CLIAuthError detection from stderr."""

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_auth_error_not_authenticated(self, mock_run, tmp_path):
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        mock_run.return_value = MagicMock(
            returncode=1, stdout="",
            stderr="Error: Not authenticated. Please sign in.",
        )
        with pytest.raises(CLIAuthError):
            client.send("test")

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_auth_error_invalid_api_key(self, mock_run, tmp_path):
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        mock_run.return_value = MagicMock(
            returncode=1, stdout="",
            stderr="Invalid API key provided.",
        )
        with pytest.raises(CLIAuthError):
            client.send("test")

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_auth_error_401(self, mock_run, tmp_path):
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        mock_run.return_value = MagicMock(
            returncode=1, stdout="",
            stderr="HTTP 401 Unauthorized",
        )
        with pytest.raises(CLIAuthError):
            client.send("test")

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_port_number_not_auth_error(self, mock_run, tmp_path):
        """Port numbers containing '401' should NOT trigger auth error."""
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        mock_run.return_value = MagicMock(
            returncode=1, stdout="",
            stderr="Connection refused to localhost:14012",
        )
        # Should return connection error, not raise CLIAuthError
        result = client.send("test")
        assert "network" in result.lower() or "reach" in result.lower()

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_non_auth_error_returns_stderr_detail(self, mock_run, tmp_path):
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        mock_run.return_value = MagicMock(
            returncode=1, stdout="",
            stderr="Some unknown error",
        )
        result = client.send("test")
        assert "Some unknown error" in result
        assert "claude_auth" in result.lower()

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_empty_stderr_returns_generic(self, mock_run, tmp_path):
        """Empty stderr falls back to generic error response."""
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        mock_run.return_value = MagicMock(
            returncode=1, stdout="",
            stderr="",
        )
        result = client.send("test")
        assert result == _CLI_ERROR_RESPONSE

    def test_is_auth_error_patterns(self):
        assert ClaudeClient._is_auth_error("Not authenticated")
        assert ClaudeClient._is_auth_error("Please sign in to continue")
        assert ClaudeClient._is_auth_error("Invalid API key")
        assert ClaudeClient._is_auth_error("HTTP 401 Unauthorized")
        assert ClaudeClient._is_auth_error("error 401:")
        assert ClaudeClient._is_auth_error("status 401 received")
        assert ClaudeClient._is_auth_error("Authentication failed")
        assert ClaudeClient._is_auth_error("not logged in")
        assert ClaudeClient._is_auth_error("please log in")
        assert ClaudeClient._is_auth_error("expired token")
        assert ClaudeClient._is_auth_error("invalid credentials")
        assert not ClaudeClient._is_auth_error("Connection timeout")
        assert not ClaudeClient._is_auth_error("File not found")
        assert not ClaudeClient._is_auth_error("")
        # Must NOT match port numbers containing "401"
        assert not ClaudeClient._is_auth_error("connecting to localhost:14012")
        assert not ClaudeClient._is_auth_error("port 54012 in use")


class TestCheckAuth:
    """Tests for the check_auth() / diagnose() method."""

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_check_auth_success(self, mock_run, tmp_path):
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        # diagnose() makes two calls: --version then test prompt
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="1.0.0", stderr=""),
            MagicMock(returncode=0, stdout="OK", stderr=""),
        ]
        ok, msg = client.check_auth()
        assert ok is True
        assert "Authenticated" in msg

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_check_auth_failure(self, mock_run, tmp_path):
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        # --version succeeds, test prompt returns auth error
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="1.0.0", stderr=""),
            MagicMock(returncode=1, stdout="", stderr="Not authenticated"),
        ]
        ok, msg = client.check_auth()
        assert ok is False
        assert "not authenticated" in msg.lower() or "log in" in msg.lower()

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_check_auth_timeout_not_false_positive(self, mock_run, tmp_path):
        """Timeout should NOT be reported as successful auth."""
        import subprocess
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli, timeout=5)
        # --version succeeds, test prompt times out
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="1.0.0", stderr=""),
            subprocess.TimeoutExpired(cmd="claude", timeout=5),
        ]
        ok, msg = client.check_auth()
        assert ok is False

    @patch("healthbot.llm.claude_client.subprocess.run")
    def test_check_auth_version_fails(self, mock_run, tmp_path):
        """Binary exists but --version fails."""
        cli = tmp_path / "claude"
        cli.touch()
        client = ClaudeClient(cli_path=cli)
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="segfault",
        )
        ok, msg = client.check_auth()
        assert ok is False
        assert "failed to run" in msg.lower()

    @patch("healthbot.llm.claude_client.resolve_cli", return_value=None)
    def test_check_auth_cli_not_installed(self, mock_resolve):
        client = ClaudeClient()
        ok, msg = client.check_auth()
        assert ok is False
        assert "not installed" in msg.lower()
