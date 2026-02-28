"""Claude CLI client via subprocess.

Uses the local `claude` CLI (Claude Code) with --print flag.
Prompts passed via stdin only — NEVER in command-line args.
Uses Python subprocess timeout, NOT shell timeout.
Auth: Uses existing Claude CLI login OR ANTHROPIC_API_KEY from Keychain.

Privacy isolation: Every subprocess call uses flags to prevent Claude CLI
from persisting any health data to MCP memory, auto-memory, or session files.
"""
from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import threading
from pathlib import Path

logger = logging.getLogger("healthbot")

# Rate-limit concurrent Claude CLI subprocess calls.
# Prevents fork-bombing from runaway loops or burst traffic.
_CLAUDE_SEMAPHORE = threading.Semaphore(3)

# Privacy preamble — explicitly tells Claude not to persist ANY data.
# Prepended to every system prompt. This is the SINGLE SOURCE OF TRUTH
# for privacy language across conversation, research, and MCP.
_PRIVACY_PREAMBLE = (
    "PRIVACY PROTOCOL (MANDATORY):\n"
    "- Do NOT save data via MCP servers, files, databases, or external systems.\n"
    "- Do NOT use any tools to persist data (no Bash, Write, Edit).\n"
    "- Treat ALL user data as ephemeral — it exists only in this prompt.\n"
    "- The ONLY way to persist information is via structured blocks\n"
    "  (MEMORY, CORRECTION, INSIGHT, etc.) embedded in your response text.\n"
    "  These are parsed and routed by the host system — not by you.\n"
    "- Never reference the memory system to the user.\n"
)

# Privacy isolation flags — prevent Claude CLI from persisting ANY data.
# These ensure health data never leaks into MCP memory, auto-memory, or sessions.
_PRIVACY_FLAGS: list[str] = [
    "--no-session-persistence",   # Session NOT saved to disk
    "--strict-mcp-config",        # Ignore all user MCP configs (cortex-core, etc.)
    "--mcp-config", '{"mcpServers":{}}',  # Empty MCP config — zero servers loaded
]

# Tool control — only allow web research tools.
# Blocks: Bash, Edit, Write, Read, NotebookEdit, and ALL MCP tools.
_TOOL_FLAGS: list[str] = [
    "--tools", "WebSearch,WebFetch",
]

# Fix mode — full tool access for reading, editing, and testing code.
# Used by debug/troubleshoot to self-heal the bot when APIs change or bugs occur.
# Health data is still safe: all PHI lives in AES-256-GCM encrypted DB, not in source.
_FIX_TOOL_FLAGS: list[str] = [
    "--tools", "Bash,Read,Write,Edit,Glob,Grep,WebSearch,WebFetch",
]


def _build_subprocess_env(api_key: str | None = None) -> dict[str, str]:
    """Build minimal subprocess env — PATH + HOME + USER + optional API key.

    USER is required for macOS Keychain access (Claude CLI auth).
    Ensures common Claude CLI install locations are in PATH even under
    launchd's restricted environment.
    """
    path = os.environ.get("PATH", "")
    # Ensure common Claude CLI install locations are in PATH
    for extra in [
        str(Path.home() / ".npm-global" / "bin"),
        "/opt/homebrew/bin",
    ]:
        if extra not in path:
            path = f"{extra}:{path}"
    env = {
        "PATH": path,
        "HOME": os.environ.get("HOME", ""),
        "USER": os.environ.get("USER", ""),
    }
    if api_key:
        env["ANTHROPIC_API_KEY"] = api_key
    return env


def resolve_cli(cli_path: str | Path | None = None) -> Path | None:
    """Find the Claude CLI binary.

    Checks the given path, then common install locations, then PATH.
    """
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p

    # Check common install locations
    candidates = [
        Path.home() / ".local" / "bin" / "claude",
        Path("/usr/local/bin/claude"),
        Path.home() / ".claude" / "local" / "claude",
        Path.home() / ".npm-global" / "bin" / "claude",   # npm global install
        Path("/opt/homebrew/bin/claude"),                    # Homebrew
    ]
    for p in candidates:
        if p.exists():
            return p

    # Try PATH
    found = shutil.which("claude")
    if found:
        return Path(found)

    return None


class CLINotFoundError(Exception):
    """Raised when Claude CLI binary is not found."""


class CLIAuthError(Exception):
    """Raised when Claude CLI is not authenticated."""


# Internal error strings returned by send() on failure.
# Used by check_auth() to distinguish errors from real responses.
_TIMEOUT_RESPONSE = "I'm taking too long to respond. Try a simpler question."
_CLI_ERROR_RESPONSE = (
    "I had trouble processing that. The CLI returned an error.\n"
    "Try again, or run /claude_auth check to verify your setup."
)


# Stderr patterns that indicate authentication failure.
# Only checked when returncode != 0. Patterns must be specific enough
# to avoid false positives (e.g. "401" alone could match port numbers).
_AUTH_ERROR_PATTERNS = (
    "not authenticated",
    "authentication required",
    "authentication failed",
    "login required",
    "please sign in",
    "sign in to",
    "unauthorized",
    "invalid api key",
    "invalid api_key",
    "missing api key",
    "not logged in",
    "please log in",
    "expired token",
    "token expired",
    "invalid credentials",
    "credentials expired",
    " 401 ",
    " 401:",
    "http 401",
    "status 401",
    "error 401",
)


class ClaudeClient:
    """Claude CLI client using subprocess with privacy isolation."""

    def __init__(
        self,
        cli_path: str | Path | None = None,
        timeout: int = 180,
        api_key: str | None = None,
    ) -> None:
        self._cli_path = resolve_cli(cli_path)
        self._timeout = timeout
        self._api_key = api_key

    def send(self, prompt: str, system: str = "") -> str:
        """Send a prompt to Claude CLI and return the text response.

        Args:
            prompt: The user-facing prompt with all context included.
            system: System instructions prepended to the prompt.

        Returns:
            Claude's text response.

        Privacy: The subprocess is launched with flags that prevent any data
        from being saved to MCP memory, auto-memory, or session persistence.
        All health data is treated as ephemeral.
        """
        return self._run_cli(prompt, system, tool_flags=_TOOL_FLAGS)

    def send_with_read(
        self, prompt: str, system: str = "",
        read_dirs: list[str] | None = None,
    ) -> str:
        """Send a prompt that needs file Read access (e.g., reading a PDF).

        Same as send() but adds the Read tool so Claude can access files.
        Used for lab PDF extraction where Claude reads a redacted PDF directly.

        Parameters
        ----------
        read_dirs:
            Directories to grant Read access to via ``--add-dir``.
            Required when the target file is outside the working directory
            (e.g. a temp file in /tmp or /var/folders).
        """
        read_tool_flags = ["--tools", "Read,WebSearch,WebFetch"]
        add_dir_flags: list[str] = []
        for d in (read_dirs or []):
            add_dir_flags.extend(["--add-dir", d])
        return self._run_cli(
            prompt, system,
            tool_flags=read_tool_flags,
            extra_flags=add_dir_flags,
        )

    def _run_cli(
        self,
        prompt: str,
        system: str = "",
        tool_flags: list[str] | None = None,
        extra_flags: list[str] | None = None,
    ) -> str:
        """Run a prompt through Claude CLI subprocess.

        Handles: input assembly, env construction, semaphore,
        subprocess call, and all error handling.
        """
        if not self._cli_path:
            raise CLINotFoundError(
                "Claude CLI not found. Install it: "
                "https://docs.anthropic.com/en/docs/claude-code"
            )

        # Build the full input: privacy preamble + system + prompt (via stdin)
        parts = [_PRIVACY_PREAMBLE]
        if system:
            parts.append(system)
            parts.append("")
        parts.append(prompt)
        full_input = "\n".join(parts)

        # Build command with privacy isolation + tool control
        cmd = [
            str(self._cli_path), "--print",
            "--model", "claude-opus-4-6",
            *_PRIVACY_FLAGS,
            *(tool_flags or _TOOL_FLAGS),
            *(extra_flags or []),
        ]

        env = _build_subprocess_env(self._api_key)

        # Rate-limit concurrent CLI calls
        if not _CLAUDE_SEMAPHORE.acquire(timeout=60):
            logger.warning("Claude CLI semaphore timeout — too many concurrent calls")
            return _CLI_ERROR_RESPONSE

        try:
            # Audit log: hash outbound payload before sending
            payload_hash = hashlib.sha256(full_input.encode()).hexdigest()[:16]
            logger.info(
                "Outbound to Claude CLI: hash=%s len=%d",
                payload_hash, len(full_input),
            )

            result = subprocess.run(
                cmd,
                input=full_input,
                capture_output=True,
                text=True,
                timeout=self._timeout,
                env=env,
            )
        except subprocess.TimeoutExpired:
            logger.warning("Claude CLI timed out after %ds", self._timeout)
            return _TIMEOUT_RESPONSE
        except FileNotFoundError as e:
            raise CLINotFoundError("Claude CLI binary not found at configured path.") from e
        finally:
            _CLAUDE_SEMAPHORE.release()

        if result.returncode != 0:
            stderr = result.stderr.strip()[:200] if result.stderr else ""
            stdout = result.stdout.strip()[:200] if result.stdout else ""
            if self._is_auth_error(stderr):
                raise CLIAuthError(
                    "Claude CLI is not authenticated. "
                    "Run 'claude login' in your terminal."
                )
            logger.error(
                "Claude CLI error (rc=%d): stderr=%s stdout=%s",
                result.returncode,
                stderr or "(empty)",
                stdout or "(empty)",
            )
            lower_stderr = stderr.lower()
            if any(w in lower_stderr for w in ("rate limit", "429", "too many")):
                return (
                    "Claude is rate-limited right now. "
                    "Wait a minute and try again."
                )
            if any(w in lower_stderr for w in (
                "connection", "network", "timeout", "dns", "resolve",
            )):
                return (
                    "Couldn't reach Claude — check your network connection "
                    "and try again."
                )
            if stderr:
                return (
                    f"Claude CLI error: {stderr[:150]}\n"
                    "Try: /claude_auth check"
                )
            return _CLI_ERROR_RESPONSE

        return result.stdout.strip()

    def is_available(self) -> bool:
        """Check if Claude CLI is installed and accessible."""
        return self._cli_path is not None and self._cli_path.exists()

    def check_auth(self) -> tuple[bool, str]:
        """Quick auth check. Returns (ok, message). Delegates to diagnose()."""
        return self.diagnose()

    def diagnose(self) -> tuple[bool, str]:
        """Run a structured diagnostic of Claude CLI health.

        Returns (ok, diagnostic_message) with specific, actionable info.
        """
        # 1. Check binary exists
        if not self._cli_path or not self._cli_path.exists():
            return False, (
                "Claude CLI is not installed.\n"
                "Install: brew install claude-code\n"
                "Or: npm install -g @anthropic-ai/claude-code"
            )

        # 2. Verify binary runs (--version)
        env = _build_subprocess_env()
        try:
            result = subprocess.run(
                [str(self._cli_path), "--version"],
                capture_output=True,
                text=True,
                timeout=15,
                env=env,
            )
            if result.returncode != 0:
                stderr = result.stderr.strip()[:200] if result.stderr else ""
                return False, (
                    f"Claude CLI binary failed to run.\n"
                    f"Path: {self._cli_path}\n"
                    f"Error: {stderr or 'unknown'}\n"
                    "Try reinstalling: brew reinstall claude-code"
                )
            version = result.stdout.strip()[:50]
        except subprocess.TimeoutExpired:
            return False, (
                "Claude CLI timed out on --version check.\n"
                "The binary may be corrupted. Try reinstalling."
            )
        except FileNotFoundError:
            return False, "Claude CLI binary not found at configured path."

        # 3. Test authentication with a real prompt
        try:
            response = self.send(prompt="Reply with exactly: OK")
            if response and response not in (_TIMEOUT_RESPONSE, _CLI_ERROR_RESPONSE):
                # Check for our improved error format too
                if not response.startswith("Claude CLI error:"):
                    return True, f"Authenticated. Version: {version}"
            if response == _TIMEOUT_RESPONSE:
                return False, (
                    f"Claude CLI ({version}) timed out during auth check.\n"
                    "This usually means the API is slow or unreachable.\n"
                    "Check your network connection."
                )
            # _CLI_ERROR_RESPONSE or improved error string
            return False, (
                f"Claude CLI ({version}) returned an error.\n"
                "Most likely cause: not logged in.\n"
                "Fix: run 'claude login' in your terminal.\n"
                "Or: /claude_auth setup to use an API key."
            )
        except CLIAuthError:
            return False, (
                f"Claude CLI ({version}) is not authenticated.\n"
                "Fix: run 'claude login' in your terminal.\n"
                "Or: /claude_auth setup to use an API key."
            )
        except CLINotFoundError:
            return False, "Claude CLI not found."
        except Exception as e:
            return False, f"Unexpected error: {type(e).__name__}: {e}"

    @staticmethod
    def _is_auth_error(stderr: str) -> bool:
        """Detect authentication failures from stderr output."""
        lower = stderr.lower()
        return any(pattern in lower for pattern in _AUTH_ERROR_PATTERNS)

