"""Claude CLI research client.

Uses subprocess.run to invoke the Claude CLI with sanitized prompts
passed via stdin. NEVER passes prompts or PHI in command-line args.
Uses Python timeout, NOT shell timeout.
"""
from __future__ import annotations

import hashlib
import logging
import subprocess
from pathlib import Path

from healthbot.config import Config
from healthbot.llm.claude_client import (
    _CLAUDE_SEMAPHORE,
    _FIX_TOOL_FLAGS,
    _PRIVACY_FLAGS,
    _PRIVACY_PREAMBLE,
    _TOOL_FLAGS,
    _build_subprocess_env,
    resolve_cli,
)
from healthbot.research.research_packet import ResearchQueryPacket, build_research_packet
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")


class ClaudeCLIResearchClient:
    """Research health questions using Claude CLI."""

    def __init__(self, config: Config, firewall: PhiFirewall) -> None:
        self._config = config
        self._firewall = firewall
        self._cli_path = resolve_cli(config.claude_cli_path)
        self._cache: dict[str, str] = {}
        self._api_key: str | None = self._load_api_key()

    @staticmethod
    def _load_api_key() -> str | None:
        """Load Claude API key from Keychain if configured."""
        try:
            from healthbot.security.keychain import Keychain
            return Keychain().retrieve("claude_api_key")
        except Exception:
            return None

    def research(self, query: str, context: str = "") -> str:
        """Research a health question using Claude CLI.

        1. Build ResearchQueryPacket (hard-block if PHI detected)
        2. Pass sanitized prompt via stdin (NEVER in args)
        3. Check response for PHI leakage
        4. Cache by query_hash
        """
        packet = build_research_packet(query, context, self._firewall)

        if packet.blocked:
            return f"Research blocked: {packet.block_reason}"

        # Check cache
        if packet.query_hash in self._cache:
            return self._cache[packet.query_hash]

        if not self._cli_path:
            return "Claude CLI not found or not authenticated. Research unavailable."

        # Build prompt
        prompt = self._build_prompt(packet)

        # Call CLI via subprocess — stdin only, NEVER in args
        if not _CLAUDE_SEMAPHORE.acquire(timeout=60):
            logger.warning("Claude CLI semaphore timeout — too many concurrent calls")
            return "Research unavailable: too many concurrent requests."

        try:
            payload_hash = hashlib.sha256(prompt.encode()).hexdigest()[:16]
            logger.info(
                "Outbound to Claude CLI (research): hash=%s len=%d",
                payload_hash, len(prompt),
            )

            result = subprocess.run(
                [str(self._cli_path), "--print", "--model", "claude-opus-4-6",
                 *_PRIVACY_FLAGS, *_TOOL_FLAGS],
                input=prompt,
                capture_output=True,
                text=True,
                timeout=self._config.claude_cli_timeout,
                env=_build_subprocess_env(self._api_key),
            )
        except subprocess.TimeoutExpired:
            return "Research timed out. Please try a simpler question."
        except FileNotFoundError:
            return "Claude CLI not found. Please install it first."
        finally:
            _CLAUDE_SEMAPHORE.release()

        if result.returncode != 0:
            return "Research unavailable: CLI returned error."

        response = result.stdout.strip()

        # Check response for PHI leakage
        if self._firewall.contains_phi(response):
            response = self._firewall.redact(response)

        # Cache
        self._cache[packet.query_hash] = response

        return response

    def debug(self, question: str, error_context: str = "") -> str:
        """Debug a technical issue using Claude CLI with full tool access.

        Claude CLI runs from the project directory with full code access
        (Read, Edit, Bash, Grep, etc.) so it can diagnose AND fix issues.
        It reads the project's CLAUDE.md for architecture context.

        Unlike research(), this:
        - Uses _FIX_TOOL_FLAGS (full tools) instead of _TOOL_FLAGS (web only)
        - Runs with cwd=project_dir so Claude picks up CLAUDE.md
        - Skips ResearchQueryPacket PHI gate (error context is technical)
        - Still checks prompt and response through PhiFirewall
        """
        if not self._cli_path:
            return "Claude CLI not found or not authenticated. Debug unavailable."

        # Safety: ensure no PHI leaked into debug context
        if self._firewall.contains_phi(question):
            question = self._firewall.redact(question)
        if self._firewall.contains_phi(error_context):
            error_context = self._firewall.redact(error_context)

        prompt = self._build_debug_prompt(question, error_context)

        # Run from project directory so Claude loads CLAUDE.md automatically
        project_dir = self._find_project_dir()

        if not _CLAUDE_SEMAPHORE.acquire(timeout=60):
            logger.warning("Claude CLI semaphore timeout — too many concurrent calls")
            return "Debug unavailable: too many concurrent requests."

        try:
            payload_hash = hashlib.sha256(prompt.encode()).hexdigest()[:16]
            logger.info(
                "Outbound to Claude CLI (debug): hash=%s len=%d",
                payload_hash, len(prompt),
            )

            result = subprocess.run(
                [str(self._cli_path), "--print", "--model", "claude-opus-4-6",
                 *_PRIVACY_FLAGS, *_FIX_TOOL_FLAGS],
                input=prompt,
                capture_output=True,
                text=True,
                timeout=self._config.claude_cli_timeout,
                cwd=str(project_dir) if project_dir else None,
                env=_build_subprocess_env(self._api_key),
            )
        except subprocess.TimeoutExpired:
            return "Debug timed out. Try describing the issue more simply."
        except FileNotFoundError:
            return "Claude CLI not found. Please install it first."
        finally:
            _CLAUDE_SEMAPHORE.release()

        if result.returncode != 0:
            return "Debug unavailable: CLI returned error."

        response = result.stdout.strip()

        if self._firewall.contains_phi(response):
            response = self._firewall.redact(response)

        return response

    def _find_project_dir(self) -> Path | None:
        """Find the HealthBot project directory (contains CLAUDE.md + src/)."""
        # Walk up from this file to find the project root
        current = Path(__file__).resolve()
        for parent in current.parents:
            if (parent / "CLAUDE.md").exists() and (parent / "src").exists():
                return parent
        return None

    def _build_debug_prompt(self, question: str, error_context: str) -> str:
        """Build the prompt for technical debugging with code fix capability."""
        parts = [
            "You are a technical debugging specialist for HealthBot, a "
            "personal health data app. You have FULL ACCESS to read and "
            "edit the source code to fix issues. The project's CLAUDE.md "
            "has the full architecture reference.",
            "",
            "RULES:",
            "- You CAN read source code, edit files, run tests, and "
            "restart the bot to fix issues.",
            "- You MUST run `ruff check src/ tests/` and "
            "`pytest tests/ -q` after any code changes.",
            "- NEVER touch the encrypted vault (~/.healthbot/) or its "
            "database — that contains private health data.",
            "- NEVER modify security code (phi_firewall, key_manager, "
            "vault, log_scrubber) without explicit approval.",
            "- If the fix requires re-authentication (OAuth), tell the "
            "user to run the relevant /auth command.",
            "- After fixing, restart the bot with: make bot-restart "
            "(or tell the user if manual restart is needed).",
            "",
            f"User's question: {question}",
        ]
        if error_context:
            parts.extend(["", f"Recent error context:\n{error_context}"])
        parts.extend([
            "",
            "Diagnose the issue. If you can fix it by editing code, do it. "
            "If it requires user action (re-auth, config change), explain "
            "the specific steps. Be concise.",
        ])
        return "\n".join(parts)

    def _build_prompt(self, packet: ResearchQueryPacket) -> str:
        """Build the prompt for Claude CLI."""
        parts = [
            _PRIVACY_PREAMBLE,
            "You are a medical research specialist. Provide thorough, "
            "evidence-based analysis with citations from authoritative sources.",
            "",
            f"Question: {packet.query}",
        ]
        if packet.context:
            parts.extend(["", f"Context: {packet.context}"])
        return "\n".join(parts)

