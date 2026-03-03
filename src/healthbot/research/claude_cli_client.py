"""Claude CLI research client.

Uses subprocess.run to invoke the Claude CLI with sanitized prompts
passed via stdin. NEVER passes prompts or PHI in command-line args.
Uses Python timeout, NOT shell timeout.
"""
from __future__ import annotations

import hashlib
import logging
import re
import subprocess
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path

from healthbot.config import Config
from healthbot.llm.anonymizer import heuristic_name_scan
from healthbot.llm.claude_client import (
    _CLAUDE_SEMAPHORE,
    _EVOLUTION_TOOL_FLAGS,
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

# Read-only tools for debug/diagnosis — no Write, Edit, or Bash to prevent
# accidental modifications when invoked from the research client.
_READ_ONLY_TOOL_FLAGS: list[str] = [
    "--tools", "Read,Glob,Grep,WebSearch,WebFetch",
    "--allowedTools", "Read,Glob,Grep,WebSearch,WebFetch",
]


class ClaudeCLIResearchClient:
    """Research health questions using Claude CLI."""

    _CACHE_MAX_SIZE = 100

    def __init__(self, config: Config, firewall: PhiFirewall) -> None:
        self._config = config
        self._firewall = firewall
        self._cli_path = resolve_cli(config.claude_cli_path)
        self._cache: OrderedDict[str, str] = OrderedDict()
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
        packet = build_research_packet(
            query, context, self._firewall,
            heuristic_name_check=heuristic_name_scan,
        )

        if packet.blocked:
            return f"Research blocked: {packet.block_reason}"

        # Check cache (move to end on access for LRU ordering)
        if packet.query_hash in self._cache:
            self._cache.move_to_end(packet.query_hash)
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
                cwd="/tmp",  # Neutral dir — prevents CLAUDE.md pickup
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

        # Cache with LRU eviction
        self._cache[packet.query_hash] = response
        if len(self._cache) > self._CACHE_MAX_SIZE:
            self._cache.popitem(last=False)

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

    def evolve_schema(
        self,
        data_type: str,
        fields: list[dict],
        reason: str,
        sample_data: dict | None = None,
    ) -> SchemaEvolutionResult:
        """Autonomously create new DB table + sync worker for a new medical data type.

        Uses _FIX_TOOL_FLAGS — Claude gets full code access
        (Read, Write, Edit, Bash, Glob, Grep).
        Runs from project directory so Claude can read existing patterns.
        """
        if not self._cli_path:
            return SchemaEvolutionResult(
                success=False,
                data_type=data_type,
                summary="Claude CLI not found",
                error="Claude CLI not found or not authenticated.",
            )

        # ── PHI gate on inputs ──────────────────────────────
        # Hard-block if data_type itself contains PHI
        if self._firewall.contains_phi(data_type):
            from healthbot.security.pii_alert import PiiAlertService
            PiiAlertService.get_instance().record(
                category="PHI_in_schema_evolution",
                destination="claude_cli",
            )
            return SchemaEvolutionResult(
                success=False,
                data_type=data_type,
                summary="PHI in data_type",
                error="PHI detected in data_type — evolution blocked.",
            )
        # Redact reason if PHI detected
        if self._firewall.contains_phi(reason):
            from healthbot.security.pii_alert import PiiAlertService
            PiiAlertService.get_instance().record(
                category="PHI_in_schema_evolution",
                destination="claude_cli",
            )
            reason = self._firewall.redact(reason)
        # Drop sample_data entirely if PHI detected (too risky to redact structured data)
        if sample_data and self._firewall.contains_phi(str(sample_data)):
            from healthbot.security.pii_alert import PiiAlertService
            PiiAlertService.get_instance().record(
                category="PHI_in_schema_evolution",
                destination="claude_cli",
            )
            sample_data = None
        # Redact field description values if PHI detected
        for f in fields:
            desc = f.get("description", "")
            if desc and self._firewall.contains_phi(desc):
                from healthbot.security.pii_alert import PiiAlertService
                PiiAlertService.get_instance().record(
                    category="PHI_in_schema_evolution",
                    destination="claude_cli",
                )
                f["description"] = self._firewall.redact(desc)

        from healthbot.research.schema_evolution_prompt import build_evolution_prompt
        prompt = build_evolution_prompt(data_type, fields, reason, sample_data)

        project_dir = self._find_project_dir()

        if not _CLAUDE_SEMAPHORE.acquire(timeout=120):
            return SchemaEvolutionResult(
                success=False,
                data_type=data_type,
                summary="Semaphore timeout",
                error="Too many concurrent Claude CLI calls.",
            )

        try:
            payload_hash = hashlib.sha256(prompt.encode()).hexdigest()[:16]
            logger.info(
                "Outbound to Claude CLI (schema evolution): hash=%s type=%s",
                payload_hash, data_type,
            )

            vault_dir = str(Path.home() / ".healthbot")
            result = subprocess.run(
                [str(self._cli_path), "--print", "--model", "claude-opus-4-6",
                 *_PRIVACY_FLAGS, *_EVOLUTION_TOOL_FLAGS,
                 "--deny-dir", vault_dir],
                input=prompt,
                capture_output=True,
                text=True,
                timeout=300,  # Schema evolution may take longer
                cwd=str(project_dir) if project_dir else None,
                env=_build_subprocess_env(self._api_key),
            )
        except subprocess.TimeoutExpired:
            return SchemaEvolutionResult(
                success=False,
                data_type=data_type,
                summary="Timed out",
                error="Schema evolution timed out after 300s.",
            )
        except FileNotFoundError:
            return SchemaEvolutionResult(
                success=False,
                data_type=data_type,
                summary="CLI not found",
                error="Claude CLI binary not found.",
            )
        finally:
            _CLAUDE_SEMAPHORE.release()

        if result.returncode != 0:
            return SchemaEvolutionResult(
                success=False,
                data_type=data_type,
                summary="CLI error",
                error=f"Claude CLI returned exit code {result.returncode}.",
            )

        response = result.stdout.strip()

        # Check response for PHI leakage (matches research() and debug() pattern)
        if self._firewall.contains_phi(response):
            response = self._firewall.redact(response)

        return self._parse_evolution_result(data_type, response)

    @staticmethod
    def _parse_evolution_result(
        data_type: str, response: str,
    ) -> SchemaEvolutionResult:
        """Parse Claude's schema evolution output to extract what was created."""
        files_modified: list[str] = []
        ddl_executed: list[str] = []
        migration_version: int | None = None

        # Extract file paths from the output
        file_pat = re.compile(
            r"(?:modified|created|edited|wrote)\s+[`']?([^\s`']+\.py)",
            re.IGNORECASE,
        )
        for m in file_pat.finditer(response):
            path = m.group(1)
            if path not in files_modified:
                files_modified.append(path)

        # Extract DDL statements
        ddl_pat = re.compile(
            r"```sql\s*(CREATE\s+TABLE[^`]+?)```",
            re.IGNORECASE | re.DOTALL,
        )
        for m in ddl_pat.finditer(response):
            ddl_executed.append(m.group(1).strip())

        # Extract migration version
        version_match = re.search(r"version\s*[=:]\s*(\d+)", response, re.IGNORECASE)
        if version_match:
            migration_version = int(version_match.group(1))

        success = bool(files_modified) or "success" in response.lower()
        summary_lines = response.strip().split("\n")
        summary = summary_lines[-1][:200] if summary_lines else "No output"

        return SchemaEvolutionResult(
            success=success,
            data_type=data_type,
            files_modified=files_modified,
            ddl_executed=ddl_executed,
            migration_version=migration_version,
            summary=summary,
        )

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


@dataclass
class SchemaEvolutionResult:
    """Result of an autonomous schema evolution operation."""

    success: bool
    data_type: str
    files_modified: list[str] = field(default_factory=list)
    ddl_executed: list[str] = field(default_factory=list)
    migration_version: int | None = None
    summary: str = ""
    error: str = ""

