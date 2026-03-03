"""Claude CLI conversation manager.

Routes messages through Claude CLI with anonymized health data context.
Prompt building in conversation_context.py, block routing in conversation_routing.py.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
from datetime import UTC, datetime

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.llm.claude_client import ClaudeClient
from healthbot.llm.claude_context import ensure_claude_dir, load_context
from healthbot.llm.conversation_context import (
    append_health_sections as _ctx_append_health_sections,
)
from healthbot.llm.conversation_context import (
    append_hypotheses as _ctx_append_hypotheses,
)
from healthbot.llm.conversation_context import (
    append_kb_findings as _ctx_append_kb_findings,
)
from healthbot.llm.conversation_context import (
    append_research_evidence as _ctx_append_research_evidence,
)
from healthbot.llm.conversation_context import (
    append_user_memory as _ctx_append_user_memory,
)
from healthbot.llm.conversation_context import (
    build_prompt,
)
from healthbot.llm.conversation_context import (
    safe_anonymize as _ctx_safe_anonymize,
)
from healthbot.llm.conversation_routing import (
    format_quality_notifications,
    get_clean_db,
    reconcile_demographics_to_ltm,
    route_block,
)
from healthbot.llm.conversation_routing import (
    handle_data_quality as _rt_handle_data_quality,
)
from healthbot.llm.conversation_routing import (
    handle_memory_block as _rt_handle_memory_block,
)
from healthbot.llm.conversation_routing import (
    handle_system_improvement as _rt_handle_system_improvement,
)
from healthbot.llm.conversation_routing import (
    sync_memory_to_demographics as _rt_sync_memory_to_demographics,
)
from healthbot.llm.conversation_routing import (
    sync_memory_to_ltm as _rt_sync_memory_to_ltm,
)
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")

# Pattern for all structured medical blocks.
# Matches block type labels followed by a JSON object with up to 2 levels
# of brace nesting (e.g. {"key": {"nested": "value"}}).
_BLOCK_PATTERN = re.compile(
    r"(HYPOTHESIS|ACTION|RESEARCH|INSIGHT|CONDITION|DATA_QUALITY|MEMORY|CORRECTION|SYSTEM_IMPROVEMENT|HEALTH_DATA|ANALYSIS_RULE|CHART|CITATION|CHECK_INTERACTION|SCHEMA_EVOLVE):\s*(\{[^{}]*(?:\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}[^{}]*)*\})",
)


class ClaudeConversationManager:
    """Claude CLI conversation manager.

    Maintains conversation history (in-memory) and persistent memory
    (on disk, encrypted). Feeds Claude CLI with anonymized health data
    + context. Outbound defense is on the anonymization side.
    """

    def __init__(
        self,
        config: object,
        claude_client: ClaudeClient,
        phi_firewall: PhiFirewall,
        key_manager: object | None = None,
    ) -> None:
        self._config = config
        self._claude = claude_client
        self._fw = phi_firewall
        self._km = key_manager
        self._claude_dir = ensure_claude_dir(config.vault_home)
        self._history: list[dict[str, str]] = []
        self._history_lock = threading.Lock()
        self._memory: list[dict] = []
        self._health_data: str = ""
        self._health_sections: dict[str, str] = {}
        self._context_prompt: str = ""
        self._db: object | None = None
        self._clean_db_available: bool = False
        self._user_id: int = 0
        self._vault: object | None = None
        self._pending_quality_notifications: list[dict] = []
        self._integration_status: str = ""
        self._status_builder: object | None = None
        self._on_system_improvement: object | None = None  # Callable[[dict], None]
        self._cached_user_memory: list[dict] | None = None
        self._memory_cache_ts: float = 0
        self._memory_feedback: list[str] = []
        self._pending_charts: list[dict] = []
        self._last_citations: list[dict] = []
        self._interaction_feedback: list[str] = []

    def load(self) -> None:
        """Load context.md, health data, and memory from disk."""
        self._context_prompt = load_context(self._claude_dir)
        self._health_data = self._load_encrypted_or_migrate(
            enc_name="health_data.enc",
            plain_name="health_data.md",
            aad="relaxed.health_data",
        )
        memory_text = self._load_encrypted_or_migrate(
            enc_name="memory.enc",
            plain_name="memory.json",
            aad="relaxed.memory",
        )
        if memory_text:
            try:
                data = json.loads(memory_text)
                if not isinstance(data, list):
                    logger.warning("Claude memory is not a list, resetting")
                    data = []
                self._memory = data
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning("Failed to parse Claude memory: %s", e)
                self._memory = []

    def handle_message(
        self, user_text: str, user_id: int | None = None,
    ) -> tuple[str, list[str]]:
        """Process a message through Claude CLI.

        Returns (response, pii_warnings). pii_warnings is always empty —
        kept for backward compatibility with callers that unpack the tuple.
        """
        if user_id is not None and user_id > 0:
            self._user_id = user_id
        self._memory_feedback.clear()
        self._pending_charts.clear()
        self._last_citations.clear()
        self._interaction_feedback.clear()
        # Anonymize user text before sending outbound to Claude CLI.
        # Keep the original for local-only operations (interaction scan,
        # medical classification) — safe_anonymize strips PII patterns
        # but preserves clinical vocabulary.
        safe_user_text = _ctx_safe_anonymize(self._get_anonymizer(), user_text)
        system, prompt = build_prompt(self, safe_user_text)
        raw_response = self._claude.send(prompt=prompt, system=system)

        # Extract structured blocks before PII scan (NER can corrupt JSON)
        response, insights = self._extract_insights(raw_response)
        for insight in insights:
            self._store_insight(insight)
        if insights:
            self.save_state()

        # Fallback substance mention scanner (Phase 6)
        if not self._interaction_feedback:
            self._run_fallback_interaction_scan(user_text)

        quality_note = format_quality_notifications(self)
        if quality_note:
            response = f"{response}\n\n---\n{quality_note}"

        # Append interaction feedback (plain text — no markdown)
        if self._interaction_feedback:
            ix_footer = "\n".join(self._interaction_feedback)
            response = f"{response}\n\n---\nInteraction Check:\n{ix_footer}"

        # Append memory feedback footer
        if self._memory_feedback:
            footer = "\n".join(self._memory_feedback)
            response = f"{response}\n\n---\n{footer}"

        # Auto-append Sources footer from CITATION blocks if not already present
        if self._last_citations and "sources:" not in response.lower():
            source_lines: list[str] = []
            for i, cit in enumerate(self._last_citations, 1):
                title = cit.get("title", "")
                url = cit.get("url", "")
                ref = cit.get("reference", "")
                if title and url:
                    source_lines.append(f"{i}. {title} — {url}")
                elif title:
                    source_lines.append(f"{i}. {title}")
                elif ref:
                    source_lines.append(f"{i}. {ref}")
                elif url:
                    source_lines.append(f"{i}. {url}")
            if source_lines:
                sources_text = "\n".join(source_lines)
                response = f"{response}\n\nSources:\n{sources_text}"

        with self._history_lock:
            self._history.append({"role": "user", "content": safe_user_text})
            self._history.append({"role": "assistant", "content": response})
            if len(self._history) > 20 * 2:
                self._history = self._history[-20 * 2:]

        return response, []

    def refresh_data(
        self,
        db: object,
        anonymizer: object,
        phi_firewall: PhiFirewall,
        ollama: object | None = None,
    ) -> str:
        """Trigger fresh AI export and update health data."""
        from healthbot.export.ai_export import AiExporter

        exporter = AiExporter(
            db=db,
            anonymizer=anonymizer,
            phi_firewall=phi_firewall,
            ollama=ollama,
        )
        user_id = 0
        if hasattr(self._config, "allowed_user_ids") and self._config.allowed_user_ids:
            user_id = self._config.allowed_user_ids[0]
        self._db = db
        self._user_id = user_id

        result = exporter.export(user_id)

        # Save encrypted
        self._save_encrypted(
            result.markdown, "health_data.enc", "relaxed.health_data",
        )
        self._health_data = result.markdown

        logger.info("Claude health data refreshed (encrypted)")
        reconcile_demographics_to_ltm(self)
        return result.validation.summary()

    def refresh_data_from_clean_db(self, clean_db: object) -> str:
        """Refresh health data from the clean (pre-anonymized) DB."""
        self._clean_db_available = True
        if hasattr(clean_db, "get_health_summary_sections"):
            self._health_sections = clean_db.get_health_summary_sections()
        markdown = clean_db.get_health_summary_markdown()
        self._save_encrypted(
            markdown, "health_data.enc", "relaxed.health_data",
        )
        self._health_data = markdown
        logger.info("Claude health data refreshed from clean DB")
        return f"Loaded {len(markdown)} chars from clean DB"

    def save_state(self) -> None:
        """Persist memory to disk (encrypted)."""
        text = json.dumps(self._memory, indent=2, ensure_ascii=False)
        self._save_encrypted(text, "memory.enc", "relaxed.memory")

    def clear(self) -> None:
        """Clear conversation history (on lock or mode switch)."""
        with self._history_lock:
            self._history.clear()

    @property
    def has_health_data(self) -> bool:
        """Check if health data has been loaded."""
        return bool(self._health_data)

    def invalidate_memory_cache(self) -> None:
        """Clear cached user memory — call after MEMORY block writes."""
        self._cached_user_memory = None
        self._memory_cache_ts = 0

    def _get_anonymizer(self):
        """Lazily initialize an Anonymizer with NER + regex."""
        if not hasattr(self, "_anon") or self._anon is None:
            from healthbot.llm.anonymizer import Anonymizer

            self._anon = Anonymizer(phi_firewall=self._fw, use_ner=True)
        return self._anon

    def _get_vault(self) -> object | None:
        """Lazily create a Vault for PDF retrieval."""
        if self._vault is not None:
            return self._vault
        if not self._km or not self._config:
            return None
        try:
            from healthbot.security.vault import Vault

            blobs_dir = getattr(self._config, "blobs_dir", None)
            if not blobs_dir:
                return None
            self._vault = Vault(blobs_dir, self._km)
            return self._vault
        except Exception as exc:
            logger.debug("_get_vault failed: %s", exc)
            return None

    # Backward-compat aliases used by tests and other callers
    def _get_clean_db(self):
        return get_clean_db(self)

    def _build_prompt(self, user_text: str) -> tuple[str, str]:
        return build_prompt(self, user_text)

    def _route_block(self, block_type: str, block: dict) -> None:
        route_block(self, block_type, block)

    def _format_quality_notifications(self) -> str:
        return format_quality_notifications(self)

    # ── Backward-compat delegation to extracted modules ─────────

    def _safe_anonymize(self, anon, text: str) -> str:
        return _ctx_safe_anonymize(anon, text)

    def _append_hypotheses(self, parts):
        _ctx_append_hypotheses(self, parts)

    def _append_kb_findings(self, parts, query):
        _ctx_append_kb_findings(self, parts, query)

    def _append_research_evidence(self, parts, query):
        _ctx_append_research_evidence(self, parts, query)

    def _append_user_memory(self, parts):
        _ctx_append_user_memory(self, parts)

    def _append_health_sections(self, parts, user_text):
        _ctx_append_health_sections(self, parts, user_text)

    def _handle_memory_block(self, block):
        _rt_handle_memory_block(self, block)

    def _sync_memory_to_demographics(self, clean_db, key, value):
        _rt_sync_memory_to_demographics(self, clean_db, key, value)

    def _sync_memory_to_ltm(self, key, value):
        _rt_sync_memory_to_ltm(self, key, value)

    def _reconcile_demographics_to_ltm(self):
        reconcile_demographics_to_ltm(self)

    def _handle_system_improvement(self, block):
        _rt_handle_system_improvement(self, block)

    def _handle_data_quality(self, block):
        _rt_handle_data_quality(self, block)

    # ── Encryption helpers ────────────────────────────────────────

    def _save_encrypted(self, text: str, filename: str, aad: str) -> None:
        """Encrypt text with vault master key and write to file."""
        if not self._km:
            return
        path = self._claude_dir / filename
        try:
            key = self._km.get_key()
            nonce = os.urandom(12)
            aesgcm = AESGCM(key)
            ct = aesgcm.encrypt(
                nonce, text.encode("utf-8"), aad.encode("utf-8"),
            )
            path.write_bytes(nonce + ct)
        except Exception as e:
            logger.error("Encryption failed, data NOT saved: %s", e)

    def _decrypt_file(self, filename: str, aad: str) -> str | None:
        """Read and decrypt an encrypted file. Returns None on failure."""
        path = self._claude_dir / filename
        if not path.exists() or not self._km:
            return None
        try:
            key = self._km.get_key()
            blob = path.read_bytes()
            if len(blob) < 28:  # 12-byte nonce + 16-byte AES-GCM tag minimum
                logger.warning(
                    "Encrypted file %s too short: %d bytes", filename, len(blob),
                )
                return None
            nonce = blob[:12]
            ct = blob[12:]
            aesgcm = AESGCM(key)
            plaintext = aesgcm.decrypt(nonce, ct, aad.encode("utf-8"))
            return plaintext.decode("utf-8")
        except Exception as e:
            logger.warning("Failed to decrypt %s: %s", filename, e)
            return None

    def _load_encrypted_or_migrate(
        self, enc_name: str, plain_name: str, aad: str,
    ) -> str:
        """Load from encrypted file, or migrate plaintext if it exists."""
        # Try encrypted file first
        text = self._decrypt_file(enc_name, aad)
        if text is not None:
            return text

        # Fall back to plaintext (migration path)
        plain_path = self._claude_dir / plain_name
        if plain_path.exists():
            text = plain_path.read_text(encoding="utf-8")
            if self._km:
                try:
                    self._save_encrypted(text, enc_name, aad)
                    plain_path.unlink()
                    logger.info(
                        "Migrated %s → %s (encrypted)", plain_name, enc_name,
                    )
                except Exception as e:
                    logger.warning("Migration failed for %s: %s", plain_name, e)
            return text

        return ""

    # ── Insight extraction + routing ──────────────────────────────

    def _extract_insights(self, response: str) -> tuple[str, list[dict]]:
        """Parse all structured medical blocks from response."""
        blocks: list[dict] = []
        for match in _BLOCK_PATTERN.finditer(response):
            block_type = match.group(1)
            try:
                data = json.loads(match.group(2))
                data["_type"] = block_type
                blocks.append(data)
            except (json.JSONDecodeError, ValueError) as exc:
                # Fallback: try json.loads on the raw text (handles double
                # curly braces and other edge cases the regex may mangle)
                raw_text = match.group(2)
                try:
                    data = json.loads(raw_text.replace("{{", "{").replace("}}", "}"))
                    data["_type"] = block_type
                    blocks.append(data)
                    continue
                except (json.JSONDecodeError, ValueError):
                    pass
                raw_block = raw_text[:200]
                logger.warning(
                    "Malformed %s block (skipped): %s — %s",
                    block_type, exc, raw_block,
                )
                if block_type == "MEMORY":
                    self._memory_feedback.append(
                        f"[Failed to parse MEMORY block: {exc}]",
                    )
                continue

        # Strip all structured blocks from user-visible response
        cleaned = _BLOCK_PATTERN.sub("", response)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip(), blocks

    def _store_insight(self, block: dict) -> None:
        """Route a structured block to the appropriate system."""
        block_type = block.get("_type", "INSIGHT")

        # PII check on any text content
        text_to_check = json.dumps(block, ensure_ascii=False)
        if self._fw.contains_phi(text_to_check):
            logger.warning(
                "Blocked %s block with PII: %s",
                block_type, text_to_check[:40],
            )
            if block_type == "MEMORY":
                key = block.get("key", "?")
                self._memory_feedback.append(
                    f"[Could not remember '{key}' — contains sensitive data]",
                )
            return

        # CHART blocks are accumulated for post-response chart generation
        if block_type == "CHART":
            self._pending_charts.append(block)
            return

        # CITATION blocks are accumulated for source follow-up requests
        if block_type == "CITATION":
            self._last_citations.append(block)
            return

        # CHECK_INTERACTION blocks trigger interaction checking
        if block_type == "CHECK_INTERACTION":
            try:
                from healthbot.llm.interaction_block_handler import (
                    handle_check_interaction,
                )
                results = handle_check_interaction(self, block)
                self._interaction_feedback.extend(results)
            except Exception as exc:
                logger.warning("CHECK_INTERACTION handler failed: %s", exc)
            return

        # Route to specialized systems (best effort)
        try:
            if block_type == "MEMORY":
                from healthbot.llm.conversation_routing import handle_memory_block
                feedback = handle_memory_block(self, block)
                if feedback:
                    self._memory_feedback.append(feedback)
                # Skip the general route_block call for MEMORY — already handled
                return
            route_block(self, block_type, block)
        except Exception as exc:
            logger.warning("Failed to route %s block: %s", block_type, exc)
            if block_type == "MEMORY":
                key = block.get("key", "?")
                self._memory_feedback.append(
                    f"[Failed to remember '{key}': {exc}]",
                )

        # New block types have dedicated structured storage — skip flat memory
        # (MEMORY, CHART, CITATION, and CHECK_INTERACTION already returned above)
        if block_type in ("CORRECTION", "SYSTEM_IMPROVEMENT",
                          "HEALTH_DATA", "ANALYSIS_RULE"):
            return

        # Always store in flat memory for PREVIOUS INSIGHTS section
        fact_text = block.get("fact") or block.get("finding") or block.get("title", "")
        self._memory.append({
            "fact": fact_text,
            "category": block.get("category", block_type.lower()),
            "timestamp": datetime.now(UTC).isoformat(),
        })

    def _run_fallback_interaction_scan(self, user_text: str) -> None:
        """Fallback: scan user text for substance mentions if no CHECK_INTERACTION."""
        try:
            from healthbot.llm.interaction_block_handler import (
                handle_check_interaction,
                scan_for_substance_mentions,
            )

            active_meds = []
            try:
                from healthbot.llm.interaction_block_handler import (
                    get_active_medications,
                )
                active_meds = get_active_medications(self)
            except Exception:
                pass

            detected = scan_for_substance_mentions(
                user_text, active_meds,
            )
            for substance in detected[:3]:  # Limit to avoid spam
                results = handle_check_interaction(
                    self, {"substance": substance, "intent": "checking_safety"},
                )
                self._interaction_feedback.extend(results)
        except Exception as exc:
            logger.debug("Fallback interaction scan failed: %s", exc)
