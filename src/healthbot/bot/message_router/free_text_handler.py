"""Claude CLI conversation + troubleshoot methods."""
from __future__ import annotations

import asyncio
import logging

from telegram import Update

from healthbot.bot.formatters import paginate, strip_markdown
from healthbot.bot.typing_helper import TypingIndicator
from healthbot.reasoning.triage import TriageEngine

logger = logging.getLogger("healthbot")


class FreeTextMixin:
    """Mixin providing free-text conversation and troubleshooting methods."""

    async def _handle_free_text(self, update: Update, user_id: int) -> None:
        """Route all free-text conversation through Claude CLI."""
        # Emergency triage (deterministic — always runs first)
        text = update.message.text
        triage = TriageEngine()
        level, msg = triage.check_emergency_keywords(text)
        if level:
            await update.message.reply_text(f"EMERGENCY: {msg}")
            return

        claude = self._get_claude() if self._get_claude else None
        if claude is None:
            await update.message.reply_text(
                "Claude CLI not available. Install it:\n"
                "  brew install claude-code\n"
                "Use /commands for health analysis."
            )
            return

        # Send a visible placeholder so the user knows it's working
        thinking_msg = None
        try:
            thinking_msg = await update.message.reply_text("Thinking...")
            async with TypingIndicator(update.effective_chat):
                response, pii_warnings = await asyncio.to_thread(
                    claude.handle_message, text, user_id,
                )

            # Check for error sentinel responses
            from healthbot.llm.claude_client import (
                _CLI_ERROR_RESPONSE,
                _TIMEOUT_RESPONSE,
            )
            if response == _TIMEOUT_RESPONSE:
                await update.message.reply_text(
                    "Claude took too long to respond.\n"
                    "Try a shorter question, or use a /command."
                )
                return
            if _CLI_ERROR_RESPONSE in response or response.startswith(
                "Claude CLI error:"
            ):
                await update.message.reply_text(
                    "Claude CLI returned an error.\n"
                    "Run /claude_auth check to diagnose.\n"
                    "Use /commands for health analysis in the meantime."
                )
                return

            if self._exchange_cb:
                self._exchange_cb(text, response)
            self._last_user_input = text
            self._last_bot_response = response
            response = strip_markdown(response)
            chat_id = update.effective_chat.id if update.effective_chat else None
            for page in paginate(response):
                sent = await update.message.reply_text(page)
                if self._track_msg_cb and chat_id and sent:
                    self._track_msg_cb(chat_id, sent.message_id)

            # Generate charts requested by Claude via CHART blocks
            for chart_req in getattr(claude, "_pending_charts", [])[:3]:
                try:
                    import io

                    if not isinstance(chart_req, dict):
                        continue
                    from healthbot.export.chart_dispatch import dispatch as chart_dispatch

                    chart_bytes = chart_dispatch(chart_req, self._get_db(), user_id)
                    if chart_bytes:
                        chart_type = chart_req.get("type", "trend")
                        label = chart_req.get("metric", chart_type)
                        img = io.BytesIO(chart_bytes)
                        img.name = f"{chart_type}_{label}.png"
                        await update.message.reply_photo(photo=img)
                except Exception as exc:
                    logger.debug("CHART block skipped: %s", exc)
        except Exception as e:
            from healthbot.llm.claude_client import CLIAuthError

            if isinstance(e, CLIAuthError):
                await update.message.reply_text(
                    "Claude CLI is not authenticated.\n"
                    "Run 'claude login' in your terminal,\n"
                    "or /claude_auth setup to use an API key."
                )
                return
            logger.error("Claude conversation error: %s", e)
            await update.message.reply_text(
                "Error talking to Claude. Try again or use a /command.\n"
                "Run /claude_auth check if this keeps happening."
            )
        finally:
            if thinking_msg:
                try:
                    await thinking_msg.delete()
                except Exception:
                    pass

    async def _post_ingestion_analysis(
        self, update: Update, user_id: int, lab_results: list,
    ) -> None:
        """Trigger Claude deep analysis after new lab data arrives."""
        claude = self._get_claude() if self._get_claude else None
        if claude is None:
            return

        # Build a summary of new results (max 20)
        def _val(obj, key, default=""):
            """Get value from LabResult object or dict."""
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

        lines = []
        collection_date = None
        for lab in lab_results[:20]:
            name = _val(lab, "test_name", "?")
            val = _val(lab, "value", "?")
            unit = _val(lab, "unit", "")
            flag = _val(lab, "flag", "")
            cd = _val(lab, "date_collected", None)
            line = f"- {name}: {val} {unit}"
            if flag:
                line += f" ({flag})"
            lines.append(line)
            if not collection_date and cd:
                collection_date = cd
        lab_summary = "\n".join(lines)

        # Determine patient age at time of collection
        age_context = ""
        try:
            from healthbot.security.identity_profile import IdentityProfile
            db = self._get_db()
            profile = IdentityProfile(db=db)
            fields = profile.get_all_fields(user_id)
            dob_str = None
            for f in fields:
                if f.get("field_type") == "dob":
                    dob_str = f.get("value")
                    break
            if dob_str:
                from datetime import date as _date
                # Parse YYYY-MM-DD or MM/DD/YYYY
                if "-" in dob_str and len(dob_str.split("-")[0]) == 4:
                    dob = _date.fromisoformat(dob_str)
                elif "/" in dob_str:
                    parts = dob_str.split("/")
                    dob = _date(int(parts[2]), int(parts[0]), int(parts[1]))
                else:
                    dob = None
                if dob:
                    ref_date = collection_date or _date.today()
                    if isinstance(ref_date, str):
                        ref_date = _date.fromisoformat(ref_date)
                    age = (
                        ref_date.year - dob.year
                        - ((ref_date.month, ref_date.day) < (dob.month, dob.day))
                    )
                    age_context = f"Patient age: {age} at time of collection. "
        except Exception:
            pass

        date_context = ""
        if collection_date:
            date_context = f"Collection date: {collection_date}. "

        prompt = (
            f"New lab results just arrived:\n{lab_summary}\n\n"
            f"{date_context}{age_context}"
            "Give me a brief analysis — plain text only, no markdown "
            "formatting (no **, no ###, no tables). Keep it short:\n"
            "1. What stands out or changed from prior results\n"
            "2. Anything concerning (be direct, account for age)\n"
            "3. What to test next (if anything)\n"
            "Skip normal results. Only mention what matters."
        )

        try:
            async with TypingIndicator(update.effective_chat):
                response, _ = await asyncio.to_thread(
                    claude.handle_message, prompt, user_id,
                )
            if response:
                response = strip_markdown(response)
                for page in paginate(response):
                    await update.message.reply_text(page)
        except Exception as e:
            logger.warning("Post-ingestion analysis failed: %s", e)

    async def _handle_troubleshoot(self, update: Update, user_text: str) -> None:
        """Route technical questions to Claude CLI for debugging.

        Pulls recent errors from the error buffer, builds a debug prompt
        (no PHI), and calls Claude CLI with full code access (Read, Edit,
        Bash, Write, Glob, Grep, WebSearch, WebFetch) so it can diagnose
        AND fix issues. Health data stays safe — it lives in the encrypted
        database, not in source code.
        """
        await update.message.reply_text("Looking into it...")

        # Build error context from recent buffer
        error_lines: list[str] = []
        if self._get_errors:
            for rec in self._get_errors():
                line = f"- [{rec.timestamp}] {rec.error_type}: {rec.message}"
                if rec.provider:
                    line += f" (provider: {rec.provider})"
                if rec.hint:
                    line += f"\n  Hint: {rec.hint}"
                error_lines.append(line)

        error_context = "\n".join(error_lines) if error_lines else ""

        try:
            async with TypingIndicator(update.effective_chat):
                from healthbot.research.claude_cli_client import ClaudeCLIResearchClient
                from healthbot.security.phi_firewall import PhiFirewall

                fw = self._fw or PhiFirewall()
                client = ClaudeCLIResearchClient(self._config, fw)
                result = await asyncio.to_thread(
                    client.debug, user_text, error_context,
                )

            result = strip_markdown(result)
            for page in paginate(result):
                await update.message.reply_text(page)
        except Exception as e:
            logger.error("Troubleshoot error: %s", e)
            await update.message.reply_text(
                f"Couldn't reach Claude CLI for debugging: {type(e).__name__}\n"
                "Make sure Claude CLI is installed: brew install claude-code"
            )
