"""Clean sync and callback handlers mixin."""
from __future__ import annotations

import logging
import threading

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.middleware import require_unlocked

from ._helpers import _fmt_duration, _format_estimate, _format_final, _format_progress

logger = logging.getLogger("healthbot")


class CleanSyncMixin:
    """Handlers for /cleansync and its inline keyboard callback."""

    @require_unlocked
    async def cleansync(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /cleansync command -- smart mode selection + live progress.

        Usage: /cleansync          — show preview with mode selection buttons
               /cleansync fast     — regex+NER+cache only (no Ollama)
               /cleansync hybrid   — fast pass + selective Ollama review
               /cleansync full     — all 3 layers (best PII coverage)
               /cleansync reset    — clear cache + re-anonymize everything
               /cleansync rebuild  — same as reset
        """
        import asyncio

        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        args = context.args
        # Direct mode shortcuts: skip the preview prompt
        if args:
            mode = args[0].lower()
            if mode == "fast":
                await self._run_cleansync(update, "fast")
                return
            if mode in ("hybrid", "smart"):
                await self._run_cleansync(update, "hybrid")
                return
            if mode == "full":
                await self._run_cleansync(update, "full")
                return
            if mode in ("reset", "rebuild", "force"):
                await self._run_cleansync(update, "rebuild")
                return

        # Phase 1: Show estimate with mode selection buttons
        status_msg = await update.message.reply_text(
            "Calculating sync estimate..."
        )
        est = await asyncio.to_thread(self._core.estimate_clean_sync)

        if est is None:
            await status_msg.edit_text(
                "Could not estimate sync. Vault may be locked."
            )
            return

        preview = _format_estimate(est)
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "Fast", callback_data="cleansync:fast",
                ),
                InlineKeyboardButton(
                    "Hybrid", callback_data="cleansync:hybrid",
                ),
            ],
            [
                InlineKeyboardButton(
                    "Full", callback_data="cleansync:full",
                ),
                InlineKeyboardButton(
                    "Rebuild", callback_data="cleansync:rebuild",
                ),
            ],
        ])
        await status_msg.edit_text(preview, reply_markup=keyboard)

    async def handle_cleansync_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle inline keyboard button press for /cleansync mode selection."""
        if not self._check_auth(update):
            query = update.callback_query
            await query.answer()
            await query.edit_message_text("Unauthorized.")
            return
        query = update.callback_query
        await query.answer()

        data = query.data or ""
        mode = data.removeprefix("cleansync:")
        if mode not in ("fast", "hybrid", "full", "rebuild"):
            return

        await self._run_cleansync(update, mode, status_msg=query.message)

    async def _run_cleansync(
        self,
        update: Update,
        mode: str,
        status_msg=None,
    ) -> None:
        """Execute clean sync with live progress updates."""
        import asyncio
        import time

        skip_ollama = mode == "fast"
        is_rebuild = mode == "rebuild"
        is_full = mode in ("full", "fast", "hybrid")

        mode_labels = {
            "fast": "Fast (regex + NER + identity)",
            "hybrid": "Hybrid (fast + selective Ollama)",
            "full": "Full (all layers)",
            "rebuild": "Rebuild (clear cache + re-anonymize)",
        }

        if status_msg is None:
            status_msg = await update.effective_chat.send_message(
                f"Starting {mode_labels[mode]}..."
            )
        else:
            try:
                await status_msg.edit_text(
                    f"Starting {mode_labels[mode]}...",
                    reply_markup=None,
                )
            except Exception:
                pass

        sync_done = threading.Event()
        sync_result: list = []  # [report_or_none]
        engine_ref: list = []   # [engine]

        def _run_sync() -> None:
            report = self._core._trigger_clean_sync(
                full=is_full, rebuild=is_rebuild,
                skip_ollama=skip_ollama, mode=mode,
            )
            # Grab engine reference for progress reading
            eng = getattr(self._core, "_last_sync_engine", None)
            if eng:
                engine_ref.append(eng)
            sync_result.append(report)
            sync_done.set()

        # Start sync in background thread
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, _run_sync)

        # Phase 2: Live progress polling (2h safety timeout)
        max_seconds = 7200  # 2 hours
        start = time.monotonic()
        while not sync_done.is_set():
            await asyncio.sleep(5)
            elapsed = time.monotonic() - start
            if elapsed > max_seconds:
                try:
                    await status_msg.edit_text(
                        f"Sync timed out after {_fmt_duration(elapsed)}.\n"
                        "The operation exceeded the 2-hour safety limit.\n"
                        "Check /debug for details."
                    )
                except Exception:
                    pass
                break
            eng = engine_ref[0] if engine_ref else None
            prog = eng.progress if eng else None
            if prog:
                text = _format_progress(prog, elapsed)
                try:
                    await status_msg.edit_text(text)
                except Exception:
                    pass

        # Phase 3: Final report
        report = sync_result[0] if sync_result else None
        elapsed = time.monotonic() - start
        eng = engine_ref[0] if engine_ref else None

        if report is None:
            await status_msg.edit_text(
                "Clean sync failed. Check /debug for details."
            )
            return

        # Detect lock contention
        _all_zero = (
            not report.observations_synced
            and not report.medications_synced
            and not report.wearables_synced
            and not report.hypotheses_synced
            and not report.health_context_synced
            and not report.workouts_synced
            and not report.genetic_variants_synced
            and not report.health_goals_synced
            and not report.med_reminders_synced
            and not report.providers_synced
            and not report.appointments_synced
            and not report.stale_deleted
            and not report.errors
        )
        if _all_zero:
            await status_msg.edit_text(
                "Clean sync returned empty results.\n"
                "Another sync may already be running -- try again shortly."
            )
            return

        final = _format_final(report, eng.progress if eng else None, elapsed)
        try:
            await status_msg.edit_text(final)
        except Exception:
            pass
