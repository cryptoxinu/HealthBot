"""Data import/export and wearable sync command handlers."""
from __future__ import annotations

import logging
import re
import threading
from pathlib import Path

from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import ContextTypes

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.middleware import require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


_DEVELOPER_URLS: dict[str, str] = {
    "WHOOP": "https://developer.whoop.com",
    "Oura Ring": "https://developer.ouraring.com",
}


def _fmt_duration(seconds: float) -> str:
    """Format seconds as human-readable duration."""
    if seconds < 60:
        return f"{int(seconds)} sec"
    if seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}:{s:02d}"
    h, remainder = divmod(int(seconds), 3600)
    m, _ = divmod(remainder, 60)
    return f"{h}h {m}m"


def _format_estimate(est) -> str:
    """Format a SyncEstimate as a Telegram-friendly preview message."""
    lines = ["Clean Sync Preview\n"]
    record_parts = []
    for label, count in [
        ("obs", est.obs_count), ("meds", est.meds_count),
        ("hypotheses", est.hyps_count), ("context", est.ctx_count),
        ("goals", est.goals_count), ("reminders", est.reminders_count),
        ("providers", est.providers_count), ("appts", est.appointments_count),
        ("wearable days", est.wearable_count), ("genetics", est.genetics_count),
        ("ext records", est.ext_count),
    ]:
        if count:
            record_parts.append(f"{count} {label}")
    if record_parts:
        lines.append("Records: " + ", ".join(record_parts))

    lines.append(f"Text fields to anonymize: ~{est.total_text_fields:,}")
    if est.cache_size:
        cache_pct = min(100, int(est.cache_size / max(est.total_text_fields, 1) * 100))
        lines.append(f"Cache: {est.cache_size:,} already cached ({cache_pct}%)")

    uncached = max(0, est.total_text_fields - est.estimated_safe_skip - est.cache_size)
    if uncached:
        lines.append(f"Uncached fields to process: ~{uncached}")

    lines.append("\nChoose mode:\n")
    lines.append(
        f"[Fast] Regex + NER + identity -- ~{_fmt_duration(est.estimated_fast_sec)}\n"
        "  Catches known names/DOB/patterns. No LLM.\n"
    )
    lines.append(
        f"[Hybrid] Smart -- ~{_fmt_duration(est.estimated_hybrid_sec)}\n"
        f"  Fast pass first, Ollama reviews ~{est.hybrid_ollama_fields:,} uncertain fields.\n"
    )
    lines.append(
        f"[Full] All layers -- ~{_fmt_duration(est.estimated_full_sec)}\n"
        "  Ollama on every field. Most thorough.\n"
    )
    lines.append(
        f"[Rebuild] -- ~{_fmt_duration(est.estimated_rebuild_sec)}\n"
        "  Clear cache + full re-anonymize."
    )
    return "\n".join(lines)


def _format_progress(prog, elapsed: float) -> str:
    """Format live SyncProgress as a Telegram message."""
    lines = [f"Syncing... ({_fmt_duration(elapsed)} elapsed)\n"]

    all_phases = [
        "Observations", "Medications", "Wearables", "Demographics",
        "Hypotheses", "Health context", "Workouts", "Genetics",
        "Goals", "Reminders", "Providers", "Appointments", "Extended records",
        "Ollama review",
    ]
    for phase in all_phases:
        if phase in prog.phases_completed:
            lines.append(f"  [done] {phase}")
        elif phase == prog.current_phase:
            if prog.phase_total:
                lines.append(f"  [....] {phase}: {prog.phase_done}/{prog.phase_total}")
            else:
                lines.append(f"  [....] {phase}")
        # Don't show Ollama review if not in hybrid mode
        elif phase == "Ollama review" and not prog.hybrid_queued:
            continue
        else:
            lines.append(f"  [    ] {phase}")

    anon_parts = [
        f"{prog.safe_skipped} safe-skipped",
        f"{prog.cache_hits} cached",
    ]
    if prog.hybrid_queued:
        anon_parts.append(f"{prog.hybrid_reviewed}/{prog.hybrid_queued} Ollama-reviewed")
    elif prog.ollama_calls:
        anon_parts.append(f"{prog.ollama_calls} Ollama")
    lines.append(f"\nAnonymization: {', '.join(anon_parts)}")
    return "\n".join(lines)


def _format_final(report, prog, elapsed: float) -> str:
    """Format the final sync report."""
    lines = [f"Clean sync complete ({_fmt_duration(elapsed)})\n"]

    field_labels = [
        ("observations_synced", "Observations"),
        ("medications_synced", "Medications"),
        ("wearables_synced", "Wearables"),
        ("demographics_synced", "Demographics"),
        ("hypotheses_synced", "Hypotheses"),
        ("health_context_synced", "Health context"),
        ("workouts_synced", "Workouts"),
        ("genetic_variants_synced", "Genetics"),
        ("health_goals_synced", "Goals"),
        ("med_reminders_synced", "Reminders"),
        ("providers_synced", "Providers"),
        ("appointments_synced", "Appointments"),
        ("health_records_ext_synced", "Extended records"),
    ]
    parts = []
    for attr, label in field_labels:
        val = getattr(report, attr, 0)
        if isinstance(val, bool):
            parts.append(f"{label}: {'yes' if val else 'no'}")
        elif val:
            parts.append(f"{label}: {val}")
    if parts:
        lines.append(" | ".join(parts))

    extras = []
    if report.stale_deleted:
        extras.append(f"Stale removed: {report.stale_deleted}")
    if report.pii_blocked:
        extras.append(f"PII blocked: {report.pii_blocked}")
    if report.errors:
        extras.append(f"Errors: {len(report.errors)}")
    if extras:
        lines.append(" | ".join(extras))

    if prog:
        speed_parts = [
            f"{prog.safe_skipped} safe-skipped",
            f"{prog.cache_hits} cached",
        ]
        if prog.hybrid_queued:
            speed_parts.append(
                f"{prog.hybrid_reviewed}/{prog.hybrid_queued} Ollama-reviewed"
            )
        elif prog.ollama_calls:
            speed_parts.append(f"{prog.ollama_calls} via Ollama")
        lines.append(f"\nSpeed: {', '.join(speed_parts)}")
    return "\n".join(lines)


class DataHandlers:
    """Data import, export, and wearable synchronization commands."""

    def __init__(self, core: HandlerCore) -> None:
        self._core = core
        # Per-user wearable credential setup state
        self._setup_state: dict[int, dict] = {}

    @property
    def _km(self):
        return self._core._km

    def _check_auth(self, update: Update) -> bool:
        return self._core._check_auth(update)

    def is_awaiting_setup(self, user_id: int) -> bool:
        """Check if user is in the middle of wearable credential setup."""
        return user_id in self._setup_state

    @staticmethod
    def _extract_credential(text: str) -> str:
        """Extract a credential value from natural language input.

        Users often type 'This is the client ID abc-123' or 'my key is xyz'.
        Extract just the credential (UUID, hex, or long alphanumeric token).
        """
        # Try UUID pattern first (most common for OAuth client IDs)
        uuid_match = re.search(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            text, re.IGNORECASE,
        )
        if uuid_match:
            return uuid_match.group(0)

        # Try long hex or alphanumeric token (32+ chars, typical for secrets)
        token_match = re.search(r"[A-Za-z0-9_\-]{32,}", text)
        if token_match:
            return token_match.group(0)

        # Fall back to last whitespace-delimited token (the value is usually last)
        parts = text.split()
        if len(parts) > 1:
            # If last token looks like a credential (has digits+letters or dashes)
            last = parts[-1]
            if re.search(r"[0-9]", last) and len(last) >= 8:
                return last

        # Return as-is if we can't parse it
        return text

    async def handle_setup_input(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> bool:
        """Process credential input during wearable setup. Returns True if consumed."""
        user_id = update.effective_user.id if update.effective_user else 0
        state = self._setup_state.get(user_id)
        if not state:
            return False
        if not update.message.text or not update.message.text.strip():
            await update.message.reply_text(
                f"Please send your {state['name']} "
                f"{'Client ID' if state['step'] == 'client_id' else 'Client Secret'}."
            )
            return True

        from healthbot.security.keychain import Keychain

        keychain = Keychain()
        text = update.message.text.strip()

        # Handle /cancel
        if text.lower() == "/cancel":
            self._setup_state.pop(user_id, None)
            await update.message.reply_text("Setup cancelled.")
            return True

        if state["step"] == "client_id":
            # Delete the message (contains credential)
            try:
                await update.message.delete()
            except Exception:
                pass
            # Extract UUID/credential from natural language
            extracted = self._extract_credential(text)
            state["client_id"] = extracted
            state["step"] = "client_secret"
            await update.effective_chat.send_message(
                f"Got it. Now send me your {state['name']} Client Secret.\n"
                "(Your message will be deleted immediately for security.)"
            )
            return True

        if state["step"] == "client_secret":
            # Delete the message containing the secret immediately
            try:
                await update.message.delete()
            except Exception:
                pass

            secret = self._extract_credential(text)
            keychain.store(state["keychain_id_key"], state["client_id"])
            keychain.store(state["keychain_secret_key"], secret)
            name = state["name"]
            auth_cmd = state["auth_cmd"]
            del self._setup_state[user_id]

            await update.effective_chat.send_message(
                f"{name} credentials stored in Keychain.\n"
                f"Starting authorization... (same as {auth_cmd})"
            )

            # Auto-trigger the auth flow now that credentials exist
            if "whoop" in auth_cmd.lower():
                await self.whoop_auth(update, context)
            else:
                await self.oura_auth(update, context)
            return True

        return False

    def _rebuild_search_index(self) -> None:
        """Rebuild the search index after data ingestion."""
        try:
            from healthbot.retrieval.search import SearchEngine
            from healthbot.security.vault import Vault

            db = self._core._get_db()
            vault = Vault(self._core._config.blobs_dir, self._core._km)
            engine = SearchEngine(self._core._config, db, vault)
            count = engine.build_index()
            logger.info("Search index rebuilt: %d documents", count)
        except Exception as e:
            logger.debug("Search index rebuild skipped: %s", e)

    async def _post_sync_claude_analysis(
        self,
        update: Update,
        user_id: int,
        count: int,
        days: int,
        provider: str,
    ) -> None:
        """Trigger Claude deep analysis after wearable sync."""
        import asyncio

        from healthbot.bot.formatters import paginate, strip_markdown
        from healthbot.bot.typing_helper import TypingIndicator

        claude_getter = getattr(self._core._router, "_get_claude", None)
        claude = claude_getter() if claude_getter else None
        if claude is None:
            return

        # Refresh Claude context so it has the new wearable data
        try:
            clean = self._core._get_clean_db()
            if clean:
                try:
                    claude.refresh_data_from_clean_db(clean)
                finally:
                    clean.close()
        except Exception as e:
            logger.warning("Post-sync context refresh failed: %s", e)

        # Build wearable summary from DB so Claude has actual numbers
        data_summary = "No detailed metrics available"
        try:
            db = self._core._get_db()
            rows = db.query_wearable_daily(
                provider=provider.lower(), limit=1, user_id=user_id,
            )
            if rows:
                recent = rows[0]
                bits = []
                for key, label, suffix in [
                    ("hrv", "HRV", "ms"),
                    ("rhr", "RHR", "bpm"),
                    ("recovery_score", "Recovery", ""),
                    ("sleep_score", "Sleep score", ""),
                    ("strain", "Strain", ""),
                    ("sleep_hours", "Sleep", "h"),
                ]:
                    val = recent.get(key)
                    if val is not None:
                        bits.append(f"{label}: {val}{suffix}")
                if bits:
                    data_summary = ", ".join(bits)
        except Exception:
            pass

        # Include total DB record count so Claude knows the full picture
        total_info = ""
        try:
            stats = db.query_wearable_stats(provider.lower())
            if stats:
                total_info = (
                    f"Total in database: {stats['count']} records "
                    f"({stats['first_date']} to {stats['last_date']}). "
                )
        except Exception:
            pass

        prompt = (
            f"Synced {count} new {provider} records. {total_info}"
            f"Latest: {data_summary}.\n\n"
            "Analyze what stands out across the FULL history in your context. "
            "Cross-reference with labs. Only mention what matters. "
            "Reply in plain text only — no markdown formatting."
        )

        try:
            async with TypingIndicator(update.effective_chat):
                response, _ = await asyncio.to_thread(
                    claude.handle_message, prompt,
                )
            if response:
                response = strip_markdown(response)
                for page in paginate(response):
                    await update.message.reply_text(page)
        except Exception as e:
            logger.warning("Post-%s sync analysis failed: %s", provider, e)

    @require_unlocked
    async def whoop_auth(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /whoop_auth -- complete WHOOP OAuth 2.0 authorization."""
        from healthbot.importers.whoop_client import WhoopClient

        await self._wearable_auth(
            update, context, "WHOOP", "whoop_client_id",
            WhoopClient, "/sync", "/whoop_auth",
        )

    @require_unlocked
    async def sync_whoop(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /sync command -- sync WHOOP data."""
        await update.message.reply_text("Syncing WHOOP data...")
        try:
            async with TypingIndicator(update.effective_chat):
                from healthbot.importers.whoop_client import WhoopAuthError, WhoopClient
                from healthbot.security.keychain import Keychain
                from healthbot.security.vault import Vault

                db = self._core._get_db()
                keychain = Keychain()
                vault = Vault(self._core._config.blobs_dir, self._core._km)
                client = WhoopClient(self._core._config, keychain, vault)

                days = 365
                if context.args:
                    try:
                        days = int(context.args[0])
                    except ValueError:
                        pass

                uid = update.effective_user.id if update.effective_user else 0
                # Write directly to Clean DB (wearable data is numeric, no PII)
                clean = self._core._get_clean_db()
                try:
                    count = await client.sync_daily(
                        db, days=days, user_id=uid, clean_db=clean,
                    )
                finally:
                    if clean:
                        clean.close()
                await update.message.reply_text(
                    f"WHOOP sync complete: {count} records imported ({days} days)."
                )
                # Cross-source dedup check against Apple Health
                if count > 0:
                    try:
                        from healthbot.reasoning.wearable_dedup import WearableDedup

                        dedup = WearableDedup(db)
                        dedup_report = dedup.check(
                            days=days, user_id=uid, provider="whoop",
                        )
                        if dedup_report.duplicates_found:
                            await update.message.reply_text(dedup_report.summary())
                    except Exception as e:
                        logger.debug("Dedup check skipped: %s", e)

                # Post-sync Claude analysis
                if count > 0:
                    await self._post_sync_claude_analysis(
                        update, uid, count, days, "WHOOP",
                    )
        except WhoopAuthError as e:
            self._core.record_error(
                "WhoopAuthError", str(e), provider="whoop",
                hint="OAuth token may be expired — try /whoop_auth",
            )
            await update.message.reply_text(
                f"WHOOP auth error: {e}\nRun: healthbot --setup to configure WHOOP."
            )
        except Exception as e:
            self._core.record_error(
                type(e).__name__, str(e), provider="whoop",
                hint="WHOOP sync failure",
            )
            logger.error("WHOOP sync error: %s", e)
            await update.message.reply_text(f"WHOOP sync failed: {type(e).__name__}")

    @require_unlocked
    async def oura_auth(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /oura_auth -- complete Oura Ring OAuth 2.0 authorization."""
        from healthbot.importers.oura_client import OuraClient

        await self._wearable_auth(
            update, context, "Oura Ring", "oura_client_id",
            OuraClient, "/oura", "/oura_auth",
        )

    @staticmethod
    def _is_valid_credential(value: str) -> bool:
        """Check if a stored credential looks like a valid UUID or token."""
        # UUID format (most OAuth client IDs)
        if re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            value, re.IGNORECASE,
        ):
            return True
        # Long alphanumeric token (32+ chars, no spaces)
        if len(value) >= 32 and " " not in value:
            return True
        return False

    # ── Universal sync + connectors ─────────────────────────────────

    def _get_connected_sources(self) -> list[dict]:
        """Detect all connected wearable/API data sources."""
        from healthbot.security.keychain import Keychain

        keychain = Keychain()
        sources = []

        # WHOOP
        whoop_id = keychain.retrieve("whoop_client_id")
        if whoop_id and self._is_valid_credential(whoop_id):
            sources.append({"name": "WHOOP", "provider": "whoop"})

        # Oura Ring
        oura_id = keychain.retrieve("oura_client_id")
        if oura_id and self._is_valid_credential(oura_id):
            sources.append({"name": "Oura Ring", "provider": "oura"})

        # Apple Health (file-based — configured if export path exists)
        export_path = getattr(self._core._config, "apple_health_export_path", "")
        if export_path:
            path = Path(export_path).expanduser()
            if path.exists():
                sources.append({"name": "Apple Health", "provider": "apple_health"})

        return sources

    def _get_last_sync_date(self, db, provider: str, user_id: int) -> str:
        """Get the most recent sync date for a wearable provider."""
        try:
            rows = db.query_wearable_daily(
                provider=provider, limit=1, user_id=user_id,
            )
            if rows:
                return rows[0].get("_date", rows[0].get("date", ""))
        except Exception:
            pass
        return ""

    @require_unlocked
    async def sync_all(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /sync — sync all connected data sources."""
        import asyncio

        sources = self._get_connected_sources()
        if not sources:
            await update.message.reply_text(
                "No data sources connected.\n"
                "Use /connectors to see available integrations."
            )
            return

        days = 365
        if context.args:
            try:
                days = int(context.args[0])
            except ValueError:
                pass

        names = [s["name"] for s in sources]
        await update.message.reply_text(f"Syncing {', '.join(names)}...")

        uid = update.effective_user.id if update.effective_user else 0

        # Build coroutines for all connected providers and run concurrently
        async with TypingIndicator(update.effective_chat):
            tasks = []
            for source in sources:
                if source["provider"] == "whoop":
                    tasks.append(self._sync_provider_whoop(days, uid))
                elif source["provider"] == "oura":
                    tasks.append(self._sync_provider_oura(days, uid))
                elif source["provider"] == "apple_health":
                    tasks.append(self._sync_provider_apple(uid))
            results = await asyncio.gather(*tasks)

        # Report
        lines = []
        for r in results:
            if r.get("error"):
                lines.append(f"  {r['name']}: {r['error']}")
            else:
                lines.append(f"  {r['name']}: {r['count']} records")
        await update.message.reply_text("Sync complete:\n" + "\n".join(lines))

        # One Claude analysis for the wearable provider with most new data
        # (Apple Health stores observations, not wearable_daily, so skip it)
        wearable_results = [
            r for r in results
            if r.get("count", 0) > 0
            and r["name"] in ("WHOOP", "Oura Ring")
        ]
        best = max(wearable_results, key=lambda r: r["count"], default=None)
        if best:
            await self._post_sync_claude_analysis(
                update, uid, best["count"], days, best["name"],
            )

    async def _sync_provider_whoop(
        self, days: int, user_id: int,
    ) -> dict:
        """Sync WHOOP data. Returns result dict."""
        try:
            from healthbot.importers.whoop_client import WhoopAuthError, WhoopClient
            from healthbot.security.keychain import Keychain
            from healthbot.security.vault import Vault

            db = self._core._get_db()
            keychain = Keychain()
            vault = Vault(self._core._config.blobs_dir, self._core._km)
            client = WhoopClient(self._core._config, keychain, vault)

            clean = self._core._get_clean_db()
            try:
                count = await client.sync_daily(
                    db, days=days, user_id=user_id, clean_db=clean,
                )
            finally:
                if clean:
                    clean.close()

            # Cross-source dedup check
            if count > 0:
                try:
                    from healthbot.reasoning.wearable_dedup import WearableDedup

                    dedup = WearableDedup(db)
                    dedup_report = dedup.check(
                        days=days, user_id=user_id, provider="whoop",
                    )
                    if dedup_report.duplicates_found:
                        logger.info("WHOOP dedup: %s", dedup_report.summary())
                except Exception as e:
                    logger.debug("WHOOP dedup check skipped: %s", e)

            return {"name": "WHOOP", "count": count, "error": None}
        except WhoopAuthError as e:
            self._core.record_error(
                "WhoopAuthError", str(e), provider="whoop",
                hint="OAuth token may be expired — try /whoop_auth",
            )
            return {"name": "WHOOP", "count": 0, "error": f"auth error: {e}"}
        except Exception as e:
            self._core.record_error(
                type(e).__name__, str(e), provider="whoop",
                hint="WHOOP sync failure",
            )
            logger.error("WHOOP sync error: %s", e)
            return {"name": "WHOOP", "count": 0, "error": type(e).__name__}

    async def _sync_provider_oura(
        self, days: int, user_id: int,
    ) -> dict:
        """Sync Oura Ring data. Returns result dict."""
        try:
            from healthbot.importers.oura_client import OuraAuthError, OuraClient
            from healthbot.security.keychain import Keychain
            from healthbot.security.vault import Vault

            db = self._core._get_db()
            keychain = Keychain()
            vault = Vault(self._core._config.blobs_dir, self._core._km)
            client = OuraClient(self._core._config, keychain, vault)

            clean = self._core._get_clean_db()
            try:
                count = await client.sync_daily(
                    db, days=days, user_id=user_id, clean_db=clean,
                )
            finally:
                if clean:
                    clean.close()

            # Cross-source dedup check
            if count > 0:
                try:
                    from healthbot.reasoning.wearable_dedup import WearableDedup

                    dedup = WearableDedup(db)
                    dedup_report = dedup.check(
                        days=days, user_id=user_id, provider="oura",
                    )
                    if dedup_report.duplicates_found:
                        logger.info("Oura dedup: %s", dedup_report.summary())
                except Exception as e:
                    logger.debug("Oura dedup check skipped: %s", e)

            return {"name": "Oura Ring", "count": count, "error": None}
        except OuraAuthError as e:
            self._core.record_error(
                "OuraAuthError", str(e), provider="oura",
                hint="OAuth token may be expired — try /oura_auth",
            )
            return {"name": "Oura Ring", "count": 0, "error": f"auth error: {e}"}
        except Exception as e:
            self._core.record_error(
                type(e).__name__, str(e), provider="oura",
                hint="Oura sync failure",
            )
            logger.error("Oura sync error: %s", e)
            return {"name": "Oura Ring", "count": 0, "error": type(e).__name__}

    async def _sync_provider_apple(self, user_id: int) -> dict:
        """Sync Apple Health data. Returns result dict."""
        try:
            from healthbot.importers.apple_health_auto import (
                AppleHealthAutoImporter,
            )

            export_path = self._core._config.apple_health_export_path
            if not export_path:
                return {"name": "Apple Health", "count": 0, "error": "not configured"}
            path = Path(export_path).expanduser()
            if not path.exists():
                return {"name": "Apple Health", "count": 0, "error": "path not found"}

            json_files = sorted(path.glob("*.json"))
            if not json_files:
                return {"name": "Apple Health", "count": 0, "error": "no files"}

            db = self._core._get_db()
            importer = AppleHealthAutoImporter()
            total = 0
            errors = 0
            processed_dir = path / "processed"
            processed_dir.mkdir(exist_ok=True)

            for json_path in json_files:
                if json_path.name.startswith("."):
                    continue
                try:
                    data = json_path.read_bytes()
                    result = importer.import_from_json(
                        data, db, user_id=user_id,
                    )
                    total += result.imported
                    json_path.rename(processed_dir / json_path.name)
                except Exception as e:
                    errors += 1
                    logger.warning(
                        "Apple Health file %s failed: %s",
                        json_path.name, e,
                    )

            if total == 0 and errors > 0:
                return {
                    "name": "Apple Health", "count": 0,
                    "error": f"{errors} file(s) failed",
                }
            return {"name": "Apple Health", "count": total, "error": None}
        except Exception as e:
            logger.error("Apple Health sync error: %s", e)
            return {"name": "Apple Health", "count": 0, "error": type(e).__name__}

    @require_unlocked
    async def connectors(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /connectors — show available data sources and status."""
        from healthbot.security.keychain import Keychain

        keychain = Keychain()
        db = self._core._get_db()
        uid = update.effective_user.id if update.effective_user else 0

        lines = ["DATA CONNECTORS", "=" * 20]

        # WHOOP
        whoop_id = keychain.retrieve("whoop_client_id")
        if whoop_id and self._is_valid_credential(whoop_id):
            last = self._get_last_sync_date(db, "whoop", uid)
            status = f"Connected (last sync: {last})" if last else "Connected"
            lines.append(f"\n  WHOOP: {status}")
            lines.append("    Sync: /sync")
        else:
            lines.append("\n  WHOOP: Not configured")
            lines.append("    Set up: /whoop_auth")

        # Oura Ring
        oura_id = keychain.retrieve("oura_client_id")
        if oura_id and self._is_valid_credential(oura_id):
            last = self._get_last_sync_date(db, "oura", uid)
            status = f"Connected (last sync: {last})" if last else "Connected"
            lines.append(f"\n  Oura Ring: {status}")
            lines.append("    Sync: /sync")
        else:
            lines.append("\n  Oura Ring: Not configured")
            lines.append("    Set up: /oura_auth")

        # Apple Health
        export_path = getattr(self._core._config, "apple_health_export_path", "")
        if export_path:
            path = Path(export_path).expanduser()
            if path.exists():
                pending = len(list(path.glob("*.json")))
                if pending:
                    lines.append(f"\n  Apple Health: Configured ({pending} pending files)")
                else:
                    lines.append("\n  Apple Health: Configured")
                lines.append(f"    Path: {export_path}")
                lines.append("    Sync: /apple_sync or /sync")
            else:
                lines.append("\n  Apple Health: Path not found")
                lines.append(f"    Expected: {export_path}")
        else:
            lines.append("\n  Apple Health: Not configured")
            lines.append("    Set apple_health_export_path in app.json")

        # MyChart/FHIR
        incoming = self._core._config.incoming_dir
        lines.append("\n  MyChart/FHIR: Available (file-based import)")
        lines.append(f"    Drop CCDA/FHIR files into: {incoming}")
        lines.append("    Then: /mychart")

        lines.append("")
        lines.append("Use /sync to sync all connected sources at once.")

        await update.message.reply_text("\n".join(lines))

    async def _wearable_auth(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        name: str,
        keychain_key: str,
        client_cls: type,
        sync_cmd: str,
        auth_cmd: str,
    ) -> None:
        """Shared OAuth 2.0 flow for wearable devices (WHOOP, Oura)."""
        from healthbot.bot.oauth_callback import wait_for_oauth_callback
        from healthbot.security.keychain import Keychain
        from healthbot.security.vault import Vault

        await update.effective_chat.send_action(ChatAction.TYPING)

        keychain = Keychain()
        secret_key = keychain_key.replace("_client_id", "_client_secret")

        # Handle /whoop_auth reset — clear stored credentials
        if context.args and context.args[0].lower() == "reset":
            keychain.delete(keychain_key)
            keychain.delete(secret_key)
            await update.message.reply_text(
                f"{name} credentials cleared from Keychain.\n"
                f"Run {auth_cmd} to set up fresh credentials."
            )
            return

        stored_id = keychain.retrieve(keychain_key)

        # Validate stored credential format — auto-clear if corrupted
        if stored_id and not self._is_valid_credential(stored_id):
            logger.warning(
                "%s client_id looks invalid (contains spaces or "
                "natural language). Clearing for re-entry.",
                name,
            )
            keychain.delete(keychain_key)
            keychain.delete(secret_key)
            stored_id = None
            await update.message.reply_text(
                f"{name} credentials look corrupted. "
                "Let's set them up again."
            )

        if not stored_id:
            user_id = update.effective_user.id if update.effective_user else 0
            prior = self._setup_state.get(user_id)
            if prior and prior["name"] != name:
                await update.message.reply_text(
                    f"Aborting {prior['name']} setup. Starting {name} setup..."
                )
            dev_url = _DEVELOPER_URLS.get(name, "the developer portal")
            self._setup_state[user_id] = {
                "step": "client_id",
                "name": name,
                "keychain_id_key": keychain_key,
                "keychain_secret_key": secret_key,
                "client_cls": client_cls,
                "sync_cmd": sync_cmd,
                "auth_cmd": auth_cmd,
            }
            scopes_note = ""
            if name == "WHOOP":
                scopes_note = (
                    "  \u2022 Scopes: check all of them (recovery,"
                    " cycles, sleep, workout, profile, body"
                    " measurement)\n"
                    "  \u2022 Webhooks: leave empty (skip this)\n"
                )
            await update.message.reply_text(
                f"Let's connect your {name}.\n\n"
                f"Go to {dev_url} and sign in, then click"
                f" Create New Application. Fill it out:\n\n"
                f"  \u2022 Name: anything (e.g. \"HealthBot\")\n"
                f"  \u2022 Logo: optional, skip it\n"
                f"  \u2022 Contacts: your email\n"
                f"  \u2022 Privacy Policy:"
                f" https://example.com/privacy\n"
                f"  \u2022 Redirect URL:"
                f" http://localhost:8765/callback\n"
                f"{scopes_note}\n"
                f"After saving, you'll see your Client ID and"
                f" Client Secret on the app page.\n\n"
                f"Send me your {name} Client ID:"
            )
            return

        vault = Vault(self._core._config.blobs_dir, self._core._km)
        client = client_cls(self._core._config, keychain, vault)

        redirect_uri = "http://localhost:8765/callback"
        try:
            auth_url, expected_state = client.get_authorization_url(redirect_uri)
        except Exception as e:
            self._core.record_error(
                type(e).__name__, str(e), provider=name.lower(),
                hint=f"{name} OAuth URL generation failed",
            )
            await update.message.reply_text(f"{name} auth error: {type(e).__name__}")
            return

        await update.message.reply_text(
            f"Open this link to authorize {name}:\n\n"
            f"{auth_url}\n\n"
            "Waiting for approval (60s)..."
        )

        try:
            result = await wait_for_oauth_callback(port=8765, timeout=60)
        except TimeoutError:
            self._core.record_error(
                "TimeoutError", "OAuth callback timed out after 60s",
                provider=name.lower(), hint="User didn't approve in browser",
            )
            # Auto-clear credentials and start re-entry flow
            keychain.delete(keychain_key)
            keychain.delete(secret_key)
            user_id = update.effective_user.id if update.effective_user else 0
            dev_url = _DEVELOPER_URLS.get(name, "the developer portal")
            self._setup_state[user_id] = {
                "step": "client_id",
                "name": name,
                "keychain_id_key": keychain_key,
                "keychain_secret_key": secret_key,
                "client_cls": client_cls,
                "sync_cmd": sync_cmd,
                "auth_cmd": auth_cmd,
            }
            await update.message.reply_text(
                f"{name} authorization timed out. This usually means "
                f"the credentials were wrong (invalid_client).\n\n"
                f"I've cleared the old credentials. Let's fix it.\n\n"
                f"Go to {dev_url}, open your app, and copy the "
                f"Client ID from the app page.\n\n"
                f"Send me your {name} Client ID:"
            )
            return
        except OSError as e:
            await update.message.reply_text(
                f"Cannot start callback server on port 8765: {e}\n"
                "Is another process using that port?"
            )
            return

        if result["error"]:
            await update.message.reply_text(
                f"{name} authorization denied: {result['error']}"
            )
            return

        code = result["code"]
        if not code:
            await update.message.reply_text(
                f"No authorization code received. Try {auth_cmd} again."
            )
            return

        if result["state"] != expected_state:
            await update.message.reply_text(
                "State mismatch (possible CSRF). Authorization aborted."
            )
            return

        try:
            await client.exchange_code(code, redirect_uri)
            # Track wearable connection state
            wearable_key = name.lower().replace(" ", "_").replace("_ring", "")
            self._core._config.set_wearable_connected(wearable_key, True)
            await update.message.reply_text(
                f"{name} connected! Syncing all available data..."
            )
            # Auto-sync full history (365 days on first connect)
            uid = update.effective_user.id if update.effective_user else 0
            try:
                db = self._core._get_db()
                clean = self._core._get_clean_db()
                try:
                    count = await client.sync_daily(
                        db, days=365, user_id=uid, clean_db=clean,
                    )
                finally:
                    if clean:
                        clean.close()
                if count > 0:
                    await update.message.reply_text(
                        f"Synced {count} records (365 days of history)."
                    )
                    provider_short = name.split()[0]  # "WHOOP" or "Oura"
                    await self._post_sync_claude_analysis(
                        update, uid, count, 365, provider_short,
                    )
                else:
                    await update.message.reply_text(
                        f"{name} connected. No historical data found."
                    )
            except Exception as sync_err:
                logger.warning(
                    "%s auto-sync after connect failed: %s", name, sync_err,
                )
                await update.message.reply_text(
                    f"Connected but auto-sync failed: "
                    f"{type(sync_err).__name__}\n"
                    f"Try {sync_cmd} manually."
                )
        except Exception as e:
            self._core.record_error(
                type(e).__name__, str(e), provider=name.lower(),
                hint=f"{name} OAuth code exchange failed",
            )
            logger.error("%s auth exchange error: %s", name, e)
            # Auto-clear and start re-entry on exchange failure too
            keychain.delete(keychain_key)
            keychain.delete(secret_key)
            user_id = update.effective_user.id if update.effective_user else 0
            dev_url = _DEVELOPER_URLS.get(name, "the developer portal")
            self._setup_state[user_id] = {
                "step": "client_id",
                "name": name,
                "keychain_id_key": keychain_key,
                "keychain_secret_key": secret_key,
                "client_cls": client_cls,
                "sync_cmd": sync_cmd,
                "auth_cmd": auth_cmd,
            }
            await update.message.reply_text(
                f"{name} authorization failed. Credentials may be "
                f"wrong.\n\n"
                f"I've cleared them. Go to {dev_url}, open your "
                f"app, and copy the Client ID.\n\n"
                f"Send me your {name} Client ID:"
            )

    @require_unlocked
    async def sync_oura(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /oura command — sync Oura Ring data."""
        await update.message.reply_text("Syncing Oura Ring data...")
        try:
            async with TypingIndicator(update.effective_chat):
                from healthbot.importers.oura_client import OuraAuthError, OuraClient
                from healthbot.security.keychain import Keychain
                from healthbot.security.vault import Vault

                db = self._core._get_db()
                keychain = Keychain()
                vault = Vault(self._core._config.blobs_dir, self._core._km)
                client = OuraClient(self._core._config, keychain, vault)

                days = 365
                if context.args:
                    try:
                        days = int(context.args[0])
                    except ValueError:
                        pass

                uid = update.effective_user.id if update.effective_user else 0
                # Write directly to Clean DB (wearable data is numeric, no PII)
                clean = self._core._get_clean_db()
                try:
                    count = await client.sync_daily(
                        db, days=days, user_id=uid, clean_db=clean,
                    )
                finally:
                    if clean:
                        clean.close()
                await update.message.reply_text(
                    f"Oura sync complete: {count} records imported ({days} days)."
                )
                # Cross-source dedup check against Apple Health
                if count > 0:
                    try:
                        from healthbot.reasoning.wearable_dedup import WearableDedup

                        dedup = WearableDedup(db)
                        dedup_report = dedup.check(
                            days=days, user_id=uid, provider="oura",
                        )
                        if dedup_report.duplicates_found:
                            await update.message.reply_text(dedup_report.summary())
                    except Exception as e:
                        logger.debug("Dedup check skipped: %s", e)

                # Post-sync Claude analysis
                if count > 0:
                    await self._post_sync_claude_analysis(
                        update, uid, count, days, "Oura",
                    )
        except OuraAuthError as e:
            self._core.record_error(
                "OuraAuthError", str(e), provider="oura",
                hint="OAuth token may be expired — try /oura_auth",
            )
            await update.message.reply_text(
                f"Oura auth error: {e}\nRun: healthbot --setup to configure Oura."
            )
        except Exception as e:
            self._core.record_error(
                type(e).__name__, str(e), provider="oura",
                hint="Oura sync failure",
            )
            logger.error("Oura sync error: %s", e)
            await update.message.reply_text(f"Oura sync failed: {type(e).__name__}")

    @require_unlocked
    async def apple_sync(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /apple_sync -- import from Health Auto Export (iCloud Drive)."""
        from healthbot.importers.apple_health_auto import (
            AppleHealthAutoImporter,
        )

        export_path = self._core._config.apple_health_export_path
        if not export_path:
            await update.message.reply_text(
                "Apple Health sync not configured.\n"
                "Set apple_health_export_path in app.json to your "
                "Health Auto Export iCloud Drive folder."
            )
            return

        path = Path(export_path).expanduser()
        if not path.exists():
            await update.message.reply_text(
                f"Export folder not found: {path}\n"
                "Make sure Health Auto Export is saving to iCloud Drive."
            )
            return

        json_files = sorted(path.glob("*.json"))
        if not json_files:
            await update.message.reply_text(
                "No JSON files found in Apple Health export folder.\n"
                "Check that Health Auto Export is configured to save as JSON."
            )
            return

        await update.message.reply_text(
            f"Found {len(json_files)} file(s). Importing..."
        )
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id if update.effective_user else 0
            importer = AppleHealthAutoImporter()
            total = 0
            processed_dir = path / "processed"
            processed_dir.mkdir(exist_ok=True)

            for json_path in json_files:
                if json_path.name.startswith("."):
                    continue
                try:
                    data = json_path.read_bytes()
                    result = importer.import_from_json(
                        data, db, user_id=uid,
                    )
                    total += result.imported
                    json_path.rename(processed_dir / json_path.name)
                except Exception as e:
                    logger.warning(
                        "Apple Health file %s failed: %s",
                        json_path.name, e,
                    )

        if total:
            await update.message.reply_text(
                f"Apple Health sync complete: {total} records imported."
            )
        else:
            await update.message.reply_text(
                "No new records to import (all duplicates or empty)."
            )

    @require_unlocked
    async def import_health(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /import command -- import Apple Health ZIP from incoming/.

        Parses XML first, then inserts in batches with progress updates.
        All DB work runs on the event loop thread (no asyncio.to_thread)
        to avoid SQLite cross-thread deadlocks.
        """
        import asyncio
        import zipfile

        incoming = self._core._config.incoming_dir
        zips = list(incoming.glob("*.zip"))
        if not zips:
            await update.message.reply_text(
                "No ZIP files found in incoming/.\n"
                "Drop your Apple Health export.zip into:\n"
                f"  {incoming}"
            )
            return

        await update.message.reply_text(f"Found {len(zips)} ZIP file(s). Importing...")
        total_imported = 0
        async with TypingIndicator(update.effective_chat):
            for zip_path in zips:
                try:
                    with zipfile.ZipFile(str(zip_path)) as zf:
                        has_export = any(
                            name.endswith("export.xml") for name in zf.namelist()
                        )
                    if not has_export:
                        await update.message.reply_text(
                            f"Skipping {zip_path.name}: not an Apple Health export."
                        )
                        continue

                    from healthbot.ingest.apple_health_import import (
                        SUPPORTED_TYPES,
                        AppleHealthImporter,
                        AppleHealthImportResult,
                    )

                    db = self._core._get_db()
                    importer = AppleHealthImporter(db)
                    privacy_mode = self._core._config.privacy_mode
                    uid = update.effective_user.id if update.effective_user else 0

                    # Phase 1: parse (no DB writes)
                    vitals, workouts, xml_bytes = importer.parse_zip_bytes(
                        zip_path.read_bytes(), privacy_mode,
                    )
                    total = len(vitals) + len(workouts)
                    if total == 0:
                        await update.message.reply_text(
                            f"{zip_path.name}: no supported records found."
                        )
                        continue

                    await update.message.reply_text(
                        f"{zip_path.name}: {len(vitals)} vitals, "
                        f"{len(workouts)} workouts. Importing...",
                    )

                    # Phase 2: insert vitals in batches with progress
                    result = AppleHealthImportResult()
                    canonical_names = list(SUPPORTED_TYPES.values())
                    existing_keys = db.get_existing_observation_keys(
                        record_type="vital_sign",
                        canonical_names=canonical_names,
                    )

                    batch_size = 5000
                    last_pct = -1
                    for i in range(0, len(vitals), batch_size):
                        batch = vitals[i : i + batch_size]
                        importer.insert_vitals_batch(
                            batch, existing_keys, uid, result,
                        )
                        pct = int((i + len(batch)) / total * 100)
                        if pct >= last_pct + 10:
                            await update.message.reply_text(f"Importing... {pct}%")
                            last_pct = pct
                        await asyncio.sleep(0)

                    # Phase 3: insert workouts
                    existing_wo_keys = db.get_existing_workout_keys(user_id=uid)
                    importer.insert_workouts_batch(
                        workouts, existing_wo_keys, uid, result,
                    )

                    # Phase 4: clinical records
                    if xml_bytes:
                        importer._parse_clinical_records(xml_bytes, uid, result)

                    # Move to processed/
                    processed = incoming / "processed"
                    processed.mkdir(exist_ok=True)
                    zip_path.rename(processed / zip_path.name)

                    total_imported += result.records_imported
                    lines = [
                        f"{zip_path.name}: {result.records_imported} vitals, "
                        f"{result.workouts_imported} workouts",
                    ]
                    if result.types_found:
                        type_summary = ", ".join(
                            f"{t}: {c}" for t, c in result.types_found.items()
                        )
                        lines.append(f"  ({type_summary})")
                    if result.clinical_records:
                        clin_parts = ", ".join(
                            f"{c} {t}" for t, c
                            in result.clinical_breakdown.items()
                        )
                        lines.append(
                            f"  {result.clinical_records} clinical records"
                            f" ({clin_parts})"
                        )
                    elif privacy_mode == "strict":
                        lines.append(
                            "  (Clinical records skipped — "
                            "/privacy relaxed to enable)"
                        )
                    await update.message.reply_text("\n".join(lines))
                except Exception as e:
                    logger.error("Import error for %s: %s", zip_path.name, e)
                    await update.message.reply_text(
                        f"Error importing {zip_path.name}: {type(e).__name__}"
                    )

        if total_imported:
            self._rebuild_search_index()
            await update.message.reply_text(
                f"Import complete: {total_imported} total records imported."
            )

    @require_unlocked
    async def import_mychart(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /mychart command -- import MyChart CCDA/FHIR files from incoming/."""
        incoming = self._core._config.incoming_dir
        ccda_files = list(incoming.glob("*.xml")) + list(incoming.glob("*.json"))
        if not ccda_files:
            await update.message.reply_text(
                "No MyChart files found in incoming/.\n"
                "Drop your CCDA (.xml) or FHIR (.json) export into:\n"
                f"  {incoming}"
            )
            return

        await update.message.reply_text(f"Found {len(ccda_files)} file(s). Importing...")
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.ingest.mychart_import import MyChartImporter
            from healthbot.security.vault import Vault

            vault = Vault(self._core._config.blobs_dir, self._core._km)
            importer = MyChartImporter(db, vault, phi_firewall=self._core._fw)

            total_labs = 0
            total_meds = 0
            for fpath in ccda_files:
                try:
                    raw = fpath.read_bytes()
                    if fpath.suffix == ".json":
                        result = importer.import_fhir_bundle(raw)
                    else:
                        result = importer.import_ccda_bytes(raw)

                    total_labs += result.get("labs", 0)
                    total_meds += result.get("meds", 0)

                    # Move to processed/
                    processed = incoming / "processed"
                    processed.mkdir(exist_ok=True)
                    fpath.rename(processed / fpath.name)

                    await update.message.reply_text(
                        f"{fpath.name}: {result.get('labs', 0)} labs, "
                        f"{result.get('meds', 0)} medications"
                    )
                except Exception as e:
                    logger.error("MyChart import error for %s: %s", fpath.name, e)
                    await update.message.reply_text(
                        f"Error importing {fpath.name}: {type(e).__name__}"
                    )

        if total_labs or total_meds:
            self._rebuild_search_index()
            await update.message.reply_text(
                f"MyChart import complete: {total_labs} labs, {total_meds} medications."
            )

    @require_unlocked
    async def export_fhir(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /export command -- export health data as FHIR R4 JSON or CSV."""
        args = context.args
        fmt = args[0].lower() if args else "fhir"

        if fmt == "csv":
            await self._export_csv(update)
            return

        if fmt != "fhir":
            await update.message.reply_text(
                f"Unknown format: {fmt}. Supported: fhir, csv"
            )
            return
        await update.message.reply_text("Generating FHIR R4 export...")
        try:
            async with TypingIndicator(update.effective_chat):
                import io

                db = self._core._get_db()
                from healthbot.export.fhir_export import FhirExporter

                uid = update.effective_user.id
                exporter = FhirExporter(db)
                all_flag = "--all" in args or len(args) <= 1
                json_str = exporter.export_json(
                    include_labs=all_flag or "--labs" in args,
                    include_meds=all_flag or "--meds" in args,
                    include_vitals=all_flag or "--vitals" in args,
                    include_symptoms=all_flag or "--symptoms" in args,
                    include_wearables=all_flag or "--wearables" in args,
                    include_concerns=all_flag or "--concerns" in args,
                    user_id=uid,
                )
                doc = io.BytesIO(json_str.encode("utf-8"))
                doc.name = "health_export_fhir_r4.json"
                await update.message.reply_document(document=doc)
            await update.message.reply_text(
                "FHIR R4 Bundle exported. Import into EHR systems or share with your provider."
            )
        except Exception as e:
            logger.error("FHIR export error: %s", e)
            await update.message.reply_text(f"Export failed: {type(e).__name__}")

    async def _export_csv(self, update: Update) -> None:
        """Export lab results and medications as CSV files."""
        import io

        from healthbot.export.csv_exporter import export_labs_csv, export_medications_csv

        db = self._core._get_db()
        uid = update.effective_user.id
        try:
            labs_csv = export_labs_csv(db, uid)
            meds_csv = export_medications_csv(db, uid)

            if labs_csv.count("\n") > 1:
                doc = io.BytesIO(labs_csv.encode("utf-8"))
                doc.name = "lab_results.csv"
                await update.message.reply_document(document=doc)

            if meds_csv.count("\n") > 1:
                doc = io.BytesIO(meds_csv.encode("utf-8"))
                doc.name = "medications.csv"
                await update.message.reply_document(document=doc)

            if labs_csv.count("\n") <= 1 and meds_csv.count("\n") <= 1:
                await update.message.reply_text("No data to export.")
            else:
                await update.message.reply_text(
                    "CSV export complete. Open in Excel or Google Sheets."
                )
        except Exception as e:
            logger.error("CSV export error: %s", e)
            await update.message.reply_text(f"CSV export failed: {type(e).__name__}")

    @require_unlocked
    async def ai_export(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /ai_export — export anonymized health data for AI analysis."""
        await update.message.reply_text("Generating anonymized health data export...")
        try:
            async with TypingIndicator(update.effective_chat):
                import io

                from healthbot.export.ai_export import AiExporter
                from healthbot.llm.anonymizer import Anonymizer
                from healthbot.llm.ollama_client import OllamaClient
                from healthbot.security.phi_firewall import PhiFirewall

                db = self._core._get_db()
                fw = PhiFirewall()
                anon = Anonymizer(phi_firewall=fw, use_ner=True)
                ollama = OllamaClient(
                    model=self._core._config.ollama_model,
                    base_url=self._core._config.ollama_url,
                    timeout=self._core._config.ollama_timeout,
                )

                uid = update.effective_user.id
                exporter = AiExporter(
                    db=db, anonymizer=anon, phi_firewall=fw,
                    ollama=ollama, key_manager=self._core._km,
                )
                result = exporter.export_to_file(uid, self._core._config.exports_dir)

                doc = io.BytesIO(result.markdown.encode("utf-8"))
                doc.name = result.file_path.name
                await update.message.reply_document(document=doc)
                await update.message.reply_text(result.validation.summary())
        except Exception as e:
            logger.error("AI export error: %s", e)
            await update.message.reply_text(f"Export failed: {type(e).__name__}")

    # ── Document retrieval ──────────────────────────────────────────

    @require_unlocked
    async def docs(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /docs — list uploaded documents or send one back."""
        db = self._core._get_db()
        uid = update.effective_user.id
        docs = db.list_documents(user_id=uid)

        if not docs:
            await update.message.reply_text("No documents uploaded yet.")
            return

        args = context.args or []

        # /docs <number> [redacted] — send the file
        if args:
            try:
                idx = int(args[0]) - 1
            except ValueError:
                await update.message.reply_text(
                    "Usage: /docs [number] [redacted]"
                )
                return
            if idx < 0 or idx >= len(docs):
                await update.message.reply_text(
                    f"Invalid number. You have {len(docs)} document(s)."
                )
                return

            doc = docs[idx]
            want_redacted = len(args) > 1 and args[1].lower() == "redacted"

            from healthbot.security.vault import Vault
            vault = Vault(self._core._config.blobs_dir, self._core._km)

            if want_redacted:
                meta = db.get_document_meta(doc["doc_id"])
                redacted_blob_id = meta.get("redacted_blob_id")
                if not redacted_blob_id:
                    await update.message.reply_text(
                        "No redacted version available for this document."
                    )
                    return
                try:
                    import io
                    pdf_bytes = vault.retrieve_blob(redacted_blob_id)
                    fname = meta.get("redacted_filename", "redacted.pdf")
                    buf = io.BytesIO(pdf_bytes)
                    buf.name = fname
                    await update.message.reply_document(document=buf)
                except Exception as e:
                    logger.error("Redacted document retrieval error: %s", e)
                    await update.message.reply_text(
                        "Error retrieving redacted document."
                    )
                return

            # Send original
            blob_id = doc["enc_blob_path"]
            if not blob_id:
                await update.message.reply_text("Document has no stored file.")
                return
            try:
                import io
                pdf_bytes = vault.retrieve_blob(blob_id)
                fname = doc["filename"] or f"document_{doc['received_at'][:10]}.pdf"
                buf = io.BytesIO(pdf_bytes)
                buf.name = fname
                await update.message.reply_document(document=buf)
            except Exception as e:
                logger.error("Document retrieval error: %s", e)
                await update.message.reply_text("Error retrieving document.")
            return

        # /docs — list all
        lines = ["Uploaded Documents:", ""]
        for i, doc in enumerate(docs, 1):
            fname = doc["filename"] or "untitled"
            size_kb = (doc.get("size_bytes") or 0) / 1024
            date = (doc["received_at"] or "")[:10]
            src = doc["source"].replace("_", " ")
            meta = db.get_document_meta(doc["doc_id"])
            tag = " [R]" if meta.get("redacted_blob_id") else ""
            lines.append(
                f"{i}. {fname} ({size_kb:.0f} KB) — {date} [{src}]{tag}"
            )
        lines.append("")
        lines.append("/docs <n> — download original")
        lines.append("/docs <n> redacted — download redacted version")
        lines.append("[R] = redacted version available")
        await update.message.reply_text("\n".join(lines))

    @require_unlocked
    async def rescan(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /rescan <n> — re-ingest document with current redaction pipeline.

        Safety: retrieves PDF bytes before deletion. If re-ingest fails,
        the original encrypted blob and document row are restored so no
        data is permanently lost.
        """
        import asyncio

        args = context.args or []
        if not args:
            await update.message.reply_text(
                "Usage: /rescan <n>\n"
                "Re-ingests document #n with the current (fixed) redaction pipeline.\n"
                "Use /docs to see document numbers."
            )
            return

        try:
            idx = int(args[0]) - 1
        except ValueError:
            await update.message.reply_text("Invalid number. Use /docs to see document list.")
            return

        db = self._core._get_db()
        user_id = update.effective_user.id
        docs = db.list_documents(user_id=user_id)

        if idx < 0 or idx >= len(docs):
            await update.message.reply_text(
                f"Invalid number. You have {len(docs)} document(s)."
            )
            return

        doc = docs[idx]
        doc_id = doc["doc_id"]
        filename = doc.get("filename") or "document.pdf"
        blob_path = doc.get("enc_blob_path", "")

        if not blob_path:
            await update.message.reply_text("Document has no stored file — cannot rescan.")
            return

        await update.message.reply_text(f"Rescanning '{filename}' with updated redaction...")

        # 1. Retrieve original PDF from vault (before any deletion)
        from healthbot.security.vault import Vault
        vault = Vault(self._core._config.blobs_dir, self._core._km)
        try:
            pdf_bytes = vault.retrieve_blob(blob_path)
        except Exception as e:
            await update.message.reply_text(f"Failed to retrieve PDF: {e}")
            return

        # 2. Snapshot document row for rollback on failure
        doc_row = db.conn.execute(
            "SELECT * FROM documents WHERE doc_id = ?", (doc_id,),
        ).fetchone()
        doc_snapshot = dict(doc_row) if doc_row else None

        # 3. Delete existing data for this document
        from healthbot.data.bulk_ops import BulkOps

        clean_db = None
        try:
            from healthbot.data.clean_db import CleanDB
            if self._core._config.clean_db_path.exists():
                clean_db = CleanDB(self._core._config.clean_db_path)
                clean_db.open(clean_key=self._core._km.get_clean_key())
        except Exception:
            pass

        try:
            ops = BulkOps(db, vault, clean_db=clean_db, config=self._core._config)
            ops.delete_document_cascade(doc_id)
        finally:
            if clean_db:
                try:
                    clean_db.close()
                except Exception:
                    pass

        # 4. Re-ingest with current pipeline
        from healthbot.ingest.lab_pdf_parser import LabPdfParser
        from healthbot.ingest.telegram_pdf_ingest import TelegramPdfIngest
        from healthbot.reasoning.triage import TriageEngine
        from healthbot.security.pdf_safety import PdfSafety

        safety = PdfSafety(self._core._config)
        parser = LabPdfParser(safety, config=self._core._config)
        triage = TriageEngine()

        ingest = TelegramPdfIngest(
            vault, db, parser, safety, triage,
            config=self._core._config,
            phi_firewall=self._core._fw,
        )

        try:
            result = await asyncio.to_thread(
                ingest.ingest,
                bytes(pdf_bytes),
                filename=filename,
                user_id=user_id,
            )
        except Exception as e:
            # Re-ingest failed — restore original blob and document row
            self._restore_document(vault, db, blob_path, pdf_bytes, doc_snapshot)
            await update.message.reply_text(
                f"Rescan failed: {e}\nOriginal document preserved."
            )
            return

        if result.success:
            n_labs = len(result.lab_results) if result.lab_results else 0
            await update.message.reply_text(
                f"Rescanned '{filename}': {n_labs} lab result(s) extracted "
                f"with updated redaction."
            )
        else:
            warnings = "; ".join(result.warnings) if result.warnings else "unknown error"
            await update.message.reply_text(f"Rescan completed with issues: {warnings}")

    @staticmethod
    def _restore_document(
        vault, db, blob_path: str, pdf_bytes: bytes,
        doc_snapshot: dict | None,
    ) -> None:
        """Restore an encrypted blob and document row after a failed rescan."""
        try:
            vault.store_blob(pdf_bytes, blob_id=blob_path)
        except Exception as e:
            logger.error("Failed to restore blob %s: %s", blob_path, e)

        if doc_snapshot:
            cols = [c for c in doc_snapshot if c != "doc_id"]
            placeholders = ", ".join("?" for _ in cols)
            col_names = ", ".join(["doc_id"] + cols)
            values = [doc_snapshot["doc_id"]] + [doc_snapshot[c] for c in cols]
            try:
                db.conn.execute(
                    f"INSERT OR IGNORE INTO documents ({col_names}) "  # noqa: S608
                    f"VALUES (?, {placeholders})",
                    values,
                )
                db.conn.commit()
            except Exception as e:
                logger.error("Failed to restore document row: %s", e)

    @require_unlocked
    async def import_fasten(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /fasten command -- import Fasten Health FHIR data from incoming/."""
        incoming = self._core._config.incoming_dir
        fhir_files = (
            list(incoming.glob("*.ndjson"))
            + list(incoming.glob("*.fhir.json"))
            + [
                f for f in incoming.glob("*.json")
                if not f.name.endswith(".fhir.json")
                and "fhir" in f.name.lower()
            ]
        )
        if not fhir_files:
            await update.message.reply_text(
                "No Fasten FHIR files found in incoming/.\n"
                "Export your data from Fasten Health and drop "
                "the .ndjson or .json file into:\n"
                f"  {incoming}\n\n"
                "All PII will be stripped before import."
            )
            return

        await update.message.reply_text(
            f"Found {len(fhir_files)} FHIR file(s). "
            "De-identifying and importing..."
        )
        async with TypingIndicator(update.effective_chat):
            from healthbot.ingest.fasten_import import FastenImporter
            from healthbot.security.vault import Vault

            db = self._core._get_db()
            vault = Vault(self._core._config.blobs_dir, self._core._km)
            uid = update.effective_user.id if update.effective_user else 0
            importer = FastenImporter(db, vault, self._core._fw)

            for fpath in fhir_files:
                try:
                    raw = fpath.read_bytes()
                    if fpath.suffix == ".ndjson":
                        result = importer.import_ndjson(raw, user_id=uid)
                    else:
                        result = importer.import_bundle(raw, user_id=uid)

                    # Move to processed/
                    processed = incoming / "processed"
                    processed.mkdir(exist_ok=True)
                    fpath.rename(processed / fpath.name)

                    parts = []
                    if result.labs:
                        parts.append(f"{result.labs} labs")
                    if result.medications:
                        parts.append(f"{result.medications} medications")
                    if result.vitals:
                        parts.append(f"{result.vitals} vitals")
                    if result.conditions:
                        parts.append(f"{result.conditions} conditions")
                    if result.allergies:
                        parts.append(f"{result.allergies} allergies")
                    if result.immunizations:
                        parts.append(f"{result.immunizations} immunizations")

                    summary = ", ".join(parts) if parts else "no records"
                    demo = ""
                    if result.demographics:
                        d = result.demographics
                        demo_parts = []
                        if d.get("age"):
                            demo_parts.append(f"age {d['age']}")
                        if d.get("sex"):
                            demo_parts.append(d["sex"])
                        demo = f"\nDemographics: {', '.join(demo_parts)}" if demo_parts else ""

                    await update.message.reply_text(
                        f"{fpath.name}: {summary}{demo}\n"
                        f"All PII stripped. {result.skipped} resources filtered."
                    )
                    if result.errors:
                        await update.message.reply_text(
                            f"Warnings: {len(result.errors)} errors\n"
                            + "\n".join(result.errors[:5])
                        )
                except Exception as e:
                    logger.error("Fasten import error for %s: %s", fpath.name, e)
                    await update.message.reply_text(
                        f"Error importing {fpath.name}: {type(e).__name__}"
                    )

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

    @require_unlocked
    async def scrub_pii(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /scrub_pii command -- remove PII from existing vault data."""
        await update.message.reply_text(
            "Scrubbing PII from existing records...\n"
            "This strips: provider names, lab names, "
            "patient name, exact DOB."
        )
        async with TypingIndicator(update.effective_chat):
            from healthbot.vault_ops.scrub_pii import VaultPiiScrubber

            db = self._core._get_db()
            uid = update.effective_user.id if update.effective_user else 0
            scrubber = VaultPiiScrubber(db, self._core._fw)
            result = scrubber.scrub_all(user_id=uid)

            parts = []
            if result.observations_scrubbed:
                parts.append(f"{result.observations_scrubbed} lab records cleaned")
            if result.medications_scrubbed:
                parts.append(f"{result.medications_scrubbed} medications cleaned")
            if result.ltm_entries_removed:
                parts.append(f"{result.ltm_entries_removed} PII entries removed")
            if result.ltm_entries_redacted:
                parts.append(f"{result.ltm_entries_redacted} entries redacted")

            if parts:
                await update.message.reply_text(
                    "PII scrub complete:\n" + "\n".join(f"  - {p}" for p in parts)
                )
            else:
                await update.message.reply_text(
                    "No PII found to scrub. Records are already clean."
                )

            if result.errors:
                await update.message.reply_text(
                    f"{len(result.errors)} errors during scrub:\n"
                    + "\n".join(result.errors[:5])
                )

    @require_unlocked
    async def debug(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /debug command — troubleshoot technical issues via Claude CLI."""
        topic = " ".join(context.args) if context.args else ""
        if not topic:
            # Show recent errors if no question provided
            errors = self._core.get_recent_errors()
            if not errors:
                await update.message.reply_text(
                    "No recent errors recorded.\n"
                    "Describe your issue: /debug why is whoop sync failing"
                )
                return
            lines = ["Recent errors:"]
            for rec in errors:
                line = f"- [{rec.timestamp}] {rec.error_type}: {rec.message}"
                if rec.provider:
                    line += f" ({rec.provider})"
                lines.append(line)
            lines.append("\nAsk about any of these: /debug <your question>")
            await update.message.reply_text("\n".join(lines))
            return

        # Route to troubleshoot handler (same as natural language path)
        await self._core._router._handle_troubleshoot(update, topic)

    @require_unlocked
    async def wearable_status(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /wearable_status — show connected devices and last sync."""
        db = self._core._get_db()
        uid = update.effective_user.id if update.effective_user else 0

        from healthbot.security.keychain import Keychain
        keychain = Keychain()

        lines = ["WEARABLE STATUS", "=" * 20]

        for provider, cred_key, label, auth_cmd in [
            ("whoop", "whoop_client_id", "WHOOP", "/whoop_auth"),
            ("oura", "oura_client_id", "Oura Ring", "/oura_auth"),
        ]:
            stored_id = keychain.retrieve(cred_key)
            connected = bool(stored_id)
            # Validate credential format
            if stored_id and not self._is_valid_credential(stored_id):
                line = (
                    f"\n{label}: Credentials corrupted\n"
                    f"  Run {auth_cmd} reset to fix"
                )
                lines.append(line)
                continue
            records = db.query_wearable_daily(
                provider=provider, limit=1, user_id=uid,
            )
            if connected or records:
                status = "Connected" if connected else "Data only"
                line = f"\n{label}: {status}"
                if records:
                    w = records[0]
                    date = w.get("_date", w.get("date", ""))
                    line += f"\n  Last sync: {date}"
                    bits = []
                    if w.get("hrv"):
                        bits.append(f"HRV {w['hrv']}ms")
                    if w.get("rhr"):
                        bits.append(f"RHR {w['rhr']}bpm")
                    if w.get("recovery_score"):
                        bits.append(f"Recovery {w['recovery_score']}")
                    if w.get("sleep_score"):
                        bits.append(f"Sleep {w['sleep_score']}")
                    if bits:
                        line += f"\n  Latest: {', '.join(bits)}"
                else:
                    line += "\n  No data synced yet"
                lines.append(line)
            else:
                lines.append(f"\n{label}: Not connected")

        # Apple Health
        apple_path = getattr(self._core._config, "apple_health_export_path", None)
        if apple_path:
            lines.append(f"\nApple Health: Configured\n  Path: {apple_path}")
        else:
            lines.append("\nApple Health: Not configured")

        await update.message.reply_text("\n".join(lines))

    async def genetics(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /genetics — view genetic data and risk findings.

        /genetics         — summary (variant count + risk findings)
        /genetics risks   — detailed risk scan
        /genetics research <rsid> — research a variant via Claude CLI
        """
        if not self._check_auth(update) or not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        args = context.args or []
        user_id = update.effective_user.id if update.effective_user else 0
        db = self._core._get_db()

        if not args or args[0].lower() == "summary":
            await self._genetics_summary(update, db, user_id)
        elif args[0].lower() == "risks":
            await self._genetics_risks(update, db, user_id)
        elif args[0].lower() == "research" and len(args) > 1:
            await self._genetics_research(update, db, user_id, args[1])
        else:
            await update.message.reply_text(
                "Usage:\n"
                "/genetics — summary of stored variants + risks\n"
                "/genetics risks — detailed risk scan\n"
                "/genetics research <rsid> — research a specific variant"
            )

    async def _genetics_summary(
        self, update: Update, db: object, user_id: int,
    ) -> None:
        """Show genetic data summary."""
        import asyncio

        from healthbot.bot.formatters import paginate
        from healthbot.bot.typing_helper import TypingIndicator

        count = db.get_genetic_variant_count(user_id)
        if count == 0:
            await update.message.reply_text(
                "No genetic data on file.\n\n"
                "Upload your TellMeGen or 23andMe raw data file "
                "(TXT or CSV) to get started."
            )
            return

        async with TypingIndicator(update.effective_chat):
            from healthbot.reasoning.genetic_risk import GeneticRiskEngine

            engine = GeneticRiskEngine(db)
            findings = await asyncio.to_thread(engine.scan_variants, user_id)

        msg = f"Genetic variants on file: {count:,}\n\n"
        if findings:
            msg += engine.format_summary(findings)
        else:
            msg += "No clinically significant risk variants detected."

        for page in paginate(msg):
            await update.message.reply_text(page)

        # Genetic risk chart
        if findings:
            from healthbot.export.chart_generator import genetic_risk_chart
            chart_bytes = genetic_risk_chart(findings)
            if chart_bytes:
                import io as iomod
                await update.message.reply_photo(photo=iomod.BytesIO(chart_bytes))

    async def _genetics_risks(
        self, update: Update, db: object, user_id: int,
    ) -> None:
        """Run detailed genetic risk scan with lab cross-references."""
        import asyncio

        from healthbot.bot.formatters import paginate
        from healthbot.bot.typing_helper import TypingIndicator
        from healthbot.reasoning.genetic_risk import GeneticRiskEngine

        count = db.get_genetic_variant_count(user_id)
        if count == 0:
            await update.message.reply_text("No genetic data on file.")
            return

        async with TypingIndicator(update.effective_chat):
            engine = GeneticRiskEngine(db)
            findings = await asyncio.to_thread(engine.scan_variants, user_id)
            correlations = await asyncio.to_thread(
                engine.cross_reference_labs, findings, user_id,
            )

        if not findings:
            await update.message.reply_text(
                f"{count:,} variants scanned — no clinically significant risks found."
            )
            return

        msg = engine.format_summary(findings)

        if correlations:
            msg += "\n\nLab Correlations:\n"
            for corr in correlations:
                f = corr["finding"]
                msg += f"\n{f.gene} + abnormal labs:\n"
                for lab in corr["matching_labs"]:
                    name = lab.get("test_name") or lab.get("canonical_name", "?")
                    val = lab.get("value", "?")
                    unit = lab.get("unit", "")
                    flag = lab.get("flag", "")
                    msg += f"  - {name}: {val} {unit} ({flag})\n"

        for page in paginate(msg):
            await update.message.reply_text(page)

        # Send genetic risk chart
        try:
            import io

            from healthbot.export.chart_generator import genetic_risk_chart
            chart_bytes = genetic_risk_chart(findings)
            if chart_bytes:
                img = io.BytesIO(chart_bytes)
                img.name = "genetic_risk.png"
                await update.message.reply_photo(photo=img)
        except Exception as e:
            logger.debug("Genetic risk chart skipped: %s", e)

    async def _genetics_research(
        self, update: Update, db: object, user_id: int, rsid: str,
    ) -> None:
        """Research a specific variant via Claude CLI."""
        import asyncio

        from healthbot.bot.formatters import paginate, strip_markdown
        from healthbot.bot.typing_helper import TypingIndicator

        # Look up the variant
        variants = db.get_genetic_variants(user_id, rsids=[rsid])
        if not variants:
            await update.message.reply_text(
                f"Variant {rsid} not found in your data."
            )
            return

        var = variants[0]
        genotype = var.get("genotype", "?")

        # Build research query (no PII — just rsid + genotype)
        query = (
            f"Health implications of {rsid} {genotype} genotype. "
            f"Clinical significance, monitoring recommendations, "
            f"drug interactions, and relevant studies."
        )

        await update.message.reply_text(f"Researching {rsid} ({genotype})...")

        try:
            async with TypingIndicator(update.effective_chat):
                from healthbot.research.claude_cli_client import (
                    ClaudeCLIResearchClient,
                )

                client = ClaudeCLIResearchClient(
                    self._core._config, self._core._fw,
                )
                result = await asyncio.to_thread(client.research, query, "")

            result = strip_markdown(result)
            for page in paginate(result):
                await update.message.reply_text(page)
        except Exception as e:
            await update.message.reply_text(
                f"Research failed: {type(e).__name__}\n"
                "Make sure Claude CLI is installed and authenticated."
            )
