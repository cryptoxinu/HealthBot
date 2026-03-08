"""WHOOP/Oura auth, sync, and wearable status handlers mixin."""
from __future__ import annotations

import logging
from pathlib import Path

from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import ContextTypes

from healthbot.bot.middleware import rate_limited, require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

from ._helpers import _DEVELOPER_URLS

logger = logging.getLogger("healthbot")


class WearableSyncMixin:
    """WHOOP/Oura auth, sync, and wearable status handlers."""

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

    @rate_limited(max_per_minute=5)
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

    @rate_limited(max_per_minute=5)
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

    @rate_limited(max_per_minute=5)
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

    @rate_limited(max_per_minute=5)
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

    @rate_limited(max_per_minute=5)
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

    @rate_limited(max_per_minute=5)
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
