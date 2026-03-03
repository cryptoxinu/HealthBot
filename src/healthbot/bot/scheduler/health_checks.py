"""Periodic health checks: timeout warnings, auth health, wearable gap detection."""
from __future__ import annotations

import logging

from telegram.ext import ContextTypes

from healthbot.bot.scheduler.scheduler_core import (
    WEARABLE_GAP_THRESHOLD_DAYS,
)

logger = logging.getLogger("healthbot")


class HealthChecksMixin:
    """Mixin for periodic health-check jobs."""

    async def _periodic_check(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Periodic check. Silently skips if vault locked."""
        if not self._km.is_unlocked:
            return
        try:
            from healthbot.reasoning.watcher import HealthWatcher

            db = self._get_db()
            watcher = HealthWatcher(db, user_id=self._primary_user_id)
            alerts = watcher.check_all()
            await self._send_alerts(alerts, context.bot)
        except Exception as e:
            logger.warning("Periodic alert check failed: %s", e)

    async def _check_timeout_warnings(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Check time remaining and warn at 5 min and 1 min before auto-lock.

        Also triggers the lock cascade if the session has expired — this is
        the primary proactive timeout detector (runs every 30 seconds).
        """
        if not self._km.is_unlocked:
            # is_unlocked triggers lock() cascade if timeout expired.
            # Use the scheduler's bot reference to wipe chat — more
            # reliable than the fire-and-forget task in _on_vault_lock.
            self._warned_5min = False
            self._warned_1min = False
            if self._timeout_wipe_cb and not self._timeout_wiped:
                self._timeout_wiped = True  # Prevent repeated wipe calls
                try:
                    await self._timeout_wipe_cb(context.bot)
                except Exception as e:
                    logger.warning("Timeout wipe callback failed: %s", e)
            return

        remaining = self._km.get_remaining_seconds()

        if remaining > 300:
            # Activity refreshed — reset warning flags
            self._warned_5min = False
            self._warned_1min = False
            self._timeout_wiped = False
        elif remaining <= 300 and not self._warned_5min:
            self._warned_5min = True
            await self._tracked_send(
                context.bot,
                "Session expires in ~5 minutes. Send any message to stay unlocked.",
            )
        if remaining <= 60 and not self._warned_1min:
            self._warned_1min = True
            await self._tracked_send(
                context.bot,
                "Session expires in ~1 minute. Send any message to stay unlocked.",
            )

    async def _check_wearable_gaps(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Check for stale wearable data and alert user. Skips if locked."""
        if not self._km.is_unlocked:
            return

        try:
            from datetime import date

            from healthbot.security.keychain import Keychain

            kc = Keychain()
            db = self._get_db()
            user_id = self._primary_user_id

            for provider, cred_key, auth_cmd in [
                ("whoop", "whoop_client_id", "/whoop_auth"),
                ("oura", "oura_client_id", "/oura_auth"),
            ]:
                if not kc.retrieve(cred_key):
                    continue  # Not connected — skip

                # Check last data date for this provider
                try:
                    rows = db.conn.execute(
                        "SELECT MAX(date) as last_date FROM wearable_daily "
                        "WHERE provider = ? AND user_id = ?",
                        (provider, user_id),
                    ).fetchall()
                    last_date_str = rows[0]["last_date"] if rows else None
                except Exception:
                    continue

                if not last_date_str:
                    continue

                try:
                    last_date = date.fromisoformat(last_date_str)
                except ValueError:
                    continue

                gap_days = (date.today() - last_date).days
                if gap_days >= WEARABLE_GAP_THRESHOLD_DAYS:
                    dedup = f"wearable_gap_{provider}_{last_date_str}"
                    if self._has_sent_key(dedup):
                        continue
                    self._record_sent_key(dedup)

                    name = provider.upper() if provider == "whoop" else provider.title()
                    await self._tracked_send(
                        context.bot,
                        f"{name} data is {gap_days} days stale "
                        f"(last sync: {last_date_str}). "
                        f"Try {auth_cmd} to reconnect or sync.",
                    )
        except Exception as e:
            logger.debug("Wearable gap check: %s", e)

    async def _check_auth_health(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Validate integration tokens and alert user on failures."""
        if not self._km.is_unlocked:
            return

        try:
            from healthbot.security.keychain import Keychain
            from healthbot.security.vault import Vault

            kc = Keychain()
            vault = Vault(self._config.blobs_dir, self._km)

            checks = [
                ("WHOOP", "whoop_client_id", "whoop_client_secret",
                 "whoop_refresh_token", "/whoop_auth"),
                ("Oura", "oura_client_id", "oura_client_secret",
                 "oura_refresh_token", "/oura_auth"),
            ]

            for name, cid_key, secret_key, token_blob, auth_cmd in checks:
                client_id = kc.retrieve(cid_key)
                if not client_id:
                    continue  # Not configured — skip

                dedup = f"auth_health_{name}"
                if self._has_sent_key(dedup):
                    continue

                # Validate credential format (catch corrupted values)
                if " " in client_id or len(client_id) < 8:
                    self._record_sent_key(dedup)
                    await self._tracked_send(
                        context.bot,
                        f"{name} client ID looks corrupted. "
                        f"Run {auth_cmd} reset to fix it.",
                    )
                    continue

                # Check credentials exist
                client_secret = kc.retrieve(secret_key)
                if not client_secret:
                    self._record_sent_key(dedup)
                    await self._tracked_send(
                        context.bot,
                        f"{name} credentials incomplete — client secret "
                        f"missing. Run {auth_cmd} to reconnect.",
                    )
                    continue

                # Check refresh token exists
                try:
                    token = vault.retrieve_blob(token_blob)
                    if not token:
                        raise ValueError("empty")
                except Exception:
                    self._record_sent_key(dedup)
                    await self._tracked_send(
                        context.bot,
                        f"{name} auth token expired or missing. "
                        f"Run {auth_cmd} to reconnect.",
                    )
                    continue

                # Check token validity without refreshing
                try:
                    # Token exists and credentials are present — check
                    # if the token looks valid (non-empty, decodable).
                    # Only flag if the token is clearly broken. Do NOT
                    # call the refresh endpoint — that consumes the
                    # token and should only happen during actual sync.
                    token_str = (
                        token.decode() if isinstance(token, bytes)
                        else token
                    )
                    if not token_str or len(token_str) < 10:
                        self._record_sent_key(dedup)
                        await self._tracked_send(
                            context.bot,
                            f"{name} refresh token looks corrupted "
                            f"(too short). Run {auth_cmd} to "
                            f"re-authorize.",
                        )
                    else:
                        logger.info("%s auth health check: OK", name)
                except Exception as e:
                    logger.warning(
                        "%s auth health check failed: %s", name, e,
                    )
        except Exception as e:
            logger.debug("Auth health check: %s", e)
