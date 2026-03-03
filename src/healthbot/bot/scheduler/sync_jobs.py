"""Apple Health and wearable sync job methods."""
from __future__ import annotations

import logging

from telegram.ext import ContextTypes

logger = logging.getLogger("healthbot")


class SyncJobsMixin:
    """Mixin for wearable and Apple Health sync jobs."""

    async def _daily_wearable_sync(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Daily background sync — pulls last 2 days from connected wearables."""
        if not self._km.is_unlocked:
            return
        try:
            from healthbot.security.keychain import Keychain
            from healthbot.security.vault import Vault

            kc = Keychain()
            db = self._get_db()
            vault = Vault(self._config.blobs_dir, self._km)

            for provider, cred_key, client_cls in [
                ("whoop", "whoop_client_id", "WhoopClient"),
                ("oura", "oura_client_id", "OuraClient"),
            ]:
                if not kc.retrieve(cred_key):
                    continue
                try:
                    if client_cls == "WhoopClient":
                        from healthbot.importers.whoop_client import WhoopClient
                        client = WhoopClient(self._config, kc, vault)
                    else:
                        from healthbot.importers.oura_client import OuraClient
                        client = OuraClient(self._config, kc, vault)
                    clean = self._get_clean_db()
                    try:
                        count = await client.sync_daily(
                            db, days=2, clean_db=clean,
                            user_id=self._primary_user_id,
                        ) or 0
                    finally:
                        if clean:
                            clean.close()
                    if count:
                        logger.info(
                            "Daily %s sync: %d records", provider, count,
                        )
                except Exception as e:
                    logger.warning("Daily %s sync failed: %s", provider, e)
        except Exception as e:
            logger.debug("Daily wearable sync: %s", e)
