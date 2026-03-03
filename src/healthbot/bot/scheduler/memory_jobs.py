"""STM consolidation and search index rebuild jobs."""
from __future__ import annotations

import logging

from telegram.ext import ContextTypes

logger = logging.getLogger("healthbot")


class MemoryJobsMixin:
    """Mixin for memory consolidation and search index jobs."""

    async def _consolidate_stm(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Periodic STM consolidation + cleanup. Silently skips if locked."""
        if not self._km.is_unlocked or not self._memory_store:
            return
        for uid in self._config.allowed_user_ids:
            try:
                count = self._memory_store.consolidate(uid)
                if count:
                    logger.info("Periodic consolidation: %d facts for user %d", count, uid)
            except Exception as e:
                logger.warning("Periodic consolidation failed for user %d: %s", uid, e)
        # Clean up old STM entries
        try:
            cleanup_days = getattr(self._config, "stm_cleanup_days", 30)
            deleted = self._memory_store.cleanup(days=cleanup_days)
            if deleted:
                logger.info("STM cleanup: removed %d old entries", deleted)
        except Exception as e:
            logger.debug("STM cleanup skipped: %s", e)

    def _rebuild_search_index(self) -> None:
        """Rebuild the search index after data ingestion."""
        try:
            from healthbot.retrieval.search import SearchEngine
            from healthbot.security.vault import Vault

            db = self._get_db()
            vault = Vault(self._config.blobs_dir, self._km)
            engine = SearchEngine(self._config, db, vault)
            count = engine.build_index()
            logger.info("Search index rebuilt: %d documents", count)
        except Exception as e:
            logger.debug("Search index rebuild skipped: %s", e)
