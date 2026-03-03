"""Unlock-triggered jobs: welcome briefing and run_on_unlock."""
from __future__ import annotations

import logging

from healthbot.bot.formatters import paginate

logger = logging.getLogger("healthbot")


class UnlockJobsMixin:
    """Mixin for vault-unlock triggered jobs."""

    async def run_on_unlock(self, bot: object) -> None:
        """Build welcome briefing after vault unlock.

        Sends briefing immediately (all local DB), then syncs WHOOP after.
        Wearable alerts and deep analysis run on the periodic schedule.
        """
        import asyncio

        self._sent_keys.clear()
        if not self._km.is_unlocked:
            return

        # Build briefing in a thread — DB queries on large vaults can block
        briefing = await asyncio.to_thread(self._build_welcome_briefing)
        if briefing:
            for page in paginate(briefing):
                await self._tracked_send(bot, page)

        # Auto-sync WHOOP after briefing (network call, may take a while)
        try:
            from healthbot.importers.whoop_client import WhoopAuthError, WhoopClient
            from healthbot.security.keychain import Keychain
            from healthbot.security.vault import Vault

            keychain = Keychain()
            if keychain.retrieve("whoop_client_id"):
                db = self._get_db()
                vault = Vault(self._config.blobs_dir, self._km)
                client = WhoopClient(self._config, keychain, vault)
                clean = self._get_clean_db()
                try:
                    count = await client.sync_daily(
                        db, days=7, clean_db=clean,
                        user_id=self._primary_user_id,
                    ) or 0
                finally:
                    if clean:
                        clean.close()
                if count:
                    await self._tracked_send(
                        bot, f"WHOOP synced ({count} records).",
                    )
        except (ImportError, WhoopAuthError):
            pass
        except Exception as e:
            logger.warning("WHOOP auto-sync failed: %s", e)

    def _build_welcome_briefing(self) -> str:
        """Build intelligence briefing on unlock.

        Combines: pending research (from locked period) + fresh analysis.
        Returns empty string if nothing notable.
        """
        parts: list[str] = []

        try:
            db = self._get_db()
            user_id = self._primary_user_id

            # 1. Overdue screenings (respect pause state)
            try:
                from healthbot.bot.overdue_pause import get_pause_until, is_overdue_paused
                from healthbot.reasoning.overdue import OverdueDetector

                if is_overdue_paused(self._config):
                    deadline = get_pause_until(self._config)
                    if deadline:
                        local = deadline.astimezone()
                        parts.append(
                            f"Overdue notifications paused until "
                            f"{local.strftime('%b %d, %Y %H:%M %Z')}."
                        )
                else:
                    detector = OverdueDetector(db)
                    overdue = detector.check_overdue()
                    if overdue:
                        urgent = [o for o in overdue if o.days_overdue > 180]
                        if urgent:
                            lines = []
                            for o in urgent[:5]:
                                months = o.days_overdue // 30
                                lines.append(f"  {o.test_name} — {months} months overdue")
                            parts.append(
                                "Overdue labs:\n" + "\n".join(lines)
                                + "\n  Use /overdue for full list."
                                + "\n  Use /snooze 2w to pause these reminders."
                            )
            except Exception as e:
                logger.debug("Welcome briefing (overdue): %s", e)

            # 2. Hypothesis check
            try:
                from healthbot.reasoning.hypothesis_generator import (
                    HypothesisGenerator,
                )

                gen = HypothesisGenerator(db)
                demographics = db.get_user_demographics(user_id)
                new_hyps = gen.scan_all(
                    user_id,
                    sex=demographics.get("sex"),
                    age=demographics.get("age"),
                )
                for h in new_hyps[:2]:
                    evidence = ", ".join(h.evidence_for[:2])
                    parts.append(
                        f"Pattern: {h.title} "
                        f"({h.confidence:.0%} confidence, based on {evidence})."
                    )
            except Exception as e:
                logger.debug("Welcome briefing (hypotheses): %s", e)

            # 3. Trend alerts (worsening >15%)
            try:
                from healthbot.reasoning.trends import TrendAnalyzer

                analyzer = TrendAnalyzer(db)
                # Check key tests for trends
                for test_name in [
                    "glucose", "hba1c", "ldl", "alt", "tsh", "creatinine",
                ]:
                    trend = analyzer.analyze_test(test_name)
                    if trend and abs(trend.pct_change) > 15:
                        parts.append(
                            f"Trend: {trend.canonical_name} "
                            f"{trend.direction} {trend.pct_change:+.0f}% "
                            f"over {trend.data_points} results."
                        )
            except Exception as e:
                logger.debug("Welcome briefing (trends): %s", e)

            # 4. Research findings gathered while locked
            if self._cached_conditions:
                try:
                    from healthbot.research.external_evidence_store import (
                        ExternalEvidenceStore,
                    )

                    store = ExternalEvidenceStore(db)
                    evidence = store.list_evidence(limit=5)
                    recent = [
                        e for e in evidence
                        if e.get("source") == "pubmed_monitor"
                        and not e.get("expired")
                    ]
                    if recent:
                        parts.append(
                            f"{len(recent)} research article"
                            f"{'s' if len(recent) != 1 else ''} "
                            f"found for your conditions. Use /evidence to browse."
                        )
                except Exception as e:
                    logger.debug("Welcome briefing (research): %s", e)

        except Exception as e:
            logger.debug("Welcome briefing failed: %s", e)

        # 5. Wearable connection hints (with connection history awareness)
        try:
            from healthbot.security.keychain import Keychain

            kc = Keychain()
            hints: list[str] = []
            for name, cred_key, _sync_cmd, auth_cmd, _desc in [
                ("WHOOP", "whoop_client_id", "/sync", "/whoop_auth",
                 "sleep, recovery, strain"),
                ("Oura Ring", "oura_client_id", "/oura", "/oura_auth",
                 "sleep, readiness, activity"),
            ]:
                has_creds = bool(kc.retrieve(cred_key))
                config_name = name.lower().replace(" ", "_").replace("_ring", "")
                was_connected = self._config.was_wearable_ever_connected(config_name)

                if was_connected and not has_creds:
                    hints.append(
                        f"{name} was connected but credentials are missing. "
                        f"Run {auth_cmd} to reconnect."
                    )

            if hints:
                parts.append(
                    "Wearables:\n" + "\n".join(f"  {h}" for h in hints)
                )
        except Exception as e:
            logger.debug("Welcome briefing (wearables): %s", e)

        if not parts:
            return ""

        return "Welcome back.\n\n" + "\n".join(parts)
