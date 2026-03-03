"""Deep analysis and condition research jobs."""
from __future__ import annotations

import logging
import time

from telegram.ext import ContextTypes

logger = logging.getLogger("healthbot")


class ResearchJobsMixin:
    """Mixin for deep analysis and condition research jobs."""

    async def _deep_analysis(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Periodic deep analysis — runs reasoning modules automatically.

        Runs every 4 hours while unlocked (same interval as periodic check).
        Auto-runs: correlations, delta, panel gaps, hypothesis generation.
        """
        if not self._km.is_unlocked:
            return

        try:
            t_start = time.monotonic()
            db = self._get_db()
            user_id = self._primary_user_id
            demographics = db.get_user_demographics(user_id)

            # 1. Auto-discover correlations (lab <-> wearable)
            try:
                from healthbot.reasoning.correlate import CorrelationEngine

                engine = CorrelationEngine(db)
                stored = engine.discover_and_store(
                    user_id=user_id, days=90,
                )
                if stored:
                    logger.info(
                        "Discovered %d significant correlations",
                        len(stored),
                    )
            except Exception as e:
                logger.debug("Deep analysis (correlate): %s", e)

            # 2. Auto-hypothesis generation
            try:
                from healthbot.reasoning.hypothesis_generator import (
                    HypothesisGenerator,
                )
                from healthbot.reasoning.hypothesis_tracker import (
                    HypothesisTracker,
                )

                gen = HypothesisGenerator(db)
                new_hyps = gen.scan_all(
                    user_id,
                    sex=demographics.get("sex"),
                    age=demographics.get("age"),
                )
                if new_hyps:
                    tracker = HypothesisTracker(db)
                    for h in new_hyps:
                        tracker.upsert_hypothesis(
                            user_id,
                            {
                                "title": h.title,
                                "confidence": h.confidence,
                                "evidence_for": h.evidence_for,
                                "evidence_against": h.evidence_against,
                                "missing_tests": h.missing_tests,
                                "pattern_id": h.pattern_id,
                            },
                        )
                    logger.info(
                        "Deep analysis: %d hypotheses generated", len(new_hyps),
                    )
            except Exception as e:
                logger.debug("Deep analysis (hypotheses): %s", e)

            # 3. Panel gap check
            try:
                from healthbot.reasoning.panel_gaps import PanelGapDetector

                detector = PanelGapDetector(db)
                detector.detect()  # Results cached for /gaps command
            except Exception as e:
                logger.debug("Deep analysis (panel gaps): %s", e)

            # 4. Intelligence audit (unfollowed flags, condition gaps, screening gaps)
            try:
                from healthbot.reasoning.intelligence_auditor import (
                    IntelligenceAuditor,
                )

                auditor = IntelligenceAuditor(db)
                gaps = auditor.audit(user_id, demographics)
                if gaps:
                    logger.info(
                        "Intelligence audit: %d gaps found (%s)",
                        len(gaps),
                        ", ".join(g.gap_type for g in gaps[:3]),
                    )
            except Exception as e:
                logger.debug("Deep analysis (intelligence audit): %s", e)

            # 5. Wearable trend analysis
            try:
                from healthbot.reasoning.wearable_trends import (
                    WearableTrendAnalyzer,
                )

                wt_analyzer = WearableTrendAnalyzer(db)
                wearable_trends = wt_analyzer.detect_all_trends(
                    days=14, user_id=user_id,
                )
                anomalies = wt_analyzer.detect_anomalies(
                    days=1, user_id=user_id,
                )
                if wearable_trends or anomalies:
                    logger.info(
                        "Deep analysis: %d wearable trends, %d anomalies",
                        len(wearable_trends), len(anomalies),
                    )
            except Exception as e:
                logger.debug("Deep analysis (wearable trends): %s", e)

            # 6. Recovery readiness check
            try:
                from healthbot.reasoning.recovery_readiness import (
                    RecoveryReadinessEngine,
                )

                readiness = RecoveryReadinessEngine(db).compute(
                    user_id=user_id,
                )
                if readiness and readiness.score < 40:
                    msg = (
                        f"Recovery alert: {readiness.score:.0f}/100 "
                        f"({readiness.grade}). "
                        f"{readiness.recommendation}"
                    )
                    await self._tracked_send(context.bot, msg)
            except Exception as e:
                logger.debug("Deep analysis (recovery readiness): %s", e)

            # 7. KB enrichment — store significant findings, cleanup stale
            try:
                from healthbot.reasoning.kb_enrichment import (
                    KBEnrichmentEngine,
                )

                enricher = KBEnrichmentEngine(db)
                enricher.cleanup_stale(max_age_days=90)
            except Exception as e:
                logger.debug("Deep analysis (kb enrichment): %s", e)

            # 8. Claude synthesis — reviews new data against full patient profile
            try:
                conv = self._claude_getter() if self._claude_getter else None
                if conv:
                    import asyncio as _asyncio

                    from healthbot.llm.background_analysis import (
                        BackgroundAnalysisEngine,
                    )

                    engine = BackgroundAnalysisEngine(db, self._config)
                    prompt = engine.build_health_synthesis_prompt(user_id)
                    if prompt:
                        response, _ = await _asyncio.to_thread(
                            conv.handle_message, prompt, user_id,
                        )
                        engine.commit_health_watermarks()
                        alert = engine.extract_alert(response)
                        if alert:
                            await self._tracked_send(context.bot, alert)
                        logger.info(
                            "Background synthesis: %d chars", len(response),
                        )
                    else:
                        logger.debug(
                            "Background synthesis: no new data, skipped",
                        )
            except Exception as e:
                logger.debug("Background synthesis skipped: %s", e)

            elapsed = time.monotonic() - t_start
            logger.info("Deep analysis completed in %.1fs", elapsed)

        except Exception as e:
            logger.warning("Deep analysis failed: %s", e)

    async def _research_conditions(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Periodic research monitoring for user's conditions.

        Runs even while vault is locked (uses cached condition list).
        When unlocked, refreshes conditions from DB first.
        PubMed queries are anonymized — no PHI leaves the machine.
        """
        conditions = self._cached_conditions

        # If unlocked, refresh from DB
        if self._km.is_unlocked:
            try:
                from healthbot.reasoning.condition_extractor import extract_conditions

                db = self._get_db()
                conditions = extract_conditions(db, self._primary_user_id)
                self._cached_conditions = conditions
            except Exception as e:
                logger.debug("Research monitor: condition refresh failed: %s", e)

        if not conditions:
            return

        try:
            from healthbot.research.pubmed_client import PubMedClient
            from healthbot.security.phi_firewall import PhiFirewall

            firewall = PhiFirewall()
            client = PubMedClient(self._config, firewall)

            total_found = 0
            for condition in conditions[:5]:  # Cap at 5 to avoid rate limits
                try:
                    results = await client.search(
                        f"{condition} recent advances",
                        max_results=3,
                    )
                    if results:
                        total_found += len(results)
                        # Store in evidence cache if vault is unlocked
                        if self._km.is_unlocked:
                            self._store_research_results(
                                condition, results,
                            )
                except Exception as e:
                    logger.debug(
                        "Research monitor: PubMed search failed for '%s': %s",
                        condition, e,
                    )

            if total_found:
                logger.info(
                    "Research monitor: found %d articles for %d conditions",
                    total_found, len(conditions),
                )
                # Notify user if unlocked and notable findings
                if self._km.is_unlocked and total_found > 0:
                    msg = (
                        f"Background research: found {total_found} new "
                        f"article{'s' if total_found != 1 else ''} relevant "
                        f"to your conditions. Use /evidence to browse."
                    )
                    await self._tracked_send(context.bot, msg)

            # Claude research synthesis — cross-references articles against patient
            if self._km.is_unlocked and total_found > 0:
                try:
                    import asyncio as _asyncio

                    conv = (
                        self._claude_getter()
                        if self._claude_getter
                        else None
                    )
                    if conv:
                        from healthbot.llm.background_analysis import (
                            BackgroundAnalysisEngine,
                        )

                        engine = BackgroundAnalysisEngine(
                            self._get_db(), self._config,
                        )
                        prompt = engine.build_research_synthesis_prompt(
                            self._primary_user_id,
                        )
                        if prompt:
                            response, _ = await _asyncio.to_thread(
                                conv.handle_message, prompt,
                                self._primary_user_id,
                            )
                            engine.commit_research_watermarks()
                            alert = engine.extract_alert(response)
                            if alert:
                                await self._tracked_send(context.bot, alert)
                except Exception as e:
                    logger.debug("Research synthesis skipped: %s", e)
        except Exception as e:
            logger.debug("Research monitor failed: %s", e)

    def _store_research_results(
        self, condition: str, results: list,
    ) -> None:
        """Store PubMed results in external evidence store."""
        try:
            from healthbot.research.external_evidence_store import (
                ExternalEvidenceStore,
            )

            db = self._get_db()
            store = ExternalEvidenceStore(db)
            for r in results:
                store.store(
                    source="pubmed_monitor",
                    query=condition,
                    result={
                        "pmid": r.pmid,
                        "title": r.title,
                        "journal": r.journal,
                        "year": r.year,
                        "authors": r.authors[:3],
                        "abstract": r.abstract[:500] if r.abstract else "",
                        "condition": condition,
                    },
                    ttl_days=90,
                    condition_related=True,  # Never expires
                )
        except Exception as e:
            logger.debug("Store research results failed: %s", e)
