"""Targeted post-ingestion analysis.

Runs only the reasoning engines relevant to newly ingested data.
All logic is deterministic — no LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult

logger = logging.getLogger("healthbot")


@dataclass
class TargetedAnalysisResult:
    """Results from targeted post-ingestion analysis."""

    trends_found: list[str] = field(default_factory=list)
    interactions_found: list[str] = field(default_factory=list)
    hypotheses_updated: int = 0
    hypotheses_created: int = 0
    kb_entries_added: int = 0
    fulfilled_tests: list[str] = field(default_factory=list)
    reminder_updates: list[str] = field(default_factory=list)


class TargetedAnalyzer:
    """Run scoped analysis for specific lab tests after ingestion.

    Only runs engines relevant to the ingested tests, not the full
    deep analysis sweep. Designed to be called immediately after PDF
    ingestion completes.
    """

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def analyze_new_labs(
        self,
        lab_results: list[LabResult],
        user_id: int,
    ) -> TargetedAnalysisResult:
        """Analyze newly ingested labs with scoped engines.

        Only runs trends, interactions, and hypothesis logic for the
        specific tests that were just ingested.
        """
        result = TargetedAnalysisResult()
        if not lab_results:
            return result

        canonical_names = {
            lr.canonical_name
            for lr in lab_results
            if lr.canonical_name
        }
        if not canonical_names:
            return result

        demographics = self._db.get_user_demographics(user_id)

        # 1. Trends for ingested tests only
        result.trends_found = self._check_trends(canonical_names, user_id)

        # 2. Drug-lab interactions for ingested tests
        result.interactions_found = self._check_interactions(
            canonical_names, user_id,
        )

        # 3. Re-run hypothesis generator + upsert
        hyp_created, hyp_updated = self._run_hypotheses(
            user_id, demographics,
        )
        result.hypotheses_created = hyp_created
        result.hypotheses_updated = hyp_updated

        # 4. Validate existing hypotheses against new data
        result.hypotheses_updated += self._validate_hypotheses(
            canonical_names, user_id,
        )

        # 5. Check if any hypothesis missing_tests are now fulfilled
        result.fulfilled_tests = self._check_fulfilled_tests(
            canonical_names, user_id,
        )

        # 6. Review reminders against new data
        result.reminder_updates = self._review_reminders(
            canonical_names, user_id,
        )

        # 7. Store significant findings in KB
        result.kb_entries_added = self._enrich_kb(result, user_id)

        logger.info(
            "Post-ingestion analysis: trends=%d interactions=%d "
            "hyp_new=%d hyp_updated=%d fulfilled=%d reminders=%d kb=%d",
            len(result.trends_found),
            len(result.interactions_found),
            result.hypotheses_created,
            result.hypotheses_updated,
            len(result.fulfilled_tests),
            len(result.reminder_updates),
            result.kb_entries_added,
        )
        return result

    def _check_trends(
        self, canonical_names: set[str], user_id: int,
    ) -> list[str]:
        """Compute trends for specific ingested test names only."""
        findings: list[str] = []
        try:
            from healthbot.reasoning.trends import TrendAnalyzer

            analyzer = TrendAnalyzer(self._db)
            # Invalidate cache for ingested tests (data just changed)
            analyzer.invalidate_cache(canonical_names, user_id)
            for name in canonical_names:
                trend = analyzer.analyze_test(name, user_id=user_id)
                if trend and trend.data_points >= 3:
                    direction = trend.direction
                    pct = trend.pct_change
                    if direction != "stable" and abs(pct) > 10:
                        findings.append(
                            f"{name}: {direction} ({pct:+.1f}% over "
                            f"{trend.data_points} points)"
                        )
        except Exception as e:
            logger.debug("Post-ingestion trends: %s", e)
        return findings

    def _check_interactions(
        self, canonical_names: set[str], user_id: int,
    ) -> list[str]:
        """Check drug-lab interactions for specific ingested tests."""
        findings: list[str] = []
        try:
            from healthbot.reasoning.interactions import InteractionChecker

            checker = InteractionChecker(self._db)
            results = checker.check_drug_lab(user_id=user_id)
            # Filter to only interactions involving the ingested tests
            for r in results:
                lab_canonical = r.lab_name.lower().replace(" ", "_")
                if lab_canonical in canonical_names or r.lab_name in canonical_names:
                    findings.append(
                        f"{r.med_name} ↔ {r.lab_name}: "
                        f"{r.interaction.effect}"
                    )
        except Exception as e:
            logger.debug("Post-ingestion interactions: %s", e)
        return findings

    def _run_hypotheses(
        self, user_id: int, demographics: dict | None,
    ) -> tuple[int, int]:
        """Re-run hypothesis generator and upsert results.

        Returns (created, updated) counts.
        """
        created = 0
        updated = 0
        try:
            from healthbot.reasoning.hypothesis_generator import (
                HypothesisGenerator,
            )
            from healthbot.reasoning.hypothesis_tracker import (
                HypothesisTracker,
            )

            sex = demographics.get("sex") if demographics else None
            age = demographics.get("age") if demographics else None

            gen = HypothesisGenerator(self._db)
            new_hyps = gen.scan_all(user_id, sex=sex, age=age)
            if new_hyps:
                tracker = HypothesisTracker(self._db)
                for h in new_hyps:
                    result_id = tracker.upsert_hypothesis(
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
                    if result_id:
                        # upsert_hypothesis returns the hypothesis ID
                        # New insert vs update is opaque, count as created
                        created += 1
        except Exception as e:
            logger.debug("Post-ingestion hypotheses: %s", e)
        return created, updated

    def _validate_hypotheses(
        self, canonical_names: set[str], user_id: int,
    ) -> int:
        """Validate existing hypotheses against newly ingested labs.

        Returns count of hypotheses updated.
        """
        try:
            from healthbot.reasoning.hypothesis_tracker import (
                HypothesisTracker,
            )

            tracker = HypothesisTracker(self._db)
            updates = tracker.validate_against_new_data(
                user_id, canonical_names,
            )
            return len(updates)
        except Exception as e:
            logger.debug("Post-ingestion hypothesis validation: %s", e)
            return 0

    def _enrich_kb(
        self, result: TargetedAnalysisResult, user_id: int,
    ) -> int:
        """Store significant findings from this analysis in the KB."""
        count = 0
        try:
            from healthbot.reasoning.kb_enrichment import (
                KBEnrichmentEngine,
            )

            enricher = KBEnrichmentEngine(self._db)
            for trend in result.trends_found:
                if enricher.store_trend_finding(trend, user_id):
                    count += 1
            for interaction in result.interactions_found:
                if enricher.store_interaction_finding(
                    interaction, user_id,
                ):
                    count += 1
        except Exception as e:
            logger.debug("Post-ingestion KB enrichment: %s", e)
        return count

    def _review_reminders(
        self, canonical_names: set[str], user_id: int,
    ) -> list[str]:
        """Review medication reminders against newly ingested labs."""
        try:
            from healthbot.reasoning.med_reminders import (
                review_reminders_after_ingestion,
            )

            return review_reminders_after_ingestion(
                self._db, user_id, canonical_names,
            )
        except Exception as e:
            logger.debug("Post-ingestion reminder review: %s", e)
            return []

    def _check_fulfilled_tests(
        self, canonical_names: set[str], user_id: int,
    ) -> list[str]:
        """Check if any hypothesis missing_tests are now available.

        Returns list of 'hypothesis_title: test_name' strings for
        tests that were missing and are now present.
        """
        fulfilled: list[str] = []
        try:
            hypotheses = self._db.get_active_hypotheses(user_id)
            for hyp in hypotheses:
                status = hyp.get("status", "active")
                if status not in ("active", "investigating"):
                    continue
                missing = hyp.get("missing_tests", [])
                if not missing:
                    continue
                title = hyp.get("title", "Unknown")
                for test in missing:
                    test_lower = test.lower().replace(" ", "_")
                    if test_lower in canonical_names or test in canonical_names:
                        fulfilled.append(f"{title}: {test}")
        except Exception as e:
            logger.debug("Post-ingestion fulfilled tests: %s", e)
        return fulfilled
