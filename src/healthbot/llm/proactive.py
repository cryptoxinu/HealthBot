"""Proactive insight generation after data ingestion.

Deterministic signal gathering: triage, trends, correlations, overdue checks.
LLM interpretation is handled by Claude CLI via the enriched AI export context.
"""
from __future__ import annotations

import logging

from healthbot.data.db import HealthDB
from healthbot.data.db_memory import MemoryMixin
from healthbot.data.models import LabResult
from healthbot.reasoning.overdue import OverdueDetector
from healthbot.reasoning.recovery_readiness import RecoveryReadinessEngine
from healthbot.reasoning.reference_ranges import get_range
from healthbot.reasoning.sleep_analysis import SleepArchitectureAnalyzer
from healthbot.reasoning.trends import TrendAnalyzer
from healthbot.reasoning.triage import TriageEngine
from healthbot.reasoning.wearable_trends import WearableTrendAnalyzer

logger = logging.getLogger("healthbot")


class ProactiveInsightEngine:
    """Generate proactive insights after data ingestion.

    Gathers deterministic signals only. LLM interpretation is handled
    by Claude CLI through the enriched AI export context.
    """

    def __init__(
        self,
        db: HealthDB,
        triage: TriageEngine | None = None,
    ) -> None:
        self._db = db
        self._triage = triage or TriageEngine()

    def analyze_new_labs(self, labs: list[LabResult], user_id: int = 0) -> str | None:
        """Analyze newly ingested lab results and return insights.

        Returns formatted insight text, or None if nothing notable.
        """
        if not labs:
            return None

        # Get demographics for age-aware analysis
        demographics = self._db.get_user_demographics(user_id)

        # Phase 1: Deterministic signals
        signals = self._gather_signals(labs, user_id, demographics)

        if not signals:
            return None

        # Return deterministic signals as formatted text
        # LLM interpretation happens in Claude CLI via enriched AI export
        return self._format_raw_signals(signals)

    def _gather_signals(
        self,
        labs: list[LabResult],
        user_id: int = 0,
        demographics: dict | None = None,
    ) -> list[str]:
        """Gather deterministic signals from new lab results."""
        signals: list[str] = []
        demo = demographics or {}

        # Triage
        triage_summary = self._triage.get_triage_summary(labs)
        triage_upper = triage_summary.upper()
        if "CRITICAL" in triage_upper or "URGENT" in triage_upper:
            signals.append(f"TRIAGE FINDINGS:\n{triage_summary}")

        # Age-aware range checks (when lab has no ref ranges from PDF)
        dob = demo.get("dob")
        sex = demo.get("sex")
        for lab in labs:
            if not lab.canonical_name:
                continue
            # Only check when PDF didn't provide reference ranges
            if lab.reference_low is not None or lab.reference_high is not None:
                continue
            # Compute age at time of lab collection
            age = demo.get("age")
            if dob and lab.date_collected:
                age = MemoryMixin.age_at_date(dob, lab.date_collected)
            ref = get_range(lab.canonical_name, sex=sex, age=age)
            if not ref:
                continue
            try:
                val = float(lab.value)
            except (ValueError, TypeError):
                continue
            if val < ref["low"] or val > ref["high"]:
                flag = "LOW" if val < ref["low"] else "HIGH"
                age_note = f" (age {age} at collection)" if age else ""
                signals.append(
                    f"RANGE CHECK: {lab.test_name} = {val} {lab.unit} "
                    f"is {flag} vs age/sex-adjusted range "
                    f"{ref['low']}-{ref['high']}{age_note}"
                )

        # Trends for each lab test
        analyzer = TrendAnalyzer(self._db)
        for lab in labs:
            if not lab.canonical_name:
                continue
            trend = analyzer.analyze_test(lab.canonical_name, user_id=user_id)
            if trend and trend.direction != "stable":
                signals.append(
                    f"TREND: {trend.test_name} is {trend.direction} "
                    f"({trend.pct_change:+.1f}% over {trend.data_points} results, "
                    f"{trend.first_date} to {trend.last_date})"
                )

        # Overdue checks
        detector = OverdueDetector(self._db)
        overdue = detector.check_overdue(user_id=user_id)
        if overdue:
            names = [i.test_name for i in overdue[:5]]
            signals.append(f"OVERDUE TESTS: {', '.join(names)}")

        return signals

    def analyze_wearable_sync(self, user_id: int = 0) -> str | None:
        """Analyze wearable data after WHOOP sync.

        Two-phase like analyze_new_labs: deterministic signals → LLM.
        """
        demographics = self._db.get_user_demographics(user_id)
        signals = self._gather_wearable_signals(user_id, demographics)
        if not signals:
            return None
        return self._format_raw_signals(signals)

    def _gather_wearable_signals(
        self, user_id: int = 0, demographics: dict | None = None,
    ) -> list[str]:
        """Gather deterministic signals from wearable data."""
        signals: list[str] = []

        # Trends (14-day)
        analyzer = WearableTrendAnalyzer(self._db)
        for t in analyzer.detect_all_trends(days=14, user_id=user_id):
            if abs(t.pct_change) > 10:
                signals.append(
                    f"WEARABLE TREND: {t.display_name} is "
                    f"{t.direction} ({t.pct_change:+.1f}% over "
                    f"{t.data_points} days, "
                    f"{t.first_value:.1f} -> {t.last_value:.1f})"
                )

        # Anomalies (today)
        for a in analyzer.detect_anomalies(days=1, user_id=user_id):
            signals.append(f"WEARABLE ANOMALY: {a.message}")

        # Recovery readiness
        readiness = RecoveryReadinessEngine(self._db).compute(
            user_id=user_id,
        )
        if readiness and readiness.score < 50:
            factors = (
                ", ".join(readiness.limiting_factors)
                if readiness.limiting_factors
                else "multiple factors"
            )
            signals.append(
                f"RECOVERY: Score {readiness.score:.0f}/100 "
                f"({readiness.grade}). "
                f"Limiting factors: {factors}. "
                f"{readiness.recommendation}"
            )

        # Sleep architecture (if stage data available)
        try:
            sleep = SleepArchitectureAnalyzer(self._db)
            trends = sleep.analyze_trends(days=14, user_id=user_id)
            for t in trends:
                concern = getattr(t, "concern", None)
                if concern:
                    signals.append(f"SLEEP: {concern}")
        except Exception as exc:
            logger.warning("Sleep analysis failed: %s", exc)

        return signals

    def _format_raw_signals(self, signals: list[str]) -> str:
        """Format signals as plain text without LLM interpretation."""
        lines = list(signals)
        has_critical = any("CRITICAL" in s.upper() for s in signals)
        if has_critical:
            lines.append(
                "\nThese findings are clinically significant and "
                "warrant prompt attention."
            )
        return "\n".join(lines)
