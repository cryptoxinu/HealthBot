"""Composite health score engine.

Combines biomarker domains, wearable recovery, wearable trends, and
anomaly detection into a single 0-100 score with letter grade.
Auto-reweights when data sources are missing.

All analysis is deterministic — no LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger("healthbot")

# Default component weights (sum to 1.0)
_DEFAULT_WEIGHTS: dict[str, float] = {
    "biomarker": 0.50,
    "recovery": 0.25,
    "trend_trajectory": 0.15,
    "anomaly_penalty": 0.10,
}

# Grade thresholds
_GRADES: list[tuple[float, str]] = [
    (90, "A+"),
    (80, "A"),
    (70, "B"),
    (60, "C"),
    (50, "D"),
    (0, "F"),
]


@dataclass
class CompositeHealthScore:
    overall: float  # 0-100
    grade: str  # A+, A, B, C, D, F
    breakdown: dict[str, float]  # component name -> score
    trend_direction: str  # "improving", "declining", "stable"
    limiting_factors: list[str]
    data_coverage: dict[str, bool] = field(default_factory=dict)


def _grade(score: float) -> str:
    for threshold, letter in _GRADES:
        if score >= threshold:
            return letter
    return "F"


def _reweight(available: dict[str, float]) -> dict[str, float]:
    """Redistribute weights among available components."""
    total = sum(available.values())
    if total == 0:
        return available
    return {k: v / total for k, v in available.items()}


class CompositeHealthEngine:
    """Compute a single composite health score from all data sources."""

    def __init__(self, db: object) -> None:
        self._db = db

    def compute(self, user_id: int | None = None) -> CompositeHealthScore:
        """Compute composite score. Always returns a result (degrades gracefully)."""
        breakdown: dict[str, float] = {}
        coverage: dict[str, bool] = {}
        available_weights: dict[str, float] = {}
        limiting: list[str] = []

        # 1. Biomarker domain scores
        bio_score = self._biomarker_score(user_id)
        if bio_score is not None:
            breakdown["biomarker"] = bio_score
            available_weights["biomarker"] = _DEFAULT_WEIGHTS["biomarker"]
            coverage["biomarker"] = True
            if bio_score < 60:
                limiting.append("Biomarker scores below target")
        else:
            coverage["biomarker"] = False

        # 2. Wearable recovery
        recovery_score = self._recovery_score(user_id)
        if recovery_score is not None:
            breakdown["recovery"] = recovery_score
            available_weights["recovery"] = _DEFAULT_WEIGHTS["recovery"]
            coverage["recovery"] = True
            if recovery_score < 50:
                limiting.append("Recovery readiness is low")
        else:
            coverage["recovery"] = False

        # 3. Wearable trend trajectory
        trend_score, trend_dir = self._trend_trajectory_score(user_id)
        if trend_score is not None:
            breakdown["trend_trajectory"] = trend_score
            available_weights["trend_trajectory"] = _DEFAULT_WEIGHTS["trend_trajectory"]
            coverage["trend_trajectory"] = True
            if trend_dir == "declining":
                limiting.append("Wearable metrics trending downward")
        else:
            trend_dir = "stable"
            coverage["trend_trajectory"] = False

        # 4. Anomaly penalty
        anomaly_score = self._anomaly_score(user_id)
        if anomaly_score is not None:
            breakdown["anomaly_penalty"] = anomaly_score
            available_weights["anomaly_penalty"] = _DEFAULT_WEIGHTS["anomaly_penalty"]
            coverage["anomaly_penalty"] = True
            if anomaly_score < 60:
                limiting.append("Recent wearable anomalies detected")
        else:
            coverage["anomaly_penalty"] = False

        # Compute weighted overall
        if not available_weights:
            return CompositeHealthScore(
                overall=0.0, grade="F", breakdown={},
                trend_direction="stable", limiting_factors=["No data available"],
                data_coverage=coverage,
            )

        weights = _reweight(available_weights)
        overall = sum(breakdown[k] * weights[k] for k in weights)
        overall = max(0.0, min(100.0, round(overall, 1)))

        return CompositeHealthScore(
            overall=overall,
            grade=_grade(overall),
            breakdown=breakdown,
            trend_direction=trend_dir,
            limiting_factors=limiting,
            data_coverage=coverage,
        )

    def _biomarker_score(self, user_id: int | None) -> float | None:
        """Weighted average of domain scores from InsightEngine."""
        try:
            from healthbot.reasoning.insights import DOMAINS, InsightEngine
            from healthbot.reasoning.trends import TrendAnalyzer
            from healthbot.reasoning.triage import TriageEngine

            triage = TriageEngine(self._db)
            trends = TrendAnalyzer(self._db)
            engine = InsightEngine(self._db, triage, trends)
            scores = engine.compute_domain_scores(user_id=user_id)
            if not scores or all(s.tests_found == 0 for s in scores):
                return None
            total_weight = sum(
                DOMAINS.get(s.domain, {}).get("weight", 1.0)
                for s in scores if s.tests_found > 0
            )
            if total_weight == 0:
                return None
            weighted = sum(
                s.score * DOMAINS.get(s.domain, {}).get("weight", 1.0)
                for s in scores if s.tests_found > 0
            )
            return round(weighted / total_weight, 1)
        except Exception:
            logger.debug("Biomarker score unavailable", exc_info=True)
            return None

    def _recovery_score(self, user_id: int | None) -> float | None:
        """Recovery readiness from RecoveryReadinessEngine."""
        try:
            from healthbot.reasoning.recovery_readiness import RecoveryReadinessEngine
            engine = RecoveryReadinessEngine(self._db)
            readiness = engine.compute(user_id=user_id)
            return readiness.score if readiness else None
        except Exception:
            logger.debug("Recovery score unavailable", exc_info=True)
            return None

    def _trend_trajectory_score(
        self, user_id: int | None,
    ) -> tuple[float | None, str]:
        """Score based on wearable trend directions. Returns (score, direction)."""
        try:
            from healthbot.reasoning.wearable_trends import WearableTrendAnalyzer
            analyzer = WearableTrendAnalyzer(self._db)
            trends = analyzer.detect_all_trends(days=30, user_id=user_id)
            if not trends:
                return None, "stable"

            # Score: improving trends add, declining subtract
            # HRV/sleep_score/recovery increasing = good; RHR/strain increasing = bad
            good_up = {"hrv", "sleep_score", "recovery_score", "sleep_duration_min"}
            points = 0.0
            for t in trends:
                if t.metric_name in good_up:
                    points += 1.0 if t.direction == "increasing" else -1.0
                else:
                    points += -1.0 if t.direction == "increasing" else 1.0

            # Normalize: -len..+len → 0..100
            max_pts = max(len(trends), 1)
            score = 50.0 + (points / max_pts) * 50.0
            score = max(0.0, min(100.0, score))

            direction = "improving" if points > 0 else ("declining" if points < 0 else "stable")
            return round(score, 1), direction
        except Exception:
            logger.debug("Trend trajectory unavailable", exc_info=True)
            return None, "stable"

    def _anomaly_score(self, user_id: int | None) -> float | None:
        """Penalty based on recent anomalies. Fewer anomalies = higher score."""
        try:
            from healthbot.reasoning.wearable_trends import WearableTrendAnalyzer
            analyzer = WearableTrendAnalyzer(self._db)
            anomalies = analyzer.detect_anomalies(days=7, user_id=user_id)
            if anomalies is None:
                return None
            # 0 anomalies = 100, each urgent = -25, each watch = -15
            penalty = sum(
                25.0 if a.severity == "urgent" else 15.0
                for a in anomalies
            )
            return max(0.0, 100.0 - penalty)
        except Exception:
            logger.debug("Anomaly score unavailable", exc_info=True)
            return None
