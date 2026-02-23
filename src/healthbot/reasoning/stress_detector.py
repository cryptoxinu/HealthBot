"""Wearable-based stress detection engine.

Analyzes recent wearable data (HRV, RHR, sleep, recovery) to detect
elevated stress patterns. Uses a weighted multi-signal approach.

All logic is deterministic. No LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass(frozen=True)
class StressSignal:
    """A weighted stress signal from wearable data."""

    name: str
    metric: str          # wearable field name
    bad_direction: str   # "decreasing" or "increasing"
    weight: float        # contribution to stress score (0-1)
    threshold_pct: float  # minimum % change to trigger


STRESS_SIGNALS: tuple[StressSignal, ...] = (
    StressSignal("HRV declining", "hrv", "decreasing", 0.30, 10.0),
    StressSignal("RHR elevated", "rhr", "increasing", 0.25, 8.0),
    StressSignal("Sleep quality declining", "sleep_score", "decreasing", 0.20, 10.0),
    StressSignal("Recovery low", "recovery_score", "decreasing", 0.15, 15.0),
    StressSignal("Skin temp elevated", "skin_temp", "increasing", 0.10, 5.0),
)


@dataclass(frozen=True)
class StressRecommendation:
    """An actionable recommendation for stress management."""

    category: str
    recommendation: str
    citation: str


STRESS_RECOMMENDATIONS: dict[str, list[StressRecommendation]] = {
    "low": [],
    "moderate": [
        StressRecommendation(
            "Breathing",
            "Practice 4-7-8 breathing (inhale 4s, hold 7s, exhale 8s) "
            "for 5 minutes, 2x daily.",
            "Ma X et al. Front Psychol. 2017;8:874.",
        ),
        StressRecommendation(
            "Sleep",
            "Prioritize 7-9 hours of sleep. Set a consistent bedtime.",
            "Walker M. Why We Sleep. 2017.",
        ),
    ],
    "high": [
        StressRecommendation(
            "Recovery",
            "Reduce training intensity to active recovery only "
            "(walking, yoga, stretching) for 2-3 days.",
            "Kellmann M et al. Sports Med. 2018;48(6):1-16.",
        ),
        StressRecommendation(
            "Breathing",
            "Non-sleep deep rest (NSDR/Yoga Nidra) for 10-20 minutes daily.",
            "Huberman A. Huberman Lab Podcast. 2021.",
        ),
        StressRecommendation(
            "Nutrition",
            "Increase magnesium (400mg glycinate), omega-3 (2g EPA/DHA), "
            "and reduce caffeine after noon.",
            "Boyle NB et al. Nutrients. 2017;9(5):429.",
        ),
    ],
    "critical": [
        StressRecommendation(
            "Recovery",
            "Take a full rest day. No training. Prioritize sleep and nutrition.",
            "Meeusen R et al. Med Sci Sports Exerc. 2013;45(1):186-205.",
        ),
        StressRecommendation(
            "Medical",
            "If stress is persistent (>2 weeks), consider cortisol testing "
            "and consultation with your doctor.",
            "Hellhammer DH et al. Psychoneuroendocrinology. 2009;34(2):163-171.",
        ),
        StressRecommendation(
            "Lifestyle",
            "Evaluate major stressors. Consider professional support "
            "(therapist, counselor) if symptoms include anxiety or sleep disruption.",
            "APA Guidelines. 2023.",
        ),
    ],
}


@dataclass
class StressAssessment:
    """Result of stress detection analysis."""

    stress_score: float        # 0.0 - 1.0
    stress_level: str          # "low", "moderate", "high", "critical"
    active_signals: list[str]  # which signals triggered
    signal_details: dict[str, str] = field(default_factory=dict)
    recommendations: list[StressRecommendation] = field(default_factory=list)
    data_days: int = 0


class StressDetector:
    """Detect stress levels from wearable data patterns."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def assess(self, user_id: int, days: int = 7) -> StressAssessment:
        """Assess stress level from recent wearable data."""
        wearables = self._db.query_wearable_daily(
            limit=days, user_id=user_id,
        )

        if len(wearables) < 3:
            return StressAssessment(
                stress_score=0.0,
                stress_level="low",
                active_signals=[],
                data_days=len(wearables),
            )

        # Sort oldest-first for trend analysis
        wearables.sort(key=lambda w: w.get("_date", ""))

        stress_score = 0.0
        active_signals: list[str] = []
        signal_details: dict[str, str] = {}

        for signal in STRESS_SIGNALS:
            values = self._extract_metric(wearables, signal.metric)
            if len(values) < 3:
                continue

            pct_change = self._compute_trend_pct(values)

            triggered = False
            if signal.bad_direction == "decreasing" and pct_change < -signal.threshold_pct:
                triggered = True
            elif signal.bad_direction == "increasing" and pct_change > signal.threshold_pct:
                triggered = True

            if triggered:
                stress_score += signal.weight
                active_signals.append(signal.name)
                direction = "declined" if pct_change < 0 else "rose"
                signal_details[signal.name] = (
                    f"{signal.metric} {direction} {abs(pct_change):.0f}% "
                    f"over {len(values)} days "
                    f"({values[0]:.0f} -> {values[-1]:.0f})"
                )

        stress_score = min(stress_score, 1.0)
        level = self._score_to_level(stress_score)
        recs = STRESS_RECOMMENDATIONS.get(level, [])

        return StressAssessment(
            stress_score=stress_score,
            stress_level=level,
            active_signals=active_signals,
            signal_details=signal_details,
            recommendations=recs,
            data_days=len(wearables),
        )

    @staticmethod
    def _extract_metric(wearables: list[dict], metric: str) -> list[float]:
        """Extract numeric values for a metric from wearable data."""
        values: list[float] = []
        for w in wearables:
            val = w.get(metric)
            if val is not None:
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    continue
        return values

    @staticmethod
    def _compute_trend_pct(values: list[float]) -> float:
        """Compute percentage change from first half mean to second half mean."""
        if len(values) < 2:
            return 0.0
        mid = len(values) // 2
        first_half = values[:mid] or [values[0]]
        second_half = values[mid:] or [values[-1]]

        avg_first = sum(first_half) / len(first_half)
        avg_second = sum(second_half) / len(second_half)

        if avg_first == 0:
            return 0.0
        return ((avg_second - avg_first) / avg_first) * 100

    @staticmethod
    def _score_to_level(score: float) -> str:
        """Map stress score to level."""
        if score >= 0.7:
            return "critical"
        if score >= 0.45:
            return "high"
        if score >= 0.2:
            return "moderate"
        return "low"


def format_stress(assessment: StressAssessment) -> str:
    """Format stress assessment for display."""
    if assessment.data_days < 3:
        return (
            "Insufficient wearable data for stress assessment. "
            "Need at least 3 days of data."
        )

    lines = [
        "STRESS ASSESSMENT",
        "-" * 30,
        f"\nStress Level: {assessment.stress_level.upper()} "
        f"({assessment.stress_score:.0%})",
        f"Based on {assessment.data_days} days of wearable data.",
    ]

    if assessment.active_signals:
        lines.append("\nActive Signals:")
        for sig in assessment.active_signals:
            detail = assessment.signal_details.get(sig, "")
            lines.append(f"  ! {sig}: {detail}")

    if not assessment.active_signals:
        lines.append("\nNo stress signals detected. All metrics stable.")

    if assessment.recommendations:
        lines.append("\nRecommendations:")
        for rec in assessment.recommendations:
            lines.append(f"  [{rec.category}] {rec.recommendation}")
            lines.append(f"    Ref: {rec.citation}")

    return "\n".join(lines)
