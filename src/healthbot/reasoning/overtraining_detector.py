"""Overtraining syndrome detection from wearable data.

Combines 5 physiological signals into a confidence score.
All analysis is deterministic — no LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass
class OvertrainingSignal:
    signal_name: str
    present: bool
    value: str  # Human-readable evidence
    weight: float  # 0-1, contribution to confidence


@dataclass
class OvertrainingAssessment:
    confidence: float  # 0-1
    signals: list[OvertrainingSignal] = field(default_factory=list)
    positive_count: int = 0
    recommendation: str = ""
    severity: str = "none"  # "none", "watch", "likely"


RECOMMENDATIONS: dict[str, str] = {
    "none": (
        "No signs of overtraining. "
        "Current training load appears sustainable."
    ),
    "watch": (
        "Early signs of accumulated fatigue. "
        "Consider reducing intensity for 2-3 days "
        "and prioritizing sleep."
    ),
    "likely": (
        "Multiple overtraining indicators present. "
        "Recommend a deload week: reduce training volume "
        "by 40-50%, focus on sleep (8+ hours), hydration, "
        "and stress reduction."
    ),
}


class OvertrainingDetector:
    """Detect overtraining syndrome from wearable metrics."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def assess(
        self, user_id: int | None = None,
    ) -> OvertrainingAssessment:
        """Run all 5 signal checks and compute confidence."""
        cutoff = (date.today() - timedelta(days=30)).isoformat()
        rows = self._db.query_wearable_daily(
            start_date=cutoff, limit=30, user_id=user_id,
        )

        signals = [
            self._check_rhr_elevated(rows),
            self._check_hrv_declining(rows),
            self._check_high_strain(rows),
            self._check_low_recovery(rows),
            self._check_poor_sleep(rows),
        ]

        total_weight = sum(s.weight for s in signals)
        positive_weight = sum(
            s.weight for s in signals if s.present
        )
        confidence = (
            positive_weight / total_weight if total_weight > 0 else 0
        )
        positive_count = sum(1 for s in signals if s.present)

        if confidence >= 0.6:
            severity = "likely"
        elif confidence >= 0.3:
            severity = "watch"
        else:
            severity = "none"

        return OvertrainingAssessment(
            confidence=round(confidence, 2),
            signals=signals,
            positive_count=positive_count,
            recommendation=RECOMMENDATIONS[severity],
            severity=severity,
        )

    def _check_rhr_elevated(
        self, rows: list[dict],
    ) -> OvertrainingSignal:
        """RHR elevated >10% above 30-day baseline for 5+ days."""
        name = "elevated_rhr"
        weight = 0.25

        rhr_vals = [
            (r, float(r["rhr"]))
            for r in rows
            if r.get("rhr") is not None
        ]
        if len(rhr_vals) < 10:
            return OvertrainingSignal(
                name, False, "Insufficient RHR data", weight,
            )

        # Baseline excludes the 7 recent days being checked
        recent = rhr_vals[:7]
        older = rhr_vals[7:]
        if len(older) < 3:
            return OvertrainingSignal(
                name, False, "Insufficient baseline RHR data", weight,
            )

        baseline_rhr = [v for _, v in older]
        baseline = sum(baseline_rhr) / len(baseline_rhr)
        if baseline == 0:
            return OvertrainingSignal(
                name, False, "Baseline RHR is zero", weight,
            )

        elevated_days = sum(
            1 for _, v in recent if v > baseline * 1.10
        )

        present = elevated_days >= 5
        value = (
            f"RHR elevated >10% above baseline ({baseline:.0f} bpm) "
            f"for {elevated_days}/7 recent days"
        )
        return OvertrainingSignal(name, present, value, weight)

    def _check_hrv_declining(
        self, rows: list[dict],
    ) -> OvertrainingSignal:
        """HRV declining >15% over past 7 days."""
        name = "declining_hrv"
        weight = 0.25

        hrv_vals = [
            float(r["hrv"])
            for r in rows
            if r.get("hrv") is not None
        ]
        if len(hrv_vals) < 7:
            return OvertrainingSignal(
                name, False, "Insufficient HRV data", weight,
            )

        # rows are DESC, so hrv_vals[0] is newest
        # Compare newest 3 days vs oldest 3 of the 7-day window
        window = hrv_vals[:7]
        recent_avg = sum(window[:3]) / 3
        older_avg = sum(window[-3:]) / 3
        if older_avg == 0:
            return OvertrainingSignal(
                name, False, "Older HRV baseline is zero", weight,
            )

        decline_pct = (older_avg - recent_avg) / older_avg * 100
        present = decline_pct > 15

        value = (
            f"HRV change: {older_avg:.0f} -> {recent_avg:.0f}ms "
            f"({decline_pct:+.0f}% over 7 days)"
        )
        return OvertrainingSignal(name, present, value, weight)

    def _check_high_strain(
        self, rows: list[dict],
    ) -> OvertrainingSignal:
        """Average strain >16 over past 5 days."""
        name = "high_strain"
        weight = 0.20

        strain_vals = [
            float(r["strain"])
            for r in rows
            if r.get("strain") is not None
        ]
        if len(strain_vals) < 5:
            return OvertrainingSignal(
                name, False, "Insufficient strain data", weight,
            )

        # Last 5 days (DESC order)
        recent_5 = strain_vals[:5]
        avg = sum(recent_5) / len(recent_5)
        present = avg > 16

        value = f"5-day avg strain: {avg:.1f} (threshold: 16)"
        return OvertrainingSignal(name, present, value, weight)

    def _check_low_recovery(
        self, rows: list[dict],
    ) -> OvertrainingSignal:
        """Recovery <40% for 3+ of last 5 days."""
        name = "low_recovery"
        weight = 0.15

        recovery_vals = [
            float(r["recovery_score"])
            for r in rows
            if r.get("recovery_score") is not None
        ]
        if len(recovery_vals) < 5:
            return OvertrainingSignal(
                name, False, "Insufficient recovery data", weight,
            )

        recent_5 = recovery_vals[:5]
        low_days = sum(1 for v in recent_5 if v < 40)
        present = low_days >= 3

        value = (
            f"Recovery <40% for {low_days}/5 recent days"
        )
        return OvertrainingSignal(name, present, value, weight)

    def _check_poor_sleep(
        self, rows: list[dict],
    ) -> OvertrainingSignal:
        """Sleep score consistently <60 for 5+ days."""
        name = "poor_sleep"
        weight = 0.15

        sleep_vals = [
            float(r["sleep_score"])
            for r in rows
            if r.get("sleep_score") is not None
        ]
        if len(sleep_vals) < 5:
            return OvertrainingSignal(
                name, False, "Insufficient sleep data", weight,
            )

        recent_7 = sleep_vals[:7]
        poor_days = sum(1 for v in recent_7 if v < 60)
        present = poor_days >= 5

        value = (
            f"Sleep score <60 for {poor_days}/{len(recent_7)} "
            f"recent days"
        )
        return OvertrainingSignal(name, present, value, weight)

    def format_assessment(
        self, assessment: OvertrainingAssessment,
    ) -> str:
        """Format for Telegram display."""
        lines = [
            f"Overtraining Assessment: {assessment.severity.upper()} "
            f"({assessment.confidence:.0%} confidence)",
        ]
        for s in assessment.signals:
            icon = "+" if s.present else "-"
            lines.append(f"  {icon} {s.value}")
        lines.append(f"\n{assessment.recommendation}")
        return "\n".join(lines)
