"""Sleep architecture analysis from wearable data.

Computes sleep stage percentages, flags concerns (low REM, low deep),
and tracks architecture trends over time.

All analysis is deterministic — no LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")

# Normal ranges for sleep stages (% of total sleep)
SLEEP_STAGE_NORMS: dict[str, dict[str, float]] = {
    "rem": {"low": 15.0, "high": 30.0, "optimal": 22.0},
    "deep": {"low": 15.0, "high": 30.0, "optimal": 20.0},
    "light": {"low": 40.0, "high": 60.0, "optimal": 50.0},
}

CONCERN_MESSAGES: dict[str, str] = {
    "rem_low": (
        "Low REM sleep may affect memory consolidation "
        "and emotional regulation"
    ),
    "deep_low": (
        "Low deep sleep may impair physical recovery "
        "and immune function"
    ),
    "light_high": (
        "Excessive light sleep may indicate "
        "sleep quality issues"
    ),
}


@dataclass
class SleepArchitectureResult:
    date: str
    total_min: int
    rem_pct: float
    deep_pct: float
    light_pct: float
    rem_status: str  # "normal", "low", "high"
    deep_status: str
    concerns: list[str] = field(default_factory=list)


@dataclass
class SleepArchitectureTrend:
    metric: str  # "rem_pct", "deep_pct"
    direction: str  # "improving", "declining", "stable"
    avg_last_7: float
    avg_last_30: float
    concern: str | None


class SleepArchitectureAnalyzer:
    """Analyze sleep stage distribution and trends."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def analyze_night(
        self, date_str: str | None = None,
        user_id: int | None = None,
    ) -> SleepArchitectureResult | None:
        """Analyze a single night's sleep architecture.

        If date_str is None, uses the most recent available data.
        """
        if date_str:
            cutoff = date_str
        else:
            cutoff = (date.today() - timedelta(days=7)).isoformat()

        rows = self._db.query_wearable_daily(
            start_date=cutoff, limit=1, user_id=user_id,
        )
        if not rows:
            return None

        row = rows[0]
        return self._analyze_row(row)

    def analyze_range(
        self, days: int = 7, user_id: int | None = None,
    ) -> list[SleepArchitectureResult]:
        """Analyze sleep architecture for multiple nights."""
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        rows = self._db.query_wearable_daily(
            start_date=cutoff, limit=days, user_id=user_id,
        )
        results = []
        for row in rows:
            result = self._analyze_row(row)
            if result:
                results.append(result)
        return results

    def analyze_trends(
        self, days: int = 30, user_id: int | None = None,
    ) -> list[SleepArchitectureTrend]:
        """Compare 7-day vs 30-day sleep stage averages."""
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        rows = self._db.query_wearable_daily(
            start_date=cutoff, limit=days, user_id=user_id,
        )

        if len(rows) < 7:
            return []

        # Compute percentages for each day
        rem_pcts: list[float] = []
        deep_pcts: list[float] = []
        for row in rows:
            result = self._analyze_row(row)
            if result and result.total_min > 0:
                rem_pcts.append(result.rem_pct)
                deep_pcts.append(result.deep_pct)

        if len(rem_pcts) < 7:
            return []

        trends: list[SleepArchitectureTrend] = []

        for label, pcts in [("rem_pct", rem_pcts), ("deep_pct", deep_pcts)]:
            # rows are DESC (newest first), so [:7] = last 7 days
            avg_7 = sum(pcts[:7]) / len(pcts[:7])
            avg_all = sum(pcts) / len(pcts)

            norms = SLEEP_STAGE_NORMS[label.replace("_pct", "")]
            concern = None

            if avg_7 < norms["low"]:
                concern = CONCERN_MESSAGES.get(
                    f"{label.replace('_pct', '')}_low",
                )

            diff = avg_7 - avg_all
            if abs(diff) < 2.0:
                direction = "stable"
            elif diff > 0:
                direction = "improving"
            else:
                direction = "declining"

            trends.append(SleepArchitectureTrend(
                metric=label,
                direction=direction,
                avg_last_7=round(avg_7, 1),
                avg_last_30=round(avg_all, 1),
                concern=concern,
            ))

        return trends

    def _analyze_row(self, row: dict) -> SleepArchitectureResult | None:
        """Analyze a single row of wearable data."""
        rem = row.get("rem_min")
        deep = row.get("deep_min")
        light = row.get("light_min")
        total = row.get("sleep_duration_min")

        if total is None or total == 0:
            return None

        total_min = int(total)

        # Compute percentages (handle missing stages)
        rem_min = int(rem) if rem is not None else 0
        deep_min = int(deep) if deep is not None else 0
        light_min = int(light) if light is not None else 0

        # If no stage data at all, can't analyze architecture
        if rem_min == 0 and deep_min == 0 and light_min == 0:
            return None

        rem_pct = rem_min / total_min * 100 if total_min else 0
        deep_pct = deep_min / total_min * 100 if total_min else 0
        light_pct = light_min / total_min * 100 if total_min else 0

        # Status
        rem_norms = SLEEP_STAGE_NORMS["rem"]
        deep_norms = SLEEP_STAGE_NORMS["deep"]

        rem_status = "normal"
        if rem_pct < rem_norms["low"]:
            rem_status = "low"
        elif rem_pct > rem_norms["high"]:
            rem_status = "high"

        deep_status = "normal"
        if deep_pct < deep_norms["low"]:
            deep_status = "low"
        elif deep_pct > deep_norms["high"]:
            deep_status = "high"

        # Concerns
        concerns: list[str] = []
        if rem_pct < rem_norms["low"]:
            concerns.append(CONCERN_MESSAGES["rem_low"])
        if deep_pct < deep_norms["low"]:
            concerns.append(CONCERN_MESSAGES["deep_low"])
        if light_pct > 65:
            concerns.append(CONCERN_MESSAGES["light_high"])

        date_str = str(row.get("_date") or row.get("date", ""))

        return SleepArchitectureResult(
            date=date_str,
            total_min=total_min,
            rem_pct=round(rem_pct, 1),
            deep_pct=round(deep_pct, 1),
            light_pct=round(light_pct, 1),
            rem_status=rem_status,
            deep_status=deep_status,
            concerns=concerns,
        )

    def format_architecture(
        self, result: SleepArchitectureResult,
    ) -> str:
        """Format single night for display."""
        hours = result.total_min // 60
        mins = result.total_min % 60
        lines = [
            f"Sleep Architecture ({result.date})",
            f"Total: {hours}h {mins}m",
            f"REM: {result.rem_pct:.0f}% ({result.rem_status})",
            f"Deep: {result.deep_pct:.0f}% ({result.deep_status})",
            f"Light: {result.light_pct:.0f}%",
        ]
        if result.concerns:
            lines.append("Concerns: " + "; ".join(result.concerns))
        return "\n".join(lines)

    def format_summary(
        self, days: int = 7, user_id: int | None = None,
    ) -> str | None:
        """Format multi-day sleep summary."""
        results = self.analyze_range(days=days, user_id=user_id)
        if not results:
            return None

        avg_rem = sum(r.rem_pct for r in results) / len(results)
        avg_deep = sum(r.deep_pct for r in results) / len(results)
        avg_total = sum(r.total_min for r in results) / len(results)

        hours = int(avg_total) // 60
        mins = int(avg_total) % 60

        lines = [
            f"Sleep Summary ({len(results)} nights)",
            f"Avg Duration: {hours}h {mins}m",
            f"Avg REM: {avg_rem:.0f}%",
            f"Avg Deep: {avg_deep:.0f}%",
        ]

        # Check for persistent concerns
        low_rem_count = sum(1 for r in results if r.rem_status == "low")
        low_deep_count = sum(
            1 for r in results if r.deep_status == "low"
        )
        if low_rem_count >= len(results) // 2:
            lines.append(
                f"Low REM in {low_rem_count}/{len(results)} nights",
            )
        if low_deep_count >= len(results) // 2:
            lines.append(
                f"Low Deep in {low_deep_count}/{len(results)} nights",
            )

        return "\n".join(lines)
