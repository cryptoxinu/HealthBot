"""Health goal tracking and progress monitoring.

Stores user-defined health goals (e.g., "get LDL below 100") and
checks progress against latest lab/wearable data.

All analysis is deterministic — no LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass
class HealthGoal:
    """A user's health goal."""

    metric: str           # canonical lab name or wearable metric
    target_value: float
    direction: str        # "below" or "above"
    display_name: str = ""
    created_date: str = ""
    goal_id: str = ""


@dataclass
class GoalProgress:
    """Progress toward a health goal."""

    goal: HealthGoal
    current_value: float | None
    current_date: str
    pct_progress: float   # 0-100, can exceed 100 if achieved
    status: str           # "achieved", "on_track", "off_track", "no_data"
    message: str = ""


# Wearable metrics that can be goal targets
_WEARABLE_METRICS: set[str] = {
    "hrv", "rhr", "sleep_score", "recovery_score",
    "strain", "sleep_duration_min",
}


class GoalTracker:
    """Track progress toward health goals."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def add_goal(
        self, user_id: int,
        metric: str,
        target_value: float,
        direction: str,
        display_name: str = "",
    ) -> str:
        """Add a new health goal. Returns goal ID."""
        goal_data = {
            "metric": metric,
            "target_value": target_value,
            "direction": direction,
            "display_name": display_name or metric,
            "status": "active",
        }
        return self._db.insert_health_goal(user_id, goal_data)

    def get_goals(self, user_id: int) -> list[HealthGoal]:
        """Get all active goals for a user."""
        raw = self._db.get_health_goals(user_id)
        goals: list[HealthGoal] = []
        for g in raw:
            if g.get("status") != "active":
                continue
            goals.append(HealthGoal(
                metric=g.get("metric", ""),
                target_value=float(g.get("target_value", 0)),
                direction=g.get("direction", "below"),
                display_name=g.get("display_name", g.get("metric", "")),
                created_date=g.get("_created_at", ""),
                goal_id=g.get("_id", ""),
            ))
        return goals

    def check_progress(
        self, user_id: int,
    ) -> list[GoalProgress]:
        """Check progress on all active goals."""
        goals = self.get_goals(user_id)
        results: list[GoalProgress] = []
        for goal in goals:
            progress = self._check_single(goal, user_id)
            results.append(progress)
        return results

    def check_achievements(
        self, user_id: int,
    ) -> list[GoalProgress]:
        """Return only goals that have been achieved."""
        return [p for p in self.check_progress(user_id) if p.status == "achieved"]

    def remove_goal(self, goal_id: str) -> bool:
        """Remove a goal by ID."""
        return self._db.delete_health_goal(goal_id)

    def _check_single(
        self, goal: HealthGoal, user_id: int,
    ) -> GoalProgress:
        """Check progress on a single goal."""
        current_value, current_date = self._get_latest_value(
            goal.metric, user_id,
        )
        if current_value is None:
            return GoalProgress(
                goal=goal,
                current_value=None,
                current_date="",
                pct_progress=0.0,
                status="no_data",
                message=f"No recent data for {goal.display_name}.",
            )

        # Determine if achieved
        if goal.direction == "below":
            achieved = current_value <= goal.target_value
        else:
            achieved = current_value >= goal.target_value

        if achieved:
            return GoalProgress(
                goal=goal,
                current_value=current_value,
                current_date=current_date,
                pct_progress=100.0,
                status="achieved",
                message=(
                    f"Goal achieved! {goal.display_name} is {current_value:.1f} "
                    f"(target: {goal.direction} {goal.target_value:.1f})."
                ),
            )

        # Compute progress percentage
        # Get baseline (earliest value after goal creation)
        baseline = self._get_baseline_value(goal, user_id)
        if baseline is None:
            baseline = current_value

        if goal.direction == "below":
            total_distance = baseline - goal.target_value
            current_distance = baseline - current_value
        else:
            total_distance = goal.target_value - baseline
            current_distance = current_value - baseline

        if total_distance > 0:
            pct = (current_distance / total_distance) * 100
        else:
            pct = 0.0

        pct = max(0.0, min(100.0, pct))
        status = "on_track" if pct >= 25 else "off_track"

        arrow = "down" if goal.direction == "below" else "up"
        return GoalProgress(
            goal=goal,
            current_value=current_value,
            current_date=current_date,
            pct_progress=round(pct, 1),
            status=status,
            message=(
                f"{goal.display_name}: {current_value:.1f} "
                f"(target: {goal.direction} {goal.target_value:.1f}, "
                f"{pct:.0f}% there, trending {arrow})."
            ),
        )

    def _get_latest_value(
        self, metric: str, user_id: int,
    ) -> tuple[float | None, str]:
        """Get the most recent value for a metric."""
        # Try lab result first
        if metric not in _WEARABLE_METRICS:
            rows = self._db.query_observations(
                record_type="lab_result",
                canonical_name=metric,
                limit=1,
                user_id=user_id,
            )
            if rows:
                row = rows[0]
                val = row.get("value")
                dt = row.get("date_collected", "")
                if val is not None:
                    try:
                        return float(val), str(dt)
                    except (ValueError, TypeError):
                        pass

        # Try wearable metric
        if metric in _WEARABLE_METRICS:
            wearable_rows = self._db.query_wearable_daily(
                limit=1, user_id=user_id,
            )
            if wearable_rows:
                row = wearable_rows[0]
                val = row.get(metric)
                dt = row.get("_date", "")
                if val is not None:
                    try:
                        return float(val), str(dt)
                    except (ValueError, TypeError):
                        pass

        return None, ""

    def _get_baseline_value(
        self, goal: HealthGoal, user_id: int,
    ) -> float | None:
        """Get the baseline value (earliest after goal creation)."""
        if goal.metric not in _WEARABLE_METRICS:
            rows = self._db.query_observations(
                record_type="lab_result",
                canonical_name=goal.metric,
                limit=50,
                user_id=user_id,
            )
            if len(rows) >= 2:
                # Oldest value
                last = rows[-1]
                val = last.get("value")
                if val is not None:
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        pass
        return None


def format_goals(progress_list: list[GoalProgress]) -> str:
    """Format goal progress for display."""
    if not progress_list:
        return (
            "No health goals set.\n\n"
            "Set a goal: \"I want to get my LDL below 100\" or "
            "use /goals add <metric> <below|above> <value>"
        )

    lines = ["HEALTH GOALS", "-" * 30]
    for p in progress_list:
        if p.status == "achieved":
            icon = "+"
        elif p.status == "on_track":
            icon = "~"
        elif p.status == "off_track":
            icon = "!"
        else:
            icon = "?"

        bar = ""
        if p.status != "no_data":
            filled = int(p.pct_progress / 10)
            bar = f" [{'\u2588' * filled}{'.' * (10 - filled)}] {p.pct_progress:.0f}%"

        lines.append(f"\n  {icon} {p.goal.display_name}{bar}")
        lines.append(f"    {p.message}")

    return "\n".join(lines)
