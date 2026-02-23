"""Background health watcher -- deterministic alert generation.

Checks for overdue screenings and notable trends.
All logic is deterministic. No LLM calls.
Produces Alert objects that the scheduler sends to the user.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass
class Alert:
    alert_type: str          # "overdue", "trend"
    title: str               # Short human-readable title
    body: str                # Full message
    severity: str            # "info", "watch", "urgent"
    dedup_key: str           # For deduplication


class HealthWatcher:
    """Generate proactive alerts from stored health data."""

    def __init__(self, db: HealthDB, user_id: int | None = None) -> None:
        self._db = db
        self._user_id = user_id

    def check_all(self) -> list[Alert]:
        """Run all deterministic checks."""
        alerts: list[Alert] = []
        alerts.extend(self._check_overdue())
        alerts.extend(self._check_trends())
        alerts.extend(self._check_wearable_trends())
        alerts.extend(self._check_wearable_anomalies())
        alerts.extend(self._check_overtraining())
        alerts.extend(self._check_retests())
        alerts.extend(self._check_side_effects())
        alerts.extend(self._check_stress())
        alerts.extend(self._check_sleep_deficits())
        alerts.extend(self._check_correlation_alerts())
        alerts.extend(self._check_goal_achievements())
        return alerts

    def _check_overdue(self) -> list[Alert]:
        """Alerts for overdue screenings (>60 days overdue only)."""
        from healthbot.reasoning.overdue import OverdueDetector

        detector = OverdueDetector(self._db)
        items = detector.check_overdue(user_id=self._user_id)
        alerts: list[Alert] = []
        for item in items:
            if item.days_overdue > 60:
                months = item.days_overdue // 30
                alerts.append(Alert(
                    alert_type="overdue",
                    title=f"Overdue: {item.test_name}",
                    body=(
                        f"You're overdue for {item.test_name} recheck "
                        f"(last seen {item.last_date}, ~{months} months overdue)."
                    ),
                    severity="watch",
                    dedup_key=self._dedup_hash("overdue", item.canonical_name),
                ))
        return alerts

    def _check_trends(self) -> list[Alert]:
        """Alerts for concerning trends (>10% change, >=3 data points)."""
        from healthbot.reasoning.trends import TrendAnalyzer

        analyzer = TrendAnalyzer(self._db)
        trends = analyzer.detect_all_trends(months=6, user_id=self._user_id)
        alerts: list[Alert] = []
        for t in trends:
            if abs(t.pct_change) > 10 and t.data_points >= 3:
                direction = "rose" if t.pct_change > 0 else "dropped"
                alerts.append(Alert(
                    alert_type="trend",
                    title=f"Trend: {t.test_name} {direction}",
                    body=(
                        f"{t.test_name} {direction} {abs(t.pct_change):.0f}% "
                        f"over {t.data_points} results "
                        f"({t.first_date} to {t.last_date}). "
                        f"Want a doctor packet draft?"
                    ),
                    severity="urgent" if abs(t.pct_change) > 25 else "watch",
                    dedup_key=self._dedup_hash("trend", t.test_name),
                ))
        return alerts

    def _check_wearable_trends(self) -> list[Alert]:
        """Alerts for concerning wearable metric trends."""
        from healthbot.reasoning.wearable_trends import WearableTrendAnalyzer

        wearable_alert_rules: dict[str, dict] = {
            "sleep_score": {"threshold_pct": 15, "min_days": 5, "bad_direction": "decreasing"},
            "hrv": {"threshold_pct": 20, "min_days": 7, "bad_direction": "decreasing"},
            "rhr": {"threshold_pct": 10, "min_days": 7, "bad_direction": "increasing"},
            "recovery_score": {"threshold_pct": 25, "min_days": 3, "bad_direction": "decreasing"},
            "sleep_duration_min": {
                "threshold_pct": 15, "min_days": 3, "bad_direction": "decreasing",
            },
            "strain": {"threshold_pct": 20, "min_days": 3, "bad_direction": "increasing"},
        }

        analyzer = WearableTrendAnalyzer(self._db)
        trends = analyzer.detect_all_trends(days=14, user_id=self._user_id)
        alerts: list[Alert] = []

        for t in trends:
            rule = wearable_alert_rules.get(t.metric_name)
            if not rule:
                continue
            if t.data_points < rule["min_days"]:
                continue
            if t.direction != rule["bad_direction"]:
                continue
            if abs(t.pct_change) < rule["threshold_pct"]:
                continue

            severity = "watch"
            if t.metric_name == "recovery_score" and t.last_value < 33:
                severity = "urgent"
            if t.metric_name == "sleep_duration_min" and t.last_value < 360:
                severity = "urgent"
            if t.metric_name == "strain" and t.last_value > 18:
                severity = "urgent"

            verb = "declining" if t.direction == "decreasing" else "rising"
            alerts.append(Alert(
                alert_type="wearable_trend",
                title=f"Wearable: {t.display_name} {verb}",
                body=(
                    f"{t.display_name} has been {verb} {abs(t.pct_change):.0f}% "
                    f"over the past {t.data_points} days "
                    f"({t.first_value:.0f} -> {t.last_value:.0f})."
                ),
                severity=severity,
                dedup_key=self._dedup_hash("wearable_trend", t.metric_name),
            ))
        return alerts

    def _check_wearable_anomalies(self) -> list[Alert]:
        """Alerts for single-day wearable outliers."""
        from healthbot.reasoning.wearable_trends import WearableTrendAnalyzer

        analyzer = WearableTrendAnalyzer(self._db)
        anomalies = analyzer.detect_anomalies(days=1, user_id=self._user_id)
        alerts: list[Alert] = []

        for a in anomalies:
            alerts.append(Alert(
                alert_type="wearable_anomaly",
                title=f"Anomaly: {a.display_name}",
                body=a.message,
                severity=a.severity,
                dedup_key=self._dedup_hash(
                    "wearable_anomaly", f"{a.metric_name}_{a.date}",
                ),
            ))
        return alerts

    def _check_overtraining(self) -> list[Alert]:
        """Alert if overtraining syndrome signals are present."""
        from healthbot.reasoning.overtraining_detector import (
            OvertrainingDetector,
        )

        detector = OvertrainingDetector(self._db)
        assessment = detector.assess(user_id=self._user_id)

        if assessment.severity == "none":
            return []

        return [Alert(
            alert_type="overtraining",
            title=f"Overtraining: {assessment.severity}",
            body=(
                f"{assessment.positive_count}/5 overtraining signals "
                f"detected ({assessment.confidence:.0%} confidence). "
                f"{assessment.recommendation}"
            ),
            severity="urgent" if assessment.severity == "likely" else "watch",
            dedup_key=self._dedup_hash("overtraining", assessment.severity),
        )]

    def _check_retests(self) -> list[Alert]:
        """Alert for abnormal lab results needing follow-up retests."""
        from healthbot.reasoning.retest_scheduler import RetestScheduler

        scheduler = RetestScheduler(self._db)
        retests = scheduler.get_pending_retests(user_id=self._user_id)
        alerts: list[Alert] = []

        for rt in retests:
            flag_text = "HIGH" if rt.abnormal_flag.upper() in ("H", "HH") else "LOW"
            if rt.days_until_due < 0:
                timing = f"{abs(rt.days_until_due)} days overdue"
            else:
                timing = f"due in {rt.days_until_due} days"

            severity = "urgent" if rt.priority == "urgent" else "watch"
            alerts.append(Alert(
                alert_type="retest",
                title=f"Retest: {rt.display_name}",
                body=(
                    f"{rt.display_name} was {flag_text} ({rt.abnormal_value}) "
                    f"on {rt.abnormal_date}. "
                    f"Retest {timing}. {rt.reason}"
                ),
                severity=severity,
                dedup_key=self._dedup_hash("retest", rt.canonical_name),
            ))
        return alerts

    def _check_goal_achievements(self) -> list[Alert]:
        """Alert when health goals are achieved."""
        from healthbot.reasoning.goals import GoalTracker

        tracker = GoalTracker(self._db)
        try:
            achievements = tracker.check_achievements(
                user_id=self._user_id or 0,
            )
        except Exception:
            return []

        alerts: list[Alert] = []
        for ach in achievements:
            alerts.append(Alert(
                alert_type="goal_achieved",
                title=f"Goal: {ach.goal.display_name}",
                body=ach.message,
                severity="info",
                dedup_key=self._dedup_hash(
                    "goal", ach.goal.goal_id or ach.goal.metric,
                ),
            ))
        return alerts

    def _check_correlation_alerts(self) -> list[Alert]:
        """Alert when clinically meaningful lab-wearable correlations found."""
        from healthbot.reasoning.correlate import CorrelationEngine

        engine = CorrelationEngine(self._db)
        try:
            corr_alerts = engine.generate_correlation_alerts(
                days=90, user_id=self._user_id, min_r=0.5,
            )
        except Exception:
            logger.debug("Correlation alert check skipped (insufficient data)")
            return []

        alerts: list[Alert] = []
        for ca in corr_alerts[:3]:  # cap at 3 per check
            alerts.append(Alert(
                alert_type="correlation",
                title=f"Correlation: {ca.lab_metric} <-> {ca.wearable_metric}",
                body=(
                    f"{ca.clinical_context}. "
                    f"{ca.actionable_advice} "
                    f"(r={ca.pearson_r:+.2f}, {ca.n_observations} data points). "
                    f"Ref: {ca.evidence}"
                ),
                severity="watch",
                dedup_key=self._dedup_hash(
                    "correlation",
                    f"{ca.lab_metric}_{ca.wearable_metric}",
                ),
            ))
        return alerts

    def _check_side_effects(self) -> list[Alert]:
        """Alert when medication side effects are detected in lab results."""
        from healthbot.reasoning.side_effect_monitor import SideEffectMonitor

        monitor = SideEffectMonitor(self._db)
        try:
            concerns = monitor.check_active_concerns(
                user_id=self._user_id,
            )
        except Exception:
            return []

        alerts: list[Alert] = []
        for c in concerns:
            marker = c.lab_marker.replace("_", " ").title()
            flag = "HIGH" if c.lab_flag in ("H", "HH") else "LOW"
            alerts.append(Alert(
                alert_type="side_effect",
                title=f"Side effect: {c.med_name}",
                body=(
                    f"{c.med_name} may be causing {c.effect}. "
                    f"{marker} is {flag} ({c.lab_value}). "
                    f"{c.monitoring_note}"
                ),
                severity="watch",
                dedup_key=self._dedup_hash(
                    "side_effect", f"{c.med_name}_{c.lab_marker}",
                ),
            ))
        return alerts

    def _check_stress(self) -> list[Alert]:
        """Alert when wearable data indicates high/critical stress."""
        from healthbot.reasoning.stress_detector import StressDetector

        detector = StressDetector(self._db)
        try:
            assessment = detector.assess(user_id=self._user_id)
        except Exception:
            return []

        if assessment.stress_level not in ("high", "critical"):
            return []

        severity = "urgent" if assessment.stress_level == "critical" else "watch"
        rec_texts = [
            r.recommendation for r in assessment.recommendations[:2]
        ]
        return [Alert(
            alert_type="stress",
            title=f"Stress: {assessment.stress_level}",
            body=(
                f"Stress score {assessment.stress_score * 100:.0f}/100 "
                f"({assessment.stress_level}). "
                f"{'; '.join(rec_texts)}"
            ),
            severity=severity,
            dedup_key=self._dedup_hash(
                "stress", assessment.stress_level,
            ),
        )]

    def _check_sleep_deficits(self) -> list[Alert]:
        """Alert when persistent sleep deficits are detected."""
        from healthbot.reasoning.sleep_recommendations import SleepRecommender

        recommender = SleepRecommender(self._db)
        try:
            recs = recommender.get_recommendations(user_id=self._user_id)
        except Exception:
            return []

        alerts: list[Alert] = []
        for rec in recs:
            tip_text = rec.tips[0].tip if rec.tips else ""
            alerts.append(Alert(
                alert_type="sleep_deficit",
                title=f"Sleep: {rec.deficit_label}",
                body=(
                    f"{rec.deficit_label} ({rec.current_value}). "
                    f"{tip_text}"
                ),
                severity="watch",
                dedup_key=self._dedup_hash(
                    "sleep_deficit", rec.deficit_type,
                ),
            ))
        return alerts

    def _dedup_hash(self, alert_type: str, key: str) -> str:
        """Weekly dedup key: same alert won't re-fire within same ISO week."""
        week = datetime.now(UTC).strftime("%G-W%V")
        raw = f"{alert_type}:{key}:{week}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
