"""Wearable data trend analysis and anomaly detection.

Uses linear regression (numpy polyfit) on wearable_daily metrics.
Separate from trends.py because wearable data has a different schema
(one row/day with multiple metric fields vs one observation per row).

All analysis is deterministic — no LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np

logger = logging.getLogger("healthbot")

# Human-readable display names for metrics
METRIC_DISPLAY_NAMES: dict[str, str] = {
    "hrv": "HRV",
    "rhr": "Resting Heart Rate",
    "sleep_score": "Sleep Score",
    "recovery_score": "Recovery Score",
    "strain": "Strain",
    "sleep_duration_min": "Sleep Duration",
    "spo2": "SpO2",
    "skin_temp": "Skin Temperature",
    "resp_rate": "Respiratory Rate",
    "deep_min": "Deep Sleep",
    "rem_min": "REM Sleep",
}

# Metrics to analyze for trends
TREND_METRICS: list[str] = [
    "hrv", "rhr", "sleep_score", "recovery_score",
    "strain", "sleep_duration_min",
]

# Anomaly rules: (metric, threshold_type, threshold_value, severity)
ANOMALY_RULES: list[tuple[str, str, float, str]] = [
    ("sleep_score", "absolute_below", 40.0, "urgent"),
    ("hrv", "pct_below_baseline", 30.0, "urgent"),
    ("rhr", "pct_above_baseline", 15.0, "watch"),
    ("spo2", "absolute_below", 92.0, "urgent"),
    ("recovery_score", "absolute_below", 20.0, "watch"),
]

# Messages for each anomaly type
_ANOMALY_MESSAGES: dict[str, str] = {
    "sleep_score": "Sleep score critically low ({value:.0f}/100) — well below healthy range",
    "hrv": (
        "HRV dropped {deviation_pct:.0f}% below your 7-day average "
        "({value:.0f} vs baseline {baseline:.0f}ms)"
    ),
    "rhr": (
        "Resting heart rate elevated {deviation_pct:.0f}% above your 7-day average "
        "({value:.0f} vs baseline {baseline:.0f} bpm)"
    ),
    "spo2": "Blood oxygen saturation critically low ({value:.1f}%) — below 92% threshold",
    "recovery_score": "Recovery score very low ({value:.0f}/100) — body needs rest",
}


@dataclass
class WearableTrendResult:
    metric_name: str
    display_name: str
    direction: str  # "increasing", "decreasing", "stable"
    slope: float  # units per day
    r_squared: float
    data_points: int
    first_date: str
    last_date: str
    first_value: float
    last_value: float
    pct_change: float
    values: list[tuple[str, float]]


@dataclass
class WearableAnomaly:
    metric_name: str
    display_name: str
    date: str
    value: float
    baseline: float  # 7-day rolling average
    deviation_pct: float
    severity: str  # "watch" or "urgent"
    message: str


class WearableTrendAnalyzer:
    """Analyze trends and detect anomalies in wearable metrics.

    Accepts any DB with query_wearable_daily() — HealthDB or CleanDB.
    Wearable data is purely numeric (no PII), so CleanDB is preferred
    to avoid unnecessary encrypt/decrypt overhead.
    """

    def __init__(self, db: object) -> None:
        self._db = db

    def analyze_metric(
        self, metric_name: str, days: int = 30,
        user_id: int | None = None,
    ) -> WearableTrendResult | None:
        """Linear regression on a single wearable metric.

        Requires at least 5 data points. Uses same numpy polyfit
        approach as TrendAnalyzer in trends.py.
        """
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        rows = self._db.query_wearable_daily(
            start_date=cutoff, limit=days, user_id=user_id,
        )

        # Extract (date_str, value) pairs, filter None
        points: list[tuple[str, float]] = []
        for row in rows:
            date_str = row.get("_date") or row.get("date", "")
            val = row.get(metric_name)
            if date_str and val is not None:
                try:
                    points.append((str(date_str), float(val)))
                except (ValueError, TypeError):
                    continue

        if len(points) < 5:
            return None

        # Sort by date ascending
        points.sort(key=lambda x: x[0])

        # Convert dates to day offsets
        base_date = date.fromisoformat(points[0][0])
        x = np.array(
            [(date.fromisoformat(d) - base_date).days for d, _ in points],
            dtype=float,
        )
        y = np.array([v for _, v in points], dtype=float)

        if x[-1] == x[0]:
            return None

        # Linear regression
        coeffs = np.polyfit(x, y, 1)
        slope = float(coeffs[0])

        # R-squared
        y_pred = np.polyval(coeffs, x)
        ss_res = float(np.sum((y - y_pred) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        # Percentage change
        pct_change = (
            (points[-1][1] - points[0][1]) / points[0][1] * 100
            if points[0][1] != 0
            else 0.0
        )

        # Direction
        if abs(pct_change) < 5 or r_squared < 0.1:
            direction = "stable"
        elif slope > 0:
            direction = "increasing"
        else:
            direction = "decreasing"

        display = METRIC_DISPLAY_NAMES.get(metric_name, metric_name)

        return WearableTrendResult(
            metric_name=metric_name,
            display_name=display,
            direction=direction,
            slope=slope,
            r_squared=r_squared,
            data_points=len(points),
            first_date=points[0][0],
            last_date=points[-1][0],
            first_value=points[0][1],
            last_value=points[-1][1],
            pct_change=pct_change,
            values=points,
        )

    def detect_all_trends(
        self, days: int = 30, user_id: int | None = None,
    ) -> list[WearableTrendResult]:
        """Analyze all key metrics, return non-stable sorted by |pct_change|."""
        results = []
        for metric in TREND_METRICS:
            trend = self.analyze_metric(metric, days, user_id=user_id)
            if trend and trend.direction != "stable":
                results.append(trend)
        results.sort(key=lambda t: abs(t.pct_change), reverse=True)
        return results

    def detect_anomalies(
        self, days: int = 7, user_id: int | None = None,
    ) -> list[WearableAnomaly]:
        """Flag single-day outliers against 7-day rolling baseline."""
        # Need 14 days of data (7 baseline + days to check)
        cutoff = (date.today() - timedelta(days=14 + days)).isoformat()
        rows = self._db.query_wearable_daily(
            start_date=cutoff, limit=30, user_id=user_id,
        )

        if not rows:
            return []

        # Index by date
        by_date: dict[str, dict] = {}
        for row in rows:
            d = str(row.get("_date") or row.get("date", ""))
            if d:
                by_date[d] = row

        sorted_dates = sorted(by_date.keys())
        anomalies: list[WearableAnomaly] = []

        # Check the last N days
        check_start = (date.today() - timedelta(days=days - 1)).isoformat()
        check_dates = [d for d in sorted_dates if d >= check_start]

        for check_date in check_dates:
            row = by_date[check_date]

            for metric, threshold_type, threshold_val, severity in ANOMALY_RULES:
                value = row.get(metric)
                if value is None:
                    continue
                value = float(value)
                display = METRIC_DISPLAY_NAMES.get(metric, metric)

                if threshold_type == "absolute_below":
                    if value < threshold_val:
                        msg = _ANOMALY_MESSAGES.get(metric, "").format(
                            value=value, baseline=0, deviation_pct=0,
                        )
                        anomalies.append(WearableAnomaly(
                            metric_name=metric,
                            display_name=display,
                            date=check_date,
                            value=value,
                            baseline=0.0,
                            deviation_pct=0.0,
                            severity=severity,
                            message=msg,
                        ))

                elif threshold_type in ("pct_below_baseline", "pct_above_baseline"):
                    # Compute 7-day rolling average from preceding days
                    baseline_dates = [
                        d for d in sorted_dates
                        if d < check_date
                    ][-7:]

                    if len(baseline_dates) < 3:
                        continue

                    baseline_vals = []
                    for bd in baseline_dates:
                        bv = by_date[bd].get(metric)
                        if bv is not None:
                            baseline_vals.append(float(bv))

                    if not baseline_vals:
                        continue

                    baseline_avg = sum(baseline_vals) / len(baseline_vals)
                    if baseline_avg == 0:
                        continue

                    if threshold_type == "pct_below_baseline":
                        deviation = (baseline_avg - value) / baseline_avg * 100
                        if deviation > threshold_val:
                            msg = _ANOMALY_MESSAGES.get(metric, "").format(
                                value=value, baseline=baseline_avg,
                                deviation_pct=deviation,
                            )
                            anomalies.append(WearableAnomaly(
                                metric_name=metric,
                                display_name=display,
                                date=check_date,
                                value=value,
                                baseline=baseline_avg,
                                deviation_pct=deviation,
                                severity=severity,
                                message=msg,
                            ))
                    else:  # pct_above_baseline
                        deviation = (value - baseline_avg) / baseline_avg * 100
                        if deviation > threshold_val:
                            msg = _ANOMALY_MESSAGES.get(metric, "").format(
                                value=value, baseline=baseline_avg,
                                deviation_pct=deviation,
                            )
                            anomalies.append(WearableAnomaly(
                                metric_name=metric,
                                display_name=display,
                                date=check_date,
                                value=value,
                                baseline=baseline_avg,
                                deviation_pct=deviation,
                                severity=severity,
                                message=msg,
                            ))

        # Sort: urgent first, then by date desc
        severity_order = {"urgent": 0, "watch": 1}
        anomalies.sort(key=lambda a: (severity_order.get(a.severity, 2), a.date), reverse=False)
        return anomalies

    def format_trend(self, trend: WearableTrendResult) -> str:
        """Format for Telegram display."""
        if trend.direction == "increasing":
            arrow = "↑"
        elif trend.direction == "decreasing":
            arrow = "↓"
        else:
            arrow = "→"
        return (
            f"{arrow} {trend.display_name}: {trend.first_value:.0f} → "
            f"{trend.last_value:.0f} ({trend.pct_change:+.1f}%) "
            f"over {trend.data_points} days"
        )

    def format_anomaly(self, anomaly: WearableAnomaly) -> str:
        """Format anomaly for display."""
        return f"[{anomaly.date}] {anomaly.message}"
