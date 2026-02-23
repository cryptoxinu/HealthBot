"""Time-series trend analysis for health metrics.

Uses simple linear regression (numpy polyfit) and percentage change.
No ML models.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass
class TrendResult:
    test_name: str
    canonical_name: str
    direction: str  # "increasing", "decreasing", "stable"
    slope: float  # Units per day
    r_squared: float
    data_points: int
    first_date: str
    last_date: str
    first_value: float
    last_value: float
    pct_change: float
    values: list[tuple[str, float]]  # (date, value) pairs
    age_context: str = ""  # Age-contextualized interpretation


# Age-expected changes (per decade) for trend contextualization
AGE_EXPECTED_CHANGES: dict[str, dict] = {
    "testosterone_total": {
        "direction": "decreasing",
        "rate_per_decade_pct": -15,
        "note": (
            "Testosterone declines ~1-2% per year after age 30. "
            "Rapid decline warrants investigation."
        ),
        "concern_threshold_pct_decade": -25,
    },
    "egfr": {
        "direction": "decreasing",
        "rate_per_decade_pct": -10,
        "note": (
            "eGFR declines ~1 mL/min/year after 40. "
            "Faster decline suggests kidney disease."
        ),
        "concern_threshold_pct_decade": -15,
    },
    "cholesterol_total": {
        "direction": "increasing",
        "rate_per_decade_pct": 5,
        "note": (
            "Cholesterol naturally rises through middle age, "
            "plateaus ~65."
        ),
        "concern_threshold_pct_decade": 15,
    },
    "psa": {
        "direction": "increasing",
        "note": (
            "PSA velocity >0.75 ng/mL/year is concerning "
            "regardless of absolute level."
        ),
        "concern_velocity": 0.75,  # ng/mL per year
    },
    "hba1c": {
        "direction": "increasing",
        "rate_per_decade_pct": 2,
        "note": (
            "A1c rises ~0.1% per decade with aging. "
            "Faster rise suggests prediabetes progression."
        ),
        "concern_threshold_pct_decade": 8,
    },
    "hemoglobin": {
        "direction": "decreasing",
        "rate_per_decade_pct": -2,
        "note": "Slight hemoglobin decline is normal with aging.",
        "concern_threshold_pct_decade": -10,
    },
    "tsh": {
        "direction": "increasing",
        "rate_per_decade_pct": 5,
        "note": "TSH tends to rise slightly with age.",
        "concern_threshold_pct_decade": 20,
    },
    "vitamin_d": {
        "direction": "decreasing",
        "rate_per_decade_pct": -5,
        "note": (
            "Vitamin D levels may decline with age "
            "due to reduced skin synthesis."
        ),
        "concern_threshold_pct_decade": -15,
    },
}


class TrendAnalyzer:
    """Analyze trends in lab results over time."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def analyze_test(
        self, canonical_name: str, months: int = 24, user_id: int | None = None,
    ) -> TrendResult | None:
        """Analyze trend for a specific test over the last N months.

        Requires at least 3 data points.
        """
        from datetime import timedelta

        cutoff = (datetime.now(UTC) - timedelta(days=months * 30)).strftime("%Y-%m-%d")
        rows = self._db.query_observations(
            record_type="lab_result",
            canonical_name=canonical_name,
            start_date=cutoff,
            limit=100,
            user_id=user_id,
        )

        # Extract numeric values with dates
        points: list[tuple[str, float]] = []
        for row in rows:
            date_str = row.get("date_collected") or row.get("_meta", {}).get("date_effective", "")
            value = row.get("value")
            if date_str and value is not None:
                try:
                    v = float(value)
                    points.append((date_str, v))
                except (ValueError, TypeError):
                    continue

        if len(points) < 2:
            return None

        # Sort by date
        points.sort(key=lambda x: x[0])

        # Convert dates to day offsets for regression
        base_date = datetime.fromisoformat(points[0][0]).date()
        x = np.array(
            [(datetime.fromisoformat(d).date() - base_date).days for d, _ in points],
            dtype=float,
        )
        y = np.array([v for _, v in points], dtype=float)

        # Linear regression
        if len(x) < 2 or x[-1] == x[0]:
            return None

        coeffs = np.polyfit(x, y, 1)
        slope = float(coeffs[0])

        # R-squared
        y_pred = np.polyval(coeffs, x)
        ss_res = float(np.sum((y - y_pred) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        # Direction
        pct_change = (
            (points[-1][1] - points[0][1]) / points[0][1] * 100
            if points[0][1] != 0
            else 0.0
        )

        if abs(pct_change) < 5 or r_squared < 0.1:
            direction = "stable"
        elif slope > 0:
            direction = "increasing"
        else:
            direction = "decreasing"

        test_name = rows[0].get("test_name", canonical_name) if rows else canonical_name

        return TrendResult(
            test_name=test_name,
            canonical_name=canonical_name,
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
        self, months: int = 24, user_id: int | None = None,
    ) -> list[TrendResult]:
        """Find all tests with meaningful trends."""
        # Get unique canonical names
        sql = "SELECT DISTINCT canonical_name FROM observations WHERE record_type = 'lab_result'"
        params: list = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        rows = self._db.conn.execute(sql, params).fetchall()

        results = []
        for row in rows:
            name = row["canonical_name"]
            if not name:
                continue
            trend = self.analyze_test_cached(name, months, user_id=user_id)
            if trend and trend.direction != "stable":
                results.append(trend)

        # Sort by absolute pct_change descending
        results.sort(key=lambda t: abs(t.pct_change), reverse=True)
        return results

    def analyze_test_cached(
        self,
        canonical_name: str,
        months: int = 24,
        user_id: int | None = None,
    ) -> TrendResult | None:
        """Cache-first trend analysis.

        Returns cached result if the cache's last_date matches the latest
        observation. Otherwise recomputes, stores in cache, and returns.
        """
        cached = self._get_cached(canonical_name, user_id)
        if cached is not None:
            # Check if cache is still valid by comparing last_date
            latest_obs = self._db.query_observations(
                record_type="lab_result",
                canonical_name=canonical_name,
                limit=1,
                user_id=user_id,
            )
            if latest_obs:
                latest_date = (
                    latest_obs[0].get("date_collected")
                    or latest_obs[0].get("_meta", {}).get(
                        "date_effective", "",
                    )
                )
                if latest_date and latest_date[:10] == cached.last_date[:10]:
                    return cached

        # Cache miss or stale — recompute
        result = self.analyze_test(canonical_name, months, user_id)
        if result:
            self._store_cached(result, user_id)
        return result

    def invalidate_cache(
        self,
        canonical_names: set[str] | None = None,
        user_id: int | None = None,
    ) -> int:
        """Invalidate cache for specific tests or all.

        Called after bulk import/delete.
        Returns count of invalidated entries.
        """
        try:
            if canonical_names:
                placeholders = ",".join("?" for _ in canonical_names)
                params: list = list(canonical_names)
                sql = (
                    f"DELETE FROM trend_cache "
                    f"WHERE canonical_name IN ({placeholders})"
                )
                if user_id is not None:
                    sql += " AND user_id = ?"
                    params.append(user_id)
                cursor = self._db.conn.execute(sql, params)
            elif user_id is not None:
                cursor = self._db.conn.execute(
                    "DELETE FROM trend_cache WHERE user_id = ?",
                    (user_id,),
                )
            else:
                cursor = self._db.conn.execute("DELETE FROM trend_cache")
            self._db.conn.commit()
            return cursor.rowcount
        except Exception as e:
            logger.debug("Trend cache invalidation failed: %s", e)
            return 0

    def _get_cached(
        self, canonical_name: str, user_id: int | None,
    ) -> TrendResult | None:
        """Retrieve cached trend result."""
        try:
            sql = (
                "SELECT * FROM trend_cache "
                "WHERE canonical_name = ?"
            )
            params: list = [canonical_name]
            if user_id is not None:
                sql += " AND user_id = ?"
                params.append(user_id)
            row = self._db.conn.execute(sql, params).fetchone()
            if not row:
                return None
            return TrendResult(
                test_name=canonical_name,
                canonical_name=canonical_name,
                direction=row["direction"] or "stable",
                slope=row["slope"] or 0.0,
                r_squared=row["r_squared"] or 0.0,
                data_points=row["data_points"] or 0,
                first_date=row["first_date"] or "",
                last_date=row["last_date"] or "",
                first_value=row["first_value"] or 0.0,
                last_value=row["last_value"] or 0.0,
                pct_change=row["pct_change"] or 0.0,
                values=[],  # Not stored in cache
            )
        except Exception:
            return None

    def _store_cached(
        self, trend: TrendResult, user_id: int | None,
    ) -> None:
        """Store trend result in cache (upsert)."""
        try:
            cache_id = uuid.uuid4().hex
            now = datetime.now(UTC).isoformat()
            uid = user_id or 0
            aad = f"trend_cache.encrypted_data.{cache_id}"
            enc_data = self._db._encrypt(
                {"canonical_name": trend.canonical_name}, aad,
            )

            self._db.conn.execute(
                """INSERT INTO trend_cache
                   (id, user_id, canonical_name, slope, r_squared,
                    direction, pct_change, data_points,
                    first_date, last_date, first_value, last_value,
                    computed_at, encrypted_data)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(user_id, canonical_name)
                   DO UPDATE SET
                     id=excluded.id, slope=excluded.slope,
                     r_squared=excluded.r_squared,
                     direction=excluded.direction,
                     pct_change=excluded.pct_change,
                     data_points=excluded.data_points,
                     first_date=excluded.first_date,
                     last_date=excluded.last_date,
                     first_value=excluded.first_value,
                     last_value=excluded.last_value,
                     computed_at=excluded.computed_at,
                     encrypted_data=excluded.encrypted_data""",
                (
                    cache_id, uid, trend.canonical_name,
                    trend.slope, trend.r_squared, trend.direction,
                    trend.pct_change, trend.data_points,
                    trend.first_date, trend.last_date,
                    trend.first_value, trend.last_value,
                    now, enc_data,
                ),
            )
            self._db.conn.commit()
        except Exception as e:
            logger.debug("Trend cache store failed: %s", e)

    def age_contextualize(self, trend: TrendResult) -> str:
        """Compare observed trend rate to age-expected rate.

        Returns a human-readable interpretation:
        - 'faster than expected for age'
        - 'expected for age'
        - 'better than expected for age'
        Or empty string if no age-context data exists.
        """
        expected = AGE_EXPECTED_CHANGES.get(trend.canonical_name)
        if not expected:
            return ""

        # Special case: PSA velocity
        if "concern_velocity" in expected:
            days = (
                datetime.fromisoformat(trend.last_date).date()
                - datetime.fromisoformat(trend.first_date).date()
            ).days
            if days > 0:
                years = days / 365.25
                velocity = (trend.last_value - trend.first_value) / years
                threshold = expected["concern_velocity"]
                if velocity > threshold:
                    return (
                        f"concerning velocity "
                        f"({velocity:.2f}/yr > {threshold}/yr)"
                    )
                return "velocity within normal range"

        # Standard: compare % change per decade to expected
        days = (
            datetime.fromisoformat(trend.last_date).date()
            - datetime.fromisoformat(trend.first_date).date()
        ).days
        if days < 30:
            return ""

        decades = days / 3652.5
        if decades < 0.1:
            return ""

        observed_per_decade = trend.pct_change / decades
        threshold = expected.get("concern_threshold_pct_decade")

        if threshold is not None:
            expected_dir = expected.get("direction", "")
            if expected_dir == "decreasing":
                if observed_per_decade < threshold:
                    return f"faster decline than expected ({expected['note']})"
                if observed_per_decade > 0:
                    return "improving (better than expected)"
                return "expected for age"
            if expected_dir == "increasing":
                if observed_per_decade > threshold:
                    return f"faster rise than expected ({expected['note']})"
                if observed_per_decade < 0:
                    return "improving (better than expected)"
                return "expected for age"

        return ""

    def format_trend(self, trend: TrendResult) -> str:
        """Format trend for Telegram display."""
        arrow = {"increasing": "↑", "decreasing": "↓", "stable": "→"}
        symbol = arrow.get(trend.direction, "→")
        base = (
            f"{symbol} {trend.test_name}: "
            f"{trend.first_value} → {trend.last_value} "
            f"({trend.pct_change:+.1f}%) over {trend.data_points} results "
            f"({trend.first_date} to {trend.last_date})"
        )
        if trend.age_context:
            base += f" — {trend.age_context}"
        return base
