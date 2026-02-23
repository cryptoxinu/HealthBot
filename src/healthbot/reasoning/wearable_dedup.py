"""Cross-source wearable data deduplication.

Compares metrics from WHOOP/Oura (wearable_daily) against Apple Health
(observations) to detect overlapping data on the same dates. Reports
conflicts and can prefer one source over another.

All analysis is deterministic — no LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

logger = logging.getLogger("healthbot")

# Mapping: Apple Health canonical_name → wearable_daily field
METRIC_OVERLAP: dict[str, str] = {
    "resting_heart_rate": "rhr",
    "hrv": "hrv",
    "heart_rate_variability": "hrv",
    "spo2": "spo2",
    "blood_oxygen": "spo2",
    "respiratory_rate": "resp_rate",
    "sleep_duration": "sleep_duration_min",
    "body_temperature": "skin_temp",
    "active_calories": "calories",
}

# Tolerance for considering values "the same" (within this % difference)
VALUE_TOLERANCE_PCT = 15.0


@dataclass
class DupEntry:
    """A single duplicate finding between two sources."""

    date: str
    metric: str
    apple_value: float
    wearable_value: float
    wearable_provider: str
    pct_diff: float
    conflict: bool  # True if values differ significantly


@dataclass
class DedupReport:
    """Results from a cross-source dedup check."""

    checked_dates: int = 0
    duplicates_found: int = 0
    conflicts: int = 0
    entries: list[DupEntry] = field(default_factory=list)

    def summary(self) -> str:
        if not self.entries:
            return f"Dedup: checked {self.checked_dates} dates, no overlaps found."
        parts = [
            f"Dedup: {self.duplicates_found} overlapping metric(s) "
            f"across {self.checked_dates} dates",
        ]
        if self.conflicts:
            parts.append(f"  ({self.conflicts} with significant value differences)")
        for e in self.entries:
            status = "CONFLICT" if e.conflict else "match"
            parts.append(
                f"  {e.date} {e.metric}: "
                f"Apple={e.apple_value:.1f}, "
                f"{e.wearable_provider}={e.wearable_value:.1f} "
                f"({e.pct_diff:+.1f}%) [{status}]"
            )
        return "\n".join(parts)


class WearableDedup:
    """Cross-reference WHOOP/Oura data against Apple Health observations."""

    def __init__(self, db: object) -> None:
        self._db = db

    def check(
        self,
        days: int = 30,
        user_id: int | None = None,
        provider: str = "whoop",
    ) -> DedupReport:
        """Compare wearable_daily metrics against Apple Health observations.

        Args:
            days: Number of days to check.
            user_id: User ID filter.
            provider: Wearable provider to compare ('whoop' or 'oura').

        Returns:
            DedupReport with duplicate findings.
        """
        report = DedupReport()
        cutoff = (date.today() - timedelta(days=days)).isoformat()

        # Get wearable data
        wearable_rows = self._db.query_wearable_daily(
            start_date=cutoff, provider=provider, limit=days,
            user_id=user_id,
        )
        if not wearable_rows:
            return report

        # Index wearable data by date
        wearable_by_date: dict[str, dict] = {}
        for row in wearable_rows:
            d = row.get("_date") or row.get("date", "")
            if isinstance(d, date):
                d = d.isoformat()
            wearable_by_date[d] = row

        report.checked_dates = len(wearable_by_date)

        # Check each overlapping metric against Apple Health observations
        for apple_name, wearable_field in METRIC_OVERLAP.items():
            try:
                obs = self._db.query_observations(
                    canonical_name=apple_name,
                    start_date=cutoff,
                    record_type="vital_sign",
                    limit=days * 2,
                    user_id=user_id,
                )
            except (AttributeError, TypeError):
                # DB doesn't have query_observations (might be CleanDB)
                continue

            if not obs:
                continue

            for ob in obs:
                ob_date = ob.get("_meta", {}).get(
                    "date_effective", ob.get("date_effective", ""),
                )
                if not ob_date:
                    continue
                # Match date (Apple Health may have time component)
                ob_date_str = ob_date[:10]

                wearable_day = wearable_by_date.get(ob_date_str)
                if not wearable_day:
                    continue

                # Extract values
                apple_val = ob.get("value")
                wearable_val = wearable_day.get(wearable_field)
                if apple_val is None or wearable_val is None:
                    continue

                try:
                    apple_float = float(apple_val)
                    wearable_float = float(wearable_val)
                except (ValueError, TypeError):
                    continue

                # Sleep duration: Apple stores minutes, WHOOP stores minutes
                # HRV: both in ms (WHOOP is rmssd_milli)
                # Calculate % difference
                avg = (apple_float + wearable_float) / 2
                if avg == 0:
                    continue
                pct_diff = ((wearable_float - apple_float) / avg) * 100
                is_conflict = abs(pct_diff) > VALUE_TOLERANCE_PCT

                entry = DupEntry(
                    date=ob_date_str,
                    metric=apple_name,
                    apple_value=apple_float,
                    wearable_value=wearable_float,
                    wearable_provider=provider,
                    pct_diff=pct_diff,
                    conflict=is_conflict,
                )
                report.entries.append(entry)
                report.duplicates_found += 1
                if is_conflict:
                    report.conflicts += 1

        if report.entries:
            logger.info(
                "Wearable dedup (%s vs Apple Health): %d overlaps, %d conflicts",
                provider, report.duplicates_found, report.conflicts,
            )
            for e in report.entries:
                if e.conflict:
                    logger.warning(
                        "Dedup conflict %s %s: Apple=%.1f, %s=%.1f (%.1f%%)",
                        e.date, e.metric, e.apple_value,
                        e.wearable_provider, e.wearable_value, e.pct_diff,
                    )

        return report
