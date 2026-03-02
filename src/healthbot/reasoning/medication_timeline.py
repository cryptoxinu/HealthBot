"""Temporal medication tracking — week numbers, dose changes, linked metrics.

Provides MedicationTimeline for computing how long a user has been on each
active medication and correlating medication start dates with health metrics
(weight for GLP-1 agonists, lipids for statins, etc.).
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime

from healthbot.reasoning.interaction_kb import SUBSTANCE_ALIASES

logger = logging.getLogger("healthbot")

# Map canonical substance keys to metrics they are commonly linked to
_MED_METRIC_MAP: dict[str, list[str]] = {
    "glp1_agonist": ["weight", "body_weight", "a1c", "hba1c", "fasting_glucose"],
    "semaglutide": ["weight", "body_weight", "a1c", "hba1c", "fasting_glucose"],
    "tirzepatide": ["weight", "body_weight", "a1c", "hba1c", "fasting_glucose"],
    "retatrutide": ["weight", "body_weight", "a1c", "hba1c", "fasting_glucose"],
    "statin": ["ldl", "total_cholesterol", "hdl", "triglycerides", "ldl_cholesterol"],
    "atorvastatin": ["ldl", "total_cholesterol", "hdl", "triglycerides"],
    "rosuvastatin": ["ldl", "total_cholesterol", "hdl", "triglycerides"],
    "metformin": ["a1c", "hba1c", "fasting_glucose", "weight"],
    "levothyroxine": ["tsh", "free_t4", "free_t3"],
    "ssri": ["mood_score"],
    "testosterone": ["total_testosterone", "free_testosterone", "shbg"],
    "rapamycin": ["wbc", "white_blood_cell"],
    "finasteride": ["psa", "dht"],
}


@dataclass
class MetricChange:
    """A change in a health metric correlated with a medication."""

    metric: str
    start_value: float | None = None
    current_value: float | None = None
    change: str = ""
    unit: str = ""


@dataclass
class MedStatus:
    """Current status of an active medication."""

    name: str
    dose: str = ""
    start_date: str = ""
    week_number: int = 0
    linked_metrics: list[MetricChange] = field(default_factory=list)


class MedicationTimeline:
    """Temporal medication tracking and metric correlation."""

    def __init__(self, mgr) -> None:
        self._mgr = mgr

    def get_all_active_timelines(self, user_id: int) -> list[dict]:
        """Get timeline summaries for all active medications.

        Returns list of dicts with: name, dose, start_date, week_number,
        linked_metrics.
        """
        from healthbot.llm.conversation_routing import get_clean_db

        clean_db = get_clean_db(self._mgr)
        if not clean_db:
            return []

        try:
            meds = clean_db.get_medications(status="active")
            results: list[dict] = []

            for med in meds:
                name = med.get("name", "")
                if not name:
                    continue

                start_date = med.get("start_date", "")
                week_number = self._weeks_since(start_date) if start_date else 0

                linked = self._get_linked_metrics(
                    clean_db, name, start_date,
                )

                results.append({
                    "name": name,
                    "dose": med.get("dose", ""),
                    "start_date": start_date,
                    "week_number": week_number,
                    "linked_metrics": linked,
                })

            return results
        except Exception as exc:
            logger.debug("get_all_active_timelines failed: %s", exc)
            return []
        finally:
            clean_db.close()

    def get_med_status(self, user_id: int, med_name: str) -> MedStatus | None:
        """Get detailed status for a single medication."""
        from healthbot.llm.conversation_routing import get_clean_db

        clean_db = get_clean_db(self._mgr)
        if not clean_db:
            return None

        try:
            meds = clean_db.get_medications(status="active")
            target = None
            for med in meds:
                if med.get("name", "").lower() == med_name.lower():
                    target = med
                    break

            if not target:
                return None

            start_date = target.get("start_date", "")
            week_number = self._weeks_since(start_date) if start_date else 0

            linked_raw = self._get_linked_metrics(
                clean_db, target["name"], start_date,
            )
            linked = [
                MetricChange(
                    metric=m["metric"],
                    start_value=m.get("start_value"),
                    current_value=m.get("current_value"),
                    change=m.get("change", ""),
                    unit=m.get("unit", ""),
                )
                for m in linked_raw
            ]

            return MedStatus(
                name=target["name"],
                dose=target.get("dose", ""),
                start_date=start_date,
                week_number=week_number,
                linked_metrics=linked,
            )
        except Exception as exc:
            logger.debug("get_med_status failed: %s", exc)
            return None
        finally:
            clean_db.close()

    def _get_linked_metrics(
        self, clean_db, med_name: str, start_date: str,
    ) -> list[dict]:
        """Find health metrics correlated with this medication."""
        canonical = SUBSTANCE_ALIASES.get(
            med_name.lower().replace(" ", "_").replace("-", "_"),
            med_name.lower().replace(" ", "_").replace("-", "_"),
        )

        metric_names = _MED_METRIC_MAP.get(canonical, [])
        if not metric_names:
            # Try partial match
            for key, metrics in _MED_METRIC_MAP.items():
                if key in canonical or canonical in key:
                    metric_names = metrics
                    break

        if not metric_names or not start_date:
            return []

        results: list[dict] = []
        for metric_name in metric_names[:4]:  # Limit to 4 metrics
            change = self._compute_metric_change(
                clean_db, metric_name, start_date,
            )
            if change:
                results.append(change)

        return results

    def _compute_metric_change(
        self, clean_db, metric_name: str, start_date: str,
    ) -> dict | None:
        """Compute change in a metric since a medication start date."""
        try:
            # Get earliest value near start date
            labs = clean_db.get_lab_results(
                test_name=metric_name,
                start_date=self._date_minus_days(start_date, 30),
                end_date=self._date_plus_days(start_date, 14),
                limit=1,
            )
            start_value = None
            unit = ""
            if labs:
                start_value = self._parse_numeric(labs[0].get("value", ""))
                unit = labs[0].get("unit", "")

            # Get most recent value
            recent = clean_db.get_lab_results(
                test_name=metric_name,
                limit=1,
            )
            current_value = None
            if recent:
                current_value = self._parse_numeric(recent[0].get("value", ""))
                if not unit:
                    unit = recent[0].get("unit", "")

            if start_value is None and current_value is None:
                return None

            # Compute change
            change_str = ""
            if start_value is not None and current_value is not None:
                diff = current_value - start_value
                sign = "+" if diff >= 0 else ""
                change_str = f"{sign}{diff:.1f}"

            return {
                "metric": metric_name.replace("_", " ").title(),
                "start_value": start_value,
                "current_value": current_value,
                "change": change_str,
                "unit": unit,
            }
        except Exception as exc:
            logger.debug("Metric change computation failed for %s: %s", metric_name, exc)
            return None

    @staticmethod
    def _weeks_since(start_date: str) -> int:
        """Calculate whole weeks since a start date."""
        try:
            dt = datetime.fromisoformat(start_date)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            delta = datetime.now(UTC) - dt
            return max(0, delta.days // 7)
        except Exception:
            return 0

    @staticmethod
    def _parse_numeric(value: str | float | int) -> float | None:
        """Parse a numeric value from lab result."""
        if isinstance(value, (int, float)):
            return float(value) if not math.isnan(value) else None
        if not value:
            return None
        try:
            # Strip common prefixes like "<", ">", "~"
            cleaned = value.strip().lstrip("<>~≤≥ ")
            return float(cleaned)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _date_minus_days(date_str: str, days: int) -> str:
        """Subtract days from an ISO date string."""
        try:
            from datetime import timedelta
            dt = datetime.fromisoformat(date_str)
            return (dt - timedelta(days=days)).strftime("%Y-%m-%d")
        except Exception:
            return date_str

    @staticmethod
    def _date_plus_days(date_str: str, days: int) -> str:
        """Add days to an ISO date string."""
        try:
            from datetime import timedelta
            dt = datetime.fromisoformat(date_str)
            return (dt + timedelta(days=days)).strftime("%Y-%m-%d")
        except Exception:
            return date_str
