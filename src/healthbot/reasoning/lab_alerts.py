"""Lab alerts engine — proactive notifications for clinically significant lab events.

Alert types:
  - Critical value (immediate clinical concern)
  - Rapid change (>20% change in 6 months)
  - Trend concern (persistent adverse direction)
  - Threshold crossing (crossed reference range boundary)
  - Derived marker alert (from derived_markers.py)

All logic is deterministic — no LLM. Follows the pattern from triage.py.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from healthbot.data.db import HealthDB
from healthbot.reasoning.reference_ranges import get_range

logger = logging.getLogger("healthbot")


@dataclass
class LabAlert:
    """A single lab alert."""

    alert_type: str    # "critical", "rapid_change", "trend_concern", etc.
    severity: str      # "critical", "high", "medium", "low"
    test_name: str
    canonical_name: str
    message: str
    value: float | str | None = None
    unit: str = ""
    previous_value: float | str | None = None
    change_pct: float | None = None
    date: str = ""


@dataclass
class AlertReport:
    """All alerts from a scan."""

    alerts: list[LabAlert] = field(default_factory=list)
    scanned_at: str = ""

    @property
    def has_alerts(self) -> bool:
        return bool(self.alerts)

    @property
    def critical_count(self) -> int:
        return sum(1 for a in self.alerts if a.severity == "critical")

    @property
    def high_count(self) -> int:
        return sum(1 for a in self.alerts if a.severity == "high")


# Critical thresholds that require immediate attention
CRITICAL_THRESHOLDS: dict[str, dict[str, float]] = {
    "glucose": {"critical_low": 40, "critical_high": 500},
    "potassium": {"critical_low": 2.5, "critical_high": 6.5},
    "sodium": {"critical_low": 120, "critical_high": 160},
    "hemoglobin": {"critical_low": 5.0, "critical_high": 20.0},
    "platelets": {"critical_low": 20, "critical_high": 1000},
    "wbc": {"critical_low": 1.0, "critical_high": 50.0},
    "calcium": {"critical_low": 6.0, "critical_high": 14.0},
    "inr": {"critical_high": 5.0},
    "troponin": {"critical_high": 0.04},  # ng/mL conventional; see unit check below
    "creatinine": {"critical_high": 10.0},
    "hba1c": {"critical_high": 14.0},
    "bilirubin": {"critical_high": 15.0},
    "alt": {"critical_high": 1000},
    "ast": {"critical_high": 1000},
}

# Tests where rapid change is clinically concerning
RAPID_CHANGE_TESTS: dict[str, dict] = {
    "creatinine": {"threshold_pct": 25, "severity": "high", "concern": "acute kidney injury"},
    "egfr": {"threshold_pct": 20, "severity": "high", "concern": "rapid renal decline"},
    "hemoglobin": {"threshold_pct": 20, "severity": "high", "concern": "bleeding or hemolysis"},
    "platelets": {"threshold_pct": 30, "severity": "high", "concern": "thrombocytopenia risk"},
    "wbc": {"threshold_pct": 40, "severity": "medium", "concern": "immune response change"},
    "alt": {"threshold_pct": 50, "severity": "high", "concern": "acute liver injury"},
    "ast": {"threshold_pct": 50, "severity": "high", "concern": "acute liver injury"},
    "tsh": {"threshold_pct": 50, "severity": "medium", "concern": "thyroid function change"},
    "hba1c": {"threshold_pct": 15, "severity": "medium", "concern": "glycemic control change"},
    "ldl": {"threshold_pct": 25, "severity": "medium", "concern": "lipid management change"},
    "ferritin": {"threshold_pct": 40, "severity": "medium", "concern": "iron status change"},
    "potassium": {"threshold_pct": 15, "severity": "high", "concern": "electrolyte instability"},
    "sodium": {"threshold_pct": 5, "severity": "high", "concern": "electrolyte instability"},
}


class LabAlertEngine:
    """Scan for clinically significant lab events."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def scan(
        self,
        user_id: int | None = None,
        months: int = 6,
        sex: str | None = None,
        age: int | None = None,
    ) -> AlertReport:
        """Run all alert checks and return combined report."""
        report = AlertReport(scanned_at=datetime.now().isoformat())

        try:
            latest = self._get_latest_labs(user_id)
            if not latest:
                return report

            # 1. Critical value alerts
            report.alerts.extend(self._check_critical_values(latest))

            # 2. Rapid change alerts
            previous = self._get_previous_labs(user_id, months)
            if previous:
                report.alerts.extend(
                    self._check_rapid_changes(latest, previous),
                )

            # 3. Threshold crossing alerts
            if previous:
                report.alerts.extend(
                    self._check_threshold_crossings(latest, previous, sex, age),
                )

            # 4. Derived marker alerts
            report.alerts.extend(self._check_derived_markers(user_id))

            # 5. Qualitative change alerts
            if previous:
                report.alerts.extend(
                    self._check_qualitative_changes(latest, previous),
                )

        except Exception as e:
            logger.warning("Lab alert scan failed: %s", e)

        # Sort: critical first, then high, then medium
        severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        report.alerts.sort(key=lambda a: severity_order.get(a.severity, 99))

        return report

    def _get_latest_labs(self, user_id: int | None) -> dict[str, dict]:
        """Get most recent observation for each canonical name."""
        obs = self._db.query_observations(
            record_type="lab_result", limit=200, user_id=user_id,
        )
        latest: dict[str, dict] = {}
        for o in obs:
            name = (o.get("canonical_name") or "").lower()
            if name and name not in latest:
                latest[name] = o
        return latest

    def _get_previous_labs(
        self, user_id: int | None, months: int,
    ) -> dict[str, dict]:
        """Get previous (second-most-recent) value for each test within window."""
        cutoff = (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")
        obs = self._db.query_observations(
            record_type="lab_result", start_date=cutoff, limit=500,
            user_id=user_id,
        )
        # Group by canonical_name, keep second entry (first is latest)
        seen: dict[str, int] = {}
        previous: dict[str, dict] = {}
        for o in obs:
            name = (o.get("canonical_name") or "").lower()
            if not name:
                continue
            seen[name] = seen.get(name, 0) + 1
            if seen[name] == 2:
                previous[name] = o
        return previous

    def _check_critical_values(
        self, latest: dict[str, dict],
    ) -> list[LabAlert]:
        """Check for values in critical range."""
        alerts = []
        for name, obs in latest.items():
            if name not in CRITICAL_THRESHOLDS:
                continue
            val = self._to_float(obs.get("value"))
            if val is None:
                continue

            thresholds = dict(CRITICAL_THRESHOLDS[name])
            unit = (obs.get("unit") or "").lower().strip()

            # Unit-aware troponin: hs-troponin uses ng/L (threshold 52)
            if name == "troponin" and unit in ("ng/l", "pg/ml"):
                thresholds = {"critical_high": 52}

            # Sanity check: if value is wildly out of range for the unit,
            # skip the alert (likely a unit mismatch)
            if name == "glucose" and unit in ("mmol/l", "mmol"):
                val = val * 18.0182  # Convert to mg/dL for comparison

            if "critical_low" in thresholds and val <= thresholds["critical_low"]:
                alerts.append(LabAlert(
                    alert_type="critical",
                    severity="critical",
                    test_name=obs.get("test_name", name),
                    canonical_name=name,
                    message=f"CRITICAL LOW: {name} = {val} {obs.get('unit', '')} "
                            f"(critical threshold: {thresholds['critical_low']})",
                    value=val,
                    unit=obs.get("unit", ""),
                    date=obs.get("date_effective", ""),
                ))
            elif "critical_high" in thresholds and val >= thresholds["critical_high"]:
                alerts.append(LabAlert(
                    alert_type="critical",
                    severity="critical",
                    test_name=obs.get("test_name", name),
                    canonical_name=name,
                    message=f"CRITICAL HIGH: {name} = {val} {obs.get('unit', '')} "
                            f"(critical threshold: {thresholds['critical_high']})",
                    value=val,
                    unit=obs.get("unit", ""),
                    date=obs.get("date_effective", ""),
                ))
        return alerts

    def _check_rapid_changes(
        self,
        latest: dict[str, dict],
        previous: dict[str, dict],
    ) -> list[LabAlert]:
        """Check for rapid changes between most recent panels."""
        alerts = []
        for name, config in RAPID_CHANGE_TESTS.items():
            curr_obs = latest.get(name)
            prev_obs = previous.get(name)
            if not curr_obs or not prev_obs:
                continue

            curr_val = self._to_float(curr_obs.get("value"))
            prev_val = self._to_float(prev_obs.get("value"))
            if curr_val is None or prev_val is None:
                continue

            # When previous value is zero, percentage change is undefined.
            # A jump from 0 to any positive value is clinically significant,
            # so flag it using absolute delta instead.
            if prev_val == 0:
                if curr_val != 0:
                    direction = "increased" if curr_val > 0 else "decreased"
                    alerts.append(LabAlert(
                        alert_type="rapid_change",
                        severity=config["severity"],
                        test_name=curr_obs.get("test_name", name),
                        canonical_name=name,
                        message=f"Rapid change: {name} {direction} from zero "
                                f"(0 -> {curr_val}) — {config['concern']}",
                        value=curr_val,
                        unit=curr_obs.get("unit", ""),
                        previous_value=prev_val,
                        change_pct=None,
                        date=curr_obs.get("date_effective", ""),
                    ))
                continue

            change_pct = abs((curr_val - prev_val) / prev_val * 100)
            if change_pct >= config["threshold_pct"]:
                direction = "increased" if curr_val > prev_val else "decreased"
                alerts.append(LabAlert(
                    alert_type="rapid_change",
                    severity=config["severity"],
                    test_name=curr_obs.get("test_name", name),
                    canonical_name=name,
                    message=f"Rapid change: {name} {direction} {change_pct:.0f}% "
                            f"({prev_val} -> {curr_val}) — {config['concern']}",
                    value=curr_val,
                    unit=curr_obs.get("unit", ""),
                    previous_value=prev_val,
                    change_pct=round(change_pct, 1),
                    date=curr_obs.get("date_effective", ""),
                ))
        return alerts

    def _check_threshold_crossings(
        self,
        latest: dict[str, dict],
        previous: dict[str, dict],
        sex: str | None,
        age: int | None,
    ) -> list[LabAlert]:
        """Detect when a test crosses a reference range boundary."""
        alerts = []
        for name in latest:
            if name not in previous:
                continue

            curr_val = self._to_float(latest[name].get("value"))
            prev_val = self._to_float(previous[name].get("value"))
            if curr_val is None or prev_val is None:
                continue

            ref = get_range(name, sex=sex, age=age)
            if not ref:
                continue

            low = ref.get("low")
            high = ref.get("high")

            # Check if crossed from in-range to out-of-range
            prev_in = self._in_range(prev_val, low, high)
            curr_in = self._in_range(curr_val, low, high)

            if prev_in and not curr_in:
                direction = "above" if high is not None and curr_val > high else "below"
                alerts.append(LabAlert(
                    alert_type="threshold_crossing",
                    severity="medium",
                    test_name=latest[name].get("test_name", name),
                    canonical_name=name,
                    message=f"Threshold crossing: {name} moved {direction} reference range "
                            f"({prev_val} -> {curr_val}, range {low}-{high})",
                    value=curr_val,
                    unit=latest[name].get("unit", ""),
                    previous_value=prev_val,
                    date=latest[name].get("date_effective", ""),
                ))
        return alerts

    def _check_derived_markers(
        self, user_id: int | None,
    ) -> list[LabAlert]:
        """Check derived markers for concerning values."""
        try:
            from healthbot.reasoning.derived_markers import DerivedMarkerEngine
            engine = DerivedMarkerEngine(self._db)
            report = engine.compute_all(user_id=user_id)
        except Exception:
            return []

        alerts = []
        for m in report.markers:
            if m.interpretation in ("elevated", "high", "low"):
                severity = "high" if m.interpretation == "high" else "medium"
                alerts.append(LabAlert(
                    alert_type="derived_marker",
                    severity=severity,
                    test_name=m.name,
                    canonical_name=m.name.lower().replace(" ", "_").replace("/", "_"),
                    message=f"Derived marker alert: {m.name} = {m.value} {m.unit} "
                            f"({m.interpretation}) — {m.clinical_note}",
                    value=m.value,
                    unit=m.unit,
                ))
        return alerts

    # Qualitative values that indicate an abnormal/positive result
    _ABNORMAL_QUALITATIVE = frozenset({
        "detected", "positive", "reactive", "mutation detected",
        "heterozygous", "homozygous",
    })

    def _check_qualitative_changes(
        self,
        latest: dict[str, dict],
        previous: dict[str, dict],
    ) -> list[LabAlert]:
        """Check for clinically significant qualitative value changes.

        Fires when a string-valued test flips to an abnormal result
        (Detected, Positive, Reactive, Mutation Detected, etc.).
        Skips numeric values — those are handled by other checks.
        """
        alerts = []
        for name in latest:
            if name not in previous:
                continue

            curr_val = latest[name].get("value")
            prev_val = previous[name].get("value")

            # Skip if either value is numeric (handled by other checks)
            if self._to_float(curr_val) is not None:
                continue
            if self._to_float(prev_val) is not None:
                continue

            # Both must be non-empty strings
            if not isinstance(curr_val, str) or not isinstance(prev_val, str):
                continue

            curr_lower = curr_val.strip().lower()
            prev_lower = prev_val.strip().lower()

            if curr_lower == prev_lower:
                continue

            # Alert when the new value is an abnormal/positive result
            if curr_lower in self._ABNORMAL_QUALITATIVE:
                alerts.append(LabAlert(
                    alert_type="qualitative_change",
                    severity="high",
                    test_name=latest[name].get("test_name", name),
                    canonical_name=name,
                    message=(
                        f"Qualitative change: {name} changed from "
                        f'"{prev_val}" to "{curr_val}"'
                    ),
                    value=curr_val,
                    unit=latest[name].get("unit", ""),
                    previous_value=prev_val,
                    date=latest[name].get("date_effective", ""),
                ))
        return alerts

    @staticmethod
    def _in_range(val: float, low: float | None, high: float | None) -> bool:
        if low is not None and val < low:
            return False
        if high is not None and val > high:
            return False
        return True

    @staticmethod
    def _to_float(val) -> float | None:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def format_alerts(self, report: AlertReport) -> str:
        """Format alerts for display."""
        if not report.alerts:
            return "No lab alerts."

        icons = {"critical": "!!!", "high": "!!", "medium": "!", "low": "~"}
        lines = [
            "LAB ALERTS",
            f"Scanned: {report.scanned_at[:10] if report.scanned_at else 'now'}",
            "-" * 40,
        ]

        for a in report.alerts:
            icon = icons.get(a.severity, "?")
            lines.append(f"  [{icon}] [{a.alert_type.upper()}] {a.message}")

        if report.critical_count:
            lines.append("")
            lines.append(
                f"  {report.critical_count} CRITICAL alert(s) — immediate attention needed"
            )

        return "\n".join(lines)
