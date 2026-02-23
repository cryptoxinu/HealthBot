"""Delta engine — "what changed since last time."

Compares the most recent lab panel to the previous one.
All logic is deterministic — no LLM involvement.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from healthbot.data.db import HealthDB
from healthbot.reasoning.reference_ranges import DEFAULT_RANGES


@dataclass
class DeltaItem:
    """Single test change between two panels."""

    canonical_name: str
    test_name: str
    status: str  # "new", "resolved", "improving", "worsening", "stable"
    current_value: float | str | None = None
    previous_value: float | str | None = None
    unit: str = ""
    change_pct: float = 0.0


@dataclass
class DeltaReport:
    """Full delta between two panel dates."""

    current_date: str
    previous_date: str
    items: list[DeltaItem] = field(default_factory=list)


class DeltaEngine:
    """Compute what changed between the two most recent lab panels."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def compute_delta(self, user_id: int | None = None) -> DeltaReport | None:
        """Compare most recent panel to the previous distinct panel date.

        Returns None if fewer than 2 distinct lab dates exist.
        """
        dates = self._get_distinct_lab_dates(limit=10, user_id=user_id)
        if len(dates) < 2:
            return None

        current_date = dates[0]
        previous_date = dates[1]

        current_obs = self._get_labs_by_date(current_date, user_id=user_id)
        previous_obs = self._get_labs_by_date(previous_date, user_id=user_id)

        report = DeltaReport(current_date=current_date, previous_date=previous_date)

        # Build lookup by canonical_name
        prev_map: dict[str, dict] = {}
        for obs in previous_obs:
            cn = obs.get("canonical_name") or obs.get("_meta", {}).get("canonical_name", "")
            if cn:
                prev_map[cn] = obs

        curr_map: dict[str, dict] = {}
        for obs in current_obs:
            cn = obs.get("canonical_name") or obs.get("_meta", {}).get("canonical_name", "")
            if cn:
                curr_map[cn] = obs

        # Compare current vs previous
        all_names = set(curr_map.keys()) | set(prev_map.keys())
        for cn in sorted(all_names):
            curr = curr_map.get(cn)
            prev = prev_map.get(cn)

            if curr and not prev:
                raw = curr.get("value")
                report.items.append(DeltaItem(
                    canonical_name=cn,
                    test_name=curr.get("test_name", cn),
                    status="new",
                    current_value=self._to_float(raw) if self._to_float(raw) is not None else raw,
                    unit=curr.get("unit", ""),
                ))
                continue

            if prev and not curr:
                raw = prev.get("value")
                report.items.append(DeltaItem(
                    canonical_name=cn,
                    test_name=prev.get("test_name", cn),
                    status="resolved",
                    previous_value=self._to_float(raw) if self._to_float(raw) is not None else raw,
                    unit=prev.get("unit", ""),
                ))
                continue

            # Both exist — classify direction
            curr_val = self._to_float(curr.get("value"))
            prev_val = self._to_float(prev.get("value"))

            if curr_val is None or prev_val is None:
                # Qualitative comparison — raw string values
                curr_raw = str(curr.get("value", "")).strip()
                prev_raw = str(prev.get("value", "")).strip()
                if curr_raw and prev_raw:
                    if curr_raw.lower() == prev_raw.lower():
                        status = "stable"
                    else:
                        status = "changed"
                    report.items.append(DeltaItem(
                        canonical_name=cn,
                        test_name=curr.get("test_name", cn),
                        status=status,
                        current_value=curr_raw,
                        previous_value=prev_raw,
                        unit=curr.get("unit", ""),
                        change_pct=0.0,
                    ))
                continue

            change_pct = ((curr_val - prev_val) / prev_val * 100) if prev_val != 0 else 0.0
            status = self._classify_change(cn, curr_val, prev_val, change_pct)

            report.items.append(DeltaItem(
                canonical_name=cn,
                test_name=curr.get("test_name", cn),
                status=status,
                current_value=curr_val,
                previous_value=prev_val,
                unit=curr.get("unit", ""),
                change_pct=round(change_pct, 1),
            ))

        return report

    def _classify_change(
        self, canonical_name: str, curr: float, prev: float, pct: float
    ) -> str:
        """Classify: improving, worsening, or stable.

        "Improving" = moving toward reference range midpoint.
        "Worsening" = moving away from it.
        """
        if abs(pct) < 3.0:
            return "stable"

        ref = DEFAULT_RANGES.get(canonical_name)
        if not ref:
            # No reference range — use absolute change only
            return "stable" if abs(pct) < 5.0 else ("increasing" if curr > prev else "decreasing")

        mid = (ref["low"] + ref["high"]) / 2
        prev_dist = abs(prev - mid)
        curr_dist = abs(curr - mid)

        if curr_dist < prev_dist:
            return "improving"
        elif curr_dist > prev_dist:
            return "worsening"
        return "stable"

    def _get_distinct_lab_dates(
        self, limit: int = 10, user_id: int | None = None,
    ) -> list[str]:
        """Get distinct lab panel dates, most recent first."""
        if user_id:
            rows = self._db.conn.execute(
                "SELECT DISTINCT date_effective FROM observations "
                "WHERE record_type = 'lab_result' AND date_effective IS NOT NULL "
                "AND user_id = ? "
                "ORDER BY date_effective DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
        else:
            rows = self._db.conn.execute(
                "SELECT DISTINCT date_effective FROM observations "
                "WHERE record_type = 'lab_result' AND date_effective IS NOT NULL "
                "ORDER BY date_effective DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [row["date_effective"] for row in rows]

    def _get_labs_by_date(
        self, date_str: str, user_id: int | None = None,
    ) -> list[dict]:
        """Get all lab observations for a specific date."""
        return self._db.query_observations(
            record_type="lab_result",
            start_date=date_str,
            end_date=date_str,
            limit=200,
            user_id=user_id,
        )

    @staticmethod
    def _to_float(val) -> float | None:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def format_delta(self, report: DeltaReport) -> str:
        """Format delta report for display."""
        lines = [
            "WHAT CHANGED",
            f"Comparing {report.current_date} vs {report.previous_date}",
            "-" * 40,
        ]

        if not report.items:
            lines.append("No comparable tests between panels.")
            return "\n".join(lines)

        icons = {
            "improving": "+",
            "worsening": "!",
            "stable": "=",
            "new": "*",
            "resolved": "-",
            "increasing": "^",
            "decreasing": "v",
            "changed": "!",
        }

        for item in report.items:
            icon = icons.get(item.status, "?")

            if item.status == "new":
                lines.append(
                    f"  [{icon}] {item.test_name}: {item.current_value} {item.unit} (NEW)"
                )
            elif item.status == "resolved":
                lines.append(
                    f"  [{icon}] {item.test_name}: was {item.previous_value} {item.unit}"
                    " (not in latest panel)"
                )
            elif item.status == "changed" and isinstance(item.current_value, str):
                # Qualitative change (e.g. "Not Detected" → "Detected")
                lines.append(
                    f"  [{icon}] {item.test_name}: "
                    f"{item.previous_value} -> {item.current_value} (CHANGED)"
                )
            elif item.status == "stable" and isinstance(item.current_value, str):
                # Qualitative stable
                lines.append(
                    f"  [{icon}] {item.test_name}: {item.current_value} (stable)"
                )
            else:
                lines.append(
                    f"  [{icon}] {item.test_name}: "
                    f"{item.previous_value} -> {item.current_value} {item.unit} "
                    f"({item.change_pct:+.1f}%) [{item.status}]"
                )

        return "\n".join(lines)
