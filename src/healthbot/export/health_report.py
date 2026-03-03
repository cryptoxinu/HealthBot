"""Periodic health report generation.

Builds weekly/monthly health summaries aggregating all data sources.
All logic is deterministic. No LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from healthbot.data.db import HealthDB
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")


@dataclass
class ReportSection:
    """A section of the health report."""

    title: str
    items: list[str] = field(default_factory=list)


@dataclass
class HealthReport:
    """Complete periodic health report."""

    period: str             # "weekly" or "monthly"
    start_date: str
    end_date: str
    generated_at: str
    sections: list[ReportSection] = field(default_factory=list)
    summary: str = ""


class HealthReportBuilder:
    """Build periodic health reports from all data sources.

    All free-text fields are passed through PhiFirewall.redact() before
    inclusion in the report to prevent PII leakage from Tier 1 data.
    """

    def __init__(self, db: HealthDB, phi_firewall: PhiFirewall | None = None) -> None:
        self._db = db
        self._fw = phi_firewall or PhiFirewall()

    def _safe(self, value: str) -> str:
        """Redact any PHI from a string value before including in report."""
        if not value:
            return value
        return self._fw.redact(str(value))

    def build_weekly(self, user_id: int) -> HealthReport:
        """Build a report for the past 7 days."""
        end = date.today()
        start = end - timedelta(days=7)
        return self._build(user_id, "weekly", start, end)

    def build_monthly(
        self, user_id: int,
        year: int | None = None,
        month: int | None = None,
    ) -> HealthReport:
        """Build a report for the past 30 days (or a specific month)."""
        if year and month:
            start = date(year, month, 1)
            if month == 12:
                end = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                end = date(year, month + 1, 1) - timedelta(days=1)
        else:
            end = date.today()
            start = end - timedelta(days=30)
        return self._build(user_id, "monthly", start, end)

    def _build(
        self, user_id: int, period: str,
        start: date, end: date,
    ) -> HealthReport:
        """Build the full report."""
        report = HealthReport(
            period=period,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            generated_at=datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        )

        start_str = start.isoformat()
        end_str = end.isoformat()

        # 1. Lab results summary
        lab_section = self._build_lab_section(user_id, start_str, end_str)
        if lab_section.items:
            report.sections.append(lab_section)

        # 2. Wearable summary
        wearable_section = self._build_wearable_section(
            user_id, start_str, end_str,
        )
        if wearable_section.items:
            report.sections.append(wearable_section)

        # 3. Medication changes
        med_section = self._build_medication_section(user_id)
        if med_section.items:
            report.sections.append(med_section)

        # 4. Workout summary
        workout_section = self._build_workout_section(
            user_id, start_str, end_str,
        )
        if workout_section.items:
            report.sections.append(workout_section)

        # 5. Symptoms logged
        symptom_section = self._build_symptom_section(
            user_id, start_str, end_str,
        )
        if symptom_section.items:
            report.sections.append(symptom_section)

        # 5. Goal progress
        goal_section = self._build_goal_section(user_id)
        if goal_section.items:
            report.sections.append(goal_section)

        # 6. Concerns / action items
        concern_section = self._build_concern_section(user_id)
        if concern_section.items:
            report.sections.append(concern_section)

        # Build summary line
        section_count = len(report.sections)
        report.summary = (
            f"{period.title()} report: {section_count} sections, "
            f"{start.isoformat()} to {end.isoformat()}"
        )

        return report

    def _build_lab_section(
        self, user_id: int, start: str, end: str,
    ) -> ReportSection:
        """Summarize lab results in the period."""
        section = ReportSection(title="Lab Results")
        rows = self._db.query_observations(
            record_type="lab_result",
            start_date=start,
            end_date=end,
            limit=100,
            user_id=user_id,
        )
        if not rows:
            return section

        # Group by date
        by_date: dict[str, list[dict]] = {}
        for row in rows:
            dt = row.get("date_collected", "")
            if dt:
                by_date.setdefault(dt, []).append(row)

        for dt in sorted(by_date.keys()):
            labs = by_date[dt]
            flagged = [
                r for r in labs
                if r.get("flag") or r.get("triage_level") in (
                    "watch", "urgent", "critical",
                )
            ]
            total = len(labs)
            flag_count = len(flagged)

            if flag_count:
                flag_names = ", ".join(
                    self._safe(r.get("test_name", "?")) for r in flagged[:5]
                )
                section.items.append(
                    f"{dt}: {total} tests, {flag_count} flagged ({flag_names})",
                )
            else:
                section.items.append(f"{dt}: {total} tests, all normal")

        return section

    def _build_wearable_section(
        self, user_id: int, start: str, end: str,
    ) -> ReportSection:
        """Summarize wearable metrics in the period."""
        section = ReportSection(title="Wearable Summary")
        rows = self._db.query_wearable_daily(
            start_date=start, end_date=end,
            limit=365, user_id=user_id,
        )
        if not rows:
            return section

        # Compute averages for key metrics
        metrics = {
            "hrv": [], "rhr": [], "sleep_score": [],
            "recovery_score": [], "strain": [],
            "sleep_duration_min": [],
        }
        for row in rows:
            for key in metrics:
                val = row.get(key)
                if val is not None:
                    try:
                        metrics[key].append(float(val))
                    except (ValueError, TypeError):
                        pass

        display_names = {
            "hrv": "HRV", "rhr": "RHR",
            "sleep_score": "Sleep Score",
            "recovery_score": "Recovery",
            "strain": "Strain",
            "sleep_duration_min": "Sleep Duration",
        }

        section.items.append(f"{len(rows)} days of wearable data")
        for key, values in metrics.items():
            if values:
                avg = sum(values) / len(values)
                low = min(values)
                high = max(values)
                name = display_names.get(key, key)
                if key == "sleep_duration_min":
                    section.items.append(
                        f"  {name}: avg {avg / 60:.1f}h "
                        f"(range {low / 60:.1f}-{high / 60:.1f}h)",
                    )
                else:
                    section.items.append(
                        f"  {name}: avg {avg:.0f} "
                        f"(range {low:.0f}-{high:.0f})",
                    )

        return section

    def _build_workout_section(
        self, user_id: int, start: str, end: str,
    ) -> ReportSection:
        """Summarize workouts in the period."""
        section = ReportSection(title="Workouts")
        rows = self._db.query_workouts(
            start_after=start, user_id=user_id, limit=200,
        )
        # Filter to end date
        rows = [
            r for r in rows
            if (r.get("_start_date", "") or "") <= end
        ]
        if not rows:
            return section

        # Group by sport type
        by_sport: dict[str, list[dict]] = {}
        for row in rows:
            sport = row.get("sport_type", row.get("_sport_type", "other"))
            by_sport.setdefault(sport, []).append(row)

        total_mins = sum(
            float(r.get("duration_minutes", 0) or 0) for r in rows
        )
        total_cal = sum(
            float(r.get("calories_burned", 0) or 0) for r in rows
        )
        section.items.append(
            f"{len(rows)} workouts, "
            f"{total_mins / 60:.1f}h total, "
            f"{total_cal:.0f} cal burned"
        )

        for sport, entries in sorted(
            by_sport.items(),
            key=lambda x: len(x[1]),
            reverse=True,
        ):
            label = sport.replace("_", " ").title()
            mins = sum(float(e.get("duration_minutes", 0) or 0) for e in entries)
            section.items.append(
                f"  {label}: {len(entries)}x, {mins:.0f}min"
            )

        return section

    def _build_medication_section(self, user_id: int) -> ReportSection:
        """List active medications."""
        section = ReportSection(title="Active Medications")
        meds = self._db.get_active_medications(user_id=user_id)
        for med in meds:
            name = self._safe(med.get("name", ""))
            dose = self._safe(med.get("dose", ""))
            unit = self._safe(med.get("unit", ""))
            freq = self._safe(med.get("frequency", ""))
            section.items.append(f"{name} {dose} {unit} {freq}".strip())
        return section

    def _build_symptom_section(
        self, user_id: int, start: str, end: str,
    ) -> ReportSection:
        """Summarize logged symptoms in the period."""
        section = ReportSection(title="Logged Symptoms")
        rows = self._db.query_observations(
            record_type="user_event",
            start_date=start,
            end_date=end,
            limit=50,
            user_id=user_id,
        )
        categories: dict[str, int] = {}
        for row in rows:
            cat = self._safe(row.get("symptom_category", "general"))
            categories[cat] = categories.get(cat, 0) + 1

        for cat, count in sorted(
            categories.items(), key=lambda x: x[1], reverse=True,
        ):
            section.items.append(f"{cat}: {count} occurrence{'s' if count != 1 else ''}")

        return section

    def _build_goal_section(self, user_id: int) -> ReportSection:
        """Summarize health goal progress."""
        section = ReportSection(title="Health Goals")
        try:
            from healthbot.reasoning.goals import GoalTracker

            tracker = GoalTracker(self._db)
            progress = tracker.check_progress(user_id)
            for p in progress:
                status_icon = {
                    "achieved": "+", "on_track": "~",
                    "off_track": "!", "no_data": "?",
                }.get(p.status, "?")
                section.items.append(
                    f"{status_icon} {self._safe(p.goal.display_name)}: "
                    f"{p.pct_progress:.0f}% ({p.status})",
                )
        except Exception:
            pass
        return section

    def _build_concern_section(self, user_id: int) -> ReportSection:
        """List active concerns and action items."""
        section = ReportSection(title="Action Items")

        # Overdue tests
        try:
            from healthbot.reasoning.overdue import OverdueDetector

            detector = OverdueDetector(self._db)
            overdue = detector.check_overdue(user_id=user_id)
            for item in overdue[:5]:
                section.items.append(
                    f"Overdue: {self._safe(item.test_name)} "
                    f"(last {item.last_date}, {item.days_overdue} days ago)",
                )
        except Exception:
            pass

        # Pending retests
        try:
            from healthbot.reasoning.retest_scheduler import RetestScheduler

            scheduler = RetestScheduler(self._db)
            retests = scheduler.get_pending_retests(user_id=user_id)
            for rt in retests[:5]:
                section.items.append(
                    f"Retest: {self._safe(rt.display_name)} (due in {rt.days_until_due} days)",
                )
        except Exception:
            pass

        return section


def format_report(report: HealthReport) -> str:
    """Format a health report for display."""
    if not report.sections:
        return (
            f"No data found for {report.period} report "
            f"({report.start_date} to {report.end_date}).\n\n"
            "Upload labs, sync wearables, or log symptoms to build reports."
        )

    title = f"{report.period.upper()} HEALTH REPORT"
    lines = [
        title,
        f"{report.start_date} to {report.end_date}",
        "-" * 30,
    ]

    for section in report.sections:
        lines.append(f"\n{section.title}:")
        for item in section.items:
            lines.append(f"  {item}")

    lines.append("\n" + "-" * 30)
    lines.append(f"Generated: {report.generated_at}")

    return "\n".join(lines)
