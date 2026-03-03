"""Auto-generated weekly/monthly PDF health reports.

Combines HealthReportBuilder data, chart_generator visuals, and fpdf2
rendering into an in-memory PDF. Never writes plaintext to disk.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from io import BytesIO

from fpdf import FPDF

from healthbot.data.db import HealthDB
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")


@dataclass
class PdfReportData:
    """Data assembled for the PDF report."""

    period: str = "weekly"
    start_date: str = ""
    end_date: str = ""
    generated_at: str = ""
    # Sections
    lab_items: list[str] = field(default_factory=list)
    wearable_items: list[str] = field(default_factory=list)
    workout_items: list[str] = field(default_factory=list)
    medication_items: list[str] = field(default_factory=list)
    action_items: list[str] = field(default_factory=list)
    goal_items: list[str] = field(default_factory=list)
    memory_items: list[str] = field(default_factory=list)
    # Charts (PNG bytes)
    dashboard_chart: bytes | None = None
    trend_charts: list[bytes] = field(default_factory=list)


class WeeklyPdfReportGenerator:
    """Generate weekly/monthly PDF health reports in memory.

    All free-text fields are passed through PhiFirewall.redact() before
    inclusion in the PDF to prevent PII leakage from Tier 1 data.
    """

    def __init__(self, db: HealthDB, phi_firewall: PhiFirewall | None = None) -> None:
        self._db = db
        self._fw = phi_firewall or PhiFirewall()

    def _safe(self, value: str) -> str:
        """Redact any PHI from a string value before including in PDF."""
        if not value:
            return value
        return self._fw.redact(str(value))

    def generate_weekly(
        self, user_id: int, days: int = 7,
        memory_items: list[str] | None = None,
    ) -> bytes:
        """Generate a weekly PDF report."""
        end = date.today()
        start = end - timedelta(days=days)
        data = self._gather_data(user_id, "weekly", start, end)
        if memory_items:
            data.memory_items = [self._safe(m) for m in memory_items]
        return self._render_pdf(data)

    def generate_monthly(
        self, user_id: int, days: int = 30,
        memory_items: list[str] | None = None,
    ) -> bytes:
        """Generate a monthly PDF report."""
        end = date.today()
        start = end - timedelta(days=days)
        data = self._gather_data(user_id, "monthly", start, end)
        if memory_items:
            data.memory_items = [self._safe(m) for m in memory_items]
        return self._render_pdf(data)

    def _gather_data(
        self, user_id: int, period: str,
        start: date, end: date,
    ) -> PdfReportData:
        """Gather all data for the report."""
        data = PdfReportData(
            period=period,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            generated_at=datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        )

        start_str = start.isoformat()
        end_str = end.isoformat()

        # Labs
        try:
            rows = self._db.query_observations(
                record_type="lab_result",
                start_date=start_str, end_date=end_str,
                limit=100, user_id=user_id,
            )
            by_date: dict[str, list[dict]] = {}
            for row in rows:
                dt = row.get("date_collected", "")
                if dt:
                    by_date.setdefault(dt, []).append(row)

            for dt in sorted(by_date.keys()):
                labs = by_date[dt]
                flagged = [r for r in labs if r.get("flag")]
                if flagged:
                    names = ", ".join(self._safe(r.get("test_name", "?")) for r in flagged[:5])
                    data.lab_items.append(
                        f"{dt}: {len(labs)} tests, {len(flagged)} flagged ({names})"
                    )
                else:
                    data.lab_items.append(f"{dt}: {len(labs)} tests, all normal")
        except Exception as e:
            logger.debug("PDF report labs: %s", e)

        # Wearables
        try:
            wearables = self._db.query_wearable_daily(
                start_date=start_str, end_date=end_str,
                limit=365, user_id=user_id,
            )
            if wearables:
                metrics = {"hrv": [], "rhr": [], "recovery_score": [], "sleep_score": []}
                for w in wearables:
                    for key in metrics:
                        val = w.get(key)
                        if val is not None:
                            try:
                                metrics[key].append(float(val))
                            except (ValueError, TypeError):
                                pass

                data.wearable_items.append(f"{len(wearables)} days of wearable data")
                names = {
                    "hrv": "HRV", "rhr": "RHR",
                    "recovery_score": "Recovery", "sleep_score": "Sleep",
                }
                for key, vals in metrics.items():
                    if vals:
                        avg = sum(vals) / len(vals)
                        data.wearable_items.append(
                            f"  {names[key]}: avg {avg:.0f} (range {min(vals):.0f}-{max(vals):.0f})"
                        )
        except Exception as e:
            logger.debug("PDF report wearables: %s", e)

        # Workouts
        try:
            workouts = self._db.query_workouts(
                start_after=start_str, user_id=user_id, limit=200,
            )
            workouts = [w for w in workouts if (w.get("_start_date", "") or "") <= end_str]
            if workouts:
                by_sport: dict[str, int] = {}
                total_mins = 0.0
                for w in workouts:
                    sport = self._safe(w.get("sport_type", w.get("_sport_type", "other")))
                    by_sport[sport] = by_sport.get(sport, 0) + 1
                    total_mins += float(w.get("duration_minutes", 0) or 0)

                data.workout_items.append(
                    f"{len(workouts)} workouts, {total_mins / 60:.1f}h total"
                )
                for sport, count in sorted(by_sport.items(), key=lambda x: x[1], reverse=True):
                    label = sport.replace("_", " ").title()
                    data.workout_items.append(f"  {label}: {count}x")
        except Exception as e:
            logger.debug("PDF report workouts: %s", e)

        # Medications
        try:
            meds = self._db.get_active_medications(user_id=user_id)
            for med in meds:
                name = self._safe(med.get("name", ""))
                dose = self._safe(med.get("dose", ""))
                freq = self._safe(med.get("frequency", ""))
                data.medication_items.append(f"{name} {dose} {freq}".strip())
        except Exception as e:
            logger.debug("PDF report meds: %s", e)

        # Action items
        try:
            from healthbot.reasoning.overdue import OverdueDetector
            detector = OverdueDetector(self._db)
            overdue = detector.check_overdue(user_id=user_id)
            for item in overdue[:5]:
                data.action_items.append(
                    f"Overdue: {self._safe(item.test_name)} ({item.days_overdue} days)"
                )
        except Exception as e:
            logger.debug("PDF report overdue: %s", e)

        try:
            from healthbot.reasoning.retest_scheduler import RetestScheduler
            rs = RetestScheduler(self._db)
            retests = rs.get_pending_retests(user_id=user_id)
            for rt in retests[:5]:
                data.action_items.append(
                    f"Retest: {self._safe(rt.display_name)} (due in {rt.days_until_due} days)"
                )
        except Exception as e:
            logger.debug("PDF report retests: %s", e)

        # Goals
        try:
            from healthbot.reasoning.goals import GoalTracker
            gt = GoalTracker(self._db)
            progress = gt.check_progress(user_id)
            for gp in progress:
                icon = "+" if gp.status == "achieved" else "*"
                data.goal_items.append(f"{icon} {self._safe(gp.message)}")
        except Exception as e:
            logger.debug("PDF report goals: %s", e)

        # Generate charts
        try:
            from healthbot.export.chart_generator import dashboard_chart
            from healthbot.reasoning.insights import InsightEngine
            from healthbot.reasoning.trends import TrendAnalyzer

            analyzer = TrendAnalyzer(self._db)
            from healthbot.reasoning.triage import TriageEngine
            engine = InsightEngine(self._db, TriageEngine(), analyzer)
            scores = engine.compute_domain_scores(user_id=user_id)
            chart_bytes = dashboard_chart(scores)
            if chart_bytes:
                data.dashboard_chart = chart_bytes
        except Exception as e:
            logger.debug("PDF report dashboard chart: %s", e)

        return data

    def _render_pdf(self, data: PdfReportData) -> bytes:
        """Render PdfReportData into PDF bytes in memory."""
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=20)

        # Page 1: Executive Summary
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 18)
        title = f"{data.period.title()} Health Report"
        pdf.cell(0, 12, title, new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(
            0, 6,
            f"{data.start_date} to {data.end_date}",
            new_x="LMARGIN", new_y="NEXT", align="C",
        )
        pdf.cell(
            0, 6,
            f"Generated: {data.generated_at}",
            new_x="LMARGIN", new_y="NEXT", align="C",
        )
        pdf.ln(8)

        # Dashboard chart (if available)
        if data.dashboard_chart:
            try:
                img_buf = BytesIO(data.dashboard_chart)
                img_buf.name = "dashboard.png"
                pdf.image(img_buf, x=15, w=180)
                pdf.ln(5)
            except Exception as e:
                logger.debug("PDF chart embed: %s", e)

        # Page 2: Lab Results + Medications
        self._section_header(pdf, "Lab Results")
        if data.lab_items:
            for item in data.lab_items:
                self._body_text(pdf, item)
        else:
            self._body_text(pdf, "No lab results in this period.")
        pdf.ln(4)

        self._section_header(pdf, "Active Medications")
        if data.medication_items:
            for item in data.medication_items:
                self._body_text(pdf, item)
        else:
            self._body_text(pdf, "No active medications.")
        pdf.ln(4)

        # Wearable + Workouts
        if data.wearable_items or data.workout_items:
            self._section_header(pdf, "Wearable & Activity")
            for item in data.wearable_items:
                self._body_text(pdf, item)
            if data.workout_items:
                pdf.ln(2)
                self._body_text(pdf, "Workouts:")
                for item in data.workout_items:
                    self._body_text(pdf, item)
            pdf.ln(4)

        # Goals
        if data.goal_items:
            self._section_header(pdf, "Health Goals")
            for item in data.goal_items:
                self._body_text(pdf, item)
            pdf.ln(4)

        # Memory Summary
        if data.memory_items:
            self._section_header(pdf, "What I Know About You")
            for item in data.memory_items:
                self._body_text(pdf, item)
            pdf.ln(4)

        # Action Items
        self._section_header(pdf, "Action Items")
        if data.action_items:
            for item in data.action_items:
                self._body_text(pdf, item)
        else:
            self._body_text(pdf, "No pending action items.")

        # Footer
        pdf.ln(8)
        pdf.set_font("Helvetica", "I", 8)
        pdf.multi_cell(
            0, 4,
            "Personal Health Summary - Generated from encrypted vault data.",
            new_x="LMARGIN", new_y="NEXT",
        )

        return bytes(pdf.output())

    def _section_header(self, pdf: FPDF, text: str) -> None:
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 8, text, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    def _body_text(self, pdf: FPDF, text: str) -> None:
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 5, text, new_x="LMARGIN", new_y="NEXT")
