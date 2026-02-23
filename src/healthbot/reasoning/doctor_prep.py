"""Doctor visit preparation summaries.

Generates structured appointment prep packets with citations.
All logic is deterministic.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from healthbot.data.db import HealthDB
from healthbot.reasoning.overdue import OverdueDetector
from healthbot.reasoning.trends import TrendAnalyzer
from healthbot.reasoning.triage import TriageEngine

if TYPE_CHECKING:
    from healthbot.export.pdf_generator import PrepData


class DoctorPrepEngine:
    """Generate doctor visit preparation summaries."""

    def __init__(
        self,
        db: HealthDB,
        triage: TriageEngine,
        trends: TrendAnalyzer,
        overdue: OverdueDetector,
    ) -> None:
        self._db = db
        self._triage = triage
        self._trends = trends
        self._overdue = overdue

    def generate_prep_data(self, user_id: int | None = None) -> PrepData:
        """Return structured data for PDF generation."""
        from healthbot.export.pdf_generator import PrepData

        gen_date = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

        # Urgent/critical findings
        urgent_rows = self._db.query_observations(
            record_type="lab_result", triage_level="urgent", limit=20,
            user_id=user_id,
        )
        critical_rows = self._db.query_observations(
            record_type="lab_result", triage_level="critical", limit=10,
            user_id=user_id,
        )
        flagged = critical_rows + urgent_rows
        urgent_items = []
        for row in flagged:
            urgent_items.append({
                "level": row.get("_meta", {}).get("triage_level", "?").upper(),
                "name": row.get("test_name", "Unknown"),
                "value": str(row.get("value", "?")),
                "unit": row.get("unit", ""),
                "date": row.get("_meta", {}).get("date_effective", ""),
                "citation": row.get("_meta", {}).get("source_doc_id", "")[:8],
            })

        # Trends
        all_trends = self._trends.detect_all_trends(months=12, user_id=user_id)
        trend_items = []
        for t in (all_trends or [])[:5]:
            trend_items.append({
                "test_name": t.test_name,
                "direction": t.direction,
                "pct_change": f"{t.pct_change:+.1f}",
                "first_val": str(getattr(t, "first_value", "")),
                "last_val": str(getattr(t, "last_value", "")),
            })

        # Medications
        meds_raw = self._db.get_active_medications(user_id=user_id)
        medications = [
            {"name": m.get("name", ""), "dose": m.get("dose", ""),
             "frequency": m.get("frequency", "")}
            for m in (meds_raw or [])
        ]

        # Overdue
        overdue_items_raw = self._overdue.check_overdue(user_id=user_id)
        overdue_items = [
            {"test_name": o.test_name, "last_date": str(o.last_date),
             "months_overdue": str(o.days_overdue // 30)}
            for o in (overdue_items_raw or [])
        ]

        # Questions
        questions = self._generate_questions(flagged, all_trends, overdue_items_raw)

        return PrepData(
            generated_date=gen_date,
            urgent_items=urgent_items,
            trends=trend_items,
            medications=medications,
            overdue_items=overdue_items,
            questions=questions,
        )

    def generate_prep(self, user_id: int | None = None) -> str:
        """Generate a doctor visit preparation summary."""
        lines = [
            "DOCTOR VISIT PREPARATION",
            "=" * 40,
            f"Generated: {datetime.now(UTC).strftime('%Y-%m-%d')}",
            "",
        ]

        # 1. Urgent/Critical findings
        lines.append("1. ITEMS REQUIRING ATTENTION")
        lines.append("-" * 30)
        urgent_rows = self._db.query_observations(
            record_type="lab_result",
            triage_level="urgent",
            limit=20,
            user_id=user_id,
        )
        critical_rows = self._db.query_observations(
            record_type="lab_result",
            triage_level="critical",
            limit=10,
            user_id=user_id,
        )
        flagged = critical_rows + urgent_rows
        if flagged:
            for row in flagged:
                level = row.get("_meta", {}).get("triage_level", "?")
                name = row.get("test_name", "Unknown")
                value = row.get("value", "?")
                unit = row.get("unit", "")
                date_str = row.get("_meta", {}).get("date_effective", "")
                source = row.get("_meta", {}).get("source_doc_id", "")
                page = row.get("_meta", {}).get("source_page", "")
                cite = f" [doc:{source[:8]}, p.{page}]" if source else ""
                lines.append(f"  [{level.upper()}] {name}: {value} {unit} ({date_str}){cite}")
        else:
            lines.append("  No urgent or critical findings.")

        # 2. Trending concerns
        lines.append("")
        lines.append("2. NOTABLE TRENDS")
        lines.append("-" * 30)
        trends = self._trends.detect_all_trends(months=12, user_id=user_id)
        if trends:
            for t in trends[:5]:
                lines.append(f"  {self._trends.format_trend(t)}")
        else:
            lines.append("  No significant trends.")

        # 3. Active medications
        lines.append("")
        lines.append("3. ACTIVE MEDICATIONS")
        lines.append("-" * 30)
        meds = self._db.get_active_medications(user_id=user_id)
        if meds:
            for med in meds:
                name = med.get("name", "Unknown")
                dose = med.get("dose", "")
                freq = med.get("frequency", "")
                lines.append(f"  - {name} {dose} {freq}".strip())
        else:
            lines.append("  No medications on record.")

        # 4. Overdue screenings
        lines.append("")
        lines.append("4. OVERDUE SCREENINGS")
        lines.append("-" * 30)
        overdue_items = self._overdue.check_overdue(user_id=user_id)
        if overdue_items:
            for item in overdue_items:
                lines.append(
                    f"  - {item.test_name}: last {item.last_date} "
                    f"(~{item.days_overdue // 30} months overdue)"
                )
        else:
            lines.append("  All screenings up to date.")

        # 5. Suggested questions
        lines.append("")
        lines.append("5. SUGGESTED QUESTIONS FOR YOUR DOCTOR")
        lines.append("-" * 30)
        questions = self._generate_questions(flagged, trends, overdue_items)
        for q in questions:
            lines.append(f"  - {q}")

        return "\n".join(lines)

    def _generate_questions(self, flagged: list, trends: list, overdue: list) -> list[str]:
        """Generate suggested questions based on findings."""
        questions = []

        if flagged:
            names = {r.get("test_name", "Unknown") for r in flagged[:3]}
            questions.append(
                f"My {', '.join(names)} results were flagged — what do they mean for my health?"
            )

        if trends:
            for t in trends[:2]:
                if t.direction == "increasing":
                    questions.append(
                        f"My {t.test_name} has been increasing ({t.pct_change:+.0f}%). "
                        f"Should I be concerned?"
                    )
                elif t.direction == "decreasing":
                    questions.append(
                        f"My {t.test_name} has been decreasing ({t.pct_change:+.0f}%). "
                        f"Is this a concern?"
                    )

        if overdue:
            test_names = [o.test_name for o in overdue[:3]]
            questions.append(
                f"Should we order {', '.join(test_names)}? "
                f"They appear overdue based on my records."
            )

        if not questions:
            questions.append(
                "Are there any screenings you'd recommend based on my age and history?"
            )

        return questions
