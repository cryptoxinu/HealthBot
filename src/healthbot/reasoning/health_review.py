"""Structured health review with stable rubric.

WHOOP-style output: domain scores, action plan, doctor questions,
"what changed since last time." All deterministic — no LLM involvement.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from healthbot.data.db import HealthDB
from healthbot.reasoning.delta import DeltaEngine
from healthbot.reasoning.insights import DOMAINS, TRIAGE_DEDUCTIONS, InsightEngine
from healthbot.reasoning.overdue import OverdueDetector
from healthbot.reasoning.reference_ranges import DEFAULT_RANGES
from healthbot.reasoning.trends import TrendAnalyzer
from healthbot.reasoning.triage import TriageEngine


@dataclass
class ActionItem:
    """A prioritised action for the user."""

    priority: int  # 1 = highest
    category: str  # "discuss", "monitor", "schedule", "supplement"
    message: str
    domain: str = ""


@dataclass
class DomainSummary:
    """Score + top drivers for a single domain."""

    domain: str
    label: str
    score: float
    drivers: list[str] = field(default_factory=list)


@dataclass
class HealthReviewPacket:
    """Complete structured health review."""

    domains: list[DomainSummary] = field(default_factory=list)
    overall_score: float = 0.0
    actions: list[ActionItem] = field(default_factory=list)
    doctor_questions: list[str] = field(default_factory=list)
    delta_summary: str = ""


class HealthReviewEngine:
    """Generate a comprehensive, stable health review."""

    def __init__(
        self,
        db: HealthDB,
        triage: TriageEngine,
        trends: TrendAnalyzer,
        overdue: OverdueDetector,
        delta: DeltaEngine,
    ) -> None:
        self._db = db
        self._triage = triage
        self._trends = trends
        self._overdue = overdue
        self._delta = delta
        self._insights = InsightEngine(db, triage, trends)

    def generate_review(self, user_id: int | None = None) -> HealthReviewPacket:
        """Build the full review packet."""
        packet = HealthReviewPacket()

        # 1. Domain scores with drivers
        packet.domains = self._compute_domains_with_drivers(user_id=user_id)

        # 2. Overall weighted score
        packet.overall_score = self._compute_overall(packet.domains)

        # 3. Action plan
        packet.actions = self._build_actions(packet.domains, user_id=user_id)

        # 4. Doctor questions
        packet.doctor_questions = self._build_doctor_questions(
            packet.domains, user_id=user_id,
        )

        # 5. Delta summary
        delta_report = self._delta.compute_delta()
        if delta_report:
            packet.delta_summary = self._delta.format_delta(delta_report)

        return packet

    def _compute_domains_with_drivers(
        self, user_id: int | None = None,
    ) -> list[DomainSummary]:
        """Compute domain scores and extract top 3 drivers per domain."""
        domain_scores = self._insights.compute_domain_scores(user_id=user_id)
        summaries = []

        for ds in domain_scores:
            drivers = self._get_domain_drivers(ds.domain, user_id=user_id)
            summaries.append(DomainSummary(
                domain=ds.domain,
                label=ds.label,
                score=ds.score,
                drivers=drivers[:3],
            ))

        return summaries

    def _get_domain_drivers(
        self, domain_key: str, user_id: int | None = None,
    ) -> list[str]:
        """Get top reasons for a domain's score (what's pulling it down or up)."""
        domain_info = DOMAINS.get(domain_key)
        if not domain_info:
            return []

        drivers: list[tuple[float, str]] = []

        for test_name in domain_info["tests"]:
            rows = self._db.query_observations(
                record_type="lab_result",
                canonical_name=test_name,
                limit=1,
                user_id=user_id,
            )
            if not rows:
                continue

            row = rows[0]
            value = row.get("value")
            unit = row.get("unit", "")
            triage = row.get("_meta", {}).get("triage_level", "normal")

            from healthbot.data.models import TriageLevel
            try:
                level = TriageLevel(triage)
            except ValueError:
                level = TriageLevel.NORMAL

            deduction = TRIAGE_DEDUCTIONS.get(level, 0)

            if deduction > 0:
                display_name = row.get("test_name", test_name)
                drivers.append((
                    deduction,
                    f"{display_name}: {value} {unit} [{level.value}]",
                ))

            # Check if trending in a concerning direction
            trend = self._trends.analyze_test(test_name, months=12, user_id=user_id)
            if trend and trend.direction != "stable":
                ref = DEFAULT_RANGES.get(test_name)
                if ref:
                    ref_low = ref.get("low")
                    ref_high = ref.get("high")
                    # Skip midpoint calculation if either bound is None
                    if ref_low is None or ref_high is None:
                        continue
                    mid = (ref_low + ref_high) / 2
                    moving_away = (
                        (trend.direction == "increasing" and trend.last_value > mid)
                        or (trend.direction == "decreasing" and trend.last_value < mid)
                    )
                    if moving_away:
                        display_name = trend.test_name
                        drivers.append((
                            10,
                            f"{display_name}: trending {trend.direction} "
                            f"({trend.pct_change:+.1f}%)",
                        ))

        # Sort by impact (deduction) descending
        drivers.sort(key=lambda x: x[0], reverse=True)
        return [d[1] for d in drivers]

    def _compute_overall(self, domains: list[DomainSummary]) -> float:
        """Weighted average across all domains."""
        total_weight = 0.0
        weighted_sum = 0.0

        for ds in domains:
            weight = DOMAINS.get(ds.domain, {}).get("weight", 1.0)
            weighted_sum += ds.score * weight
            total_weight += weight

        if total_weight == 0:
            return 0.0
        return round(weighted_sum / total_weight, 1)

    def _build_actions(
        self, domains: list[DomainSummary], user_id: int | None = None,
    ) -> list[ActionItem]:
        """Build ranked action plan from domain scores, trends, and overdue items."""
        actions: list[ActionItem] = []

        # Low-scoring domains -> "Discuss with doctor"
        for ds in domains:
            if ds.score < 70:
                actions.append(ActionItem(
                    priority=1,
                    category="discuss",
                    message=f"Discuss {ds.label} with your doctor (score: {ds.score:.0f})",
                    domain=ds.domain,
                ))

        # Worsening trends -> "Monitor"
        trends = self._trends.detect_all_trends(months=12, user_id=user_id)
        for t in trends:
            ref = DEFAULT_RANGES.get(t.canonical_name)
            if not ref:
                continue
            ref_low = ref.get("low")
            ref_high = ref.get("high")
            if ref_low is None or ref_high is None:
                continue
            mid = (ref_low + ref_high) / 2
            moving_away = (
                (t.direction == "increasing" and t.last_value > mid)
                or (t.direction == "decreasing" and t.last_value < mid)
            )
            if moving_away:
                actions.append(ActionItem(
                    priority=2,
                    category="monitor",
                    message=f"Monitor {t.test_name}: trending {t.direction} ({t.pct_change:+.1f}%)",
                ))

        # Overdue screenings -> "Schedule"
        overdue = self._overdue.check_overdue(user_id=user_id)
        for item in overdue:
            actions.append(ActionItem(
                priority=2,
                category="schedule",
                message=(
                    f"Schedule {item.test_name} "
                    f"(last: {item.last_date}, ~{item.days_overdue // 30}mo overdue)"
                ),
            ))

        # Low nutrition markers -> "Supplement"
        for marker in ("vitamin_d", "vitamin_b12", "iron", "ferritin"):
            rows = self._db.query_observations(
                record_type="lab_result",
                canonical_name=marker,
                limit=1,
                user_id=user_id,
            )
            if not rows:
                continue
            row = rows[0]
            triage = row.get("_meta", {}).get("triage_level", "normal")
            if triage in ("urgent", "critical"):
                display = row.get("test_name", marker)
                actions.append(ActionItem(
                    priority=3,
                    category="supplement",
                    message=f"Discuss {display} supplementation with your doctor",
                    domain="nutrition",
                ))

        actions.sort(key=lambda a: a.priority)
        return actions

    def _build_doctor_questions(
        self, domains: list[DomainSummary], user_id: int | None = None,
    ) -> list[str]:
        """Generate questions to ask your doctor, based on flagged results + trends + overdue."""
        questions: list[str] = []

        # From flagged results
        for ds in domains:
            if ds.drivers:
                questions.append(
                    f"My {ds.label.lower()} results show: {ds.drivers[0]}. "
                    f"What does this mean and should I be concerned?"
                )

        # From overdue
        overdue = self._overdue.check_overdue(user_id=user_id)
        if overdue:
            names = ", ".join(o.test_name for o in overdue[:3])
            questions.append(f"I'm overdue for: {names}. Should I schedule these?")

        # From worsening trends
        trends = self._trends.detect_all_trends(months=12, user_id=user_id)
        worsening = [t for t in trends if t.direction != "stable"]
        for t in worsening[:2]:
            questions.append(
                f"My {t.test_name} has been {t.direction} "
                f"({t.first_value} -> {t.last_value}). Is this a concern?"
            )

        return questions

    def format_review(self, packet: HealthReviewPacket) -> str:
        """Format the full review for Telegram display."""
        lines = [
            "HEALTH REVIEW",
            "=" * 40,
            "",
        ]

        # Overall score
        lines.append(f"Overall Health Score: {packet.overall_score:.0f}/100")
        lines.append("")

        # Domain scores
        lines.append("DOMAIN SCORES")
        lines.append("-" * 30)
        for ds in packet.domains:
            bar = self._score_bar(ds.score)
            lines.append(f"  {ds.label}: {ds.score:.0f}/100 {bar}")
            for driver in ds.drivers:
                lines.append(f"    - {driver}")
        lines.append("")

        # Action plan
        if packet.actions:
            lines.append("ACTION PLAN")
            lines.append("-" * 30)
            for a in packet.actions:
                prio = {1: "P1", 2: "P2", 3: "P3"}.get(a.priority, "P?")
                lines.append(f"  [{prio}] {a.message}")
            lines.append("")

        # Doctor questions
        if packet.doctor_questions:
            lines.append("QUESTIONS FOR YOUR DOCTOR")
            lines.append("-" * 30)
            for i, q in enumerate(packet.doctor_questions, 1):
                lines.append(f"  {i}. {q}")
            lines.append("")

        # Delta
        if packet.delta_summary:
            lines.append(packet.delta_summary)
            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _score_bar(score: float, width: int = 10) -> str:
        filled = int(score / 100 * width)
        return "[" + "#" * filled + "." * (width - filled) + "]"
