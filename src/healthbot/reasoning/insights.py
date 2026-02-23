"""Deterministic insight generation.

WHOOP-style domain scoring and health dashboard.
All logic is rule-based — no LLM involvement.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from healthbot.data.db import HealthDB
from healthbot.data.models import TriageLevel
from healthbot.reasoning.trends import TrendAnalyzer
from healthbot.reasoning.triage import TriageEngine

logger = logging.getLogger("healthbot")

# Biomarker domains with component tests
DOMAINS: dict[str, dict] = {
    "metabolic": {
        "label": "Metabolic Health",
        "tests": ["glucose", "hba1c", "bun", "creatinine", "egfr"],
        "weight": 1.0,
    },
    "cardiovascular": {
        "label": "Cardiovascular",
        "tests": ["cholesterol_total", "ldl", "hdl", "triglycerides"],
        "weight": 1.0,
    },
    "blood": {
        "label": "Blood Health",
        "tests": ["hemoglobin", "hematocrit", "wbc", "platelets", "rbc"],
        "weight": 0.8,
    },
    "liver": {
        "label": "Liver Function",
        "tests": ["alt", "ast", "alkaline_phosphatase", "bilirubin", "albumin"],
        "weight": 0.8,
    },
    "thyroid": {
        "label": "Thyroid",
        "tests": ["tsh", "free_t4", "free_t3"],
        "weight": 0.7,
    },
    "nutrition": {
        "label": "Nutrition",
        "tests": ["vitamin_d", "vitamin_b12", "folate", "iron", "ferritin"],
        "weight": 0.6,
    },
    "inflammation": {
        "label": "Inflammation",
        "tests": ["crp", "esr"],
        "weight": 0.5,
    },
}

# Score deductions by triage level
TRIAGE_DEDUCTIONS: dict[TriageLevel, float] = {
    TriageLevel.NORMAL: 0,
    TriageLevel.WATCH: 15,
    TriageLevel.URGENT: 35,
    TriageLevel.CRITICAL: 60,
}


@dataclass
class DomainScore:
    domain: str
    label: str
    score: float  # 0-100
    tests_found: int
    tests_total: int
    issues: list[str]


class InsightEngine:
    """Generate structured health insights from stored data."""

    def __init__(self, db: HealthDB, triage: TriageEngine, trends: TrendAnalyzer) -> None:
        self._db = db
        self._triage = triage
        self._trends = trends

    def compute_domain_scores(self, user_id: int | None = None) -> list[DomainScore]:
        """Compute 0-100 scores for each biomarker domain."""
        scores = []

        for domain_key, domain_info in DOMAINS.items():
            score = 100.0
            tests_found = 0
            issues: list[str] = []

            for test_name in domain_info["tests"]:
                rows = self._db.query_observations(
                    record_type="lab_result",
                    canonical_name=test_name,
                    limit=1,
                    user_id=user_id,
                )
                if not rows:
                    continue
                tests_found += 1
                row = rows[0]
                triage = row.get("_meta", {}).get("triage_level", "normal")
                try:
                    level = TriageLevel(triage)
                except ValueError:
                    level = TriageLevel.NORMAL

                deduction = TRIAGE_DEDUCTIONS.get(level, 0)
                score -= deduction / len(domain_info["tests"])

                if level in (TriageLevel.URGENT, TriageLevel.CRITICAL):
                    issues.append(
                        f"{row.get('test_name', test_name)}: "
                        f"{row.get('value', '?')} {row.get('unit', '')} [{level.value}]"
                    )

            score = max(0, min(100, score))
            scores.append(DomainScore(
                domain=domain_key,
                label=domain_info["label"],
                score=round(score, 1),
                tests_found=tests_found,
                tests_total=len(domain_info["tests"]),
                issues=issues,
            ))

        return scores

    def generate_dashboard(self, user_id: int | None = None) -> str:
        """Generate a full health dashboard summary."""
        lines = ["HEALTH DASHBOARD", "=" * 40, ""]

        # Domain scores
        scores = self.compute_domain_scores(user_id=user_id)
        for ds in scores:
            bar = self._score_bar(ds.score)
            coverage = f"({ds.tests_found}/{ds.tests_total} tests)"
            lines.append(f"{ds.label}: {ds.score:.0f}/100 {bar} {coverage}")
            for issue in ds.issues:
                lines.append(f"  ! {issue}")

        # Trends
        lines.append("")
        lines.append("NOTABLE TRENDS")
        lines.append("-" * 30)
        trends = self._trends.detect_all_trends(months=12, user_id=user_id)
        if trends:
            for t in trends[:5]:
                lines.append(self._trends.format_trend(t))
        else:
            lines.append("No significant trends detected.")

        # Wearable summary
        try:
            from healthbot.reasoning.recovery_readiness import (
                RecoveryReadinessEngine,
            )
            from healthbot.reasoning.wearable_trends import (
                WearableTrendAnalyzer,
            )

            wt = WearableTrendAnalyzer(self._db)
            w_trends = wt.detect_all_trends(days=14, user_id=user_id)
            readiness = RecoveryReadinessEngine(self._db).compute(
                user_id=user_id,
            )

            if w_trends or readiness:
                lines.append("")
                lines.append("WEARABLE STATUS")
                lines.append("-" * 30)
                if readiness:
                    bar = self._score_bar(readiness.score)
                    lines.append(
                        f"Recovery Readiness: "
                        f"{readiness.score:.0f}/100 {bar} "
                        f"({readiness.grade})"
                    )
                for wt_item in w_trends[:3]:
                    lines.append(wt.format_trend(wt_item))
        except Exception:
            logger.debug("Wearable dashboard section unavailable", exc_info=True)

        # Last lab date
        lines.append("")
        sql = (
            "SELECT MAX(date_effective) as last_date FROM observations "
            "WHERE record_type = 'lab_result'"
        )
        params: list = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        rows = self._db.conn.execute(sql, params).fetchone()
        last_date = rows["last_date"] if rows else None
        if last_date:
            days_ago = (
                datetime.now(UTC).date()
                - datetime.fromisoformat(last_date).date()
            ).days
            lines.append(
                f"Last lab work: {last_date} ({days_ago} days ago)",
            )

        return "\n".join(lines)

    def _score_bar(self, score: float, width: int = 10) -> str:
        """Generate a visual score bar."""
        filled = int(score / 100 * width)
        return "[" + "#" * filled + "." * (width - filled) + "]"
