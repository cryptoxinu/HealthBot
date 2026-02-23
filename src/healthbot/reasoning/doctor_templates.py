"""Condition-specific doctor communication templates.

Each template pulls the user's actual lab values for relevant tests,
shows trends, and provides discussion points and questions to ask.
"""
from __future__ import annotations

import logging

from healthbot.data.db import HealthDB
from healthbot.reasoning.reference_ranges import get_default_range
from healthbot.reasoning.trends import TrendAnalyzer

logger = logging.getLogger("healthbot")


# Template registry: key -> (title, relevant lab canonical names, discussion points)
TEMPLATE_REGISTRY: dict[str, dict] = {
    "pots": {
        "title": "POTS (Postural Orthostatic Tachycardia Syndrome)",
        "relevant_labs": [
            "sodium", "potassium", "magnesium", "cortisol",
            "aldosterone", "tsh", "free_t4", "ferritin", "vitamin_b12",
        ],
        "discussion_points": [
            "Heart rate response to standing (>30 BPM increase within 10 min)",
            "Salt and fluid intake adequacy",
            "Tilt table test results or need for one",
            "Medication options (fludrocortisone, midodrine, beta-blockers)",
            "Exercise reconditioning protocol",
        ],
    },
    "thyroid": {
        "title": "Thyroid Function",
        "relevant_labs": ["tsh", "free_t4", "free_t3"],
        "discussion_points": [
            "TSH trend over time — stable, rising, or falling?",
            "Symptoms: fatigue, weight changes, hair loss, cold intolerance",
            "Need for thyroid antibody testing (TPO, thyroglobulin)",
            "Medication dosage adequacy (if on levothyroxine)",
            "Timing of lab draws relative to medication",
        ],
    },
    "prediabetes": {
        "title": "Pre-diabetes / Metabolic Health",
        "relevant_labs": [
            "glucose", "hba1c", "triglycerides", "cholesterol_total",
            "ldl", "hdl",
        ],
        "discussion_points": [
            "Fasting vs. non-fasting glucose values",
            "HbA1c trajectory — improving or worsening?",
            "Diet and exercise interventions",
            "Need for oral glucose tolerance test (OGTT)",
            "Metformin consideration if lifestyle changes insufficient",
        ],
    },
    "cardiovascular": {
        "title": "Cardiovascular Risk Assessment",
        "relevant_labs": [
            "cholesterol_total", "ldl", "hdl", "triglycerides", "crp",
        ],
        "discussion_points": [
            "10-year ASCVD risk score",
            "LDL target based on risk factors",
            "HDL optimization strategies",
            "Triglyceride/HDL ratio as insulin resistance marker",
            "High-sensitivity CRP for inflammation assessment",
            "Statin benefit/risk discussion if applicable",
        ],
    },
    "anemia": {
        "title": "Anemia Workup",
        "relevant_labs": [
            "hemoglobin", "hematocrit", "iron", "ferritin",
            "vitamin_b12", "folate", "mcv", "mch", "rdw",
        ],
        "discussion_points": [
            "Type of anemia: iron-deficiency, B12, folate, chronic disease",
            "MCV classification: microcytic vs. normocytic vs. macrocytic",
            "Iron studies interpretation",
            "Supplementation plan and follow-up timeline",
            "Investigation of underlying cause (GI bleeding, malabsorption)",
        ],
    },
    "inflammation": {
        "title": "Inflammation Markers",
        "relevant_labs": ["crp", "esr", "wbc", "ferritin"],
        "discussion_points": [
            "Acute vs. chronic inflammation differentiation",
            "Autoimmune screening if persistently elevated",
            "Infection workup if acutely elevated",
            "Diet and lifestyle anti-inflammatory strategies",
            "Follow-up timeline for repeat testing",
        ],
    },
}


class DoctorTemplateEngine:
    """Generate populated doctor communication templates."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db
        self._trends = TrendAnalyzer(db)

    def list_templates(self) -> list[tuple[str, str]]:
        """Return available templates as (key, title) pairs."""
        return [(k, v["title"]) for k, v in TEMPLATE_REGISTRY.items()]

    def generate(self, key: str, user_id: int) -> str:
        """Generate a populated template for the given condition.

        Pulls the user's actual lab values and trends for relevant tests.
        """
        template = TEMPLATE_REGISTRY.get(key)
        if not template:
            available = ", ".join(TEMPLATE_REGISTRY.keys())
            return f"Unknown template: {key}\nAvailable: {available}"

        lines = [
            f"DOCTOR DISCUSSION: {template['title']}",
            "=" * 50,
            "",
        ]

        # Lab values section
        lines.append("YOUR LAB VALUES:")
        lines.append("-" * 30)

        has_data = False
        for lab_name in template["relevant_labs"]:
            results = self._db.query_observations(
                canonical_name=lab_name, limit=3
            )
            ref = get_default_range(lab_name)
            unit = ref.get("unit", "") if ref else ""

            if results:
                has_data = True
                lines.append(f"\n  {lab_name.replace('_', ' ').title()}:")
                for r in results:
                    val = r.get("value", "")
                    date = r.get("date_effective", r.get("_date", ""))
                    flag = r.get("flag", "")
                    flag_str = f" [{flag}]" if flag else ""
                    lines.append(f"    {date}: {val} {unit}{flag_str}")

                # Show trend if available
                trend = self._trends.analyze_test(lab_name)
                if trend and trend.direction != "stable":
                    arrow = {"increasing": "^", "decreasing": "v"}.get(
                        trend.direction, ""
                    )
                    lines.append(
                        f"    Trend: {arrow} {trend.pct_change:+.1f}% "
                        f"over {trend.data_points} readings"
                    )

                # Show reference range
                if ref:
                    low = ref.get("low", "")
                    high = ref.get("high", "")
                    note = ref.get("note", "")
                    note_str = f" ({note})" if note else ""
                    lines.append(f"    Range: {low}-{high} {unit}{note_str}")
            else:
                lines.append(f"\n  {lab_name.replace('_', ' ').title()}: No data")

        if not has_data:
            lines.append("  No lab data available for this template's tests.")

        # Discussion points
        lines.append("")
        lines.append("DISCUSSION POINTS:")
        lines.append("-" * 30)
        for i, point in enumerate(template["discussion_points"], 1):
            lines.append(f"  {i}. {point}")

        # Suggested questions
        lines.append("")
        lines.append("SUGGESTED QUESTIONS TO ASK:")
        lines.append("-" * 30)
        questions = self._generate_questions(key, template)
        for i, q in enumerate(questions, 1):
            lines.append(f"  {i}. {q}")

        return "\n".join(lines)

    def _generate_questions(
        self, key: str, template: dict
    ) -> list[str]:
        """Generate condition-specific questions based on available data."""
        questions = [
            f"Based on my lab results, what is your assessment of my {template['title'].lower()}?",
            "Are there any additional tests you'd recommend?",
            "What changes should I make before my next lab draw?",
        ]

        if key == "pots":
            questions.append("Should we schedule a tilt table test?")
            questions.append("What is the optimal daily sodium and fluid intake for me?")
        elif key == "thyroid":
            questions.append("Should we check thyroid antibodies?")
            questions.append("Is my current TSH level optimal, or just 'normal'?")
        elif key == "prediabetes":
            questions.append("At what point would you consider medication?")
            questions.append("How often should I be monitoring my glucose?")
        elif key == "cardiovascular":
            questions.append("What is my calculated ASCVD risk score?")
            questions.append("Should we discuss statin therapy?")
        elif key == "anemia":
            questions.append("What type of anemia does my lab pattern suggest?")
            questions.append("Should we investigate GI absorption issues?")
        elif key == "inflammation":
            questions.append("Should we screen for autoimmune conditions?")
            questions.append("What follow-up testing timeline do you recommend?")

        return questions
