"""Self-audit for health intelligence gaps.

Identifies missing data, unfollowed flags, and knowledge gaps
that the system can proactively address.

All logic is deterministic — no LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")

# Condition -> required test mappings
CONDITION_TEST_MAP: dict[str, list[str]] = {
    "diabetes": ["glucose", "hba1c"],
    "prediabetes": ["glucose", "hba1c"],
    "thyroid": ["tsh", "free_t4"],
    "hypothyroidism": ["tsh", "free_t4"],
    "iron deficiency": ["ferritin", "hemoglobin", "iron"],
    "anemia": ["hemoglobin", "hematocrit", "ferritin"],
    "kidney disease": ["creatinine", "egfr", "bun"],
    "liver": ["alt", "ast", "bilirubin"],
    "dyslipidemia": ["ldl", "hdl", "triglycerides", "cholesterol_total"],
    "vitamin d deficiency": ["vitamin_d"],
    "b12 deficiency": ["vitamin_b12"],
}

# Age-appropriate screenings
AGE_SCREENINGS: list[dict] = [
    {
        "name": "Lipid panel",
        "test": "cholesterol_total",
        "start_age": 20,
        "sex": "any",
    },
    {
        "name": "HbA1c / glucose screening",
        "test": "hba1c",
        "start_age": 35,
        "sex": "any",
    },
    {
        "name": "TSH screening",
        "test": "tsh",
        "start_age": 35,
        "sex": "any",
    },
    {
        "name": "PSA screening",
        "test": "psa",
        "start_age": 50,
        "sex": "male",
    },
    {
        "name": "Vitamin D check",
        "test": "vitamin_d",
        "start_age": 30,
        "sex": "any",
    },
]


@dataclass
class IntelligenceGap:
    """A gap in the system's health intelligence."""

    gap_type: str  # missing_test, unfollowed_flag, no_research, age_screening
    description: str
    priority: str = "medium"  # low, medium, high
    auto_fixable: bool = False
    related_tests: list[str] = field(default_factory=list)


class IntelligenceAuditor:
    """Self-audit the system's health intelligence for gaps."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def audit(
        self,
        user_id: int,
        demographics: dict | None = None,
    ) -> list[IntelligenceGap]:
        """Run a full intelligence audit. Returns list of gaps found."""
        gaps: list[IntelligenceGap] = []

        # 1. Condition <-> lab coverage gaps
        gaps.extend(self._check_condition_test_gaps(user_id))

        # 2. Unfollowed abnormal results
        gaps.extend(self._check_unfollowed_flags(user_id))

        # 3. Age-appropriate screening gaps
        if demographics:
            gaps.extend(
                self._check_age_screenings(user_id, demographics),
            )

        # 4. Hypothesis <-> missing test gaps
        gaps.extend(self._check_hypothesis_gaps(user_id))

        return gaps

    def _check_condition_test_gaps(
        self, user_id: int,
    ) -> list[IntelligenceGap]:
        """Check if user has conditions but is missing key tests."""
        gaps: list[IntelligenceGap] = []
        try:
            conditions = self._db.get_ltm_by_category(user_id, "condition")
            condition_texts = [
                f.get("fact", "").lower() for f in conditions
            ]

            for condition, tests in CONDITION_TEST_MAP.items():
                # Check if user has this condition in LTM
                has_condition = any(
                    condition in text for text in condition_texts
                )
                if not has_condition:
                    continue

                missing = []
                for test in tests:
                    obs = self._db.query_observations(
                        record_type="lab_result",
                        canonical_name=test,
                        user_id=user_id,
                        limit=1,
                    )
                    if not obs:
                        missing.append(test)

                if missing:
                    gaps.append(IntelligenceGap(
                        gap_type="missing_test",
                        description=(
                            f"Condition '{condition}' detected but missing "
                            f"tests: {', '.join(missing)}."
                        ),
                        priority="high",
                        related_tests=missing,
                    ))
        except Exception as e:
            logger.debug("Audit (condition gaps): %s", e)

        return gaps

    def _check_unfollowed_flags(
        self, user_id: int,
    ) -> list[IntelligenceGap]:
        """Find historically abnormal results with no follow-up recheck."""
        gaps: list[IntelligenceGap] = []
        try:
            labs = self._db.query_observations(
                record_type="lab_result",
                user_id=user_id,
                limit=100,
            )

            # Group by canonical name, find flagged results
            flagged: dict[str, dict] = {}
            latest: dict[str, str] = {}

            for lab in labs:
                canonical = lab.get("canonical_name", "")
                date = lab.get("date_collected", "")
                flag = lab.get("flag", "")

                if not canonical or not date:
                    continue

                # Track latest date for each test
                if canonical not in latest or date > latest[canonical]:
                    latest[canonical] = date

                # Track flagged results
                if flag and flag.upper().startswith(("H", "L")):
                    if canonical not in flagged or date > flagged[canonical].get("date", ""):
                        flagged[canonical] = {
                            "test_name": lab.get("test_name", canonical),
                            "value": lab.get("value", ""),
                            "unit": lab.get("unit", ""),
                            "flag": flag,
                            "date": date,
                        }

            # Check if flagged result was ever followed up
            for canonical, info in flagged.items():
                flag_date = info["date"]
                latest_date = latest.get(canonical, flag_date)

                # If the latest result IS the flagged one, it was never rechecked
                if latest_date == flag_date:
                    gaps.append(IntelligenceGap(
                        gap_type="unfollowed_flag",
                        description=(
                            f"{info['test_name']} was {info['flag']} "
                            f"({info['value']} {info['unit']}) on {flag_date} "
                            f"and hasn't been rechecked since."
                        ),
                        priority="high",
                        related_tests=[canonical],
                    ))
        except Exception as e:
            logger.debug("Audit (unfollowed flags): %s", e)

        return gaps

    def _check_age_screenings(
        self,
        user_id: int,
        demographics: dict,
    ) -> list[IntelligenceGap]:
        """Check for age-appropriate screenings without data."""
        gaps: list[IntelligenceGap] = []
        age = demographics.get("age")
        sex = (demographics.get("sex") or "").lower()

        if not age:
            return gaps

        try:
            for screening in AGE_SCREENINGS:
                if age < screening["start_age"]:
                    continue
                if screening["sex"] != "any" and screening["sex"] != sex:
                    continue

                # Check if we have any data for this test
                obs = self._db.query_observations(
                    record_type="lab_result",
                    user_id=user_id,
                    limit=1,
                )
                has_test = any(
                    o.get("canonical_name") == screening["test"]
                    for o in obs
                )
                if not has_test:
                    gaps.append(IntelligenceGap(
                        gap_type="age_screening",
                        description=(
                            f"At age {age}, {screening['name']} is "
                            f"recommended but no data found."
                        ),
                        priority="medium",
                        related_tests=[screening["test"]],
                    ))
        except Exception as e:
            logger.debug("Audit (age screenings): %s", e)

        return gaps

    def _check_hypothesis_gaps(
        self, user_id: int,
    ) -> list[IntelligenceGap]:
        """Check if active hypotheses are missing confirmatory tests."""
        gaps: list[IntelligenceGap] = []
        try:
            hypotheses = self._db.get_active_hypotheses(user_id)
            for h in hypotheses:
                missing = h.get("missing_tests", [])
                if not missing:
                    continue
                title = h.get("title", "Unknown")
                gaps.append(IntelligenceGap(
                    gap_type="hypothesis_missing_test",
                    description=(
                        f"Hypothesis '{title}' needs "
                        f"{', '.join(missing)} to confirm/rule out."
                    ),
                    priority="high",
                    auto_fixable=False,
                    related_tests=missing,
                ))
        except Exception as e:
            logger.debug("Audit (hypothesis gaps): %s", e)

        return gaps

    def format_gaps(self, gaps: list[IntelligenceGap]) -> str:
        """Format gaps for display."""
        if not gaps:
            return "No intelligence gaps detected. Data looks complete."

        lines = [f"Intelligence audit: {len(gaps)} gap(s) found:"]
        for gap in gaps:
            icon = {"high": "!", "medium": "~", "low": ""}.get(
                gap.priority, "",
            )
            lines.append(f"  {icon} {gap.description}")

        return "\n".join(lines)
