"""USPSTF/ACS preventive screening calendar.

Age/sex/family-history-based screening recommendations.
Uses demographics from LTM and family history from family_risk module.

All logic is deterministic. No LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass(frozen=True)
class ScreeningGuideline:
    """A preventive screening recommendation."""

    name: str
    test_type: str       # "procedure", "lab", "imaging"
    start_age: int
    end_age: int         # 0 = no upper limit
    sex: str             # "any", "male", "female"
    interval_months: int
    source: str          # guideline body
    grade: str           # USPSTF grade (A, B, C)
    notes: str
    family_start_modifier: int  # age subtracted if positive family history
    family_conditions: tuple[str, ...]  # which family conditions trigger modifier


SCREENING_GUIDELINES: tuple[ScreeningGuideline, ...] = (
    ScreeningGuideline(
        name="Colonoscopy",
        test_type="procedure",
        start_age=45,
        end_age=75,
        sex="any",
        interval_months=120,  # 10 years
        source="USPSTF 2021",
        grade="A",
        notes="Alternatives: FIT annually, stool DNA every 3 years, "
              "CT colonography every 5 years, flexible sigmoidoscopy every 5 years.",
        family_start_modifier=5,
        family_conditions=("colon cancer", "colorectal cancer", "colon polyps"),
    ),
    ScreeningGuideline(
        name="Mammogram",
        test_type="imaging",
        start_age=40,
        end_age=74,
        sex="female",
        interval_months=24,  # every 2 years
        source="USPSTF 2024",
        grade="B",
        notes="Annual screening may be preferred with dense breasts or family history.",
        family_start_modifier=10,
        family_conditions=("breast cancer",),
    ),
    ScreeningGuideline(
        name="Cervical cancer screening (Pap/HPV)",
        test_type="procedure",
        start_age=21,
        end_age=65,
        sex="female",
        interval_months=36,  # every 3 years (Pap) or 5 years (HPV co-test)
        source="USPSTF 2018",
        grade="A",
        notes="Age 21-29: Pap every 3 years. Age 30-65: Pap every 3 years, "
              "HPV every 5 years, or co-test every 5 years.",
        family_start_modifier=0,
        family_conditions=(),
    ),
    ScreeningGuideline(
        name="PSA (Prostate cancer screening)",
        test_type="lab",
        start_age=55,
        end_age=69,
        sex="male",
        interval_months=24,  # every 2 years if elected
        source="USPSTF 2018",
        grade="C",
        notes="Shared decision-making recommended. Earlier screening (age 40-55) "
              "for family history of prostate cancer or Black men.",
        family_start_modifier=15,
        family_conditions=("prostate cancer",),
    ),
    ScreeningGuideline(
        name="Lung cancer screening (Low-dose CT)",
        test_type="imaging",
        start_age=50,
        end_age=80,
        sex="any",
        interval_months=12,  # annual
        source="USPSTF 2021",
        grade="B",
        notes="For adults with 20+ pack-year smoking history who currently smoke "
              "or quit within the past 15 years.",
        family_start_modifier=0,
        family_conditions=(),
    ),
    ScreeningGuideline(
        name="DEXA bone density scan",
        test_type="imaging",
        start_age=65,
        end_age=0,
        sex="female",
        interval_months=24,  # every 2 years
        source="USPSTF 2018",
        grade="B",
        notes="Postmenopausal women age 65+. Earlier if risk factors "
              "(low BMI, fracture history, corticosteroid use).",
        family_start_modifier=0,
        family_conditions=(),
    ),
    ScreeningGuideline(
        name="Diabetes screening (HbA1c/Glucose)",
        test_type="lab",
        start_age=35,
        end_age=70,
        sex="any",
        interval_months=36,  # every 3 years
        source="USPSTF 2021",
        grade="B",
        notes="For overweight/obese adults. Earlier screening if BMI >= 25 "
              "and additional risk factors (family history, GDM, PCOS).",
        family_start_modifier=5,
        family_conditions=("diabetes", "type 2 diabetes"),
    ),
    ScreeningGuideline(
        name="Lipid panel",
        test_type="lab",
        start_age=40,
        end_age=75,
        sex="any",
        interval_months=60,  # every 5 years
        source="USPSTF 2023",
        grade="B",
        notes="More frequent if risk factors present. Earlier screening (age 20+) "
              "if family history of premature cardiovascular disease.",
        family_start_modifier=20,
        family_conditions=(
            "heart disease", "cardiovascular disease", "heart attack",
            "high cholesterol",
        ),
    ),
    ScreeningGuideline(
        name="Hepatitis C screening",
        test_type="lab",
        start_age=18,
        end_age=79,
        sex="any",
        interval_months=0,  # one-time
        source="USPSTF 2020",
        grade="B",
        notes="One-time screening for all adults 18-79. "
              "More frequent for ongoing risk factors (injection drug use).",
        family_start_modifier=0,
        family_conditions=(),
    ),
    ScreeningGuideline(
        name="AAA ultrasound (Abdominal aortic aneurysm)",
        test_type="imaging",
        start_age=65,
        end_age=75,
        sex="male",
        interval_months=0,  # one-time
        source="USPSTF 2019",
        grade="B",
        notes="One-time screening for men 65-75 who have ever smoked.",
        family_start_modifier=0,
        family_conditions=(),
    ),
    ScreeningGuideline(
        name="Skin cancer check",
        test_type="procedure",
        start_age=35,
        end_age=0,
        sex="any",
        interval_months=12,  # annual
        source="ACS 2024",
        grade="",
        notes="Full-body skin exam by dermatologist. "
              "Higher risk: fair skin, history of sunburns, family history of melanoma.",
        family_start_modifier=10,
        family_conditions=("melanoma", "skin cancer"),
    ),
)


@dataclass
class DueScreening:
    """A screening that is currently due or overdue."""

    guideline: ScreeningGuideline
    status: str           # "due", "overdue", "never_done"
    last_done: str        # ISO date or ""
    months_since: int     # -1 if never done
    effective_start_age: int  # adjusted for family history


class ScreeningCalendar:
    """Check which preventive screenings are due."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def get_due_screenings(self, user_id: int) -> list[DueScreening]:
        """Get all screenings due for this user based on demographics."""
        demographics = self._db.get_demographics(user_id)
        age = demographics.get("age")
        sex = demographics.get("sex")

        if age is None:
            return []

        family_conditions = self._get_family_conditions(user_id)
        due: list[DueScreening] = []

        for g in SCREENING_GUIDELINES:
            screening = self._check_guideline(
                g, age, sex, family_conditions, user_id,
            )
            if screening:
                due.append(screening)

        # Sort: never_done first, then overdue, then due
        priority = {"never_done": 0, "overdue": 1, "due": 2}
        due.sort(key=lambda d: priority.get(d.status, 3))
        return due

    def _check_guideline(
        self,
        g: ScreeningGuideline,
        age: int,
        sex: str | None,
        family_conditions: set[str],
        user_id: int,
    ) -> DueScreening | None:
        """Check if a guideline applies and is due."""
        # Sex filter
        if g.sex != "any" and sex and g.sex != sex:
            return None

        # Age with family history modifier
        effective_start = g.start_age
        if g.family_start_modifier > 0 and g.family_conditions:
            for fc in g.family_conditions:
                if fc in family_conditions:
                    effective_start = g.start_age - g.family_start_modifier
                    break

        if age < effective_start:
            return None
        if g.end_age > 0 and age > g.end_age:
            return None

        # One-time screenings (interval_months == 0)
        if g.interval_months == 0:
            if self._has_screening_record(g.name, user_id):
                return None
            return DueScreening(
                guideline=g,
                status="never_done",
                last_done="",
                months_since=-1,
                effective_start_age=effective_start,
            )

        # Recurring screenings
        last_date = self._last_screening_date(g.name, user_id)
        if not last_date:
            return DueScreening(
                guideline=g,
                status="never_done",
                last_done="",
                months_since=-1,
                effective_start_age=effective_start,
            )

        from datetime import UTC, datetime
        now = datetime.now(UTC).date()
        try:
            last = datetime.fromisoformat(last_date).date()
        except ValueError:
            return None

        months_since = (now - last).days // 30
        if months_since >= g.interval_months:
            return DueScreening(
                guideline=g,
                status="overdue",
                last_done=last_date[:10],
                months_since=months_since,
                effective_start_age=effective_start,
            )
        return None

    def _get_family_conditions(self, user_id: int) -> set[str]:
        """Extract family history conditions from LTM."""
        conditions: set[str] = set()
        try:
            facts = self._db.get_ltm_by_category(user_id, "family_history")
            for f in facts:
                text = f.get("fact", "").lower()
                if text:
                    conditions.add(text)
        except Exception:
            pass
        return conditions

    def _has_screening_record(self, name: str, user_id: int) -> bool:
        """Check if there's any record of this screening."""
        try:
            facts = self._db.get_ltm_by_category(user_id, "screening")
            for f in facts:
                if name.lower() in f.get("fact", "").lower():
                    return True
        except Exception:
            pass

        # Also check observations for lab-type screenings
        try:
            rows = self._db.query_observations(
                canonical_name=self._screening_to_canonical(name),
                limit=1,
                user_id=user_id,
            )
            if rows:
                return True
        except Exception:
            pass
        return False

    def _last_screening_date(self, name: str, user_id: int) -> str:
        """Find the date of the last screening."""
        # Check LTM screening records
        try:
            facts = self._db.get_ltm_by_category(user_id, "screening")
            for f in facts:
                if name.lower() in f.get("fact", "").lower():
                    return f.get("_updated_at", "")
        except Exception:
            pass

        # Check observations for lab-type screenings
        canonical = self._screening_to_canonical(name)
        if canonical:
            try:
                rows = self._db.query_observations(
                    canonical_name=canonical,
                    limit=1,
                    user_id=user_id,
                )
                if rows:
                    return rows[0].get("date_collected", "")
            except Exception:
                pass
        return ""

    @staticmethod
    def _screening_to_canonical(name: str) -> str:
        """Map screening name to canonical lab name if applicable."""
        mapping = {
            "Diabetes screening (HbA1c/Glucose)": "hba1c",
            "Lipid panel": "cholesterol_total",
            "PSA (Prostate cancer screening)": "psa",
            "Hepatitis C screening": "hepatitis_c_antibody",
        }
        return mapping.get(name, "")


def format_screenings(screenings: list[DueScreening]) -> str:
    """Format due screenings for display."""
    if not screenings:
        return "All preventive screenings are up to date."

    lines = ["PREVENTIVE SCREENING CALENDAR", "-" * 30]

    never = [s for s in screenings if s.status == "never_done"]
    overdue = [s for s in screenings if s.status == "overdue"]

    if never:
        lines.append("\nNever done:")
        for s in never:
            lines.append(f"  * {s.guideline.name}")
            lines.append(f"    {s.guideline.source} (Grade {s.guideline.grade})"
                         if s.guideline.grade else f"    {s.guideline.source}")
            if s.effective_start_age != s.guideline.start_age:
                lines.append(
                    f"    Age adjusted: {s.effective_start_age} "
                    f"(standard: {s.guideline.start_age}) due to family history"
                )
            if s.guideline.notes:
                lines.append(f"    Note: {s.guideline.notes}")

    if overdue:
        lines.append("\nOverdue:")
        for s in overdue:
            interval_years = s.guideline.interval_months // 12
            interval_text = (
                f"every {interval_years} years"
                if interval_years > 0
                else f"every {s.guideline.interval_months} months"
            )
            lines.append(
                f"  ! {s.guideline.name}: last done {s.last_done} "
                f"({s.months_since} months ago, {interval_text})"
            )
            lines.append(f"    {s.guideline.source} (Grade {s.guideline.grade})"
                         if s.guideline.grade else f"    {s.guideline.source}")

    return "\n".join(lines)
