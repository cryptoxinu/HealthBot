"""Tests for USPSTF preventive screening calendar."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.screening_calendar import (
    SCREENING_GUIDELINES,
    ScreeningCalendar,
    format_screenings,
)


def _make_db(
    age: int | None = 50,
    sex: str | None = "male",
    family_facts: list[dict] | None = None,
    screening_facts: list[dict] | None = None,
    observations: list[dict] | None = None,
) -> MagicMock:
    db = MagicMock()
    demographics = {"age": age, "sex": sex, "dob": None, "ethnicity": None}
    db.get_demographics.return_value = demographics

    family_facts = family_facts or []
    screening_facts = screening_facts or []
    observations = observations or []

    def get_ltm(user_id, category):
        if category == "family_history":
            return family_facts
        if category == "screening":
            return screening_facts
        return []

    db.get_ltm_by_category.side_effect = get_ltm

    def query_obs(
        record_type=None, canonical_name=None,
        start_date=None, end_date=None,
        triage_level=None, limit=200, user_id=None,
    ):
        results = []
        for obs in observations:
            if canonical_name and obs.get("canonical_name") != canonical_name:
                continue
            results.append(obs)
        return results[:limit]

    db.query_observations.side_effect = query_obs
    return db


class TestGuidelineKB:
    def test_all_guidelines_have_source(self):
        for g in SCREENING_GUIDELINES:
            assert g.source, f"{g.name} missing source"

    def test_all_have_valid_sex(self):
        for g in SCREENING_GUIDELINES:
            assert g.sex in ("any", "male", "female")

    def test_all_have_valid_ages(self):
        for g in SCREENING_GUIDELINES:
            assert g.start_age > 0
            if g.end_age > 0:
                assert g.end_age > g.start_age

    def test_expected_guidelines_exist(self):
        names = {g.name for g in SCREENING_GUIDELINES}
        assert "Colonoscopy" in names
        assert "Mammogram" in names
        assert "Lipid panel" in names


class TestAgeAndSex:
    def test_50_male_gets_colonoscopy(self):
        db = _make_db(age=50, sex="male")
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        names = {d.guideline.name for d in due}
        assert "Colonoscopy" in names

    def test_50_female_gets_mammogram(self):
        db = _make_db(age=50, sex="female")
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        names = {d.guideline.name for d in due}
        assert "Mammogram" in names

    def test_50_male_no_mammogram(self):
        db = _make_db(age=50, sex="male")
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        names = {d.guideline.name for d in due}
        assert "Mammogram" not in names

    def test_30_too_young_for_colonoscopy(self):
        db = _make_db(age=30, sex="male")
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        names = {d.guideline.name for d in due}
        assert "Colonoscopy" not in names

    def test_no_age_returns_empty(self):
        db = _make_db(age=None, sex="male")
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)
        assert due == []


class TestFamilyHistory:
    def test_colon_cancer_family_lowers_start_age(self):
        family = [{"fact": "colon cancer", "_updated_at": ""}]
        db = _make_db(age=42, sex="male", family_facts=family)
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        colon = [d for d in due if d.guideline.name == "Colonoscopy"]
        assert len(colon) == 1
        assert colon[0].effective_start_age == 40  # 45 - 5

    def test_no_family_history_standard_age(self):
        db = _make_db(age=42, sex="male")
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        colon = [d for d in due if d.guideline.name == "Colonoscopy"]
        assert len(colon) == 0  # Not old enough at standard age 45

    def test_breast_cancer_family_lowers_mammogram_age(self):
        family = [{"fact": "breast cancer", "_updated_at": ""}]
        db = _make_db(age=32, sex="female", family_facts=family)
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        mammo = [d for d in due if d.guideline.name == "Mammogram"]
        assert len(mammo) == 1
        assert mammo[0].effective_start_age == 30  # 40 - 10


class TestScreeningRecords:
    def test_lab_screening_done_via_observation(self):
        """If HbA1c exists in observations, diabetes screening is done."""
        obs = [{
            "canonical_name": "hba1c",
            "value": 5.4,
            "date_collected": "2024-01-15",
        }]
        db = _make_db(age=50, sex="male", observations=obs)
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        # Should not show diabetes screening as never_done
        diabetes = [d for d in due if "Diabetes" in d.guideline.name]
        for d in diabetes:
            assert d.status != "never_done"

    def test_procedure_screening_via_ltm(self):
        """Colonoscopy recorded in LTM screening facts."""
        screening = [{"fact": "Colonoscopy done 2023-06-15", "_updated_at": "2023-06-15"}]
        db = _make_db(age=50, sex="male", screening_facts=screening)
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        colon = [d for d in due if d.guideline.name == "Colonoscopy"]
        # Should not be never_done since it was recorded
        for c in colon:
            assert c.status != "never_done"


class TestOneTimeScreenings:
    def test_hep_c_never_done(self):
        db = _make_db(age=45, sex="male")
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        hep = [d for d in due if "Hepatitis C" in d.guideline.name]
        assert len(hep) == 1
        assert hep[0].status == "never_done"

    def test_aaa_only_males(self):
        db = _make_db(age=70, sex="female")
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        aaa = [d for d in due if "AAA" in d.guideline.name]
        assert len(aaa) == 0


class TestFormatting:
    def test_format_empty(self):
        result = format_screenings([])
        assert "up to date" in result

    def test_format_with_never_done(self):
        db = _make_db(age=50, sex="male")
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        result = format_screenings(due)
        assert "PREVENTIVE SCREENING" in result
        assert "Never done" in result

    def test_sort_never_done_first(self):
        db = _make_db(age=50, sex="male")
        cal = ScreeningCalendar(db)
        due = cal.get_due_screenings(user_id=1)

        # All should be never_done for a fresh user
        if due:
            assert due[0].status == "never_done"
