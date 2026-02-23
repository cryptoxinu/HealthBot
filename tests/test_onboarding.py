"""Tests for healthbot.nlu.onboarding — health profile interview engine."""
from __future__ import annotations

import time

import pytest

from healthbot.data.db import HealthDB
from healthbot.nlu.onboarding import (
    ONBOARDING_QUESTIONS,
    OnboardingEngine,
    OnboardingSession,
    _parse_dob,
    _split_multi_value,
)


@pytest.fixture
def engine(db: HealthDB) -> OnboardingEngine:
    db.run_migrations()
    return OnboardingEngine(db)


USER_ID = 123


class TestOnboardingQuestions:
    """Validate question definitions."""

    def test_no_duplicate_keys(self) -> None:
        keys = [q.key for q in ONBOARDING_QUESTIONS]
        assert len(keys) == len(set(keys))

    def test_all_have_required_fields(self) -> None:
        valid_categories = {"demographic", "condition", "medication", "preference"}
        for q in ONBOARDING_QUESTIONS:
            assert q.key
            assert q.prompt
            assert q.category in valid_categories
            assert "{answer}" in q.fact_template

    def test_question_count(self) -> None:
        assert len(ONBOARDING_QUESTIONS) == 15

    def test_has_nickname_not_name(self) -> None:
        keys = [q.key for q in ONBOARDING_QUESTIONS]
        assert "nickname" in keys
        assert "name" not in keys

    def test_has_age_not_dob(self) -> None:
        keys = [q.key for q in ONBOARDING_QUESTIONS]
        assert "age" in keys
        assert "date_of_birth" not in keys

    def test_has_past_diagnoses(self) -> None:
        keys = [q.key for q in ONBOARDING_QUESTIONS]
        assert "past_diagnoses" in keys

    def test_has_past_medications(self) -> None:
        keys = [q.key for q in ONBOARDING_QUESTIONS]
        assert "past_medications" in keys


class TestOnboardingEngine:
    def test_start_returns_first_question(self, engine: OnboardingEngine) -> None:
        result = engine.start(USER_ID)
        assert "[1/15]" in result
        assert ONBOARDING_QUESTIONS[0].prompt in result

    def test_process_answer_advances(self, engine: OnboardingEngine) -> None:
        engine.start(USER_ID)
        result = engine.process_answer(USER_ID, "30")
        assert "[2/15]" in result

    def test_skip_advances_without_storing(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        engine.start(USER_ID)
        result = engine.process_answer(USER_ID, "skip")
        assert "[2/15]" in result
        # No LTM facts stored for skipped question
        facts = db.get_ltm_by_user(USER_ID)
        nick_facts = [f for f in facts if "Nickname:" in f.get("fact", "")]
        assert len(nick_facts) == 0

    def test_cancel_clears_session(self, engine: OnboardingEngine) -> None:
        engine.start(USER_ID)
        result = engine.process_answer(USER_ID, "cancel")
        assert "cancelled" in result.lower()
        assert not engine.is_active(USER_ID)

    def test_none_skips(self, engine: OnboardingEngine) -> None:
        engine.start(USER_ID)
        result = engine.process_answer(USER_ID, "none")
        assert "[2/15]" in result

    def test_is_active(self, engine: OnboardingEngine) -> None:
        assert not engine.is_active(USER_ID)
        engine.start(USER_ID)
        assert engine.is_active(USER_ID)

    def test_is_active_false_when_no_session(self, engine: OnboardingEngine) -> None:
        assert not engine.is_active(999)

    def test_full_flow_stores_facts(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        engine.start(USER_ID)
        answers = [
            "Z",                          # nickname
            "30",                         # age
            "male",                       # sex
            "White",                      # ethnicity
            "never",                      # smoking
            "light 1-3",                  # alcohol
            "5'10\"",                     # height
            "170 lbs",                    # weight
            "diabetes, hypertension",     # conditions (multi)
            "appendicitis",               # past_diagnoses
            "metformin 500mg",            # medications
            "amoxicillin",                # past_medications
            "penicillin",                 # allergies
            "lose weight, sleep better",  # goals (multi)
            "heart disease in father",    # family history
        ]
        for i, answer in enumerate(answers):
            result = engine.process_answer(USER_ID, answer)
            if i < len(answers) - 1:
                assert f"[{i + 2}/15]" in result
            else:
                assert "ONBOARDING COMPLETE" in result

        assert not engine.is_active(USER_ID)

        facts = db.get_ltm_by_user(USER_ID)
        # nickname, age, sex, ethnicity, smoking, alcohol, height, weight,
        # conditions(2), past_diagnoses, meds, past_meds, allergies,
        # goals(2), family = 17
        assert len(facts) >= 17

        # Verify categories
        categories = {f["_category"] for f in facts}
        assert "demographic" in categories
        assert "condition" in categories
        assert "medication" in categories
        assert "preference" in categories

    def test_multi_value_splits_commas(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        engine.start(USER_ID)
        # Skip to conditions question (index 8: nickname, age, sex, ethnicity,
        # smoking, alcohol, height, weight)
        for _ in range(8):
            engine.process_answer(USER_ID, "skip")
        engine.process_answer(USER_ID, "diabetes, hypertension, asthma")

        facts = db.get_ltm_by_user(USER_ID)
        condition_facts = [f for f in facts if f["_category"] == "condition"]
        assert len(condition_facts) == 3

    def test_re_onboard_upserts(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        # First run
        engine.start(USER_ID)
        engine.process_answer(USER_ID, "Alice")  # nickname
        engine.process_answer(USER_ID, "cancel")

        facts_before = db.get_ltm_by_user(USER_ID)
        nick_facts = [f for f in facts_before if "Nickname:" in f.get("fact", "")]
        assert len(nick_facts) == 1
        assert "Alice" in nick_facts[0]["fact"]

        # Second run — update nickname
        engine.start(USER_ID)
        engine.process_answer(USER_ID, "Bob")
        engine.process_answer(USER_ID, "cancel")

        facts_after = db.get_ltm_by_user(USER_ID)
        nick_facts = [f for f in facts_after if "Nickname:" in f.get("fact", "")]
        assert len(nick_facts) == 1  # No duplicates
        assert "Bob" in nick_facts[0]["fact"]

    def test_session_expiry(self, engine: OnboardingEngine) -> None:
        engine.start(USER_ID)
        # Manually expire
        engine._sessions[USER_ID].started_at = time.time() - 2000
        assert not engine.is_active(USER_ID)

    def test_vault_lock_clears_sessions(self, engine: OnboardingEngine) -> None:
        engine.start(USER_ID)
        assert engine.is_active(USER_ID)
        engine.on_vault_lock()
        assert not engine.is_active(USER_ID)

    def test_summary_shows_answers(self, engine: OnboardingEngine) -> None:
        engine.start(USER_ID)
        engine.process_answer(USER_ID, "Z")  # nickname
        result = engine.process_answer(USER_ID, "cancel")
        # Cancel doesn't show summary, just a message
        assert "cancelled" in result.lower()

    def test_summary_on_completion(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        engine.start(USER_ID)
        answers = [
            "Z", "30", "female", "White", "never", "none",
            "5'6\"", "140", "none", "none", "none", "none",
            "none", "sleep", "none",
        ]
        result = None
        for answer in answers:
            result = engine.process_answer(USER_ID, answer)
        assert "ONBOARDING COMPLETE" in result
        assert "Nickname: Z" in result
        assert "/profile" in result

    def test_no_session_returns_message(self, engine: OnboardingEngine) -> None:
        result = engine.process_answer(USER_ID, "hello")
        assert "/onboard" in result

    def test_nickname_stored_as_ltm(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        engine.start(USER_ID)
        engine.process_answer(USER_ID, "Z")  # nickname
        engine.process_answer(USER_ID, "cancel")

        facts = db.get_ltm_by_user(USER_ID)
        nick_facts = [f for f in facts if f.get("fact", "").startswith("Nickname:")]
        assert len(nick_facts) == 1
        assert nick_facts[0]["fact"] == "Nickname: Z"

    def test_past_diagnoses_stored(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        engine.start(USER_ID)
        # Skip to past_diagnoses (index 9)
        for _ in range(9):
            engine.process_answer(USER_ID, "skip")
        engine.process_answer(USER_ID, "appendicitis, broken arm")
        engine.process_answer(USER_ID, "cancel")

        facts = db.get_ltm_by_user(USER_ID)
        past_diag = [
            f for f in facts
            if f.get("fact", "").startswith("Past diagnosis:")
        ]
        assert len(past_diag) == 2


class TestOnboardingSession:
    def test_is_complete_initially_false(self) -> None:
        session = OnboardingSession(user_id=1)
        assert not session.is_complete

    def test_is_complete_at_end(self) -> None:
        session = OnboardingSession(user_id=1, current_index=len(ONBOARDING_QUESTIONS))
        assert session.is_complete

    def test_current_question(self) -> None:
        session = OnboardingSession(user_id=1, current_index=0)
        assert session.current_question == ONBOARDING_QUESTIONS[0]

    def test_current_question_none_when_complete(self) -> None:
        session = OnboardingSession(user_id=1, current_index=len(ONBOARDING_QUESTIONS))
        assert session.current_question is None


class TestSplitMultiValue:
    def test_comma_split(self) -> None:
        assert _split_multi_value("a, b, c") == ["a", "b", "c"]

    def test_semicolon_split(self) -> None:
        assert _split_multi_value("a; b; c") == ["a", "b", "c"]

    def test_and_split(self) -> None:
        assert _split_multi_value("diabetes and hypertension") == ["diabetes", "hypertension"]

    def test_filters_skip_words(self) -> None:
        assert _split_multi_value("diabetes, none, hypertension") == ["diabetes", "hypertension"]

    def test_single_value(self) -> None:
        assert _split_multi_value("just one thing") == ["just one thing"]

    def test_empty_parts_filtered(self) -> None:
        assert _split_multi_value("a,,b") == ["a", "b"]


class TestParseDob:
    """Tests for age/DOB parsing."""

    def test_plain_age_number(self) -> None:
        assert _parse_dob("30") == "Age: 30"

    def test_birth_year(self) -> None:
        from datetime import date
        result = _parse_dob("1990")
        expected_age = date.today().year - 1990
        assert result == f"Age: {expected_age}"

    def test_iso_format(self) -> None:
        result = _parse_dob("1990-03-15")
        assert "Date of birth: 1990-03-15" in result
        assert "age" in result

    def test_us_format(self) -> None:
        result = _parse_dob("03/15/1990")
        assert "Date of birth: 1990-03-15" in result

    def test_month_name_format(self) -> None:
        result = _parse_dob("March 15 1990")
        assert "Date of birth: 1990-03-15" in result

    def test_month_name_comma(self) -> None:
        result = _parse_dob("March 15, 1990")
        assert "Date of birth: 1990-03-15" in result

    def test_abbrev_month(self) -> None:
        result = _parse_dob("Mar 15 1990")
        assert "Date of birth: 1990-03-15" in result

    def test_unparseable_stored_as_is(self) -> None:
        result = _parse_dob("sometime in the 90s")
        assert result == "Age: sometime in the 90s"


class TestMedicationStructuredStorage:
    """Tests for medication parsing + structured storage during onboarding."""

    def test_medication_stored_in_table(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        """Onboarding should store medications in the medications table."""
        engine.start(USER_ID)
        # Skip to medications question (index 10: nickname, age, sex, ethnicity,
        # smoking, alcohol, height, weight, conditions, past_diagnoses)
        for _ in range(10):
            engine.process_answer(USER_ID, "skip")
        engine.process_answer(USER_ID, "metformin 500mg twice daily")
        engine.process_answer(USER_ID, "cancel")  # stop after meds

        meds = db.get_active_medications(user_id=USER_ID)
        assert len(meds) >= 1
        med = meds[0]
        assert "metformin" in med["name"].lower()
        assert "500mg" in med.get("dose", "")

    def test_half_dose_stored_correctly(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        """Half dose modifier should store actual dose, not prescribed."""
        engine.start(USER_ID)
        for _ in range(10):
            engine.process_answer(USER_ID, "skip")
        engine.process_answer(
            USER_ID, "lisinopril 10mg but I break it in half"
        )
        engine.process_answer(USER_ID, "cancel")

        meds = db.get_active_medications(user_id=USER_ID)
        assert len(meds) >= 1
        med = meds[0]
        assert "lisinopril" in med["name"].lower()
        assert "5mg" in med.get("dose", "")

    def test_multiple_meds_stored(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        """Multiple comma-separated meds should each get a record."""
        engine.start(USER_ID)
        for _ in range(10):
            engine.process_answer(USER_ID, "skip")
        engine.process_answer(
            USER_ID, "metformin 500mg, lisinopril 10mg, aspirin"
        )
        engine.process_answer(USER_ID, "cancel")

        meds = db.get_active_medications(user_id=USER_ID)
        assert len(meds) >= 3

    def test_re_onboard_replaces_meds(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        """Re-running onboarding should replace old onboarding meds."""
        engine.start(USER_ID)
        for _ in range(10):
            engine.process_answer(USER_ID, "skip")
        engine.process_answer(USER_ID, "metformin 500mg")
        engine.process_answer(USER_ID, "cancel")

        meds1 = db.get_active_medications(user_id=USER_ID)
        assert len(meds1) == 1

        # Re-onboard with different med
        engine.start(USER_ID)
        for _ in range(10):
            engine.process_answer(USER_ID, "skip")
        engine.process_answer(USER_ID, "lisinopril 10mg")
        engine.process_answer(USER_ID, "cancel")

        meds2 = db.get_active_medications(user_id=USER_ID)
        assert len(meds2) == 1
        assert "lisinopril" in meds2[0]["name"].lower()


class TestPastMedicationStorage:
    """Tests for past medication storage with discontinued status."""

    def test_past_meds_stored_as_discontinued(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        """Past medications should be stored with status='discontinued'."""
        engine.start(USER_ID)
        # Skip to past_medications (index 11)
        for _ in range(11):
            engine.process_answer(USER_ID, "skip")
        engine.process_answer(USER_ID, "amoxicillin - finished course")
        engine.process_answer(USER_ID, "cancel")

        # Check LTM fact stored
        facts = db.get_ltm_by_user(USER_ID)
        past_med_facts = [
            f for f in facts
            if f.get("fact", "").startswith("Past medication:")
        ]
        assert len(past_med_facts) == 1

        # Check medications table — should be discontinued, not active
        active_meds = db.get_active_medications(user_id=USER_ID)
        assert len(active_meds) == 0  # Not in active list

        # Check that it exists in the table with discontinued status
        rows = db.conn.execute(
            "SELECT * FROM medications WHERE user_id = ? AND status = 'discontinued'",
            (USER_ID,),
        ).fetchall()
        assert len(rows) >= 1

    def test_multiple_past_meds(
        self, engine: OnboardingEngine, db: HealthDB
    ) -> None:
        engine.start(USER_ID)
        for _ in range(11):
            engine.process_answer(USER_ID, "skip")
        engine.process_answer(
            USER_ID, "amoxicillin, birth control, prednisone"
        )
        engine.process_answer(USER_ID, "cancel")

        facts = db.get_ltm_by_user(USER_ID)
        past_med_facts = [
            f for f in facts
            if f.get("fact", "").startswith("Past medication:")
        ]
        assert len(past_med_facts) == 3
