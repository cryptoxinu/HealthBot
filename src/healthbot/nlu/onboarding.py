"""Structured health profile onboarding -- deterministic multi-turn interview.

Asks one question at a time, stores each answer as LTM fact(s).
Does NOT use LLM for question generation or data extraction (security invariant).
Re-runnable: uses source="onboarding:<key>" to upsert on subsequent runs.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")

SESSION_TIMEOUT = 1800  # 30 minutes


@dataclass
class OnboardingQuestion:
    """A single question in the onboarding flow."""

    key: str  # Unique ID: "age", "sex", "conditions", ...
    prompt: str  # Text shown to user
    category: str  # LTM category: demographic, condition, medication, preference
    fact_template: str  # Template with {answer} placeholder
    multi_value: bool = False  # True = split comma/semicolon answers


ONBOARDING_QUESTIONS: list[OnboardingQuestion] = [
    OnboardingQuestion(
        key="nickname",
        prompt="What's your nickname? (Just for chat — I won't store your real name.)",
        category="demographic",
        fact_template="Nickname: {answer}",
    ),
    OnboardingQuestion(
        key="age",
        prompt=(
            "How old are you? (or birth year)\n"
            "Used for age-appropriate reference ranges.\n"
            "(e.g., 34, or 1990)"
        ),
        category="demographic",
        fact_template="{answer}",  # Handled by _parse_dob
    ),
    OnboardingQuestion(
        key="sex",
        prompt="What is your biological sex? (male / female / other)",
        category="demographic",
        fact_template="Biological sex: {answer}",
    ),
    OnboardingQuestion(
        key="ethnicity",
        prompt=(
            "What is your race/ethnicity?\n"
            "(e.g., White, Black/African American, Hispanic/Latino, "
            "Asian, Middle Eastern, Native American, Pacific Islander, Mixed)\n"
            "This affects reference ranges for some lab tests."
        ),
        category="demographic",
        fact_template="Ethnicity: {answer}",
    ),
    OnboardingQuestion(
        key="smoking",
        prompt=(
            "Do you smoke or use nicotine?\n"
            "(never / former / current)"
        ),
        category="demographic",
        fact_template="Smoking status: {answer}",
    ),
    OnboardingQuestion(
        key="alcohol",
        prompt=(
            "How much alcohol do you typically drink per week?\n"
            "(none / light 1-3 / moderate 4-7 / heavy 8+)"
        ),
        category="demographic",
        fact_template="Alcohol intake: {answer}",
    ),
    OnboardingQuestion(
        key="height",
        prompt="What is your height? (e.g., 5'10\" or 178 cm)",
        category="demographic",
        fact_template="Height: {answer}",
    ),
    OnboardingQuestion(
        key="weight",
        prompt="What is your weight? (e.g., 170 lbs or 77 kg)",
        category="demographic",
        fact_template="Weight: {answer}",
    ),
    OnboardingQuestion(
        key="conditions",
        prompt=(
            "Do you have any known medical conditions?\n"
            "(e.g., diabetes, hypertension, POTS, asthma)\n"
            "Type 'none' if none."
        ),
        category="condition",
        fact_template="Known condition: {answer}",
        multi_value=True,
    ),
    OnboardingQuestion(
        key="past_diagnoses",
        prompt=(
            "Any past diagnoses or conditions you've recovered from?\n"
            "(e.g., appendicitis, broken arm, past pneumonia)\n"
            "Type 'none' if none."
        ),
        category="condition",
        fact_template="Past diagnosis: {answer}",
        multi_value=True,
    ),
    OnboardingQuestion(
        key="medications",
        prompt=(
            "Are you currently taking any medications?\n"
            "Include name and dose if known (e.g., metformin 500mg, lisinopril 10mg).\n"
            "Type 'none' if none."
        ),
        category="medication",
        fact_template="Current medication: {answer}",
        multi_value=True,
    ),
    OnboardingQuestion(
        key="past_medications",
        prompt=(
            "Any medications you used to take but stopped?\n"
            "Include reason if known (e.g., amoxicillin - finished course).\n"
            "Type 'none' if none."
        ),
        category="medication",
        fact_template="Past medication: {answer}",
        multi_value=True,
    ),
    OnboardingQuestion(
        key="allergies",
        prompt=(
            "Do you have any known allergies?\n"
            "(medications, food, environmental)\n"
            "Type 'none' if none."
        ),
        category="condition",
        fact_template="Known allergy: {answer}",
        multi_value=True,
    ),
    OnboardingQuestion(
        key="health_goals",
        prompt=(
            "What are your health goals?\n"
            "(e.g., lose weight, manage blood sugar, improve sleep, reduce inflammation)"
        ),
        category="preference",
        fact_template="Health goal: {answer}",
        multi_value=True,
    ),
    OnboardingQuestion(
        key="family_history",
        prompt=(
            "Any significant family health history?\n"
            "(e.g., heart disease, cancer, diabetes in parents/siblings)\n"
            "Type 'none' if none."
        ),
        category="condition",
        fact_template="Family history: {answer}",
        multi_value=True,
    ),
]

_SKIP_WORDS = frozenset({"skip", "none", "n/a", "na", "no", "-"})
_CANCEL_WORDS = frozenset({"cancel", "quit", "stop", "exit"})

_DOB_FORMATS = [
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%Y-%m-%d",
    "%B %d %Y",
    "%B %d, %Y",
    "%b %d %Y",
    "%b %d, %Y",
    "%d %B %Y",
    "%d %b %Y",
]


def _parse_dob(raw: str) -> str:
    """Parse age or birth year from user input, return formatted LTM fact.

    Accepts: plain age (34), birth year (1990), or full date formats.
    Returns "Age: N" for plain numbers/years, or "Date of birth: YYYY-MM-DD (age N)".
    """
    text = raw.strip()

    # Plain number → age or birth year
    if text.isdigit():
        val = int(text)
        if 1900 <= val <= date.today().year:
            # Birth year → calculate approximate age
            age = date.today().year - val
            return f"Age: {age}"
        return f"Age: {text}"

    # Try known date formats
    for fmt in _DOB_FORMATS:
        try:
            dt = datetime.strptime(text, fmt).date()
            return _format_dob_fact(dt)
        except ValueError:
            continue

    # Fallback: store as-is
    return f"Age: {text}"


def _format_dob_fact(dob: date) -> str:
    """Format a parsed DOB as an LTM fact with calculated age."""
    today = date.today()
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return f"Date of birth: {dob.isoformat()} (age {age})"


@dataclass
class OnboardingSession:
    """Tracks a user's progress through the onboarding flow."""

    user_id: int
    current_index: int = 0
    answers: dict[str, str] = field(default_factory=dict)
    stored_fact_ids: list[str] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)

    @property
    def is_complete(self) -> bool:
        return self.current_index >= len(ONBOARDING_QUESTIONS)

    @property
    def current_question(self) -> OnboardingQuestion | None:
        if self.is_complete:
            return None
        return ONBOARDING_QUESTIONS[self.current_index]

    def is_expired(self) -> bool:
        return (time.time() - self.started_at) > SESSION_TIMEOUT


class OnboardingEngine:
    """Manages the deterministic onboarding interview flow."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db
        self._sessions: dict[int, OnboardingSession] = {}

    def start(self, user_id: int) -> str:
        """Start or restart onboarding. Returns first question prompt."""
        self._sessions[user_id] = OnboardingSession(user_id=user_id)
        return self._format_question(0)

    def is_active(self, user_id: int) -> bool:
        """Check if user has an active, non-expired onboarding session."""
        session = self._sessions.get(user_id)
        if session is None:
            return False
        if session.is_complete or session.is_expired():
            self._sessions.pop(user_id, None)
            return False
        return True

    def process_answer(self, user_id: int, text: str) -> str:
        """Process user's answer to current question.

        Returns next question prompt, or a completion summary.
        """
        session = self._sessions.get(user_id)
        if session is None or session.is_complete:
            return "No active onboarding session. Start with /onboard."

        question = session.current_question
        if question is None:
            return self._format_summary(session)

        normalized = text.strip().lower()

        # Cancel
        if normalized in _CANCEL_WORDS:
            self._sessions.pop(user_id, None)
            return "Onboarding cancelled. Use /onboard to start again."

        # Skip
        if normalized in _SKIP_WORDS:
            session.current_index += 1
            if session.is_complete:
                return self._finish(session)
            return self._format_question(session.current_index)

        # Store answer
        self._store_answer(session, question, text.strip())
        session.answers[question.key] = text.strip()
        session.current_index += 1

        if session.is_complete:
            return self._finish(session)
        return self._format_question(session.current_index)

    def cancel(self, user_id: int) -> None:
        """Cancel an active onboarding session."""
        self._sessions.pop(user_id, None)

    def on_vault_lock(self) -> None:
        """Clear all sessions on vault lock."""
        self._sessions.clear()

    # --- Private helpers ---

    def _store_answer(
        self, session: OnboardingSession, question: OnboardingQuestion, text: str
    ) -> None:
        """Store answer as LTM fact(s). Deletes old onboarding facts for this key first."""
        self._delete_existing_facts(session.user_id, question.key)

        # Special handling for age question (accepts age, birth year, or full DOB)
        if question.key == "age":
            fact = _parse_dob(text)
            fact_id = self._db.insert_ltm(
                session.user_id,
                question.category,
                fact,
                source=f"onboarding:{question.key}",
            )
            session.stored_fact_ids.append(fact_id)
            return

        # Past medications: parse + store structured with discontinued status
        if question.key == "past_medications":
            self._store_past_medications(session, question, text)
            return

        # Medications: parse + store structured AND as LTM
        if question.key == "medications":
            self._store_medications(session, question, text)
            return

        if question.multi_value:
            items = _split_multi_value(text)
            for item in items:
                fact = question.fact_template.format(answer=item)
                fact_id = self._db.insert_ltm(
                    session.user_id,
                    question.category,
                    fact,
                    source=f"onboarding:{question.key}",
                )
                session.stored_fact_ids.append(fact_id)
        else:
            fact = question.fact_template.format(answer=text)
            fact_id = self._db.insert_ltm(
                session.user_id,
                question.category,
                fact,
                source=f"onboarding:{question.key}",
            )
            session.stored_fact_ids.append(fact_id)

    def _store_medications(
        self, session: OnboardingSession, question: OnboardingQuestion, text: str
    ) -> None:
        """Parse medication text, store as LTM facts AND structured medication records."""
        from healthbot.data.models import Medication
        from healthbot.nlu.medication_parser import parse_medication

        items = _split_multi_value(text)

        # Delete old onboarding medication records from medications table
        self._delete_onboarding_medications(session.user_id)

        for item in items:
            # Store as LTM (for conversational context)
            parsed = parse_medication(item)
            if parsed.modifier and parsed.actual_dose:
                fact = (
                    f"Current medication: {parsed.name} {parsed.prescribed_dose} "
                    f"({parsed.modifier} → actual {parsed.actual_dose})"
                )
                if parsed.frequency:
                    fact += f" {parsed.frequency}"
            else:
                fact = question.fact_template.format(answer=item)

            fact_id = self._db.insert_ltm(
                session.user_id,
                question.category,
                fact,
                source=f"onboarding:{question.key}",
            )
            session.stored_fact_ids.append(fact_id)

            # Store in structured medications table
            if parsed.name:
                med = Medication(
                    id="",  # auto-generated
                    name=parsed.name,
                    dose=parsed.actual_dose or parsed.prescribed_dose,
                    frequency=parsed.frequency,
                    status="active",
                    source_blob_id=f"onboarding:{session.user_id}",
                )
                self._db.insert_medication(med, user_id=session.user_id)

    def _store_past_medications(
        self, session: OnboardingSession, question: OnboardingQuestion, text: str
    ) -> None:
        """Parse past medication text, store as LTM facts AND discontinued medication records."""
        from healthbot.data.models import Medication
        from healthbot.nlu.medication_parser import parse_medication

        items = _split_multi_value(text)

        # Delete old onboarding past-medication records
        source_prefix = f"onboarding:past_meds:{session.user_id}"
        self._db.conn.execute(
            "DELETE FROM medications WHERE source_doc_id = ? AND user_id = ?",
            (source_prefix, session.user_id),
        )
        self._db.conn.commit()

        for item in items:
            # Store as LTM
            fact = question.fact_template.format(answer=item)
            fact_id = self._db.insert_ltm(
                session.user_id,
                question.category,
                fact,
                source=f"onboarding:{question.key}",
            )
            session.stored_fact_ids.append(fact_id)

            # Store in structured medications table as discontinued
            parsed = parse_medication(item)
            if parsed.name:
                med = Medication(
                    id="",
                    name=parsed.name,
                    dose=parsed.actual_dose or parsed.prescribed_dose,
                    frequency=parsed.frequency,
                    status="discontinued",
                    source_blob_id=source_prefix,
                )
                self._db.insert_medication(med, user_id=session.user_id)

    def _delete_onboarding_medications(self, user_id: int) -> None:
        """Delete medications previously inserted by onboarding for this user."""
        source_prefix = f"onboarding:{user_id}"
        self._db.conn.execute(
            "DELETE FROM medications WHERE source_doc_id = ? AND user_id = ?",
            (source_prefix, user_id),
        )
        self._db.conn.commit()

    def _delete_existing_facts(self, user_id: int, key: str) -> None:
        """Delete previous onboarding facts for this key (upsert support)."""
        source_prefix = f"onboarding:{key}"
        facts = self._db.get_ltm_by_user(user_id)
        for fact in facts:
            if fact.get("_source", "").startswith(source_prefix):
                self._db.delete_ltm(fact["_id"])

    def _finish(self, session: OnboardingSession) -> str:
        """Generate summary and clean up session."""
        summary = self._format_summary(session)
        self._sessions.pop(session.user_id, None)
        return summary

    def _format_question(self, index: int) -> str:
        """Format a question with progress indicator."""
        q = ONBOARDING_QUESTIONS[index]
        total = len(ONBOARDING_QUESTIONS)
        return f"[{index + 1}/{total}] {q.prompt}\n\n(Type 'skip' to skip, 'cancel' to stop)"

    def _format_summary(self, session: OnboardingSession) -> str:
        """Format completion summary."""
        lines = [
            "ONBOARDING COMPLETE",
            "=" * 25,
            "",
            f"Stored {len(session.stored_fact_ids)} fact(s) to your health profile.",
            "",
        ]
        for q in ONBOARDING_QUESTIONS:
            answer = session.answers.get(q.key)
            if answer:
                label = q.key.replace("_", " ").title()
                lines.append(f"  {label}: {answer}")
        lines.append("")
        lines.append("Use /profile to see your full health profile.")
        lines.append("You can re-run /onboard anytime to update.")
        return "\n".join(lines)


def _split_multi_value(text: str) -> list[str]:
    """Split comma/semicolon/and-separated values, filtering empties."""
    parts = re.split(r"[;,]|\band\b", text)
    return [
        p.strip()
        for p in parts
        if p.strip() and p.strip().lower() not in _SKIP_WORDS
    ]
