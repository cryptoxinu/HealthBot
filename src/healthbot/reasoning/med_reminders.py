"""Medication reminder schedule and checking.

Checks user's configured reminders against current time.
Includes timing advice from the interaction knowledge base.
Auto-pauses reminders when related lab tests come back normal,
and auto-resumes when the retest window opens.
Deterministic — no LLM.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


# --- Test ↔ Medication association mapping ---

_TEST_MED_ASSOCIATIONS: dict[str, list[str]] = {
    "tsh": ["levothyroxine", "synthroid", "armour thyroid", "liothyronine"],
    "hba1c": ["metformin", "insulin", "ozempic", "mounjaro", "glipizide"],
    "inr": ["warfarin", "coumadin"],
    "lithium": ["lithium"],
    "vitamin_d": ["vitamin d", "cholecalciferol", "ergocalciferol"],
    "iron": ["iron", "ferrous sulfate", "ferrous gluconate"],
    "ferritin": ["iron", "ferrous sulfate", "ferrous gluconate"],
    "b12": ["b12", "cyanocobalamin", "methylcobalamin"],
    "vitamin_b12": ["b12", "cyanocobalamin", "methylcobalamin"],
    "potassium": ["potassium", "k-dur", "klor-con"],
    "creatinine": ["metformin", "lisinopril", "losartan"],
    "alt": ["statin", "atorvastatin", "rosuvastatin", "simvastatin"],
    "ast": ["statin", "atorvastatin", "rosuvastatin", "simvastatin"],
    "testosterone": ["testosterone", "trt"],
    "estradiol": ["estradiol", "estrogen"],
    "prolactin": ["risperidone", "haloperidol"],
}


def _med_to_tests() -> dict[str, list[str]]:
    """Build reverse lookup: medication name -> list of monitoring tests."""
    reverse: dict[str, list[str]] = {}
    for test, meds in _TEST_MED_ASSOCIATIONS.items():
        for med in meds:
            med_lower = med.lower()
            if med_lower not in reverse:
                reverse[med_lower] = []
            if test not in reverse[med_lower]:
                reverse[med_lower].append(test)
    return reverse


_MED_TO_TESTS = _med_to_tests()


# --- Default retest intervals (weeks) for normal results ---
# Used when RetestScheduler doesn't have a specific rule.

_DEFAULT_RETEST_WEEKS: dict[str, int] = {
    "tsh": 26,           # 6 months
    "hba1c": 12,         # 3 months
    "inr": 4,            # 1 month
    "lithium": 12,       # 3 months
    "vitamin_d": 12,     # 3 months
    "iron": 12,          # 3 months
    "ferritin": 12,      # 3 months
    "b12": 26,           # 6 months
    "vitamin_b12": 26,   # 6 months
    "potassium": 4,      # 1 month
    "creatinine": 12,    # 3 months
    "alt": 12,           # 3 months
    "ast": 12,           # 3 months
    "testosterone": 12,  # 3 months
    "estradiol": 12,     # 3 months
    "prolactin": 12,     # 3 months
}

_DEFAULT_RETEST_WEEKS_FALLBACK = 26  # 6 months default


@dataclass
class MedReminder:
    """A medication reminder with timing notes."""

    med_name: str
    time: str  # HH:MM
    notes: str


def get_due_reminders(
    db: HealthDB, user_id: int, current_time: datetime | None = None,
) -> list[MedReminder]:
    """Get reminders that are due right now (within +-1 minute window).

    Skips paused reminders.
    """
    if current_time is None:
        current_time = datetime.now()

    now_hhmm = current_time.strftime("%H:%M")
    reminders = db.get_med_reminders(user_id)

    due: list[MedReminder] = []
    for r in reminders:
        # Skip paused reminders
        if r.get("paused_reason"):
            continue
        reminder_time = r.get("_time", "")
        if reminder_time == now_hhmm:
            due.append(MedReminder(
                med_name=r.get("med_name", ""),
                time=reminder_time,
                notes=r.get("notes", ""),
            ))
    return due


def get_timing_notes(med_name: str) -> str:
    """Get timing/administration notes for a medication.

    Uses the interaction KB timing rules where available.
    """
    try:
        from healthbot.reasoning.interaction_kb import TIMING_RULES
        lower = med_name.lower()
        for rule in TIMING_RULES:
            if lower in rule.substance.lower() or rule.substance.lower() in lower:
                return rule.advice
    except Exception:
        pass
    return ""


def format_reminder(reminder: MedReminder) -> str:
    """Format a reminder for Telegram display."""
    msg = f"Time for your {reminder.med_name}."
    if reminder.notes:
        msg += f"\n{reminder.notes}"
    timing = get_timing_notes(reminder.med_name)
    if timing:
        msg += f"\nTip: {timing}"
    return msg


def format_reminder_list(reminders: list[dict]) -> str:
    """Format all active reminders for display, showing paused state."""
    if not reminders:
        return (
            "No medication reminders set.\n"
            "Set one: /remind <medication> <HH:MM>"
        )

    lines = ["Active Medication Reminders:", ""]
    for r in reminders:
        name = r.get("med_name", "")
        time = r.get("_time", "")
        notes = r.get("notes", "")
        paused_reason = r.get("paused_reason")
        resume_after = r.get("resume_after")

        line = f"  {time} — {name}"
        if notes and not paused_reason:
            line += f" ({notes})"
        lines.append(line)

        if paused_reason:
            resume_str = ""
            if resume_after:
                try:
                    rd = date.fromisoformat(resume_after)
                    resume_str = f" Resumes ~{rd.strftime('%b %d')}."
                except ValueError:
                    resume_str = ""
            lines.append(
                f"  \u23f8 PAUSED — {paused_reason}.{resume_str}"
            )

    lines.append("\nDisable: /remind off <medication>")
    return "\n".join(lines)


def _get_retest_weeks(test_name: str) -> int:
    """Get retest interval in weeks for a test from RetestScheduler or defaults."""
    try:
        from healthbot.reasoning.retest_scheduler import _RULE_INDEX

        # Check for "any" condition first, then specific
        for condition in ("any", "high", "low"):
            rule = _RULE_INDEX.get((test_name, condition))
            if rule:
                return rule.retest_weeks_max
    except Exception:
        pass
    return _DEFAULT_RETEST_WEEKS.get(test_name, _DEFAULT_RETEST_WEEKS_FALLBACK)


def _is_normal_result(obs: dict) -> bool:
    """Check if an observation is within normal range.

    Conservative: only returns True if we can confirm normal.
    """
    flag = obs.get("flag", "")
    triage = obs.get("_meta", {}).get("triage_level", "")

    # If explicitly flagged abnormal, not normal
    if flag and flag.upper() in ("H", "HH", "L", "LL", "A", "AA"):
        return False

    # If triage says abnormal, not normal
    if triage in ("urgent", "watch"):
        return False

    # Check reference ranges if available
    value = obs.get("value")
    ref_low = obs.get("reference_low")
    ref_high = obs.get("reference_high")

    if value is not None and ref_low is not None and ref_high is not None:
        try:
            v = float(value)
            lo = float(ref_low)
            hi = float(ref_high)
            return lo <= v <= hi
        except (ValueError, TypeError):
            pass

    # If no flag and no reference range issue, treat as normal
    if not flag:
        return True

    return False


def review_reminders_after_ingestion(
    db: HealthDB, user_id: int, ingested_tests: set[str],
) -> list[str]:
    """Auto-pause reminders when related tests arrive normal; schedule auto-resume.

    Returns list of human-readable messages about what changed.
    """
    messages: list[str] = []

    try:
        reminders = db.get_med_reminders(user_id)
    except Exception:
        return messages

    if not reminders:
        return messages

    today = date.today()

    for reminder in reminders:
        # Skip already-paused reminders
        if reminder.get("paused_reason"):
            continue

        med_name = reminder.get("med_name", "")
        med_lower = med_name.lower()

        # Find which monitoring tests apply to this medication
        monitoring_tests = _MED_TO_TESTS.get(med_lower, [])
        if not monitoring_tests:
            # Try partial matching (e.g., "atorvastatin 40mg" matches "atorvastatin")
            for known_med, tests in _MED_TO_TESTS.items():
                if known_med in med_lower or med_lower in known_med:
                    monitoring_tests = tests
                    break

        if not monitoring_tests:
            continue

        # Check if any monitoring test was just ingested
        matched_test = None
        for test in monitoring_tests:
            if test in ingested_tests:
                matched_test = test
                break

        if not matched_test:
            continue

        # Get the most recent observation for this test
        try:
            obs_list = db.query_observations(
                record_type="lab_result",
                canonical_name=matched_test,
                user_id=user_id,
                limit=1,
            )
        except Exception:
            continue

        if not obs_list:
            continue

        obs = obs_list[0]

        # Only pause if result is normal
        if not _is_normal_result(obs):
            continue

        # Calculate resume date
        retest_weeks = _get_retest_weeks(matched_test)
        resume_date = today + timedelta(weeks=retest_weeks)
        resume_iso = resume_date.isoformat()

        # Format the pause reason
        test_display = matched_test.replace("_", " ").upper()
        paused_reason = f"{test_display} normal ({today.strftime('%-m/%-d')})"

        # Pause the reminder
        if db.pause_med_reminder(user_id, med_name, paused_reason, resume_iso):
            resume_display = resume_date.strftime("%b %d")
            messages.append(
                f"Paused {med_name} reminder — {paused_reason}. "
                f"Will resume around {resume_display}."
            )
            logger.info(
                "Auto-paused reminder %s: %s (resume %s)",
                med_name, paused_reason, resume_iso,
            )

    return messages


def check_reminder_resumes(db: HealthDB, user_id: int) -> list[str]:
    """Resume paused reminders whose resume_after date has passed.

    Called daily by the scheduler.
    Returns list of human-readable messages about resumed reminders.
    """
    messages: list[str] = []

    try:
        paused = db.get_paused_reminders(user_id)
    except Exception:
        return messages

    today = date.today()

    for reminder in paused:
        resume_after = reminder.get("resume_after")
        if not resume_after:
            continue

        try:
            resume_date = date.fromisoformat(resume_after)
        except ValueError:
            continue

        if resume_date <= today:
            med_name = reminder.get("med_name", "")
            if db.resume_med_reminder(user_id, med_name):
                messages.append(
                    f"Resumed {med_name} reminder — retest is now due."
                )
                logger.info("Auto-resumed reminder: %s", med_name)

    return messages
