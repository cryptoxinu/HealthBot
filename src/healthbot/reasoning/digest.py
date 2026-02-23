"""Daily health digest builder.

Assembles a morning summary from: wearable data, active alerts,
overdue screenings, drug-lab interactions, and active hypotheses.
All logic is deterministic. No LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass
class DigestReport:
    """Assembled daily health digest."""

    wearable_summary: str = ""
    alerts: list[str] = field(default_factory=list)
    overdue: list[str] = field(default_factory=list)
    drug_lab_flags: list[str] = field(default_factory=list)
    hypotheses: list[str] = field(default_factory=list)
    medications: list[str] = field(default_factory=list)
    treatment_reports: list[str] = field(default_factory=list)
    side_effect_alerts: list[str] = field(default_factory=list)
    pending_retests: list[str] = field(default_factory=list)
    supplement_recs: list[str] = field(default_factory=list)
    screening_due: list[str] = field(default_factory=list)
    stress_level: str = ""
    training_guidance: str = ""
    goal_progress: list[str] = field(default_factory=list)
    comorbidity_insights: list[str] = field(default_factory=list)
    workouts: list[str] = field(default_factory=list)
    workout_streak: int = 0


def build_daily_digest(db: HealthDB, user_id: int) -> DigestReport:
    """Build a daily health digest from current data.

    Gathers:
    - Latest wearable data (WHOOP/Oura, last 1 day)
    - Health watcher alerts (overdue, trends)
    - Drug-lab interaction findings
    - Active hypotheses
    - Active medications
    """
    report = DigestReport()

    # 1. Latest wearable data
    try:
        wearables = db.query_wearable_daily(limit=1, user_id=user_id)
        if wearables:
            w = wearables[0]
            parts = []
            dt = w.get("_date", w.get("date", ""))
            hrv = w.get("hrv", "")
            rhr = w.get("rhr", "")
            recovery = w.get("recovery_score", "")
            sleep = w.get("sleep_score", "")
            if hrv:
                parts.append(f"HRV: {hrv}ms")
            if rhr:
                parts.append(f"RHR: {rhr}bpm")
            if recovery:
                parts.append(f"Recovery: {recovery}")
            if sleep:
                parts.append(f"Sleep: {sleep}")
            if parts:
                report.wearable_summary = f"{dt}: {', '.join(parts)}"
    except Exception as e:
        logger.debug("Digest wearable: %s", e)

    # 2. Health watcher alerts
    try:
        from healthbot.reasoning.watcher import HealthWatcher
        watcher = HealthWatcher(db, user_id=user_id)
        alerts = watcher.check_all()
        for alert in alerts:
            icon = {"urgent": "!", "watch": "~", "info": ""}.get(alert.severity, "")
            report.alerts.append(f"{icon} {alert.title}: {alert.body}")
    except Exception as e:
        logger.debug("Digest alerts: %s", e)

    # 3. Drug-lab interaction flags
    try:
        from healthbot.reasoning.interactions import InteractionChecker
        checker = InteractionChecker(db)
        dl_results = checker.check_drug_lab(user_id=user_id)
        for r in dl_results:
            lab_display = r.lab_name.replace("_", " ").title()
            if r.lab_value and r.lab_flag:
                flag = "HIGH" if r.lab_flag == "H" else "LOW"
                report.drug_lab_flags.append(
                    f"{r.med_name} -> {lab_display}: {r.lab_value} ({flag})"
                )
    except Exception as e:
        logger.debug("Digest drug-lab: %s", e)

    # 4. Active hypotheses
    try:
        hyps = db.get_active_hypotheses(user_id)
        for h in hyps:
            title = h.get("title", "")
            conf = h.get("confidence", h.get("_confidence", 0))
            report.hypotheses.append(f"{title} ({conf:.0%})")
    except Exception as e:
        logger.debug("Digest hypotheses: %s", e)

    # 5. Condition-based lab recommendations
    try:
        from healthbot.reasoning.lab_recommendations import recommend_labs
        recs = recommend_labs(db, user_id)
        for r in recs[:5]:
            if r.months_since == -1:
                report.overdue.append(
                    f"* {r.test_name}: {r.reason}"
                )
            else:
                report.overdue.append(
                    f"! {r.test_name}: {r.months_since}mo overdue ({r.reason})"
                )
    except Exception as e:
        logger.debug("Digest lab recommendations: %s", e)

    # 6. Active medications
    try:
        meds = db.get_active_medications(user_id=user_id)
        for med in meds:
            name = med.get("name", "")
            dose = med.get("dose", "")
            freq = med.get("frequency", "")
            if name:
                report.medications.append(f"{name} {dose} {freq}".strip())
    except Exception as e:
        logger.debug("Digest meds: %s", e)

    # 7. Side effect monitoring
    try:
        from healthbot.reasoning.side_effect_monitor import SideEffectMonitor
        se_monitor = SideEffectMonitor(db)
        se_alerts = se_monitor.check_active_concerns(user_id=user_id)
        for sa in se_alerts:
            marker = sa.lab_marker.replace("_", " ").title()
            flag_text = "HIGH" if sa.lab_flag in ("H", "HH") else "LOW"
            report.side_effect_alerts.append(
                f"! {sa.med_name}: {sa.effect} "
                f"({marker} {flag_text}: {sa.lab_value})"
            )
    except Exception as e:
        logger.debug("Digest side effects: %s", e)

    # 8. Retest reminders
    try:
        from healthbot.reasoning.retest_scheduler import RetestScheduler
        rs = RetestScheduler(db)
        retests = rs.get_pending_retests(user_id=user_id)
        for rt in retests[:5]:
            flag_text = "HIGH" if rt.abnormal_flag.upper() in ("H", "HH") else "LOW"
            if rt.days_until_due < 0:
                timing = f"{abs(rt.days_until_due)}d overdue"
            else:
                timing = f"due in {rt.days_until_due}d"
            icon = "!" if rt.priority == "urgent" else "*"
            report.pending_retests.append(
                f"{icon} {rt.display_name} ({flag_text}): "
                f"retest {timing} — {rt.reason}"
            )
    except Exception as e:
        logger.debug("Digest retests: %s", e)

    # 9. Treatment effectiveness
    try:
        from healthbot.reasoning.treatment_tracker import TreatmentTracker
        tracker = TreatmentTracker(db)
        t_reports = tracker.assess_all(user_id=user_id)
        for tr in t_reports:
            if tr.verdict in ("insufficient", "worsening"):
                label = tr.biomarker.replace("_", " ").title()
                report.treatment_reports.append(
                    f"! {tr.med_name} -> {label}: {tr.verdict} "
                    f"({tr.pct_change:+.1f}% vs expected {tr.expected_pct:+.1f}%)"
                )
            elif tr.verdict in ("effective", "very_effective"):
                label = tr.biomarker.replace("_", " ").title()
                report.treatment_reports.append(
                    f"+ {tr.med_name} -> {label}: {tr.pct_change:+.1f}%"
                )
    except Exception as e:
        logger.debug("Digest treatment: %s", e)

    # 10. Supplement recommendations
    try:
        from healthbot.reasoning.supplement_protocols import SupplementAdvisor
        advisor = SupplementAdvisor(db)
        recs = advisor.get_recommendations(user_id=user_id)
        for rec in recs:
            report.supplement_recs.append(
                f"* {rec.protocol.supplement_name}: {rec.recommended_dose} "
                f"(current {rec.current_value} {rec.protocol.unit})"
            )
    except Exception as e:
        logger.debug("Digest supplements: %s", e)

    # 11. Screening due
    try:
        from healthbot.reasoning.screening_calendar import ScreeningCalendar
        cal = ScreeningCalendar(db)
        screenings = cal.get_due_screenings(user_id=user_id)
        for s in screenings[:5]:
            report.screening_due.append(
                f"* {s.guideline.name}: {s.status} "
                f"({s.guideline.source})"
            )
    except Exception as e:
        logger.debug("Digest screenings: %s", e)

    # 12. Stress level
    try:
        from healthbot.reasoning.stress_detector import StressDetector
        detector = StressDetector(db)
        assessment = detector.assess(user_id=user_id)
        if assessment.stress_level in ("high", "critical"):
            rec_texts = [
                r.recommendation for r in assessment.recommendations[:2]
            ]
            report.stress_level = (
                f"{assessment.stress_level.upper()} stress "
                f"(score {assessment.stress_score * 100:.0f}/100). "
                f"{'; '.join(rec_texts)}"
            )
    except Exception as e:
        logger.debug("Digest stress: %s", e)

    # 13. Training guidance
    try:
        from healthbot.reasoning.recovery_readiness import (
            RecoveryReadinessEngine,
        )
        rr = RecoveryReadinessEngine(db)
        guidance = rr.get_training_guidance(user_id=user_id)
        if guidance:
            report.training_guidance = (
                f"Recovery {guidance.readiness_score:.0f}/100 "
                f"({guidance.grade}): "
                f"{guidance.zone.name} — "
                f"target strain {guidance.zone.strain_target}"
            )
    except Exception as e:
        logger.debug("Digest training: %s", e)

    # 14. Goal progress
    try:
        from healthbot.reasoning.goals import GoalTracker
        gt = GoalTracker(db)
        progress = gt.check_progress(user_id=user_id)
        for gp in progress:
            icon = "+" if gp.status == "achieved" else "*"
            report.goal_progress.append(f"{icon} {gp.message}")
    except Exception as e:
        logger.debug("Digest goals: %s", e)

    # 15. Comorbidity insights
    try:
        from healthbot.reasoning.comorbidity import ComorbidityAnalyzer
        ca = ComorbidityAnalyzer(db)
        findings = ca.analyze(user_id=user_id)
        for f in findings[:3]:
            report.comorbidity_insights.append(
                f"* {f.interaction.condition_a} <-> "
                f"{f.interaction.condition_b}: "
                f"{f.interaction.clinical_implication}"
            )
    except Exception as e:
        logger.debug("Digest comorbidity: %s", e)

    # 16. Recent workouts (last 24h) + streak
    try:
        from datetime import UTC, datetime, timedelta
        yesterday = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")
        workouts = db.query_workouts(
            start_after=yesterday, user_id=user_id, limit=10,
        )
        for w in workouts:
            sport = w.get("sport_type", w.get("_sport_type", ""))
            dur = w.get("duration_minutes")
            cal = w.get("calories_burned")
            label = sport.replace("_", " ").title()
            parts = [label]
            if dur:
                parts.append(f"{float(dur):.0f}min")
            if cal:
                parts.append(f"{float(cal):.0f}cal")
            report.workouts.append(" | ".join(parts))

        # Workout streak
        summary = db.get_workout_summary(days=30, user_id=user_id)
        report.workout_streak = summary.get("streak_days", 0)
    except Exception as e:
        logger.debug("Digest workouts: %s", e)

    return report


def build_quick_summary(db: HealthDB, user_id: int) -> str:
    """Build a concise 3-5 line health status summary.

    Lightweight alternative to the full digest — shows only the most
    actionable items. Used by /summary, NL status checks, and briefings.
    """
    parts: list[str] = []

    # Top issues (urgent/critical alerts)
    try:
        from healthbot.reasoning.watcher import HealthWatcher
        watcher = HealthWatcher(db, user_id=user_id)
        alerts = watcher.check_all()
        urgent = [a for a in alerts if a.severity in ("urgent", "critical")]
        if urgent:
            items = [a.title for a in urgent[:2]]
            parts.append(f"Alerts: {', '.join(items)}")
    except Exception as e:
        logger.debug("Quick summary alerts: %s", e)

    # Active medications
    try:
        meds = db.get_active_medications(user_id=user_id)
        if meds:
            names = [m.get("name", "") for m in meds if m.get("name")]
            parts.append(f"Meds ({len(names)}): {', '.join(names[:5])}")
    except Exception as e:
        logger.debug("Quick summary meds: %s", e)

    # Overdue labs (top 2)
    try:
        from healthbot.reasoning.lab_recommendations import recommend_labs
        recs = recommend_labs(db, user_id)
        overdue = [r for r in recs if r.months_since > 0]
        if overdue:
            items = [f"{r.test_name} ({r.months_since}mo)" for r in overdue[:2]]
            parts.append(f"Overdue: {', '.join(items)}")
    except Exception as e:
        logger.debug("Quick summary overdue: %s", e)

    # Latest wearable snapshot
    try:
        wearables = db.query_wearable_daily(limit=1, user_id=user_id)
        if wearables:
            w = wearables[0]
            bits = []
            if w.get("hrv"):
                bits.append(f"HRV {w['hrv']}ms")
            if w.get("rhr"):
                bits.append(f"RHR {w['rhr']}bpm")
            if w.get("recovery_score"):
                bits.append(f"Recovery {w['recovery_score']}")
            if bits:
                parts.append(f"Wearable: {', '.join(bits)}")
    except Exception as e:
        logger.debug("Quick summary wearable: %s", e)

    if not parts:
        return ""

    return "HEALTH STATUS\n" + "\n".join(parts)


def format_digest(report: DigestReport) -> str:
    """Format a DigestReport into a Telegram message."""
    sections: list[str] = []
    sections.append("DAILY HEALTH DIGEST")
    sections.append("=" * 25)

    if report.wearable_summary:
        sections.append(f"\nWearable Data:\n  {report.wearable_summary}")

    if report.medications:
        sections.append("\nActive Medications:")
        for m in report.medications:
            sections.append(f"  {m}")

    if report.overdue:
        sections.append("\nRecommended Labs:")
        for o in report.overdue:
            sections.append(f"  {o}")

    if report.drug_lab_flags:
        sections.append("\nDrug-Lab Flags:")
        for f in report.drug_lab_flags:
            sections.append(f"  {f}")

    if report.alerts:
        sections.append("\nHealth Alerts:")
        for a in report.alerts:
            sections.append(f"  {a}")

    if report.hypotheses:
        sections.append("\nActive Hypotheses:")
        for h in report.hypotheses:
            sections.append(f"  {h}")

    if report.side_effect_alerts:
        sections.append("\nSide Effect Signals:")
        for s in report.side_effect_alerts:
            sections.append(f"  {s}")

    if report.pending_retests:
        sections.append("\nPending Retests:")
        for r in report.pending_retests:
            sections.append(f"  {r}")

    if report.treatment_reports:
        sections.append("\nTreatment Effectiveness:")
        for t in report.treatment_reports:
            sections.append(f"  {t}")

    if report.supplement_recs:
        sections.append("\nSupplement Recommendations:")
        for s in report.supplement_recs:
            sections.append(f"  {s}")

    if report.screening_due:
        sections.append("\nScreenings Due:")
        for s in report.screening_due:
            sections.append(f"  {s}")

    if report.stress_level:
        sections.append(f"\nStress: {report.stress_level}")

    if report.training_guidance:
        sections.append(f"\nTraining: {report.training_guidance}")

    if report.goal_progress:
        sections.append("\nGoal Progress:")
        for g in report.goal_progress:
            sections.append(f"  {g}")

    if report.comorbidity_insights:
        sections.append("\nComorbidity Insights:")
        for c in report.comorbidity_insights:
            sections.append(f"  {c}")

    if report.workouts:
        sections.append("\nRecent Workouts:")
        for w in report.workouts:
            sections.append(f"  {w}")
        if report.workout_streak > 1:
            sections.append(f"  Streak: {report.workout_streak} consecutive days")

    if not any([
        report.wearable_summary, report.medications, report.overdue,
        report.drug_lab_flags, report.alerts, report.hypotheses,
        report.treatment_reports, report.side_effect_alerts,
        report.pending_retests, report.supplement_recs,
        report.screening_due, report.stress_level,
        report.training_guidance, report.goal_progress,
        report.comorbidity_insights, report.workouts,
        report.workout_streak,
    ]):
        sections.append("\nAll clear. No active alerts or findings.")

    return "\n".join(sections)
