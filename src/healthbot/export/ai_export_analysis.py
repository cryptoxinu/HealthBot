"""Analysis section builders for AI health data export.

Wearable, trend, delta, interaction, and gap analysis sections.
Split from ai_export_sections.py to stay under 400 lines per file.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta

logger = logging.getLogger("healthbot")


def build_wearables(db, user_id: int, report) -> str:
    today = date.today()
    week_ago = (today - timedelta(days=7)).isoformat()
    month_ago = (today - timedelta(days=30)).isoformat()
    year_ago = (today - timedelta(days=365)).isoformat()
    weekly = db.query_wearable_daily(start_date=week_ago, user_id=user_id, limit=7)
    monthly = db.query_wearable_daily(start_date=month_ago, user_id=user_id, limit=30)
    yearly = db.query_wearable_daily(start_date=year_ago, user_id=user_id, limit=365)
    if not monthly:
        notes = _build_wearable_connection_status()
        if notes:
            return f"No recent wearable data.\n\n{notes}"
        return "No wearable data available."

    lines: list[str] = []
    if not weekly and monthly:
        lines.append(
            "**Note:** No data in the last 7 days. "
            "Device may be disconnected or not syncing.\n"
        )

    if weekly:
        lines.append("### Last 7 Days\n")
        lines.append("| Date | HRV | RHR | Sleep | Recovery | Strain |")
        lines.append("|------|-----|-----|-------|----------|--------|")
        for day in weekly:
            d = day.get("_date", "")
            hrv = day.get("hrv")
            rhr = day.get("rhr")
            slp = day.get("sleep_score")
            rec = day.get("recovery_score")
            strain = day.get("strain")
            lines.append(
                f"| {d}"
                f" | {f'{hrv:.0f}' if hrv is not None else '-'}"
                f" | {f'{rhr:.0f}' if rhr is not None else '-'}"
                f" | {f'{slp:.0f}' if slp is not None else '-'}"
                f" | {f'{rec:.0f}%' if rec is not None else '-'}"
                f" | {f'{strain:.1f}' if strain is not None else '-'} |"
            )

    if monthly:
        lines.append("\n### 30-Day Averages\n")
        metrics = {
            "HRV (ms)": [d.get("hrv") for d in monthly if d.get("hrv") is not None],
            "RHR (bpm)": [d.get("rhr") for d in monthly if d.get("rhr") is not None],
            "Sleep Score": [
                d.get("sleep_score") for d in monthly
                if d.get("sleep_score") is not None
            ],
            "Recovery (%)": [
                d.get("recovery_score") for d in monthly
                if d.get("recovery_score") is not None
            ],
            "Strain": [
                d.get("strain") for d in monthly
                if d.get("strain") is not None
            ],
            "Sleep (min)": [
                d.get("sleep_duration_min") for d in monthly
                if d.get("sleep_duration_min") is not None
            ],
        }
        for label, vals in metrics.items():
            if vals:
                avg = sum(vals) / len(vals)
                lines.append(f"- **{label}**: {avg:.1f} (n={len(vals)})")

    if yearly and len(yearly) > 30:
        by_month: dict[str, list[dict]] = defaultdict(list)
        for day in yearly:
            d = day.get("_date") or day.get("date", "")
            if d:
                by_month[str(d)[:7]].append(day)
        if len(by_month) > 1:
            lines.append("\n### Monthly Averages (Past Year)\n")
            lines.append("| Month | HRV | RHR | Sleep | Recovery | Strain |")
            lines.append("|-------|-----|-----|-------|----------|--------|")

            def _month_avg(rows: list[dict], key: str) -> str:
                vals = [d.get(key) for d in rows if d.get(key) is not None]
                return f"{sum(vals) / len(vals):.0f}" if vals else "-"

            for mk in sorted(by_month):
                dm = by_month[mk]
                lines.append(
                    f"| {mk} | {_month_avg(dm, 'hrv')} | {_month_avg(dm, 'rhr')}"
                    f" | {_month_avg(dm, 'sleep_score')} | {_month_avg(dm, 'recovery_score')}"
                    f" | {_month_avg(dm, 'strain')} |"
                )

    _append_wearable_trends(lines, db, user_id)
    _append_wearable_anomalies(lines, db, user_id)
    _append_recovery_readiness(lines, db, user_id)
    return "\n".join(lines)


def _append_wearable_trends(lines: list[str], db, user_id: int) -> None:
    try:
        from healthbot.reasoning.wearable_trends import WearableTrendAnalyzer
        wt = WearableTrendAnalyzer(db)
        w_trends = wt.detect_all_trends(days=365, user_id=user_id)
        if w_trends:
            lines.append("\n### Wearable Trends (Past Year)\n")
            arrows = {"increasing": "\u2191", "decreasing": "\u2193", "stable": "\u2192"}
            for t in w_trends:
                arrow = arrows.get(t.direction, "?")
                lines.append(
                    f"- {arrow} **{t.display_name}**: "
                    f"{t.first_value:.0f} \u2192 {t.last_value:.0f} "
                    f"({t.pct_change:+.1f}%) over {t.data_points} days"
                )
    except Exception as e:
        logger.warning("Wearable trend analysis failed: %s", e)


def _append_wearable_anomalies(lines: list[str], db, user_id: int) -> None:
    try:
        from healthbot.reasoning.wearable_trends import WearableTrendAnalyzer
        wa = WearableTrendAnalyzer(db)
        anomalies = wa.detect_anomalies(days=7, user_id=user_id)
        if anomalies:
            lines.append("\n### Recent Anomalies\n")
            for a in anomalies:
                lines.append(f"- [{a.date}] **{a.display_name}** {a.message}")
    except Exception as e:
        logger.warning("Wearable anomaly detection failed: %s", e)


def _append_recovery_readiness(lines: list[str], db, user_id: int) -> None:
    try:
        from healthbot.reasoning.recovery_readiness import RecoveryReadinessEngine
        engine = RecoveryReadinessEngine(db)
        recovery = engine.compute(user_id=user_id)
        if recovery:
            lines.append("\n### Recovery Readiness\n")
            lines.append(f"- **Score**: {recovery.score:.0f}/100 ({recovery.grade})")
            lines.append(f"- **Recommendation**: {recovery.recommendation}")
            if recovery.limiting_factors:
                lines.append(
                    f"- **Limiting factors**: {'; '.join(recovery.limiting_factors)}"
                )
    except Exception as e:
        logger.warning("Recovery readiness failed: %s", e)


def _build_wearable_connection_status() -> str:
    lines: list[str] = []
    try:
        from healthbot.security.keychain import Keychain
        kc = Keychain()
        for name, key, auth_cmd in [
            ("WHOOP", "whoop_client_id", "/whoop_auth"),
            ("Oura Ring", "oura_client_id", "/oura_auth"),
        ]:
            stored = kc.retrieve(key)
            if stored and (" " in stored or len(stored) < 8):
                lines.append(
                    f"- {name}: BROKEN — credentials corrupted. "
                    f"User should run {auth_cmd} reset to fix."
                )
            elif stored:
                lines.append(
                    f"- {name}: Credentials present but no recent "
                    f"data. User should run /sync to pull data."
                )
            else:
                lines.append(
                    f"- {name}: Not connected. "
                    f"User can run {auth_cmd} to set up."
                )
    except Exception:
        pass
    if lines:
        return "### Connection Status\n" + "\n".join(lines)
    return ""


def build_delta(db, user_id: int, report) -> str:
    try:
        from healthbot.reasoning.delta import DeltaEngine
        engine = DeltaEngine(db)
        delta = engine.compute_delta(user_id=user_id)
    except Exception as e:
        logger.warning("Delta engine failed in export: %s", e)
        return "Delta analysis unavailable."

    if not delta or not delta.items:
        return "No comparable lab panels for delta analysis."

    icons = {
        "improving": "+", "worsening": "!", "stable": "=",
        "new": "*", "resolved": "-", "increasing": "^", "decreasing": "v",
    }
    lines: list[str] = [
        f"Comparing **{delta.current_date}** vs **{delta.previous_date}**\n",
        "| Test | Previous | Current | Change | Status |",
        "|------|----------|---------|--------|--------|",
    ]
    for item in delta.items:
        icon = icons.get(item.status, "?")
        prev = f"{item.previous_value}" if item.previous_value is not None else "-"
        curr = f"{item.current_value}" if item.current_value is not None else "-"
        if item.status == "new":
            change = "NEW"
        elif item.status == "resolved":
            change = "not in latest"
        else:
            change = f"{item.change_pct:+.1f}%"
        lines.append(
            f"| {item.test_name} | {prev} | {curr} {item.unit} "
            f"| {change} | [{icon}] {item.status} |"
        )
    return "\n".join(lines)


def build_therapeutic_response(db, user_id: int, report) -> str:
    try:
        from healthbot.reasoning.interactions import InteractionChecker
        checker = InteractionChecker(db)
        results = checker.check_therapeutic_response(user_id=user_id)
    except Exception as e:
        logger.warning("Therapeutic response check failed in export: %s", e)
        return "Medication-lab correlation unavailable."

    if not results:
        return "No significant medication-lab correlations detected."

    lines: list[str] = []
    for r in results:
        lab_display = r.test_name.replace("_", " ").title()
        direction = "dropped" if r.change_pct < 0 else "rose"
        lines.append(
            f"- **{r.med_name}** \u2192 {lab_display} {direction} "
            f"{abs(r.change_pct):.1f}% "
            f"({r.before_value} \u2192 {r.after_value}) "
            f"over {r.days_after_start} days"
        )
    return "\n".join(lines)


def build_trends(db, user_id: int, report) -> str:
    try:
        from healthbot.reasoning.trends import TrendAnalyzer
        analyzer = TrendAnalyzer(db)
        results = analyzer.detect_all_trends(months=24, user_id=user_id)
    except Exception as e:
        logger.warning("Trend analysis failed in export: %s", e)
        return "Trend analysis unavailable."

    if not results:
        return "No significant lab trends detected."

    arrows = {"increasing": "\u2191", "decreasing": "\u2193", "stable": "\u2192"}
    lines: list[str] = []
    for t in results:
        arrow = arrows.get(t.direction, "?")
        lines.append(
            f"- {arrow} **{t.canonical_name}**: {t.first_value} \u2192 {t.last_value} "
            f"({t.pct_change:+.1f}%) over {t.data_points} results "
            f"({t.first_date} to {t.last_date})"
        )
        if t.age_context:
            lines.append(f"  {t.age_context}")
    return "\n".join(lines)


def build_interactions(db, user_id: int, report) -> str:
    try:
        from healthbot.reasoning.interactions import InteractionChecker
        checker = InteractionChecker(db)
        results = checker.check_drug_lab(user_id=user_id)
    except Exception as e:
        logger.warning("Interaction check failed in export: %s", e)
        return "Drug-lab interaction check unavailable."

    if not results:
        return "No active drug-lab interactions."

    flagged: list[str] = []
    monitoring: list[str] = []
    for r in results:
        desc = r.interaction.description if hasattr(r.interaction, "description") else ""
        if r.lab_flag:
            flagged.append(
                f"- **{r.med_name}** \u2192 {r.lab_name}: {r.lab_value} "
                f"({r.lab_flag}) \u2014 {desc}"
            )
        else:
            monitoring.append(
                f"- **{r.med_name}** \u2192 {r.lab_name}: {r.lab_value or 'not tested'} "
                f"\u2014 {desc}"
            )

    lines: list[str] = []
    if flagged:
        lines.append("### Active Findings")
        lines.extend(flagged)
    if monitoring:
        if flagged:
            lines.append("")
        lines.append("### Monitoring")
        lines.extend(monitoring)
    return "\n".join(lines)


def build_intelligence_gaps(db, user_id: int, report) -> str:
    try:
        from healthbot.reasoning.intelligence_auditor import IntelligenceAuditor
        auditor = IntelligenceAuditor(db)
        demo = db.get_user_demographics(user_id)
        gaps = auditor.audit(user_id=user_id, demographics=demo or {})
    except Exception as e:
        logger.warning("Intelligence audit failed in export: %s", e)
        return "Intelligence gap audit unavailable."

    if not gaps:
        return "No intelligence gaps detected."

    priority_order = {"high": 0, "medium": 1, "low": 2}
    gaps.sort(key=lambda g: priority_order.get(g.priority, 1))
    lines: list[str] = []
    for g in gaps:
        lines.append(f"- [{g.priority.upper()}] {g.description}")
        if g.related_tests:
            lines.append(f"  Tests needed: {', '.join(g.related_tests)}")
    return "\n".join(lines)


def build_panel_gaps(db, user_id: int, report) -> str:
    try:
        from healthbot.reasoning.panel_gaps import PanelGapDetector
        detector = PanelGapDetector(db)
        gap_report = detector.detect(user_id=user_id)
    except Exception as e:
        logger.warning("Panel gap detection failed in export: %s", e)
        return "Panel gap detection unavailable."

    if not gap_report.has_gaps:
        return "All common panels complete."

    lines: list[str] = []
    if gap_report.panel_gaps:
        lines.append("### Incomplete Panels")
        for pg in gap_report.panel_gaps:
            present = ", ".join(pg.present) if pg.present else "none"
            missing = ", ".join(pg.missing) if pg.missing else "none"
            lines.append(f"- **{pg.panel_name}**: have {present} \u2014 missing: {missing}")
    if gap_report.conditional_gaps:
        if gap_report.panel_gaps:
            lines.append("")
        lines.append("### Conditional Follow-ups")
        for cg in gap_report.conditional_gaps:
            missing = ", ".join(cg.missing_tests)
            lines.append(
                f"- {cg.trigger_value} ({cg.trigger_flag}) \u2192 consider: {missing}"
            )
    return "\n".join(lines)
