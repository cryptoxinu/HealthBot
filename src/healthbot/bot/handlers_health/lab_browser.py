"""Labs and profile command handlers."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import format_score_bar, paginate
from healthbot.bot.middleware import rate_limited, require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class LabBrowserMixin:
    """Mixin for labs and profile browsing commands."""

    @rate_limited(max_per_minute=10)
    @require_unlocked
    async def labs(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /labs command — quick lab record retrieval.

        /labs                  → latest complete panel summary
        /labs <test_name>      → history of that test
        /labs --pdf            → also send the source PDF
        """
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            args = context.args or []

            send_pdf = "--pdf" in args
            test_filter = [
                a for a in args if not a.startswith("--")
            ]
            test_name = " ".join(test_filter).strip().lower() if test_filter else ""

            if test_name:
                await self._labs_test_history(
                    update, db, uid, test_name, send_pdf,
                )
            else:
                await self._labs_latest_panel(
                    update, db, uid, send_pdf,
                )

    async def _labs_latest_panel(
        self,
        update: Update,
        db,
        uid: int,
        send_pdf: bool,
    ) -> None:
        """Show most recent complete panel (all tests from latest date)."""
        labs = db.query_observations(
            record_type="lab_result", limit=30, user_id=uid,
        )
        if not labs:
            await update.message.reply_text(
                "No lab results in your records yet. Upload a PDF to get started.",
            )
            return

        # Group by date — show latest date's results
        by_date: dict[str, list[dict]] = {}
        for lab in labs:
            dt = lab.get("date_collected", "")
            if dt:
                by_date.setdefault(dt, []).append(lab)

        if not by_date:
            await update.message.reply_text("No dated lab results found.")
            return

        latest_date = max(by_date.keys())
        panel = by_date[latest_date]

        # Format
        source_doc = ""
        lines = [f"Latest Labs ({latest_date}):"]
        for lab in panel:
            name = lab.get("test_name", "")
            val = lab.get("value", "")
            unit = lab.get("unit", "")
            ref_lo = lab.get("reference_low", "")
            ref_hi = lab.get("reference_high", "")
            flag = lab.get("flag", "")

            ref = f" ({ref_lo}-{ref_hi})" if ref_lo and ref_hi else ""
            status = ""
            if flag and flag.upper().startswith("H"):
                status = " HIGH"
            elif flag and flag.upper().startswith("L"):
                status = " LOW"

            lines.append(f"  {name}: {val} {unit}{ref}{status}")
            if not source_doc:
                meta = lab.get("_meta", {})
                source_doc = meta.get("source_doc_id", "")

        for page in paginate("\n".join(lines)):
            await update.message.reply_text(page)

        if send_pdf and source_doc:
            await self._send_source_pdf(update, source_doc)

    async def _labs_test_history(
        self,
        update: Update,
        db,
        uid: int,
        test_name: str,
        send_pdf: bool,
    ) -> None:
        """Show history of a specific test."""
        from healthbot.normalize.lab_normalizer import normalize_test_name

        canonical = normalize_test_name(test_name)
        labs = db.query_observations(
            record_type="lab_result",
            canonical_name=canonical,
            limit=20,
            user_id=uid,
        )
        if not labs:
            await update.message.reply_text(
                f"No results found for '{test_name}'.",
            )
            return

        lines = [f"History: {labs[0].get('test_name', canonical)}"]
        source_doc = ""
        for lab in labs:
            dt = lab.get("date_collected", "")
            val = lab.get("value", "")
            unit = lab.get("unit", "")
            ref_lo = lab.get("reference_low", "")
            ref_hi = lab.get("reference_high", "")
            flag = lab.get("flag", "")
            ref = f" ({ref_lo}-{ref_hi})" if ref_lo and ref_hi else ""
            flag_str = f" [{flag}]" if flag else ""
            lines.append(f"  {dt}: {val} {unit}{ref}{flag_str}")
            if not source_doc:
                meta = lab.get("_meta", {})
                source_doc = meta.get("source_doc_id", "")

        for page in paginate("\n".join(lines)):
            await update.message.reply_text(page)

        if send_pdf and source_doc:
            await self._send_source_pdf(update, source_doc)

    @rate_limited(max_per_minute=10)
    @require_unlocked
    async def profile(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /profile command — one-view health profile."""

        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            user_id = update.effective_user.id
            lines = ["YOUR HEALTH PROFILE", "=" * 30, ""]

            # 1. Demographics & Conditions (from LTM)
            conditions: list[dict] = []
            try:
                ltm_facts = db.get_ltm_by_user(user_id)
                demographics = [f for f in ltm_facts if f.get("category") == "demographic"]
                conditions = [f for f in ltm_facts if f.get("category") == "condition"]
                if demographics:
                    lines.append("DEMOGRAPHICS:")
                    for f in demographics:
                        lines.append(f"  - {f.get('fact', '')}")
                    lines.append("")
            except Exception as e:
                logger.debug("Profile (demographics): %s", e)

            # 2. Conditions & Active Hypotheses
            try:
                if conditions:
                    lines.append("CONDITIONS:")
                    for f in conditions:
                        lines.append(f"  - {f.get('fact', '')}")
                    lines.append("")
            except Exception as e:
                logger.debug("Profile (conditions): %s", e)

            hyps = db.get_active_hypotheses(user_id)
            if hyps:
                lines.append("ACTIVE HYPOTHESES:")
                for h in hyps:
                    title = h.get("title", "Unknown")
                    conf = h.get("confidence", h.get("_confidence", 0))
                    lines.append(f"  - {title} ({conf:.0%})")
                    missing = h.get("missing_tests", [])
                    if missing:
                        lines.append(f"    Missing tests: {', '.join(missing)}")
                lines.append("")

            # 3. Medications
            try:
                meds = db.get_active_medications(user_id=user_id)
                if meds:
                    lines.append("ACTIVE MEDICATIONS:")
                    for m in meds:
                        name = m.get("name", "Unknown")
                        dose = m.get("dose", "")
                        freq = m.get("frequency", "")
                        parts = [name]
                        if dose:
                            parts.append(dose)
                        if freq:
                            parts.append(freq)
                        lines.append(f"  - {' '.join(parts)}")
                    lines.append("")
            except Exception as e:
                logger.debug("Profile (medications): %s", e)

            # 4. Domain Scores
            from healthbot.reasoning.insights import InsightEngine
            from healthbot.reasoning.trends import TrendAnalyzer
            analyzer = TrendAnalyzer(db)
            engine = InsightEngine(db, self._core._triage, analyzer)
            scores = engine.compute_domain_scores(user_id=user_id)
            if scores:
                lines.append("DOMAIN SCORES:")
                for s in scores:
                    bar = format_score_bar(s.score)
                    lines.append(f"  {s.label}: {bar} {s.score:.0f}/100")
                    if s.issues:
                        for issue in s.issues[:2]:
                            lines.append(f"    - {issue}")
                lines.append("")

            # 5. Top Trends
            trends = analyzer.detect_all_trends(user_id=user_id)
            if trends:
                lines.append("TOP TRENDS:")
                arrow = {"increasing": "^", "decreasing": "v", "stable": "-"}
                for t in trends[:5]:
                    sym = arrow.get(t.direction, "")
                    lines.append(
                        f"  {sym} {t.test_name}: {t.first_value:.1f} -> "
                        f"{t.last_value:.1f} ({t.pct_change:+.1f}%)"
                    )
                lines.append("")

            # 6. Overdue Screenings
            try:
                from healthbot.reasoning.overdue import OverdueDetector
                detector = OverdueDetector(db)
                overdue = detector.check_overdue(user_id=user_id)
                if overdue:
                    lines.append("OVERDUE SCREENINGS:")
                    for o in overdue[:5]:
                        lines.append(
                            f"  - {o.test_name}: {o.days_overdue} days overdue "
                            f"(last: {o.last_date})"
                        )
                    lines.append("")
            except Exception as e:
                logger.debug("Profile (overdue): %s", e)

            # 7. Correlations
            try:
                from healthbot.reasoning.correlate import CorrelationEngine
                corr_engine = CorrelationEngine(db)
                corrs = corr_engine.auto_discover(user_id=user_id)
                if corrs:
                    lines.append("LAB/WEARABLE CORRELATIONS:")
                    for c in corrs[:3]:
                        lines.append(
                            f"  - {c.metric_a} vs {c.metric_b}: "
                            f"r={c.pearson_r:.2f} ({c.interpretation})"
                        )
                    lines.append("")
            except Exception as e:
                logger.debug("Profile (correlations): %s", e)

        for page in paginate("\n".join(lines)):
            await update.message.reply_text(page)

        # Send radar chart
        try:
            from healthbot.export.chart_generator import profile_radar_chart
            chart_bytes = profile_radar_chart(scores)
            if chart_bytes:
                import io
                img = io.BytesIO(chart_bytes)
                img.name = "health_profile.png"
                await update.message.reply_photo(photo=img)
        except Exception as e:
            logger.debug("Profile radar chart skipped: %s", e)
