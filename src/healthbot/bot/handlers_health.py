"""Health analysis command handlers."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import format_score_bar, paginate
from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.middleware import require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")

# Wearable metric aliases → canonical field name in wearable_daily
_WEARABLE_ALIASES: dict[str, str] = {
    "hrv": "hrv",
    "rhr": "rhr",
    "sleep_score": "sleep_score",
    "recovery_score": "recovery_score",
    "strain": "strain",
    "sleep_duration_min": "sleep_duration_min",
    "spo2": "spo2",
    "skin_temp": "skin_temp",
    "resp_rate": "resp_rate",
    "deep_min": "deep_min",
    "rem_min": "rem_min",
    # User-friendly aliases
    "sleep": "sleep_score",
    "recovery": "recovery_score",
    "heart rate": "rhr",
    "heart_rate": "rhr",
    "resting heart rate": "rhr",
    "sleep duration": "sleep_duration_min",
    "deep sleep": "deep_min",
    "rem sleep": "rem_min",
    "respiratory rate": "resp_rate",
    "skin temperature": "skin_temp",
}


class HealthHandlers:
    """Health analysis and dashboard commands."""

    def __init__(self, core: HandlerCore) -> None:
        self._core = core

    @property
    def _km(self):
        return self._core._km

    def _check_auth(self, update: Update) -> bool:
        return self._core._check_auth(update)

    @require_unlocked
    async def memory(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /memory command — view or manage user memories.

        /memory                  — show all memories grouped by category
        /memory search <term>    — search memories by keyword
        /memory export           — send all memories as .txt file
        /memory clear <key>      — delete one entry
        /memory clear all        — delete all entries
        /memory corrections      — show corrections from Clean DB
        /memory improvements     — show system improvement suggestions
        /memory approve <id>     — approve a system improvement
        /memory reject <id>      — reject a system improvement
        """
        args = context.args or []

        clean_db = self._core._get_clean_db()
        if not clean_db:
            await update.message.reply_text(
                "Memory system not available. Run /sync first."
            )
            return

        try:
            if not args:
                await self._memory_show_all(update, clean_db)
            elif args[0].lower() == "clear":
                await self._memory_clear(update, args, clean_db)
            elif args[0].lower() == "corrections":
                await self._memory_corrections(update, clean_db)
            elif args[0].lower() == "improvements":
                await self._memory_improvements(update, clean_db)
            elif args[0].lower() == "search":
                await self._memory_search(update, args, clean_db)
            elif args[0].lower() == "export":
                await self._memory_export(update, clean_db)
            elif args[0].lower() in ("approve", "reject"):
                await self._memory_approve_reject(update, args, clean_db)
            else:
                await update.message.reply_text(
                    "Usage: /memory [clear|search|export|corrections|improvements|approve|reject]"
                )
        finally:
            clean_db.close()

    async def _memory_show_all(self, update: Update, clean_db) -> None:
        """Show all memories grouped by category."""
        memories = clean_db.get_user_memory()
        if not memories:
            await update.message.reply_text(
                "No memories stored yet. As we talk, I'll remember "
                "important facts about you."
            )
            return

        by_cat: dict[str, list[dict]] = {}
        for mem in memories:
            by_cat.setdefault(mem.get("category", "general"), []).append(mem)

        lines = ["YOUR STORED MEMORIES", "=" * 25, ""]
        for cat in sorted(by_cat.keys()):
            lines.append(f"{cat.replace('_', ' ').upper()}:")
            for mem in by_cat[cat]:
                conf = mem.get("confidence", 1.0)
                src = mem.get("source", "")
                marker = ""
                if conf < 0.9:
                    marker = f" (~{conf:.0%})"
                src_tag = f" [{src}]" if src else ""
                lines.append(f"  {mem['key']}: {mem['value']}{marker}{src_tag}")
            lines.append("")

        lines.append("To remove: /memory clear <key>")
        lines.append("To clear all: /memory clear all")

        for page in paginate("\n".join(lines)):
            await update.message.reply_text(page)

    async def _memory_clear(self, update: Update, args: list[str], clean_db) -> None:
        """Handle /memory clear <key> or /memory clear all."""
        if len(args) >= 2 and args[1].lower() == "all":
            count = clean_db.clear_all_user_memory()
            await update.message.reply_text(
                f"Cleared {count} memory entries."
            )
        elif len(args) >= 2:
            key = "_".join(args[1:]).lower()
            deleted = clean_db.delete_user_memory(key)
            if deleted:
                await update.message.reply_text(f"Deleted memory: {key}")
            else:
                await update.message.reply_text(
                    f"No memory found with key '{key}'."
                )
        else:
            await update.message.reply_text(
                "Usage: /memory clear <key> or /memory clear all"
            )

    async def _memory_search(
        self, update: Update, args: list[str], clean_db,
    ) -> None:
        """Handle /memory search <term> — case-insensitive keyword search."""
        if len(args) < 2:
            await update.message.reply_text(
                "Usage: /memory search <term>\n"
                "Example: /memory search supplement"
            )
            return

        term = " ".join(args[1:]).lower()
        memories = clean_db.get_user_memory()
        matches = [
            mem for mem in memories
            if term in mem.get("key", "").lower()
            or term in mem.get("value", "").lower()
            or term in mem.get("category", "").lower()
        ]

        if not matches:
            await update.message.reply_text(
                f"No memories matching '{term}'."
            )
            return

        by_cat: dict[str, list[dict]] = {}
        for mem in matches:
            by_cat.setdefault(mem.get("category", "general"), []).append(mem)

        lines = [f"MEMORIES MATCHING '{term}'", "=" * 25, ""]
        for cat in sorted(by_cat.keys()):
            lines.append(f"{cat.replace('_', ' ').upper()}:")
            for mem in by_cat[cat]:
                conf = mem.get("confidence", 1.0)
                src = mem.get("source", "")
                marker = ""
                if conf < 0.9:
                    marker = f" (~{conf:.0%})"
                src_tag = f" [{src}]" if src else ""
                lines.append(
                    f"  {mem['key']}: {mem['value']}{marker}{src_tag}"
                )
            lines.append("")

        lines.append(f"{len(matches)} result(s) found.")

        for page in paginate("\n".join(lines)):
            await update.message.reply_text(page)

    async def _memory_export(self, update: Update, clean_db) -> None:
        """Handle /memory export — send all memories as a .txt file."""
        import io
        from datetime import datetime

        memories = clean_db.get_user_memory()
        if not memories:
            await update.message.reply_text(
                "No memories to export."
            )
            return

        by_cat: dict[str, list[dict]] = {}
        for mem in memories:
            by_cat.setdefault(mem.get("category", "general"), []).append(mem)

        lines = [
            "HEALTHBOT MEMORY EXPORT",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"Total entries: {len(memories)}",
            "=" * 40,
            "",
        ]

        for cat in sorted(by_cat.keys()):
            lines.append(f"[{cat.replace('_', ' ').upper()}]")
            for mem in by_cat[cat]:
                conf = mem.get("confidence", 1.0)
                src = mem.get("source", "")
                created = (mem.get("created_at") or "")[:10]
                updated = (mem.get("updated_at") or "")[:10]

                lines.append(f"  Key: {mem['key']}")
                lines.append(f"  Value: {mem['value']}")
                if conf < 1.0:
                    lines.append(f"  Confidence: {conf:.0%}")
                if src:
                    lines.append(f"  Source: {src}")
                if created:
                    lines.append(f"  Created: {created}")
                if updated and updated != created:
                    lines.append(f"  Updated: {updated}")
                lines.append("")
            lines.append("")

        content = "\n".join(lines)
        doc = io.BytesIO(content.encode("utf-8"))
        doc.name = "healthbot_memories.txt"
        await update.message.reply_document(document=doc)

    def _build_memory_summary(self) -> list[str]:
        """Build a memory summary list for PDF reports."""
        items: list[str] = []
        try:
            clean_db = self._core._get_clean_db()
            if not clean_db:
                return items
            try:
                memories = clean_db.get_user_memory()
            finally:
                clean_db.close()
            if not memories:
                return items

            by_cat: dict[str, int] = {}
            for mem in memories:
                cat = mem.get("category", "general")
                by_cat[cat] = by_cat.get(cat, 0) + 1

            items.append(f"{len(memories)} stored memories:")
            for cat in sorted(by_cat.keys()):
                label = cat.replace("_", " ").title()
                items.append(f"  {label}: {by_cat[cat]}")
        except Exception as e:
            logger.debug("Memory summary for report: %s", e)
        return items

    async def _memory_corrections(self, update: Update, clean_db) -> None:
        """Show corrections stored in Clean DB."""
        async with TypingIndicator(update.effective_chat):
            corrections = clean_db.get_corrections(limit=20)

        if not corrections:
            await update.message.reply_text("No corrections recorded yet.")
            return

        lines = ["CORRECTIONS", "=" * 25, ""]
        for c in corrections:
            ts = (c.get("created_at") or "")[:10]
            original = c.get("original_claim", "")
            corrected = c.get("correction", "")
            source = c.get("source", "")
            lines.append(f"[{ts}] {source}")
            if original:
                lines.append(f"  Was: {original}")
            lines.append(f"  Now: {corrected}")
            lines.append("")

        for page in paginate("\n".join(lines)):
            await update.message.reply_text(page)

    async def _memory_improvements(self, update: Update, clean_db) -> None:
        """Show system improvement suggestions."""
        async with TypingIndicator(update.effective_chat):
            improvements = clean_db.get_system_improvements(limit=20)

        if not improvements:
            await update.message.reply_text(
                "No system improvement suggestions yet."
            )
            return

        lines = ["SYSTEM IMPROVEMENT SUGGESTIONS", "=" * 35, ""]
        for imp in improvements:
            imp_id = imp.get("id", "")[:8]
            status = imp.get("status", "open")
            area = imp.get("area", "")
            suggestion = imp.get("suggestion", "")
            priority = imp.get("priority", "low")
            ts = (imp.get("created_at") or "")[:10]

            status_icon = {"open": "[ ]", "approved": "[+]", "rejected": "[-]"}.get(
                status, f"[{status}]"
            )
            lines.append(f"{status_icon} [{imp_id}] {area} ({priority})")
            lines.append(f"  {suggestion}")
            lines.append(f"  {ts}")
            lines.append("")

        lines.append("To approve: /memory approve <id>")
        lines.append("To reject: /memory reject <id>")

        for page in paginate("\n".join(lines)):
            await update.message.reply_text(page)

    async def _memory_approve_reject(
        self, update: Update, args: list[str], clean_db,
    ) -> None:
        """Handle /memory approve <id> or /memory reject <id>."""
        action = args[0].lower()
        if len(args) < 2:
            await update.message.reply_text(
                f"Usage: /memory {action} <id>"
            )
            return

        partial_id = args[1].lower()
        new_status = "approved" if action == "approve" else "rejected"

        # Find matching improvement by prefix
        improvements = clean_db.get_system_improvements()
        match = None
        for imp in improvements:
            if imp["id"].startswith(partial_id):
                match = imp
                break

        if not match:
            await update.message.reply_text(
                f"No improvement found with ID starting with '{partial_id}'."
            )
            return

        clean_db.update_system_improvement_status(match["id"], new_status)
        area = match.get("area", "")
        await update.message.reply_text(
            f"Improvement {match['id'][:8]} ({area}) marked as {new_status}."
        )

    async def handle_improvement_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle inline keyboard callbacks for system improvement suggestions.

        Callback data format: si:approve:<id> or si:reject:<id>
        """
        query = update.callback_query
        await query.answer()
        data = query.data or ""

        parts = data.split(":", 2)
        if len(parts) != 3 or parts[0] != "si":
            await query.edit_message_text("Invalid callback data.")
            return

        action = parts[1]
        imp_id = parts[2]
        new_status = "approved" if action == "approve" else "rejected"

        clean_db = self._core._get_clean_db()
        if not clean_db:
            await query.edit_message_text("Memory system not available.")
            return

        try:
            updated = clean_db.update_system_improvement_status(imp_id, new_status)
        finally:
            clean_db.close()

        if updated:
            icon = "+" if new_status == "approved" else "-"
            original_text = query.message.text or ""
            await query.edit_message_text(
                f"[{icon}] {new_status.upper()}\n\n{original_text}"
            )
        else:
            await query.edit_message_text("Improvement not found.")

    @require_unlocked
    async def insights(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /insights command."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.reasoning.insights import InsightEngine
            from healthbot.reasoning.trends import TrendAnalyzer
            uid = update.effective_user.id
            engine = InsightEngine(db, self._core._triage, TrendAnalyzer(db))
            dashboard = engine.generate_dashboard(user_id=uid)
        for page in paginate(dashboard):
            await update.message.reply_text(page)
        # Send visual dashboard chart
        try:
            from healthbot.export.chart_generator import dashboard_chart
            scores = engine.compute_domain_scores(user_id=uid)
            chart_bytes = dashboard_chart(scores)
            if chart_bytes:
                import io
                img = io.BytesIO(chart_bytes)
                img.name = "health_dashboard.png"
                await update.message.reply_photo(photo=img)
        except Exception as e:
            logger.debug("Dashboard chart skipped: %s", e)

    @require_unlocked
    async def summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /summary — concise health status snapshot."""
        from healthbot.reasoning.digest import build_quick_summary

        db = self._core._get_db()
        uid = update.effective_user.id
        summary = build_quick_summary(db, uid)
        if summary:
            await update.message.reply_text(summary)
        else:
            await update.message.reply_text(
                "No health data yet. Upload a lab PDF or use /sync to get started."
            )

    @require_unlocked
    async def trend(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /trend <test_name> command."""

        args = context.args
        if not args:
            await update.message.reply_text(
                "Usage: /trend <test_name>\n"
                "Examples: /trend ldl, /trend hrv, /trend sleep"
            )
            return

        test_name = " ".join(args).lower()
        uid = update.effective_user.id

        # Check if this is a wearable metric
        wearable_metric = _WEARABLE_ALIASES.get(test_name)
        if wearable_metric:
            await self._trend_wearable(update, wearable_metric, uid)
            return

        # Fall through to lab trend analysis
        async with TypingIndicator(update.effective_chat):
            from healthbot.normalize.lab_normalizer import normalize_test_name
            canonical = normalize_test_name(test_name)

            db = self._core._get_db()
            from healthbot.reasoning.trends import TrendAnalyzer
            analyzer = TrendAnalyzer(db)
            result = analyzer.analyze_test(canonical, user_id=uid)

        if result:
            await update.message.reply_text(analyzer.format_trend(result))
            # Send visual chart
            try:
                from healthbot.export.chart_generator import trend_chart
                chart_bytes = trend_chart(result)
                if chart_bytes:
                    import io
                    img = io.BytesIO(chart_bytes)
                    img.name = f"trend_{canonical}.png"
                    await update.message.reply_photo(photo=img)
            except Exception as e:
                logger.debug("Chart generation skipped: %s", e)
        else:
            await update.message.reply_text(f"Not enough data for trend analysis on '{test_name}'.")

    async def _trend_wearable(
        self, update: Update, metric: str, user_id: int,
    ) -> None:
        """Wearable metric trend — full year with monthly averages + chart."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.reasoning.wearable_trends import (
                METRIC_DISPLAY_NAMES,
                WearableTrendAnalyzer,
            )

            analyzer = WearableTrendAnalyzer(db)
            result = analyzer.analyze_metric(metric, days=365, user_id=user_id)

        if not result:
            await update.message.reply_text(
                f"Not enough data for trend analysis on '{metric}'.\n"
                "Need at least 5 data points."
            )
            return

        display = METRIC_DISPLAY_NAMES.get(metric, metric.upper())
        arrows = {"increasing": "\u2191", "decreasing": "\u2193", "stable": "\u2192"}
        arrow = arrows.get(result.direction, "?")

        # Unit suffixes
        units = {"hrv": "ms", "rhr": "bpm", "spo2": "%", "strain": ""}
        unit = units.get(metric, "")

        lines = [
            f"{display.upper()} TREND ({result.data_points} days)",
            "\u2550" * 28,
            "",
            f"Direction: {arrow} {result.direction} ({result.pct_change:+.1f}%)",
            f"First: {result.first_value:.0f}{unit} ({result.first_date})",
            f"Latest: {result.last_value:.0f}{unit} ({result.last_date})",
        ]

        # Monthly averages from the data already fetched
        from collections import defaultdict
        by_month: dict[str, list[float]] = defaultdict(list)
        for date_str, val in result.values:
            month_key = date_str[:7]  # YYYY-MM
            by_month[month_key].append(val)

        if by_month:
            lines.append("")
            lines.append("Monthly averages:")
            for month_key in sorted(by_month):
                vals = by_month[month_key]
                avg = sum(vals) / len(vals)
                lines.append(f"  {month_key}: {avg:.0f}{unit} (n={len(vals)})")

        await update.message.reply_text("\n".join(lines))

        # Send visual chart (WearableTrendResult has same .values shape as TrendResult)
        try:
            from healthbot.export.chart_generator import trend_chart
            chart_bytes = trend_chart(result)
            if chart_bytes:
                import io
                img = io.BytesIO(chart_bytes)
                img.name = f"trend_{metric}.png"
                await update.message.reply_photo(photo=img)
        except Exception as e:
            logger.debug("Wearable chart generation skipped: %s", e)

    @require_unlocked
    async def ask(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /ask <question> command."""

        # Emergency keyword check first
        question = " ".join(context.args) if context.args else ""
        if not question:
            await update.message.reply_text("Usage: /ask <question>")
            return

        level, msg = self._core._triage.check_emergency_keywords(question)
        if level:
            await update.message.reply_text(f"EMERGENCY: {msg}")
            return

        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.security.vault import Vault
            vault = Vault(self._core._config.blobs_dir, self._core._km)
            from healthbot.retrieval.citation_manager import CitationManager
            from healthbot.retrieval.search import SearchEngine
            search = SearchEngine(self._core._config, db, vault)
            citations = CitationManager(db)

            results = search.search(question)
            if not results:
                await update.message.reply_text("No matching records found.")
                return

            lines = [f"Results for: {question}", ""]
            for r in results[:5]:
                cite = citations.cite_observation(r.record_id)
                cite_str = cite.format() if cite else ""
                lines.append(f"- {r.snippet[:150]} {cite_str}")
                lines.append(f"  Score: {r.score:.2f} | {r.date}")
                lines.append("")

        for page in paginate("\n".join(lines)):
            await update.message.reply_text(page)

    @require_unlocked
    async def overdue(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /overdue command."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.reasoning.overdue import OverdueDetector
            uid = update.effective_user.id
            detector = OverdueDetector(db)
            items = detector.check_overdue(user_id=uid)
        await update.message.reply_text(detector.format_reminders(items))

    @require_unlocked
    async def correlate(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /correlate command."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.reasoning.correlate import CorrelationEngine
            uid = update.effective_user.id
            engine = CorrelationEngine(db)
            corrs = engine.auto_discover(user_id=uid)
        await update.message.reply_text(engine.format_correlations(corrs))

    @require_unlocked
    async def gaps(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /gaps command -- detect lab panel gaps."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.reasoning.panel_gaps import PanelGapDetector
            detector = PanelGapDetector(db)
            uid = update.effective_user.id
            report = detector.detect(user_id=uid)
        await update.message.reply_text(detector.format_gaps(report))

    @require_unlocked
    async def symptoms(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /symptoms [category] — symptom frequency and analytics."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            from healthbot.reasoning.symptom_analytics import (
                SymptomAnalyzer,
                format_frequency,
                format_overview,
            )
            analyzer = SymptomAnalyzer(db)
            args = context.args
            if args:
                category = args[0].lower()
                freq = analyzer.frequency(uid, category)
                if freq:
                    text = format_frequency(freq)
                else:
                    text = f"No '{category}' symptoms logged in the last 90 days."
            else:
                overview = analyzer.overview(uid)
                text = format_overview(overview)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def recommend(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /recommend — condition-based lab test recommendations."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            from healthbot.reasoning.lab_recommendations import (
                format_recommendations,
                recommend_labs,
            )
            recs = recommend_labs(db, uid)
            text = format_recommendations(recs)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def sleeprec(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /sleeprec — evidence-based sleep optimization."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            from healthbot.reasoning.sleep_recommendations import (
                SleepRecommender,
                format_sleep_recommendations,
            )
            recommender = SleepRecommender(db)
            recs = recommender.get_recommendations(user_id=uid)

        text = format_sleep_recommendations(recs)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def stress(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /stress — wearable-based stress assessment."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            from healthbot.reasoning.stress_detector import (
                StressDetector,
                format_stress,
            )
            detector = StressDetector(db)
            assessment = detector.assess(user_id=uid)

        text = format_stress(assessment)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def screenings(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /screenings — preventive screening calendar."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            from healthbot.reasoning.screening_calendar import (
                ScreeningCalendar,
                format_screenings,
            )
            cal = ScreeningCalendar(db)
            due = cal.get_due_screenings(user_id=uid)

        text = format_screenings(due)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def goals(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /goals — health goal tracking.

        /goals                → show progress on all goals
        /goals add <metric> below|above <value>  → add a new goal
        /goals remove <index> → remove a goal
        """
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            args = context.args or []

            from healthbot.normalize.lab_normalizer import normalize_test_name
            from healthbot.reasoning.goals import GoalTracker, format_goals

            tracker = GoalTracker(db)

            if args and args[0].lower() == "add" and len(args) >= 4:
                # /goals add LDL below 100
                metric_raw = args[1]
                direction = args[2].lower()
                if direction not in ("below", "above"):
                    await update.message.reply_text(
                        "Direction must be 'below' or 'above'.\n"
                        "Example: /goals add LDL below 100",
                    )
                    return
                try:
                    target = float(args[3])
                except ValueError:
                    await update.message.reply_text("Target must be a number.")
                    return

                canonical = normalize_test_name(metric_raw)
                display = metric_raw.replace("_", " ").title()
                tracker.add_goal(uid, canonical, target, direction, display)
                await update.message.reply_text(
                    f"Goal set: {display} {direction} {target:.1f}",
                )
                return

            if args and args[0].lower() == "remove" and len(args) >= 2:
                # /goals remove <index>
                goals = tracker.get_goals(uid)
                try:
                    idx = int(args[1]) - 1
                    if 0 <= idx < len(goals):
                        goal = goals[idx]
                        tracker.remove_goal(goal.goal_id)
                        await update.message.reply_text(
                            f"Removed goal: {goal.display_name}",
                        )
                    else:
                        await update.message.reply_text("Invalid goal number.")
                except ValueError:
                    await update.message.reply_text("Usage: /goals remove <number>")
                return

            # Default: show progress
            progress = tracker.check_progress(uid)

        text = format_goals(progress)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def timeline(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /timeline — unified medical timeline.

        /timeline              → last 12 months, all categories
        /timeline <months>     → custom range (0 = all time)
        /timeline lab          → filter to labs only
        /timeline med          → filter to medications only
        /timeline symptom      → filter to symptoms only
        """
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            args = context.args or []

            from healthbot.reasoning.timeline import (
                TIMELINE_CATEGORIES,
                MedicalTimeline,
                format_timeline,
            )

            months = 12
            categories: set[str] | None = None

            cat_aliases = {
                "med": "medication", "meds": "medication",
                "labs": "lab", "doc": "document", "docs": "document",
                "hyp": "hypothesis", "symptoms": "symptom",
            }

            for arg in args:
                lower = arg.lower()
                # Check for month count
                try:
                    months = int(lower)
                    continue
                except ValueError:
                    pass
                # Check for category filter
                resolved = cat_aliases.get(lower, lower)
                if resolved in TIMELINE_CATEGORIES:
                    if categories is None:
                        categories = set()
                    categories.add(resolved)

            tl = MedicalTimeline(db)
            events = tl.build(
                user_id=uid, months=months,
                categories=categories, limit=100,
            )
            text = format_timeline(events)

        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def report(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /report — periodic health report.

        /report                → monthly report (last 30 days)
        /report weekly         → weekly report (last 7 days)
        /report monthly        → monthly report (last 30 days)
        """
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            args = context.args or []

            from healthbot.export.health_report import (
                HealthReportBuilder,
                format_report,
            )

            builder = HealthReportBuilder(db)
            period = args[0].lower() if args else "monthly"

            if period == "weekly":
                report = builder.build_weekly(uid)
            else:
                report = builder.build_monthly(uid)

            text = format_report(report)

        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def emergency(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /emergency — show emergency medical card."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id

            from healthbot.export.emergency_card import (
                EmergencyCardBuilder,
                format_emergency_card,
            )

            builder = EmergencyCardBuilder(db)
            card = builder.build(uid)
            text = format_emergency_card(card)

        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def healthreview(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /healthreview command."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.reasoning.delta import DeltaEngine
            from healthbot.reasoning.health_review import HealthReviewEngine
            from healthbot.reasoning.overdue import OverdueDetector
            from healthbot.reasoning.trends import TrendAnalyzer
            engine = HealthReviewEngine(
                db, self._core._triage, TrendAnalyzer(db), OverdueDetector(db), DeltaEngine(db)
            )
            uid = update.effective_user.id
            packet = engine.generate_review(user_id=uid)
        for page in paginate(engine.format_review(packet)):
            await update.message.reply_text(page)

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

    @require_unlocked
    async def aboutme(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /aboutme — evolving health profile summary.

        /aboutme          — show current summary
        /aboutme refresh  — regenerate AI narrative
        """
        import asyncio

        args = context.args or []
        force_refresh = args and args[0].lower() == "refresh"

        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            lines: list[str] = ["ABOUT YOU", "\u2550" * 20, ""]

            # --- BASICS ---
            demographics = db.get_user_demographics(uid)
            basics: list[str] = []
            nick = demographics.get("nickname")
            if nick:
                basics.append(f"  {nick}")

            age_parts: list[str] = []
            if demographics.get("age"):
                age_parts.append(f"Age {demographics['age']}")
            if demographics.get("sex"):
                age_parts.append(demographics["sex"].capitalize())
            if demographics.get("ethnicity"):
                age_parts.append(demographics["ethnicity"])
            if age_parts:
                basics.append("  " + " · ".join(age_parts))

            body_parts: list[str] = []
            h = demographics.get("height_m")
            w = demographics.get("weight_kg")
            bmi = demographics.get("bmi")
            if h:
                # Convert meters back to readable
                feet = int(h // 0.3048)
                inches = int((h % 0.3048) / 0.0254 + 0.5)
                body_parts.append(f"{feet}'{inches}\"")
            if w:
                lbs = int(w * 2.205 + 0.5)
                body_parts.append(f"{lbs} lbs")
            if bmi:
                body_parts.append(f"BMI {bmi}")
            if body_parts:
                basics.append("  " + " · ".join(body_parts))

            if basics:
                lines.append("BASICS")
                lines.extend(basics)
                lines.append("")

            # --- Gather LTM facts by category ---
            ltm_facts = db.get_ltm_by_user(uid)
            by_cat: dict[str, list[str]] = {}
            for f in ltm_facts:
                cat = f.get("_category", "")
                text = f.get("fact", "")
                if cat and text:
                    by_cat.setdefault(cat, []).append(text)

            # --- CONDITIONS ---
            active_conds = [
                t for t in by_cat.get("condition", [])
                if not t.lower().startswith("past diagnosis:")
                and not t.lower().startswith("family history:")
                and not t.lower().startswith("known allergy:")
            ]
            past_conds = [
                t for t in by_cat.get("condition", [])
                if t.lower().startswith("past diagnosis:")
            ]
            family = [
                t for t in by_cat.get("condition", [])
                if t.lower().startswith("family history:")
            ]

            has_conditions = active_conds or past_conds or family
            if has_conditions:
                lines.append("CONDITIONS")
                if active_conds:
                    vals = [t.replace("Known condition: ", "") for t in active_conds]
                    lines.append(f"  Active: {', '.join(vals)}")
                if past_conds:
                    vals = [t.replace("Past diagnosis: ", "") for t in past_conds]
                    lines.append(f"  Past: {', '.join(vals)}")
                if family:
                    vals = [t.replace("Family history: ", "") for t in family]
                    lines.append(f"  Family: {', '.join(vals)}")

                # Add hypotheses (suspected conditions)
                hyps = db.get_active_hypotheses(uid)
                if hyps:
                    titles = [
                        f"{h.get('title', '?')} ({h.get('confidence', 0):.0%})"
                        for h in hyps[:5]
                    ]
                    lines.append(f"  Suspected: {', '.join(titles)}")
                lines.append("")

            # --- MEDICATIONS ---
            meds = db.get_active_medications(user_id=uid)
            past_meds_ltm = [
                t for t in by_cat.get("medication", [])
                if t.lower().startswith("past medication:")
            ]
            if meds or past_meds_ltm:
                lines.append("MEDICATIONS")
                if meds:
                    parts = []
                    for m in meds:
                        name = m.get("name", "?")
                        dose = m.get("dose", "")
                        parts.append(f"{name} {dose}".strip())
                    lines.append(f"  Current: {', '.join(parts)}")
                if past_meds_ltm:
                    vals = [
                        t.replace("Past medication: ", "")
                        for t in past_meds_ltm
                    ]
                    lines.append(f"  Stopped: {', '.join(vals)}")
                lines.append("")

            # --- ALLERGIES ---
            allergies = [
                t for t in by_cat.get("condition", [])
                if t.lower().startswith("known allergy:")
            ]
            if allergies:
                vals = [t.replace("Known allergy: ", "") for t in allergies]
                lines.append("ALLERGIES")
                lines.append(f"  {', '.join(vals)}")
                lines.append("")

            # --- LIFESTYLE ---
            lifestyle: list[str] = []
            for t in by_cat.get("demographic", []):
                tl = t.lower()
                if tl.startswith("smoking status:"):
                    val = t.split(":", 1)[1].strip()
                    if val.lower() != "never":
                        lifestyle.append(f"Smoking: {val}")
                    else:
                        lifestyle.append("Non-smoker")
                elif tl.startswith("alcohol intake:"):
                    val = t.split(":", 1)[1].strip()
                    lifestyle.append(f"Alcohol: {val}")
            if lifestyle:
                lines.append("LIFESTYLE")
                lines.append("  " + " · ".join(lifestyle))
                lines.append("")

            # --- GOALS ---
            goals = by_cat.get("preference", [])
            if goals:
                vals = [t.replace("Health goal: ", "") for t in goals]
                lines.append("GOALS")
                lines.append(f"  {', '.join(vals)}")
                lines.append("")

            # --- CLAUDE'S MEMORY (from Clean DB) ---
            try:
                clean_db = self._core._get_clean_db()
                if clean_db:
                    try:
                        memories = clean_db.get_user_memory()
                    finally:
                        clean_db.close()
                    if memories:
                        mem_by_cat: dict[str, list[dict]] = {}
                        for mem in memories:
                            mem_by_cat.setdefault(
                                mem.get("category", "general"), [],
                            ).append(mem)
                        lines.append("WHAT I REMEMBER")
                        for cat in sorted(mem_by_cat.keys()):
                            items = mem_by_cat[cat]
                            cat_label = cat.replace("_", " ").title()
                            entries = []
                            for mem in items:
                                conf = mem.get("confidence", 1.0)
                                marker = f" (~{conf:.0%})" if conf < 0.9 else ""
                                entries.append(
                                    f"{mem['key'].replace('_', ' ')}: "
                                    f"{mem['value']}{marker}"
                                )
                            lines.append(f"  {cat_label}: {', '.join(entries)}")
                        lines.append("")
            except Exception as e:
                logger.debug("About me (memory): %s", e)

            # --- AI NARRATIVE ---
            # Check for stored AI summary
            existing_summary = None
            summary_age_hours = 999
            for f in ltm_facts:
                if f.get("_source", "") == "aboutme:summary":
                    existing_summary = f.get("fact", "")
                    try:
                        from datetime import UTC, datetime
                        updated = f.get("_updated_at", "")
                        if updated:
                            dt = datetime.fromisoformat(updated)
                            age_s = (datetime.now(UTC) - dt).total_seconds()
                            summary_age_hours = age_s / 3600
                    except Exception:
                        pass
                    break

            # Generate AI narrative if: forced refresh, no existing, or stale (>24h)
            should_generate = force_refresh or existing_summary is None or summary_age_hours > 24
            ai_narrative = existing_summary

            if should_generate:
                try:
                    conv = self._core._get_claude_conversation()
                    if conv:
                        # Build context for Claude
                        prompt = (
                            "Based on everything you know about me, write a brief "
                            "health narrative (3-5 sentences). Focus on:\n"
                            "- Key trends or changes in my labs\n"
                            "- Active concerns or patterns you've noticed\n"
                            "- What to watch or test next\n\n"
                            "Be direct, no hedging. Plain text only."
                        )
                        response, _ = await asyncio.to_thread(
                            conv.handle_message, prompt, uid,
                        )
                        if response:
                            from healthbot.bot.formatters import strip_markdown
                            ai_narrative = strip_markdown(response).strip()
                            # Store/update the summary as LTM
                            self._store_aboutme_summary(db, uid, ai_narrative)
                except Exception as e:
                    logger.debug("About me AI narrative: %s", e)

            if ai_narrative:
                lines.append("\u2500\u2500\u2500 What I've learned \u2500\u2500\u2500")
                lines.append("")
                lines.append(ai_narrative)
                lines.append("")

            # Empty state
            if len(lines) <= 3:
                lines.append("No health data yet.")
                lines.append("")
                lines.append("Get started:")
                lines.append("  /onboard — Build your health profile")
                lines.append("  Upload a lab PDF")
                lines.append("  Ask a health question")

        for page in paginate("\n".join(lines)):
            await update.message.reply_text(page)

    @staticmethod
    def _store_aboutme_summary(db, user_id: int, summary: str) -> None:
        """Store or update the AI-generated about-me summary as LTM."""
        # Delete existing aboutme summary
        facts = db.get_ltm_by_user(user_id)
        for f in facts:
            if f.get("_source", "") == "aboutme:summary":
                db.delete_ltm(f["_id"])
        # Insert new
        db.insert_ltm(
            user_id,
            "profile_summary",
            summary,
            source="aboutme:summary",
        )

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

    @require_unlocked
    async def workouts(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /workouts command — workout history and summary.

        /workouts                → recent workouts (last 30 days)
        /workouts <activity>     → filter by activity type (e.g. running)
        /workouts <days>         → custom time range
        """
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            args = context.args or []

            from datetime import UTC, datetime, timedelta

            sport_filter = None
            days = 30

            for arg in args:
                try:
                    days = int(arg)
                except ValueError:
                    sport_filter = arg.lower().replace("-", "_")

            start_after = (
                datetime.now(UTC) - timedelta(days=days)
            ).strftime("%Y-%m-%d")

            rows = db.query_workouts(
                sport_type=sport_filter,
                start_after=start_after,
                user_id=uid,
                limit=50,
            )

        if not rows:
            label = f" ({sport_filter})" if sport_filter else ""
            await update.message.reply_text(
                f"No workouts found in the last {days} days{label}.\n"
                "Upload an Apple Health export to import workouts."
            )
            return

        lines = [f"WORKOUTS (last {days} days)", "=" * 30, ""]

        # Summary by sport type
        by_sport: dict[str, list[dict]] = {}
        for row in rows:
            sport = row.get("sport_type", row.get("_sport_type", "other"))
            by_sport.setdefault(sport, []).append(row)

        lines.append("Summary:")
        for sport, entries in sorted(by_sport.items()):
            total_mins = sum(
                float(e.get("duration_minutes", 0) or 0) for e in entries
            )
            total_cal = sum(
                float(e.get("calories_burned", 0) or 0) for e in entries
            )
            label = sport.replace("_", " ").title()
            parts = [f"{len(entries)}x"]
            if total_mins:
                hours = total_mins / 60
                if hours >= 1:
                    parts.append(f"{hours:.1f}h")
                else:
                    parts.append(f"{total_mins:.0f}min")
            if total_cal:
                parts.append(f"{total_cal:.0f}cal")
            lines.append(f"  {label}: {', '.join(parts)}")

        lines.append("")

        # Recent entries (last 10)
        lines.append("Recent:")
        for row in rows[:10]:
            sport = row.get("sport_type", row.get("_sport_type", ""))
            dt = row.get("_start_date", "")[:10]
            dur = row.get("duration_minutes")
            cal = row.get("calories_burned")
            avg_hr = row.get("avg_heart_rate")
            dist = row.get("distance_km")

            label = sport.replace("_", " ").title()
            parts = [f"{dt} {label}"]
            if dur:
                parts.append(f"{float(dur):.0f}min")
            if dist:
                parts.append(f"{float(dist):.1f}km")
            if cal:
                parts.append(f"{float(cal):.0f}cal")
            if avg_hr:
                parts.append(f"HR {float(avg_hr):.0f}")
            lines.append(f"  {' | '.join(parts)}")

        for page in paginate("\n".join(lines)):
            await update.message.reply_text(page)

        # Send workout summary chart
        try:
            from healthbot.export.chart_generator import workout_summary_chart
            chart_bytes = workout_summary_chart(by_sport)
            if chart_bytes:
                import io
                img = io.BytesIO(chart_bytes)
                img.name = "workout_summary.png"
                await update.message.reply_photo(photo=img)
        except Exception as e:
            logger.debug("Workout chart skipped: %s", e)

    @require_unlocked
    async def weeklyreport(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /weeklyreport — generate and send weekly PDF report.

        /weeklyreport         → report for last 7 days
        /weeklyreport <days>  → custom number of days (1-90)
        """
        args = context.args or []
        days = 7
        if args:
            try:
                days = int(args[0])
                if days < 1 or days > 90:
                    await update.message.reply_text(
                        "Invalid range. Use 1-90 days.\n"
                        "Usage: /weeklyreport [days]"
                    )
                    return
            except ValueError:
                await update.message.reply_text(
                    "Invalid argument. Expected a number of days.\n"
                    "Usage: /weeklyreport [days]"
                )
                return

        try:
            async with TypingIndicator(update.effective_chat):
                db = self._core._get_db()
                uid = update.effective_user.id

                memory_items = self._build_memory_summary()

                from healthbot.export.weekly_pdf_report import WeeklyPdfReportGenerator

                gen = WeeklyPdfReportGenerator(db)
                pdf_bytes = gen.generate_weekly(
                    uid, days=days, memory_items=memory_items,
                )

            import io
            doc = io.BytesIO(pdf_bytes)
            doc.name = "weekly_health_report.pdf"
            await update.message.reply_document(document=doc)
            await update.message.reply_text(
                f"Health report ({days} days) generated."
            )
        except Exception as e:
            logger.error("Weekly report failed: %s", e)
            await update.message.reply_text(
                "Failed to generate weekly report. "
                "Ensure you have health data imported."
            )

    @require_unlocked
    async def monthlyreport(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /monthlyreport — generate and send monthly PDF report.

        /monthlyreport         → report for last 30 days
        /monthlyreport <days>  → custom number of days (7-365)
        """
        args = context.args or []
        days = 30
        if args:
            try:
                days = int(args[0])
                if days < 7 or days > 365:
                    await update.message.reply_text(
                        "Invalid range. Use 7-365 days.\n"
                        "Usage: /monthlyreport [days]"
                    )
                    return
            except ValueError:
                await update.message.reply_text(
                    "Invalid argument. Expected a number of days.\n"
                    "Usage: /monthlyreport [days]"
                )
                return

        try:
            async with TypingIndicator(update.effective_chat):
                db = self._core._get_db()
                uid = update.effective_user.id

                memory_items = self._build_memory_summary()

                from healthbot.export.weekly_pdf_report import WeeklyPdfReportGenerator

                gen = WeeklyPdfReportGenerator(db)
                pdf_bytes = gen.generate_monthly(
                    uid, days=days, memory_items=memory_items,
                )

            import io
            doc = io.BytesIO(pdf_bytes)
            doc.name = "monthly_health_report.pdf"
            await update.message.reply_document(document=doc)
            await update.message.reply_text(
                f"Monthly health report ({days} days) generated."
            )
        except Exception as e:
            logger.error("Monthly report failed: %s", e)
            await update.message.reply_text(
                "Failed to generate monthly report. "
                "Ensure you have health data imported."
            )

    @require_unlocked
    async def analyze(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Manually trigger background analysis (/analyze)."""
        import asyncio

        await update.message.reply_text("Running full analysis...")

        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id if update.effective_user else 0
            conv = self._core._get_claude_conversation()
            if not conv:
                await update.message.reply_text("Claude CLI not available.")
                return

            from healthbot.llm.background_analysis import (
                BackgroundAnalysisEngine,
            )

            engine = BackgroundAnalysisEngine(db, self._core._config)
            prompt = engine.build_health_synthesis_prompt(uid, force=True)
            if prompt:
                response, _ = await asyncio.to_thread(
                    conv.handle_message, prompt, uid,
                )
                # Send full response to user (unlike background which only
                # sends alerts).
                text = response or "Analysis complete — no notable findings."
                for page in paginate(text):
                    await update.message.reply_text(page)
            else:
                await update.message.reply_text("No data to analyze yet.")

    async def _send_source_pdf(
        self, update: Update, source_doc_id: str,
    ) -> None:
        """Decrypt and send the source PDF document."""
        try:
            vault = self._core._get_vault()
            pdf_bytes = vault.retrieve_blob(source_doc_id)
            if pdf_bytes:
                import io as iomod

                doc = iomod.BytesIO(pdf_bytes)
                doc.name = f"lab_report_{source_doc_id[:8]}.pdf"
                await update.message.reply_document(document=doc)
            else:
                await update.message.reply_text(
                    "Source PDF not found in vault.",
                )
        except Exception as e:
            logger.debug("Send PDF failed: %s", e)
            await update.message.reply_text(
                "Could not retrieve source PDF.",
            )

    # ── New visualization commands ────────────────────────────────────

    @require_unlocked
    async def score(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /score — composite health score with gauge chart."""
        import asyncio

        uid = update.effective_user.id if update.effective_user else 0
        db = self._core._get_db()

        async with TypingIndicator(update.effective_chat):
            from healthbot.reasoning.health_score import CompositeHealthEngine

            engine = CompositeHealthEngine(db)
            result = await asyncio.to_thread(engine.compute, uid)

        # Text summary
        lines = [
            f"Health Score: {result.overall:.0f}/100 ({result.grade})",
            f"Trend: {result.trend_direction}",
            "",
        ]
        for component, val in result.breakdown.items():
            name = component.replace("_", " ").title()
            bar = format_score_bar(val)
            lines.append(f"  {name}: {val:.0f}/100 {bar}")

        if result.limiting_factors:
            lines.append("")
            lines.append("Limiting factors:")
            for lf in result.limiting_factors:
                lines.append(f"  ! {lf}")

        coverage = [k for k, v in result.data_coverage.items() if not v]
        if coverage:
            lines.append("")
            lines.append(f"Missing data: {', '.join(coverage)}")

        await update.message.reply_text("\n".join(lines))

        # Chart
        from healthbot.export.chart_generator_ext import composite_score_chart

        chart_bytes = composite_score_chart(result)
        if chart_bytes:
            import io as iomod
            await update.message.reply_photo(photo=iomod.BytesIO(chart_bytes))

    @require_unlocked
    async def wearable_chart(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /wearable_chart [days] — WHOOP sparklines."""
        import asyncio
        from datetime import date, timedelta

        args = context.args or []
        days = 14
        if args:
            try:
                days = int(args[0])
            except ValueError:
                pass

        uid = update.effective_user.id if update.effective_user else 0
        db = self._core._get_db()

        async with TypingIndicator(update.effective_chat):
            cutoff = (date.today() - timedelta(days=days)).isoformat()
            data = await asyncio.to_thread(
                db.query_wearable_daily, start_date=cutoff, limit=days, user_id=uid,
            )

        if not data:
            await update.message.reply_text("No wearable data available.")
            return

        from healthbot.export.chart_generator_ext import wearable_sparklines_chart

        chart_bytes = wearable_sparklines_chart(data, days=days)
        if chart_bytes:
            import io as iomod
            await update.message.reply_photo(photo=iomod.BytesIO(chart_bytes))
        else:
            await update.message.reply_text("Not enough wearable data for chart.")

    @require_unlocked
    async def sleep_chart(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /sleep_chart [days] — sleep architecture stacked bars."""
        import asyncio
        from datetime import date, timedelta

        args = context.args or []
        days = 30
        if args:
            try:
                days = int(args[0])
            except ValueError:
                pass

        uid = update.effective_user.id if update.effective_user else 0
        db = self._core._get_db()

        async with TypingIndicator(update.effective_chat):
            cutoff = (date.today() - timedelta(days=days)).isoformat()
            data = await asyncio.to_thread(
                db.query_wearable_daily, start_date=cutoff, limit=days, user_id=uid,
            )

        if not data:
            await update.message.reply_text("No sleep data available.")
            return

        from healthbot.export.chart_generator_ext import sleep_architecture_chart

        chart_bytes = sleep_architecture_chart(data, days=days)
        if chart_bytes:
            import io as iomod
            await update.message.reply_photo(photo=iomod.BytesIO(chart_bytes))
        else:
            await update.message.reply_text("Not enough sleep data for chart.")

    @require_unlocked
    async def lab_heatmap(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /lab_heatmap — color-coded lab results grid."""
        import asyncio

        uid = update.effective_user.id if update.effective_user else 0
        db = self._core._get_db()

        async with TypingIndicator(update.effective_chat):
            rows = await asyncio.to_thread(
                db.query_observations,
                record_type="lab_result", limit=500, user_id=uid,
            )

        if not rows:
            await update.message.reply_text("No lab data available.")
            return

        # Build heatmap data from observations
        lab_data = []
        for row in rows:
            val = row.get("value")
            if val is None:
                continue
            try:
                float(val)
            except (ValueError, TypeError):
                continue
            meta = row.get("_meta", {})
            lab_data.append({
                "test_name": row.get("test_name", row.get("canonical_name", "")),
                "date": row.get("date_effective", ""),
                "value": val,
                "ref_low": meta.get("ref_low", 0),
                "ref_high": meta.get("ref_high", 0),
            })

        if len(lab_data) < 2:
            await update.message.reply_text("Not enough lab data for heatmap.")
            return

        from healthbot.export.chart_generator_ext import lab_heatmap_chart

        chart_bytes = lab_heatmap_chart(lab_data)
        if chart_bytes:
            import io as iomod
            await update.message.reply_photo(photo=iomod.BytesIO(chart_bytes))
        else:
            await update.message.reply_text("Could not generate lab heatmap.")

    @require_unlocked
    async def scatter(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /scatter m1 m2 — plot two metrics against each other."""
        import asyncio

        args = context.args or []
        if len(args) < 2:
            await update.message.reply_text(
                "Usage: /scatter <metric1> <metric2>\n"
                "Example: /scatter hrv sleep_score"
            )
            return

        m1 = _WEARABLE_ALIASES.get(args[0].lower(), args[0].lower())
        m2 = _WEARABLE_ALIASES.get(args[1].lower(), args[1].lower())

        uid = update.effective_user.id if update.effective_user else 0
        db = self._core._get_db()

        async with TypingIndicator(update.effective_chat):
            from datetime import date, timedelta
            cutoff = (date.today() - timedelta(days=90)).isoformat()
            data = await asyncio.to_thread(
                db.query_wearable_daily, start_date=cutoff, limit=90, user_id=uid,
            )

        if not data:
            await update.message.reply_text("No wearable data available.")
            return

        # Extract paired values
        x_vals, y_vals = [], []
        for row in data:
            xv, yv = row.get(m1), row.get(m2)
            if xv is not None and yv is not None:
                try:
                    x_vals.append(float(xv))
                    y_vals.append(float(yv))
                except (ValueError, TypeError):
                    continue

        if len(x_vals) < 3:
            await update.message.reply_text(
                f"Not enough paired data for {m1} vs {m2}."
            )
            return

        # Compute Pearson r
        import numpy as np
        x_arr = np.array(x_vals)
        y_arr = np.array(y_vals)
        r_val = float(np.corrcoef(x_arr, y_arr)[0, 1])

        from healthbot.reasoning.wearable_trends import METRIC_DISPLAY_NAMES
        x_label = METRIC_DISPLAY_NAMES.get(m1, m1)
        y_label = METRIC_DISPLAY_NAMES.get(m2, m2)

        from healthbot.export.chart_generator_ext import correlation_scatter_chart

        chart_bytes = correlation_scatter_chart(x_vals, y_vals, x_label, y_label, r_val)
        if chart_bytes:
            import io as iomod
            await update.message.reply_photo(photo=iomod.BytesIO(chart_bytes))
            if abs(r_val) > 0.7:
                strength = "strong"
            elif abs(r_val) > 0.4:
                strength = "moderate"
            else:
                strength = "weak"
            direction = "positive" if r_val > 0 else "negative"
            await update.message.reply_text(
                f"{x_label} vs {y_label}: r={r_val:.2f} ({strength} {direction} correlation)"
            )
        else:
            await update.message.reply_text("Could not generate scatter plot.")

    @require_unlocked
    async def trends_chart(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /trends_chart — lab trend sparkline grid."""
        import asyncio

        uid = update.effective_user.id if update.effective_user else 0
        db = self._core._get_db()

        async with TypingIndicator(update.effective_chat):
            from healthbot.reasoning.trends import TrendAnalyzer

            analyzer = TrendAnalyzer(db)
            trends = await asyncio.to_thread(
                analyzer.detect_all_trends, months=12, user_id=uid,
            )

        if not trends:
            await update.message.reply_text("No significant lab trends detected.")
            return

        from healthbot.export.chart_generator import multi_trend_chart

        chart_bytes = multi_trend_chart(trends, max_panels=6)
        if chart_bytes:
            import io as iomod
            await update.message.reply_photo(photo=iomod.BytesIO(chart_bytes))
            # Text summary
            lines = ["Lab Trends:"]
            for t in trends[:6]:
                arrow = {"increasing": "↑", "decreasing": "↓", "stable": "→"}.get(t.direction, "→")
                lines.append(
                    f"  {arrow} {t.test_name}: {t.first_value:.1f} → "
                    f"{t.last_value:.1f} ({t.pct_change:+.1f}%)"
                )
            await update.message.reply_text("\n".join(lines))
        else:
            await update.message.reply_text("Not enough data for trends chart.")
