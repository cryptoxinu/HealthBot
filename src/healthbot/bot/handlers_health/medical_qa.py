"""Ask, analyze, recommend, symptoms, screenings, stress, sleeprec handlers."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate
from healthbot.bot.middleware import rate_limited, require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class MedicalQAMixin:
    """Mixin for medical Q&A and recommendation commands."""

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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
