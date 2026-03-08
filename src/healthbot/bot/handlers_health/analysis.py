"""Insights, summary, dashboard, healthreview, correlate, gaps, trend handlers."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate
from healthbot.bot.middleware import rate_limited, require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class AnalysisMixin:
    """Mixin for health analysis and trend commands."""

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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
        from healthbot.bot.handlers_health._wearable_aliases import _WEARABLE_ALIASES
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
            logger.warning("Wearable chart generation skipped: %s", e)

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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

    @rate_limited(max_per_minute=10)
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
