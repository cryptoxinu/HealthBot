"""Trends_chart, lab_heatmap, scatter, sleep_chart, wearable_chart handlers."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import format_score_bar
from healthbot.bot.middleware import require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class ChartingMixin:
    """Mixin for chart and visualization commands."""

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
                arrows = {"increasing": "\u2191", "decreasing": "\u2193", "stable": "\u2192"}
                arrow = arrows.get(t.direction, "\u2192")
                lines.append(
                    f"  {arrow} {t.test_name}: {t.first_value:.1f} \u2192 "
                    f"{t.last_value:.1f} ({t.pct_change:+.1f}%)"
                )
            await update.message.reply_text("\n".join(lines))
        else:
            await update.message.reply_text("Not enough data for trends chart.")

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

        from healthbot.bot.handlers_health._wearable_aliases import _WEARABLE_ALIASES

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
