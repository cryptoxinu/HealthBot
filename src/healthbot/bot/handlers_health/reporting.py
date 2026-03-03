"""Report, weeklyreport, monthlyreport, emergency, doctorpacket source PDF handlers."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate
from healthbot.bot.middleware import require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class ReportingMixin:
    """Mixin for report generation and emergency card commands."""

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
