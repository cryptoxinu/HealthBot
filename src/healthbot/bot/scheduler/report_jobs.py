"""Daily digest, weekly/monthly PDF reports, and appointment prep methods."""
from __future__ import annotations

import logging

from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate
from healthbot.bot.scheduler.scheduler_core import _SPECIALTY_LABS
from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


class ReportJobsMixin:
    """Mixin for report generation and appointment prep jobs."""

    def _compute_digest_first_delay(self) -> float:
        """Compute seconds until the configured digest time.

        If the configured time has already passed today, schedules for tomorrow.
        Returns interval (24h) if digest is disabled (empty digest_time).
        """
        digest_time = getattr(self._config, "digest_time", "")
        if not digest_time:
            return float(getattr(self._config, "digest_interval", 86400))
        try:
            from datetime import datetime, timedelta
            now = datetime.now()
            hour, minute = int(digest_time.split(":")[0]), int(digest_time.split(":")[1])
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            return (target - now).total_seconds()
        except (ValueError, IndexError):
            return float(getattr(self._config, "digest_interval", 86400))

    async def _send_daily_digest(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Send daily health digest. Skips if vault is locked."""
        if not self._km.is_unlocked:
            return

        digest_time = getattr(self._config, "digest_time", "")
        if not digest_time:
            return  # Digest disabled

        try:
            from healthbot.reasoning.digest import build_daily_digest, format_digest

            db = self._get_db()
            user_id = self._primary_user_id
            report = build_daily_digest(db, user_id)
            text = format_digest(report)
            for page in paginate(text):
                await self._tracked_send(context.bot, page)
            logger.info("Daily digest sent")
        except Exception as e:
            logger.warning("Daily digest failed: %s", e)

    def _compute_weekly_first_delay(self) -> float:
        """Compute seconds until next configured weekly report time."""
        weekly_day = getattr(self._config, "weekly_report_day", "")
        weekly_time = getattr(self._config, "weekly_report_time", "20:00")
        if not weekly_day:
            return 86400.0

        try:
            from datetime import datetime, timedelta

            day_map = {
                "monday": 0, "tuesday": 1, "wednesday": 2,
                "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
            }
            target_day = day_map.get(weekly_day.lower(), 6)
            hour, minute = int(weekly_time.split(":")[0]), int(weekly_time.split(":")[1])

            now = datetime.now()
            days_ahead = (target_day - now.weekday()) % 7
            if days_ahead == 0:
                # Today is the day — check if time has passed
                target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if target <= now:
                    days_ahead = 7
            target = now.replace(
                hour=hour, minute=minute, second=0, microsecond=0,
            ) + timedelta(days=days_ahead)
            return max(60.0, (target - now).total_seconds())
        except (ValueError, IndexError):
            return 86400.0

    def _compute_monthly_first_delay(self) -> float:
        """Compute seconds until next configured monthly report time."""
        monthly_day = getattr(self._config, "monthly_report_day", 0)
        monthly_time = getattr(self._config, "monthly_report_time", "20:00")
        if not monthly_day:
            return 86400.0

        try:
            from datetime import datetime

            hour, minute = int(monthly_time.split(":")[0]), int(monthly_time.split(":")[1])
            now = datetime.now()

            target = now.replace(
                day=min(monthly_day, 28),
                hour=hour, minute=minute, second=0, microsecond=0,
            )
            if target <= now:
                # Next month
                if now.month == 12:
                    target = target.replace(year=now.year + 1, month=1)
                else:
                    target = target.replace(month=now.month + 1)
            return max(60.0, (target - now).total_seconds())
        except (ValueError, IndexError):
            return 86400.0

    async def _send_weekly_pdf_report(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Send weekly PDF health report. Skips if locked or wrong day."""
        if not self._km.is_unlocked:
            return

        weekly_day = getattr(self._config, "weekly_report_day", "")
        if not weekly_day:
            return

        from datetime import datetime
        day_map = {
            "monday": 0, "tuesday": 1, "wednesday": 2,
            "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
        }
        target_day = day_map.get(weekly_day.lower(), -1)
        if datetime.now().weekday() != target_day:
            return

        try:
            import io

            from healthbot.export.weekly_pdf_report import WeeklyPdfReportGenerator

            db = self._get_db()
            gen = WeeklyPdfReportGenerator(db)
            pdf_bytes = gen.generate_weekly(self._primary_user_id)

            doc = io.BytesIO(pdf_bytes)
            doc.name = f"weekly_report_{datetime.now().strftime('%Y%m%d')}.pdf"
            await context.bot.send_document(
                chat_id=self._chat_id, document=doc,
            )
            await self._tracked_send(
                context.bot, "Weekly health report attached.",
            )
            logger.info("Weekly PDF report sent")
        except Exception as e:
            logger.warning("Weekly PDF report failed: %s", e)

    async def _send_monthly_pdf_report(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Send monthly PDF health report. Skips if locked or wrong day."""
        if not self._km.is_unlocked:
            return

        monthly_day = getattr(self._config, "monthly_report_day", 0)
        if not monthly_day:
            return

        from datetime import datetime
        if datetime.now().day != min(monthly_day, 28):
            return

        try:
            import io

            from healthbot.export.weekly_pdf_report import WeeklyPdfReportGenerator

            db = self._get_db()
            gen = WeeklyPdfReportGenerator(db)
            pdf_bytes = gen.generate_monthly(self._primary_user_id)

            doc = io.BytesIO(pdf_bytes)
            doc.name = f"monthly_report_{datetime.now().strftime('%Y%m')}.pdf"
            await context.bot.send_document(
                chat_id=self._chat_id, document=doc,
            )
            await self._tracked_send(
                context.bot, "Monthly health report attached.",
            )
            logger.info("Monthly PDF report sent")
        except Exception as e:
            logger.warning("Monthly PDF report failed: %s", e)

    async def _check_appointment_prep(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Auto-send prep packet for tomorrow's appointments."""
        if not self._km.is_unlocked:
            return

        try:
            db = self._get_db()
            user_id = self._primary_user_id
            upcoming = db.get_upcoming_appointments(user_id, within_days=1)

            for appt in upcoming:
                if appt.get("_prep_sent"):
                    continue

                specialty = appt.get("specialty", "")
                provider_name = appt.get("provider_name", "Unknown")
                appt_date = appt.get("date", appt.get("_appt_date", ""))
                reason = appt.get("reason", "")

                # Build prep text
                prep_text = self._build_appointment_prep(
                    db, user_id, specialty, provider_name, appt_date, reason,
                )

                if prep_text:
                    for page in paginate(prep_text):
                        await self._tracked_send(context.bot, page)
                    db.mark_appointment_prep_sent(appt["_id"])
                    logger.info(
                        "Appointment prep sent for %s on %s",
                        provider_name, appt_date,
                    )
        except Exception as e:
            logger.debug("Appointment prep check: %s", e)

    def _build_appointment_prep(
        self, db: HealthDB, user_id: int,
        specialty: str, provider_name: str,
        appt_date: str, reason: str,
    ) -> str:
        """Build a specialty-aware appointment prep packet."""
        lines = [
            f"APPOINTMENT PREP: {provider_name}",
            f"Date: {appt_date}",
        ]
        if reason:
            lines.append(f"Reason: {reason}")
        lines.append("-" * 30)

        # Specialty-relevant lab metrics
        specialty_labs = _SPECIALTY_LABS.get(
            specialty.lower(), _SPECIALTY_LABS.get("general", []),
        )

        # Pull relevant labs
        relevant_labs = []
        for canonical in specialty_labs:
            rows = db.query_observations(
                record_type="lab_result",
                canonical_name=canonical,
                limit=3,
                user_id=user_id,
            )
            for row in rows:
                name = row.get("test_name", canonical)
                val = row.get("value", "")
                unit = row.get("unit", "")
                dt = row.get("date_collected", "")
                flag = row.get("flag", "")
                flag_str = f" [{flag}]" if flag else ""
                relevant_labs.append(f"  {name}: {val} {unit}{flag_str} ({dt})")

        if relevant_labs:
            lines.append(f"\nRelevant Labs ({specialty or 'general'}):")
            lines.extend(relevant_labs[:15])

        # Active medications
        meds = db.get_active_medications(user_id=user_id)
        if meds:
            lines.append("\nActive Medications:")
            for med in meds[:10]:
                name = med.get("name", "")
                dose = med.get("dose", "")
                unit = med.get("unit", "")
                freq = med.get("frequency", "")
                lines.append(f"  {name} {dose} {unit} {freq}".strip())

        # Active hypotheses
        try:
            hyps = db.get_active_hypotheses(user_id)
            if hyps:
                lines.append("\nActive Hypotheses:")
                for h in hyps[:5]:
                    conf = h.get("confidence", 0)
                    lines.append(
                        f"  {h.get('title', '?')} ({float(conf) * 100:.0f}%)",
                    )
        except Exception:
            pass

        # Questions to ask
        lines.append("\nSuggested Discussion Points:")
        if relevant_labs:
            flagged = [lab for lab in relevant_labs if "[" in lab]
            if flagged:
                lines.append("  - Review flagged lab results above")
        lines.append("  - Any changes to medications?")
        lines.append("  - Next follow-up schedule?")
        if reason:
            lines.append(f"  - Follow up on: {reason}")

        return "\n".join(lines)
