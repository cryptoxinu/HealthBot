"""Medical tracking, research, and doctor-facing command handlers."""
from __future__ import annotations

import asyncio
import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate
from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.middleware import require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class MedicalHandlers:
    """Medical tracking, research, and doctor-preparation commands."""

    def __init__(self, core: HandlerCore) -> None:
        self._core = core

    @property
    def _km(self):
        return self._core._km

    def _check_auth(self, update: Update) -> bool:
        return self._core._check_auth(update)

    @require_unlocked
    async def doctorprep(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /doctorprep command."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.reasoning.doctor_prep import DoctorPrepEngine
            from healthbot.reasoning.overdue import OverdueDetector
            from healthbot.reasoning.trends import TrendAnalyzer
            engine = DoctorPrepEngine(
                db, self._core._triage, TrendAnalyzer(db), OverdueDetector(db)
            )
            uid = update.effective_user.id
            prep = engine.generate_prep(user_id=uid)
        for page in paginate(prep):
            await update.message.reply_text(page)

    @require_unlocked
    async def research_cloud(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /research_cloud <topic> command."""

        topic = " ".join(context.args) if context.args else ""
        if not topic:
            await update.message.reply_text("Usage: /research_cloud <topic>")
            return

        # PHI hard-block
        if self._core._fw.contains_phi(topic):
            await update.message.reply_text(
                "Research blocked: PHI detected in query. "
                "Remove personal information and try again."
            )
            return

        await update.message.reply_text("Researching (sanitized)...")

        async with TypingIndicator(update.effective_chat):
            from healthbot.research.claude_cli_client import ClaudeCLIResearchClient
            client = ClaudeCLIResearchClient(self._core._config, self._core._fw)
            result = await asyncio.to_thread(client.research, topic)

        from healthbot.research.external_evidence_store import ExternalEvidenceStore
        db = self._core._get_db()
        store = ExternalEvidenceStore(db)
        store.store("claude_cli", topic, result)

        for page in paginate(result):
            await update.message.reply_text(page)

    @require_unlocked
    async def doctorpacket(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /doctorpacket -- generate PDF doctor visit packet."""
        await update.message.reply_text("Generating doctor packet PDF...")
        try:
            async with TypingIndicator(update.effective_chat):
                db = self._core._get_db()
                from healthbot.export.pdf_generator import DoctorPacketPdf
                from healthbot.reasoning.doctor_prep import DoctorPrepEngine
                from healthbot.reasoning.overdue import OverdueDetector
                from healthbot.reasoning.trends import TrendAnalyzer
                engine = DoctorPrepEngine(
                    db, self._core._triage, TrendAnalyzer(db), OverdueDetector(db)
                )
                uid = update.effective_user.id
                data = engine.generate_prep_data(user_id=uid)
                pdf_gen = DoctorPacketPdf()
                pdf_bytes = pdf_gen.generate(data)

            import io
            doc = io.BytesIO(pdf_bytes)
            doc.name = "doctor_packet.pdf"
            await update.message.reply_document(document=doc)
            await update.message.reply_text(
                "PDF generated. Print or share with your provider.\n"
                "Use /export for an encrypted copy."
            )
        except Exception as e:
            logger.error("Doctor packet PDF error: %s", e)
            await update.message.reply_text(
                "Error generating PDF. Try /doctorprep for text version."
            )

    @require_unlocked
    async def interactions(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /interactions -- check drug-drug, drug-lab, and drug-condition."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.reasoning.interactions import InteractionChecker
            uid = update.effective_user.id

            meds = db.get_active_medications(user_id=uid)
            if not meds:
                await update.message.reply_text(
                    "No medications on file — can't check interactions.\n\n"
                    "Add them during /onboard or tell me what you take."
                )
                return

            checker = InteractionChecker(db)
            dd_results = checker.check_all(user_id=uid)
            dl_results = checker.check_drug_lab(user_id=uid)
            dc_results = checker.check_drug_condition(user_id=uid)

        # Drug-drug interactions
        dd_text = InteractionChecker.format_results(dd_results)
        for page in paginate(dd_text):
            await update.message.reply_text(page)

        # Drug-lab interactions
        dl_text = InteractionChecker.format_drug_lab_results(dl_results)
        if dl_text:
            for page in paginate(dl_text):
                await update.message.reply_text(page)

        # Drug-condition interactions
        dc_text = InteractionChecker.format_drug_condition_results(dc_results)
        if dc_text:
            for page in paginate(dc_text):
                await update.message.reply_text(page)

    @require_unlocked
    async def evidence(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /evidence command — browse cached research evidence."""

        db = self._core._get_db()
        args = context.args or []

        from healthbot.research.external_evidence_store import ExternalEvidenceStore
        store = ExternalEvidenceStore(db)

        if args:
            entries = store.list_evidence()
            try:
                idx = int(args[0]) - 1
                if 0 <= idx < len(entries):
                    detail = store.get_evidence_detail(entries[idx]["evidence_id"])
                    if detail:
                        text = detail.get("text", str(detail.get("result_json", "")))
                        source = detail.get("_source", "unknown")
                        date = detail.get("_created_at", "")
                        header = f"Source: {source} | Date: {date}\n\n"
                        for page in paginate(header + text):
                            await update.message.reply_text(page)
                        return
                await update.message.reply_text("Evidence entry not found.")
            except ValueError:
                await update.message.reply_text("Usage: /evidence [number]")
            return

        entries = store.list_evidence(limit=10)
        if not entries:
            await update.message.reply_text("No cached research evidence.")
            return

        lines = ["Cached Research Evidence:", ""]
        for i, e in enumerate(entries, 1):
            expired = " [expired]" if e.get("expired") else ""
            lines.append(f"{i}. [{e['source']}] {e['query'][:60]}{expired}")
            lines.append(f"   {e['summary']}")
            lines.append(f"   Date: {e['created_at'][:10]}")
            lines.append("")

        lines.append("Use /evidence <number> for full detail.")
        await update.message.reply_text("\n".join(lines))

    @require_unlocked
    async def template(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /template command — doctor discussion templates."""

        db = self._core._get_db()
        user_id = update.effective_user.id
        args = context.args or []

        from healthbot.reasoning.doctor_templates import DoctorTemplateEngine
        engine = DoctorTemplateEngine(db)

        if not args:
            templates = engine.list_templates()
            lines = ["Available doctor templates:", ""]
            for key, title in templates:
                lines.append(f"  /template {key} — {title}")
            await update.message.reply_text("\n".join(lines))
            return

        key = args[0].lower()
        result = engine.generate(key, user_id)
        for page in paginate(result):
            await update.message.reply_text(page)

    @require_unlocked
    async def hypotheses(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /hypotheses command.

        Subcommands:
          /hypotheses           — list all active hypotheses
          /hypotheses all       — list all (including ruled_out, confirmed)
          /hypotheses ruleout <n> [reason] — rule out hypothesis #n
          /hypotheses confirm <n> [reason] — confirm hypothesis #n
        """

        db = self._core._get_db()
        user_id = update.effective_user.id
        args = context.args or []

        from healthbot.reasoning.hypothesis_tracker import HypothesisTracker
        tracker = HypothesisTracker(db)

        if not args or args[0].lower() == "all":
            show_all = args and args[0].lower() == "all"
            hyps = (
                db.get_all_hypotheses(user_id)
                if show_all
                else db.get_active_hypotheses(user_id)
            )
            if not hyps:
                label = "hypotheses" if show_all else "active hypotheses"
                await update.message.reply_text(f"No {label} being tracked.")
                return

            lines = [
                "All Hypotheses:" if show_all else "Active Hypotheses:",
                "",
            ]
            for i, h in enumerate(hyps, 1):
                title = h.get("title", "Unknown")
                conf = h.get("confidence", h.get("_confidence", 0))
                status = h.get("_status", "active")
                icons = {"confirmed": " [confirmed]", "ruled_out": " [ruled out]"}
                status_icon = icons.get(status, "")
                lines.append(f"{i}. {title} ({conf:.0%}){status_icon}")

                ev_for = h.get("evidence_for", [])
                if ev_for:
                    lines.append(f"   For: {'; '.join(ev_for[:5])}")
                ev_against = h.get("evidence_against", [])
                if ev_against:
                    lines.append(f"   Against: {'; '.join(ev_against[:5])}")
                missing = h.get("missing_tests", [])
                if missing:
                    lines.append(f"   Missing tests: {', '.join(missing)}")
                lines.append("")

            for page in paginate("\n".join(lines)):
                await update.message.reply_text(page)
            return

        subcmd = args[0].lower()

        if subcmd in ("ruleout", "confirm"):
            if len(args) < 2:
                await update.message.reply_text(
                    f"Usage: /hypotheses {subcmd} <number> [reason]"
                )
                return

            try:
                idx = int(args[1]) - 1
            except ValueError:
                await update.message.reply_text("Please provide a hypothesis number.")
                return

            hyps = db.get_active_hypotheses(user_id)
            if idx < 0 or idx >= len(hyps):
                await update.message.reply_text(
                    f"Invalid number. You have {len(hyps)} active hypotheses."
                )
                return

            hyp = hyps[idx]
            reason = " ".join(args[2:]) if len(args) > 2 else ""
            title = hyp.get("title", "Unknown")

            if subcmd == "ruleout":
                tracker.ruleout(hyp["_id"], reason)
                await update.message.reply_text(f"Ruled out: {title}")
            else:
                tracker.confirm(hyp["_id"], reason)
                await update.message.reply_text(f"Confirmed: {title}")
            return

        await update.message.reply_text(
            "Usage:\n"
            "/hypotheses — list active\n"
            "/hypotheses all — list all\n"
            "/hypotheses ruleout <n> [reason]\n"
            "/hypotheses confirm <n> [reason]"
        )

    @require_unlocked
    async def log_event(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /log <text> -- log a health event."""
        text = " ".join(context.args) if context.args else ""
        if not text:
            await update.message.reply_text(
                "Usage: /log <event>\n"
                "Example: /log headache since yesterday, moderate"
            )
            return
        try:
            db = self._core._get_db()
            from healthbot.reasoning.event_logger import EventLogger
            el = EventLogger(db)
            event = el.parse(text)
            obs_id = el.store(event, update.effective_user.id)
            # Track for undo
            self._core._router._last_logged_obs[
                update.effective_user.id
            ] = obs_id
            await update.message.reply_text(el.format_confirmation(event))
        except Exception as e:
            logger.error("Event logging error: %s", e)
            await update.message.reply_text("Error logging event.")

    @require_unlocked
    async def undo(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /undo command -- undo last logged event."""
        uid = update.effective_user.id
        obs_id = self._core._router._last_logged_obs.get(uid)
        if not obs_id:
            await update.message.reply_text("Nothing to undo.")
            return
        try:
            db = self._core._get_db()
            deleted = db.delete_observation(obs_id)
            if deleted:
                self._core._router._last_logged_obs.pop(uid, None)
                await update.message.reply_text(
                    "Done -- removed the last logged entry."
                )
            else:
                await update.message.reply_text(
                    "Could not find that entry. Already removed."
                )
        except Exception as exc:
            logger.warning("Undo failed: %s", exc)
            await update.message.reply_text("Undo failed.")

    @require_unlocked
    async def remind(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /remind — set, list, or disable medication reminders.

        /remind <med> <HH:MM>   — set a reminder
        /remind off <med>       — disable a reminder
        /reminders              — list all active reminders
        """
        import re

        db = self._core._get_db()
        uid = update.effective_user.id
        args = context.args or []

        if not args:
            # List reminders
            reminders = db.get_med_reminders(uid)
            from healthbot.reasoning.med_reminders import format_reminder_list
            await update.message.reply_text(format_reminder_list(reminders))
            return

        # /remind off <med> — disable
        if args[0].lower() == "off" and len(args) >= 2:
            med_name = " ".join(args[1:])
            found = db.disable_med_reminder(uid, med_name)
            if found:
                await update.message.reply_text(
                    f"Reminder for {med_name} disabled."
                )
            else:
                await update.message.reply_text(
                    f"No active reminder found for '{med_name}'."
                )
            return

        # /remind <med> <HH:MM> — set reminder
        # Find HH:MM pattern in args
        time_pattern = re.compile(r"^\d{1,2}:\d{2}(?:am|pm)?$", re.I)
        time_str = None
        med_parts = []
        for arg in args:
            if time_pattern.match(arg):
                time_str = arg
            else:
                med_parts.append(arg)

        if not time_str or not med_parts:
            await update.message.reply_text(
                "Usage: /remind <medication> <HH:MM>\n"
                "Example: /remind levothyroxine 7:00am\n"
                "Disable: /remind off <medication>\n"
                "List all: /reminders"
            )
            return

        # Parse time to 24h HH:MM
        time_24 = self._parse_reminder_time(time_str)
        if not time_24:
            await update.message.reply_text(
                f"Invalid time: {time_str}. Use HH:MM (e.g., 7:00, 14:30, 8:00am)."
            )
            return

        med_name = " ".join(med_parts)
        from healthbot.reasoning.med_reminders import get_timing_notes
        notes = get_timing_notes(med_name)
        db.upsert_med_reminder(uid, med_name, time_24, notes)

        msg = f"Reminder set: {med_name} at {time_24} daily."
        if notes:
            msg += f"\nTip: {notes}"
        await update.message.reply_text(msg)

    @require_unlocked
    async def reminders(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /reminders — list all medication reminders."""
        db = self._core._get_db()
        uid = update.effective_user.id
        reminders = db.get_med_reminders(uid)
        from healthbot.reasoning.med_reminders import format_reminder_list
        await update.message.reply_text(format_reminder_list(reminders))

    @require_unlocked
    async def sideeffects(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /sideeffects — show side effect monitoring status."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            from healthbot.reasoning.side_effect_monitor import (
                SideEffectMonitor,
                format_alerts,
                format_watch_list,
            )
            monitor = SideEffectMonitor(db)
            watches = monitor.get_watch_list(user_id=uid)
            alerts = monitor.check_active_concerns(user_id=uid)

        text = format_watch_list(watches)
        alert_text = format_alerts(alerts)
        if alert_text:
            text += "\n" + alert_text
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def comorbidity(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /comorbidity — detect condition interactions."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            from healthbot.reasoning.comorbidity import (
                ComorbidityAnalyzer,
                format_comorbidities,
            )
            analyzer = ComorbidityAnalyzer(db)
            findings = analyzer.analyze(user_id=uid)

        text = format_comorbidities(findings)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def supplements(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /supplements — evidence-based supplement recommendations."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            from healthbot.reasoning.supplement_protocols import (
                SupplementAdvisor,
                format_recommendations,
            )
            advisor = SupplementAdvisor(db)
            recs = advisor.get_recommendations(user_id=uid)

        text = format_recommendations(recs)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def retests(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /retests — show pending retest reminders."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            from healthbot.reasoning.retest_scheduler import (
                RetestScheduler,
                format_retests,
            )
            scheduler = RetestScheduler(db)
            retests = scheduler.get_pending_retests(user_id=uid)

        text = format_retests(retests)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def effectiveness(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /effectiveness — check if medications are working."""
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            from healthbot.reasoning.treatment_tracker import (
                TreatmentTracker,
                format_effectiveness,
            )
            tracker = TreatmentTracker(db)
            reports = tracker.assess_all(user_id=uid)

        text = format_effectiveness(reports)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def doctors(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /doctors — manage healthcare providers.

        /doctors                    → list all providers
        /doctors add <name> <specialty>  → add a provider
        /doctors remove <index>     → remove a provider
        """
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            args = context.args or []

            if args and args[0].lower() == "add" and len(args) >= 3:
                name = args[1]
                specialty = " ".join(args[2:])
                db.insert_provider(uid, {
                    "name": name,
                    "specialty": specialty,
                })
                await update.message.reply_text(
                    f"Added provider: {name} ({specialty})",
                )
                return

            if args and args[0].lower() == "remove" and len(args) >= 2:
                providers = db.get_providers(uid)
                try:
                    idx = int(args[1]) - 1
                    if 0 <= idx < len(providers):
                        prov = providers[idx]
                        db.delete_provider(prov["_id"])
                        await update.message.reply_text(
                            f"Removed: {prov.get('name', 'provider')}",
                        )
                    else:
                        await update.message.reply_text("Invalid number.")
                except ValueError:
                    await update.message.reply_text(
                        "Usage: /doctors remove <number>",
                    )
                return

            # Default: list all providers
            providers = db.get_providers(uid)
            if not providers:
                await update.message.reply_text(
                    "No providers on file.\n\n"
                    "Add one: /doctors add <name> <specialty>\n"
                    "Example: /doctors add DrSmith Endocrinology",
                )
                return

            lines = ["YOUR PROVIDERS", "-" * 30]
            for i, p in enumerate(providers, 1):
                name = p.get("name", "Unknown")
                spec = p.get("specialty", "")
                phone = p.get("phone", "")
                spec_str = f" ({spec})" if spec else ""
                phone_str = f" | {phone}" if phone else ""
                lines.append(f"  {i}. {name}{spec_str}{phone_str}")
            text = "\n".join(lines)

        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def appointments(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /appointments — manage appointments.

        /appointments                         → list upcoming
        /appointments add <doctor#> <date> [reason]  → add
        /appointments cancel <index>          → cancel
        """
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id
            args = context.args or []

            if args and args[0].lower() == "add" and len(args) >= 3:
                providers = db.get_providers(uid)
                try:
                    doc_idx = int(args[1]) - 1
                except ValueError:
                    await update.message.reply_text(
                        "Usage: /appointments add <doctor#> <date> [reason]",
                    )
                    return

                if not (0 <= doc_idx < len(providers)):
                    await update.message.reply_text(
                        "Invalid doctor number. Use /doctors to see list.",
                    )
                    return

                from healthbot.nlu.date_parse import parse_date
                appt_date = parse_date(args[2])
                if appt_date is None:
                    await update.message.reply_text(
                        "Could not parse date. Use YYYY-MM-DD format.",
                    )
                    return

                reason = " ".join(args[3:]) if len(args) > 3 else ""
                prov = providers[doc_idx]
                db.insert_appointment(uid, prov["_id"], {
                    "date": appt_date.isoformat(),
                    "time": "",
                    "reason": reason,
                    "provider_name": prov.get("name", ""),
                    "specialty": prov.get("specialty", ""),
                })
                await update.message.reply_text(
                    f"Appointment scheduled: {prov.get('name', '')} "
                    f"on {appt_date.isoformat()}"
                    + (f" — {reason}" if reason else ""),
                )
                return

            if args and args[0].lower() == "cancel" and len(args) >= 2:
                appts = db.get_appointments(uid, status="scheduled")
                try:
                    idx = int(args[1]) - 1
                    if 0 <= idx < len(appts):
                        appt = appts[idx]
                        db.update_appointment_status(appt["_id"], "cancelled")
                        await update.message.reply_text(
                            f"Cancelled appointment on {appt.get('date', '')}",
                        )
                    else:
                        await update.message.reply_text("Invalid number.")
                except ValueError:
                    await update.message.reply_text(
                        "Usage: /appointments cancel <number>",
                    )
                return

            # Default: list upcoming appointments
            appts = db.get_appointments(uid, status="scheduled")
            if not appts:
                await update.message.reply_text(
                    "No upcoming appointments.\n\n"
                    "Add one: /appointments add <doctor#> <date> [reason]\n"
                    "First add a doctor with /doctors add <name> <specialty>",
                )
                return

            lines = ["UPCOMING APPOINTMENTS", "-" * 30]
            for i, a in enumerate(appts, 1):
                dt = a.get("date", a.get("_appt_date", ""))
                doc_name = a.get("provider_name", "")
                spec = a.get("specialty", "")
                reason = a.get("reason", "")
                spec_str = f" ({spec})" if spec else ""
                reason_str = f" — {reason}" if reason else ""
                lines.append(f"  {i}. {dt}: {doc_name}{spec_str}{reason_str}")
            text = "\n".join(lines)

        for page in paginate(text):
            await update.message.reply_text(page)

    @staticmethod
    def _parse_reminder_time(time_str: str) -> str | None:
        """Parse time string to 24h HH:MM format."""
        import re
        m = re.match(
            r"^(\d{1,2}):(\d{2})\s*(am|pm)?$", time_str, re.I,
        )
        if not m:
            return None
        hour, minute = int(m.group(1)), int(m.group(2))
        ampm = (m.group(3) or "").lower()
        if ampm == "pm" and hour < 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
        if hour > 23 or minute > 59:
            return None
        return f"{hour:02d}:{minute:02d}"
