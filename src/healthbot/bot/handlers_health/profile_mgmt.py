"""Aboutme, timeline, goals, score, workouts handlers."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate
from healthbot.bot.middleware import require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class ProfileMgmtMixin:
    """Mixin for profile management, timeline, goals, and workout commands."""

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
                basics.append("  " + " \u00b7 ".join(age_parts))

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
                basics.append("  " + " \u00b7 ".join(body_parts))

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
                lines.append("  " + " \u00b7 ".join(lifestyle))
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
                lines.append("  /onboard \u2014 Build your health profile")
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
