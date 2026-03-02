"""Deep substance research command handlers."""
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


class ResearchHandlers:
    """Deep substance research commands."""

    def __init__(self, core: HandlerCore) -> None:
        self._core = core

    def _check_auth(self, update: Update) -> bool:
        return self._core._check_auth(update)

    @require_unlocked
    async def deep(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /deep <substance> command.

        Runs comprehensive substance research: web search, PubMed,
        CYP-450 profiling, pathway analysis. Stores results in
        substance_knowledge and updates the interaction KB overlay.
        """
        args = context.args or []
        if not args:
            await update.message.reply_text(
                "Usage: /deep <substance>\n"
                "Example: /deep bromantane\n\n"
                "Runs comprehensive research on a substance:\n"
                "- Mechanism of action\n"
                "- CYP-450 enzyme profile\n"
                "- Biological pathway effects\n"
                "- Drug interactions\n"
                "- Clinical evidence (PubMed)\n"
                "- Side effects & contraindications\n\n"
                "Results are cached and used for interaction checking."
            )
            return

        # Handle /deep update <substance>
        force_update = False
        if args[0].lower() == "update":
            force_update = True
            args = args[1:]
            if not args:
                await update.message.reply_text("Usage: /deep update <substance>")
                return

        substance = " ".join(args).strip()

        # PHI hard-block
        if self._core._fw.contains_phi(substance):
            await update.message.reply_text(
                "Research blocked: PHI detected in query. "
                "Remove personal information and try again."
            )
            return

        # Check for cached profile (unless force update)
        db = self._core._get_db()
        uid = update.effective_user.id

        if not force_update:
            existing = self._check_cached(db, uid, substance)
            if existing:
                await update.message.reply_text(existing)
                return

        # Progress callback
        progress_msg = await update.message.reply_text(
            f"Researching {substance}... (this may take 30-60 seconds)"
        )

        async def on_progress(progress) -> None:
            try:
                text = f"Researching {substance}... {progress.message}"
                await progress_msg.edit_text(text)
            except Exception:
                pass  # Telegram rate limits

        async with TypingIndicator(update.effective_chat):
            from healthbot.research.substance_researcher import SubstanceResearcher

            researcher = SubstanceResearcher(self._core._config, self._core._fw)
            profile = await asyncio.to_thread(
                researcher.research,
                substance, db, uid, on_progress,
            )

        # Format results
        result = self._format_profile(profile)

        # Delete progress message
        try:
            await progress_msg.delete()
        except Exception:
            pass

        for page in paginate(result):
            await update.message.reply_text(page)

    def _check_cached(self, db, user_id: int, substance: str) -> str | None:
        """Check for a recent high-quality cached profile. Returns formatted text or None."""
        try:
            data = db.get_substance_knowledge(user_id, substance.lower())
            if not data:
                return None
            quality = data.get("quality_score", 0)
            if quality < 0.7:
                return None

            from healthbot.research.substance_researcher import SubstanceResearcher
            researcher = SubstanceResearcher.__new__(SubstanceResearcher)
            updated = data.get("updated_at", "")
            if not updated or not researcher._is_recent(updated, days=30):
                return None

            # Build profile from cached data
            profile = researcher._profile_from_data(substance, data.get("data", {}))
            text = self._format_profile(profile)
            return (
                f"[Cached profile — quality {quality:.0%}]\n"
                f"Use /deep update {substance} to refresh.\n\n"
                + text
            )
        except Exception as e:
            logger.debug("Cached profile check failed: %s", e)
            return None

    @staticmethod
    def _format_profile(profile) -> str:
        """Format a SubstanceProfile for Telegram display."""
        lines: list[str] = []
        name = profile.name.replace("_", " ").title()
        lines.append(f"== {name} ==\n")

        if profile.aliases:
            lines.append(f"Aliases: {', '.join(profile.aliases[:8])}")

        if profile.mechanism_of_action:
            moa = profile.mechanism_of_action
            if len(moa) > 500:
                moa = moa[:497] + "..."
            lines.append(f"\nMechanism:\n{moa}")

        if profile.half_life:
            lines.append(f"\nHalf-life: {profile.half_life}")

        if profile.dosing_protocols:
            dp = profile.dosing_protocols
            if len(dp) > 300:
                dp = dp[:297] + "..."
            lines.append(f"\nDosing:\n{dp}")

        if profile.cyp_interactions:
            cyp_parts = [f"  {e}: {r}" for e, r in profile.cyp_interactions.items()]
            lines.append("\nCYP-450 Profile:")
            lines.extend(cyp_parts)

        if profile.pathway_effects:
            pw_parts = [f"  {p}: {e}" for p, e in profile.pathway_effects.items()]
            lines.append("\nPathway Effects:")
            lines.extend(pw_parts)

        if profile.drug_interactions:
            lines.append("\nKnown Interactions:")
            for di in profile.drug_interactions[:10]:
                di_text = di if len(di) <= 120 else di[:117] + "..."
                lines.append(f"  - {di_text}")

        if profile.side_effects:
            lines.append("\nSide Effects:")
            for se in profile.side_effects[:10]:
                lines.append(f"  - {se}")

        if profile.contraindications:
            lines.append("\nContraindications:")
            for ci in profile.contraindications[:8]:
                lines.append(f"  - {ci}")

        if profile.clinical_evidence_summary:
            ces = profile.clinical_evidence_summary
            if len(ces) > 400:
                ces = ces[:397] + "..."
            lines.append(f"\nEvidence:\n{ces}")

        if profile.research_sources:
            lines.append(f"\nSources ({len(profile.research_sources)}):")
            for src in profile.research_sources[:8]:
                lines.append(f"  {src}")

        lines.append(f"\nQuality: {profile.quality_score:.0%}")
        return "\n".join(lines)
