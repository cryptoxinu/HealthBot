"""Genetics analysis handlers mixin."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger("healthbot")


class GeneticsAnalysisMixin:
    """Handlers for /genetics and its sub-commands."""

    async def genetics(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /genetics — view genetic data and risk findings.

        /genetics         — summary (variant count + risk findings)
        /genetics risks   — detailed risk scan
        /genetics research <rsid> — research a variant via Claude CLI
        """
        if not self._check_auth(update) or not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        args = context.args or []
        user_id = update.effective_user.id if update.effective_user else 0
        db = self._core._get_db()

        if not args or args[0].lower() == "summary":
            await self._genetics_summary(update, db, user_id)
        elif args[0].lower() == "risks":
            await self._genetics_risks(update, db, user_id)
        elif args[0].lower() == "research" and len(args) > 1:
            await self._genetics_research(update, db, user_id, args[1])
        else:
            await update.message.reply_text(
                "Usage:\n"
                "/genetics — summary of stored variants + risks\n"
                "/genetics risks — detailed risk scan\n"
                "/genetics research <rsid> — research a specific variant"
            )

    async def _genetics_summary(
        self, update: Update, db: object, user_id: int,
    ) -> None:
        """Show genetic data summary."""
        import asyncio

        from healthbot.bot.formatters import paginate
        from healthbot.bot.typing_helper import TypingIndicator

        count = db.get_genetic_variant_count(user_id)
        if count == 0:
            await update.message.reply_text(
                "No genetic data on file.\n\n"
                "Upload your TellMeGen or 23andMe raw data file "
                "(TXT or CSV) to get started."
            )
            return

        async with TypingIndicator(update.effective_chat):
            from healthbot.reasoning.genetic_risk import GeneticRiskEngine

            engine = GeneticRiskEngine(db)
            findings = await asyncio.to_thread(engine.scan_variants, user_id)

        msg = f"Genetic variants on file: {count:,}\n\n"
        if findings:
            msg += engine.format_summary(findings)
        else:
            msg += "No clinically significant risk variants detected."

        for page in paginate(msg):
            await update.message.reply_text(page)

        # Genetic risk chart
        if findings:
            from healthbot.export.chart_generator import genetic_risk_chart
            chart_bytes = genetic_risk_chart(findings)
            if chart_bytes:
                import io as iomod
                await update.message.reply_photo(photo=iomod.BytesIO(chart_bytes))

    async def _genetics_risks(
        self, update: Update, db: object, user_id: int,
    ) -> None:
        """Run detailed genetic risk scan with lab cross-references."""
        import asyncio

        from healthbot.bot.formatters import paginate
        from healthbot.bot.typing_helper import TypingIndicator
        from healthbot.reasoning.genetic_risk import GeneticRiskEngine

        count = db.get_genetic_variant_count(user_id)
        if count == 0:
            await update.message.reply_text("No genetic data on file.")
            return

        async with TypingIndicator(update.effective_chat):
            engine = GeneticRiskEngine(db)
            findings = await asyncio.to_thread(engine.scan_variants, user_id)
            correlations = await asyncio.to_thread(
                engine.cross_reference_labs, findings, user_id,
            )

        if not findings:
            await update.message.reply_text(
                f"{count:,} variants scanned — no clinically significant risks found."
            )
            return

        msg = engine.format_summary(findings)

        if correlations:
            msg += "\n\nLab Correlations:\n"
            for corr in correlations:
                f = corr["finding"]
                msg += f"\n{f.gene} + abnormal labs:\n"
                for lab in corr["matching_labs"]:
                    name = lab.get("test_name") or lab.get("canonical_name", "?")
                    val = lab.get("value", "?")
                    unit = lab.get("unit", "")
                    flag = lab.get("flag", "")
                    msg += f"  - {name}: {val} {unit} ({flag})\n"

        for page in paginate(msg):
            await update.message.reply_text(page)

        # Send genetic risk chart
        try:
            import io

            from healthbot.export.chart_generator import genetic_risk_chart
            chart_bytes = genetic_risk_chart(findings)
            if chart_bytes:
                img = io.BytesIO(chart_bytes)
                img.name = "genetic_risk.png"
                await update.message.reply_photo(photo=img)
        except Exception as e:
            logger.debug("Genetic risk chart skipped: %s", e)

    async def _genetics_research(
        self, update: Update, db: object, user_id: int, rsid: str,
    ) -> None:
        """Research a specific variant via Claude CLI."""
        import asyncio

        from healthbot.bot.formatters import paginate, strip_markdown
        from healthbot.bot.typing_helper import TypingIndicator

        # Look up the variant
        variants = db.get_genetic_variants(user_id, rsids=[rsid])
        if not variants:
            await update.message.reply_text(
                f"Variant {rsid} not found in your data."
            )
            return

        var = variants[0]
        genotype = var.get("genotype", "?")

        # Build research query (no PII — just rsid + genotype)
        query = (
            f"Health implications of {rsid} {genotype} genotype. "
            f"Clinical significance, monitoring recommendations, "
            f"drug interactions, and relevant studies."
        )

        await update.message.reply_text(f"Researching {rsid} ({genotype})...")

        try:
            async with TypingIndicator(update.effective_chat):
                from healthbot.research.claude_cli_client import (
                    ClaudeCLIResearchClient,
                )

                client = ClaudeCLIResearchClient(
                    self._core._config, self._core._fw,
                )
                result = await asyncio.to_thread(client.research, query, "")

            result = strip_markdown(result)
            for page in paginate(result):
                await update.message.reply_text(page)
        except Exception as e:
            await update.message.reply_text(
                f"Research failed: {type(e).__name__}\n"
                "Make sure Claude CLI is installed and authenticated."
            )
