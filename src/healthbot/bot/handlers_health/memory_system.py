"""Memory, corrections, improvements, and audit command handlers."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate
from healthbot.bot.middleware import require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class MemorySystemMixin:
    """Mixin for memory, corrections, improvements, and audit commands."""

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
        /memory audit            — show memory change history
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
            elif args[0].lower() == "audit":
                await self._memory_audit(update, clean_db)
            else:
                await update.message.reply_text(
                    "Usage: /memory [clear|search|export|corrections"
                    "|improvements|approve|reject|audit]"
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

    async def _memory_audit(self, update: Update, clean_db) -> None:
        """Show memory change audit log."""
        async with TypingIndicator(update.effective_chat):
            entries = clean_db.get_memory_audit_log(limit=30)

        if not entries:
            await update.message.reply_text("No memory changes recorded yet.")
            return

        lines = ["MEMORY AUDIT LOG", "=" * 25, ""]
        for entry in entries:
            ts = (entry.get("changed_at") or "")[:19].replace("T", " ")
            key = entry.get("key", "?")
            old = entry.get("old_value", "")
            new = entry.get("new_value", "")
            source = entry.get("source_type", "")

            if old and old != new:
                lines.append(f"[{ts}] {key}")
                lines.append(f"  {old} -> {new}")
                if source:
                    lines.append(f"  source: {source}")
            else:
                lines.append(f"[{ts}] {key} = {new}")
                if source:
                    lines.append(f"  source: {source}")
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
        if not self._check_auth(update):
            query = update.callback_query
            await query.answer()
            await query.edit_message_text("Unauthorized.")
            return
        query = update.callback_query
        await query.answer()
        data = query.data or ""

        parts = data.split(":", 2)
        if len(parts) != 3 or parts[0] != "si":
            await query.edit_message_text("Invalid callback data.")
            return

        action = parts[1]
        imp_id = parts[2]

        if action not in ("approve", "reject"):
            await query.edit_message_text("Invalid callback action.")
            return
        if not imp_id or len(imp_id) > 64:
            await query.edit_message_text("Invalid improvement ID.")
            return

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
