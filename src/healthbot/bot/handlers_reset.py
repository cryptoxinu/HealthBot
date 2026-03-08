"""Telegram handlers for /reset and /delete commands.

Provides data deletion with text-based YES confirmation and automatic
backup before destructive operations.
"""
from __future__ import annotations

import logging
import time

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.middleware import rate_limited
from healthbot.config import Config
from healthbot.data.bulk_ops import CATEGORY_TABLE, BulkOps
from healthbot.security.key_manager import KeyManager

logger = logging.getLogger("healthbot")

CONFIRM_TIMEOUT = 60  # seconds


class ResetHandlers:
    """Handlers for /reset and /delete with confirmation state."""

    def __init__(
        self,
        config: Config,
        key_manager: KeyManager,
        get_db: callable,
        get_vault: callable,
        check_auth: callable,
    ) -> None:
        self._config = config
        self._km = key_manager
        self._get_db = get_db
        self._get_vault = get_vault
        self._check_auth = check_auth
        # user_id -> {action, category, expires}
        self._pending: dict[int, dict] = {}
        self._wipe_chat: callable | None = None  # async (bot) -> None
        self._start_onboard: callable | None = None  # async (update, user_id) -> None
        self._clear_claude_state: callable | None = None  # () -> None

    def set_post_reset_hooks(
        self, wipe_chat: callable, start_onboard: callable
    ) -> None:
        """Register callbacks to run after a full reset."""
        self._wipe_chat = wipe_chat
        self._start_onboard = start_onboard

    def set_clear_claude_state(self, callback: callable) -> None:
        """Register callback to nuke in-memory Claude conversation state."""
        self._clear_claude_state = callback

    def is_awaiting_confirm(self, user_id: int) -> bool:
        """Check if user has a pending, non-expired confirmation."""
        pending = self._pending.get(user_id)
        if pending is None:
            return False
        if time.time() > pending["expires"]:
            self._pending.pop(user_id, None)
            return False
        return True

    async def handle_confirm(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> bool:
        """Handle YES/NO response to pending confirmation.

        Returns True if the message was consumed (caller should stop routing).
        """
        if not self._check_auth(update):
            return False
        user_id = update.effective_user.id if update.effective_user else 0
        pending = self._pending.pop(user_id, None)
        if pending is None:
            return False

        # Verify vault is still unlocked before destructive action
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Cancelled.")
            return True

        text = (update.message.text or "").strip().upper()
        if text != "YES":
            await update.message.reply_text("Cancelled.")
            return True

        self._km.touch()

        # Execute the confirmed action
        if pending["action"] == "reset":
            await self._execute_reset(update)
        elif pending["action"] == "delete_labs":
            await self._execute_delete_labs(update)
        elif pending["action"] == "delete_doc":
            await self._execute_delete_doc(update, pending["doc_info"])
        elif pending["action"] == "delete":
            await self._execute_delete(update, pending["category"])

        return True

    @rate_limited(max_per_minute=3)
    async def reset(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /reset command -- full vault data wipe."""
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        db = self._get_db()
        ops = BulkOps(db, self._get_vault())
        counts = ops.count_all()
        total = sum(counts.values())

        if total == 0:
            await update.message.reply_text("Vault is already empty. Nothing to reset.")
            return

        # Show what will be deleted
        lines = ["This will DELETE all health data:", ""]
        for cat, count in counts.items():
            if count > 0:
                lines.append(f"  {cat}: {count} records")
        lines.append(f"\n  Total: {total} records")
        lines.append("")
        lines.append("A backup will be created first.")
        lines.append(f"Type YES to confirm (expires in {CONFIRM_TIMEOUT}s).")

        user_id = update.effective_user.id
        self._pending[user_id] = {
            "action": "reset",
            "category": None,
            "expires": time.time() + CONFIRM_TIMEOUT,
        }
        await update.message.reply_text("\n".join(lines))

    @rate_limited(max_per_minute=3)
    async def delete(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /delete <category> command -- selective deletion."""
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        args = context.args or []
        if not args:
            valid = ", ".join(sorted(CATEGORY_TABLE)) + ", all"
            await update.message.reply_text(
                f"Usage: /delete <category>\nCategories: {valid}"
            )
            return

        category = args[0].lower()
        if category != "all" and category not in CATEGORY_TABLE:
            valid = ", ".join(sorted(CATEGORY_TABLE)) + ", all"
            await update.message.reply_text(
                f"Unknown category: {category}\nValid: {valid}"
            )
            return

        db = self._get_db()
        ops = BulkOps(db, self._get_vault())
        counts = ops.count_all()

        if category == "all":
            total = sum(counts.values())
        else:
            total = counts.get(category, 0)

        if total == 0:
            await update.message.reply_text(f"No {category} data to delete.")
            return

        user_id = update.effective_user.id
        self._pending[user_id] = {
            "action": "delete",
            "category": category,
            "expires": time.time() + CONFIRM_TIMEOUT,
        }
        await update.message.reply_text(
            f"Delete {total} {category} record(s)?\n"
            f"Type YES to confirm (expires in {CONFIRM_TIMEOUT}s)."
        )

    @rate_limited(max_per_minute=3)
    async def delete_labs(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /delete_labs — delete lab results + lab PDFs only.

        Preserves survey data, demographics, medications, wearable data,
        and conversation memory.
        """
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        db = self._get_db()
        ops = BulkOps(db, self._get_vault())
        counts = ops.count_lab_records()
        total = sum(counts.values())

        if total == 0:
            await update.message.reply_text("No lab records to delete.")
            return

        lines = [
            "This will delete your lab data ONLY:",
            "",
            f"  Lab results: {counts.get('lab_results', 0)}",
            f"  Lab PDF documents: {counts.get('documents', 0)}",
            "",
            "Your profile, survey answers, medications, wearable data, "
            "and conversation history will NOT be touched.",
            "",
            f"Type YES to confirm (expires in {CONFIRM_TIMEOUT}s).",
        ]

        user_id = update.effective_user.id
        self._pending[user_id] = {
            "action": "delete_labs",
            "category": None,
            "expires": time.time() + CONFIRM_TIMEOUT,
        }
        await update.message.reply_text("\n".join(lines))

    @rate_limited(max_per_minute=3)
    async def delete_doc(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /delete_doc <n> — delete a single document and its labs."""
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        args = context.args or []
        if not args:
            await update.message.reply_text(
                "Usage: /delete_doc <n>\n"
                "Use /docs to see document numbers."
            )
            return

        try:
            idx = int(args[0]) - 1
        except ValueError:
            await update.message.reply_text("Invalid number. Use /docs to see document list.")
            return

        db = self._get_db()
        user_id = update.effective_user.id
        docs = db.list_documents(user_id=user_id)

        if idx < 0 or idx >= len(docs):
            await update.message.reply_text(
                f"Invalid number. You have {len(docs)} document(s). Use /docs to see the list."
            )
            return

        doc = docs[idx]
        doc_id = doc["doc_id"]
        filename = doc.get("filename") or "untitled"
        blob_path = doc.get("enc_blob_path", "")

        # Count associated labs
        lab_count = 0
        if blob_path:
            row = db.conn.execute(
                "SELECT COUNT(*) AS n FROM observations WHERE source_doc_id = ?",
                (blob_path,),
            ).fetchone()
            lab_count = row["n"] if row else 0

        self._pending[user_id] = {
            "action": "delete_doc",
            "doc_info": {"doc_id": doc_id, "blob_path": blob_path, "filename": filename},
            "expires": time.time() + CONFIRM_TIMEOUT,
        }
        await update.message.reply_text(
            f"Delete doc #{idx + 1} '{filename}' and {lab_count} lab result(s)?\n"
            f"Type YES to confirm (expires in {CONFIRM_TIMEOUT}s)."
        )

    async def _execute_delete_doc(self, update: Update, doc_info: dict) -> None:
        """Execute single document deletion after confirmation."""
        db = self._get_db()

        # Open Clean DB for Tier 2 cleanup
        clean_db = None
        try:
            from healthbot.data.clean_db import CleanDB

            if self._config.clean_db_path.exists():
                clean_db = CleanDB(self._config.clean_db_path)
                clean_db.open(clean_key=self._km.get_clean_key())
        except Exception as e:
            logger.debug("Clean DB open for doc delete: %s", e)

        try:
            ops = BulkOps(
                db, self._get_vault(),
                clean_db=clean_db, config=self._config,
            )
            counts = ops.delete_document_cascade(doc_info["doc_id"])
            ops.vacuum()
        finally:
            if clean_db:
                try:
                    clean_db.close()
                except Exception:
                    pass

        total = sum(counts.values())
        await update.message.reply_text(
            f"DELETED '{doc_info['filename']}' — {total} items removed.\n"
            f"Labs: {counts.get('observations', 0)}, "
            f"Search: {counts.get('search_obs', 0) + counts.get('search_clinical', 0)}, "
            f"Blobs: {counts.get('vault_blobs', 0)}\n"
            f"Database compacted."
        )
        logger.info("Document deleted: %s %s", doc_info["doc_id"], counts)

    async def _execute_delete_labs(self, update: Update) -> None:
        """Execute lab record deletion after confirmation.

        Wipes lab data from all tiers: raw vault, Clean DB, Claude state,
        and all associated PDF blobs (original + redacted).
        """
        db = self._get_db()

        # Open Clean DB for Tier 2 cleanup
        clean_db = None
        try:
            from healthbot.data.clean_db import CleanDB

            if self._config.clean_db_path.exists():
                clean_db = CleanDB(self._config.clean_db_path)
                clean_db.open(clean_key=self._km.get_clean_key())
        except Exception as e:
            logger.debug("Clean DB open for lab delete: %s", e)

        try:
            ops = BulkOps(
                db, self._get_vault(),
                clean_db=clean_db, config=self._config,
            )
            counts = ops.delete_lab_records()
            ops.vacuum()
        finally:
            if clean_db:
                try:
                    clean_db.close()
                except Exception:
                    pass

        # Clear in-memory Claude conversation state so it can't
        # reconstruct lab narrative from cached history/memory
        if self._clear_claude_state:
            try:
                self._clear_claude_state()
            except Exception as e:
                logger.debug("Clear Claude state after lab wipe: %s", e)

        total = sum(counts.values())
        await update.message.reply_text(
            f"LAB WIPE COMPLETE — {total} items removed.\n"
            f"Labs: {counts.get('lab_results', 0)}, "
            f"PDFs: {counts.get('documents', 0)}, "
            f"Hypotheses: {counts.get('hypotheses', 0)}, "
            f"KB: {counts.get('knowledge_base', 0)}\n"
            f"Clean DB, Claude memory, and vector index wiped.\n"
            f"Database compacted. You can re-upload your PDFs now."
        )
        logger.info("Lab records deleted (full wipe): %s", counts)

    async def _execute_reset(self, update: Update) -> None:
        """Execute full reset after confirmation."""
        await update.message.reply_text("Creating backup before reset...")

        try:
            from healthbot.vault_ops.backup import VaultBackup

            vb = VaultBackup(self._config, self._km)
            backup_path = vb.create_backup()
            await update.message.reply_text(f"Backup saved: {backup_path.name}")
        except Exception as e:
            logger.error("Backup failed during reset: %s", e)
            await update.message.reply_text(
                f"Backup failed: {e}\nReset aborted for safety."
            )
            return

        db = self._get_db()
        ops = BulkOps(db, self._get_vault())
        results = ops.reset_all()
        ops.vacuum()

        total = sum(results.values())
        logger.info("Vault reset: %s", results)

        # Wipe chat history so the slate is truly clean
        if self._wipe_chat:
            try:
                await self._wipe_chat(update.get_bot())
            except Exception as e:
                logger.debug("Chat wipe after reset: %s", e)

        await update.effective_chat.send_message(
            f"RESET COMPLETE — {total} records removed.\n"
            "All health data, chat history, and profile wiped.\n"
            "Database compacted."
        )

        # Auto-start onboarding on a fresh slate
        user_id = update.effective_user.id if update.effective_user else 0
        if self._start_onboard:
            try:
                await self._start_onboard(update, user_id)
            except Exception as e:
                logger.warning("Auto-onboard after reset failed: %s", e)

    async def _execute_delete(self, update: Update, category: str) -> None:
        """Execute selective deletion after confirmation."""
        db = self._get_db()
        ops = BulkOps(db, self._get_vault())
        deleted = ops.delete_category(category)
        await update.message.reply_text(
            f"Deleted {deleted} {category} record(s)."
        )
        logger.info("Deleted category '%s': %d rows", category, deleted)
