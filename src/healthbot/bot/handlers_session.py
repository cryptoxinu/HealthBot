"""Session lifecycle and system administration command handlers."""
from __future__ import annotations

import logging
from pathlib import Path

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.middleware import require_auth, require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class SessionHandlers:
    """Session lifecycle and system administration commands."""

    def __init__(self, core: HandlerCore) -> None:
        self._core = core
        # Per-user Claude CLI auth setup state (awaiting API key input)
        self._claude_auth_awaiting: set[int] = set()
        # Rekey flow state: user_id -> step (1=awaiting current pass, 2=awaiting new pass)
        self._rekey_awaiting: dict[int, int] = {}

    @property
    def _km(self):
        return self._core._km

    def _check_auth(self, update: Update) -> bool:
        return self._core._check_auth(update)

    @require_auth
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /start command — context-aware welcome."""
        # Check system state
        is_first_time = not self._core._config.manifest_path.exists()
        is_unlocked = self._core._km.is_unlocked

        claude_status = "Claude CLI: not found"
        try:
            from healthbot.llm.claude_client import ClaudeClient
            claude = ClaudeClient(
                cli_path=self._core._config.claude_cli_path, timeout=10,
            )
            if claude.is_available():
                claude_status = "Claude CLI: installed"
            else:
                claude_status = (
                    "Claude CLI: not found "
                    "(install: brew install claude-code)"
                )
        except Exception:
            pass

        if is_first_time:
            await update.message.reply_text(
                "HealthBot — your private health advisor.\n\n"
                "Everything runs on this machine. Your data is encrypted\n"
                "(AES-256-GCM), never leaves your device, and only you\n"
                "can unlock it.\n\n"
                "Getting started:\n"
                "  1. /unlock — Create your vault (choose a passphrase)\n"
                "  2. Upload a lab PDF or type a health question\n"
                "  3. /help — See all commands\n\n"
                f"System: {claude_status}\n"
            )
        elif is_unlocked:
            remaining = self._core._km.get_remaining_seconds()
            mins = max(0, int(remaining // 60))
            await update.message.reply_text(
                f"Welcome back. Vault is unlocked ({mins} min remaining).\n\n"
                "Quick actions:\n"
                "  /insights — Health dashboard\n"
                "  /upload — Upload a lab PDF\n"
                "  /trend <test> — Lab trends\n"
                "  Or just type a health question.\n\n"
                f"System: {claude_status}\n"
                "Type /help for all commands."
            )
        else:
            await update.message.reply_text(
                "Welcome back. Vault is locked.\n\n"
                "Send /unlock to start your session.\n\n"
                f"System: {claude_status}\n"
                "Type /help for all commands."
            )

    @require_auth
    async def help_cmd(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /help command — full command reference."""
        await update.message.reply_text(
            "Your health data and all /commands stay local.\n"
            "Free text goes to Claude (anonymized — no PII leaves your machine).\n\n"
            "Session:\n"
            "  /unlock — Open your vault\n"
            "  /lock — Lock your vault\n"
            "  /backup — Create encrypted backup\n"
            "  /rekey — Change vault passphrase\n"
            "  /version — Build info\n"
            "  /restart — Restart bot process\n"
            "  /audit — Vault security audit\n"
            "  /auth_status — Integration status\n\n"
            "Health Analysis:\n"
            "  /insights — Dashboard overview\n"
            "  /trend <test> — Lab trend analysis\n"
            "  /correlate — Lab + wearable correlations\n"
            "  /healthreview — Full health review\n"
            "  /hypotheses — Auto-detected patterns\n"
            "  /symptoms — Symptom analytics\n"
            "  /profile — Full health profile\n"
            "  /aboutme — What I know about you\n"
            "  /ask <question> — Search your records\n"
            "  /overdue — Overdue screenings\n"
            "  /gaps — Lab panel gap detection\n"
            "  /recommend — Condition-based lab recs\n\n"
            "Medical:\n"
            "  /interactions — Drug interaction check\n"
            "  /doctorprep — Doctor visit prep\n"
            "  /doctorpacket — PDF doctor packet\n"
            "  /template — Doctor discussion templates\n"
            "  /research_cloud <topic> — Research (sanitized)\n"
            "  /evidence — Browse cached research\n"
            "  /remind — Medication reminders\n"
            "  /digest — Daily health briefing\n"
            "  /log <event> — Log health event\n"
            "  /undo — Undo last logged event\n\n"
            "Data:\n"
            "  /sync — Sync all connected sources\n"
            "  /connectors — Data source status\n"
            "  /whoop_auth — Connect WHOOP account\n"
            "  /oura_auth — Connect Oura Ring\n"
            "  /apple_sync — Sync Apple Health\n"
            "  /import — Import Apple Health ZIP\n"
            "  /mychart — Import MyChart CCDA/FHIR\n"
            "  /upload — Secure upload mode\n"
            "  /finish — End upload mode\n"
            "  /cleansync — Full clean DB re-sync\n"
            "  /export fhir|csv — Export data\n"
            "  /docs — List uploaded PDFs\n"
            "  /onboard — Health profile interview\n"
            "  /ingest on|done — Bulk upload mode\n\n"
            "Danger Zone:\n"
            "  /reset — Wipe all health data\n"
            "  /delete <cat> — Delete data category"
        )

    @require_auth
    async def unlock(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /unlock command. Accepts inline passphrase: /unlock <passphrase>."""
        # If passphrase provided inline, delete the message and process immediately
        if context.args:
            passphrase = " ".join(context.args)
            try:
                await update.message.delete()
            except Exception:
                pass
            await self._core._router.try_unlock(
                passphrase, update, context,
            )
            return

        user_id = update.effective_user.id
        self._core._router.awaiting_passphrase.add(user_id)
        await update.message.reply_text(
            "Enter your vault passphrase.\n"
            "(Your message will be deleted after reading.)"
        )

    @require_auth
    async def lock(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /lock command — lock vault, wipe chat, notify."""
        # Track the /lock command itself
        if update.message:
            self._core.track_message(update.effective_chat.id, update.message.message_id)

        # _on_vault_lock callback handles consolidation + cleanup before key is zeroed
        self._core._km.lock()
        if self._core._db:
            self._core._db.close()
            self._core._db = None

        # Wipe all session messages (pending_wipe was set by _on_vault_lock)
        self._core._pending_wipe = False
        await self._core.wipe_session_chat(context.bot)

        # Send notification (this message remains visible)
        await update.effective_chat.send_message(
            "Vault locked. Chat cleared. Session key wiped."
        )

    @require_auth
    async def feedback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /feedback — flag a bad bot response for eval capture."""
        user_feedback = " ".join(context.args) if context.args else ""
        if not user_feedback:
            await update.message.reply_text(
                "Usage: /feedback <what was wrong>\n"
                "This captures the last response for improvement."
            )
            return

        last_input = self._core._last_user_input
        last_response = self._core._last_bot_response

        if not last_response:
            await update.message.reply_text(
                "No previous response to attach. Feedback noted."
            )

        import json
        from datetime import UTC, datetime
        from pathlib import Path

        entry = {
            "id": f"user_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}",
            "category": "user_report",
            "input": last_input,
            "bot_response": last_response,
            "user_feedback": user_feedback,
            "timestamp": datetime.now(UTC).isoformat(),
            "status": "new",
        }
        eval_dir = Path.home() / ".healthbot" / "eval"
        eval_dir.mkdir(parents=True, exist_ok=True)
        with open(eval_dir / "failing_cases.jsonl", "a") as f:
            f.write(json.dumps(entry) + "\n")

        await update.message.reply_text("Thanks — feedback captured for review.")

    @require_unlocked
    async def backup(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /backup command."""
        await update.message.reply_text("Creating encrypted backup...")
        async with TypingIndicator(update.effective_chat):
            from healthbot.vault_ops.backup import VaultBackup
            vb = VaultBackup(self._core._config, self._core._km)
            path = vb.create_backup()
        await update.message.reply_text(f"Backup created: {path.name}")

    @require_unlocked
    async def rekey(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /rekey — two-step passphrase change.

        Step 1: Verify current passphrase.
        Step 2: Set new passphrase and re-encrypt vault.
        """
        user_id = update.effective_user.id if update.effective_user else 0
        self._rekey_awaiting[user_id] = 1
        await update.message.reply_text(
            "Re-key vault: changing your passphrase.\n\n"
            "Step 1/2: Send your CURRENT passphrase.\n"
            "(Your message will be deleted immediately.)\n\n"
            "Send /cancel to abort."
        )

    def is_awaiting_rekey(self, user_id: int) -> bool:
        """Check if user is in the rekey multi-step flow."""
        return user_id in self._rekey_awaiting

    async def handle_rekey_input(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> bool:
        """Process passphrase input during rekey flow.

        Returns True if the message was consumed.
        """
        user_id = update.effective_user.id if update.effective_user else 0
        step = self._rekey_awaiting.get(user_id)
        if step is None:
            return False

        text = (update.message.text or "").strip()

        # Handle /cancel
        if text.lower() == "/cancel":
            self._rekey_awaiting.pop(user_id, None)
            await update.message.reply_text("Re-key cancelled.")
            return True

        # Delete passphrase message immediately
        try:
            await update.message.delete()
        except Exception:
            pass

        if not text:
            await update.effective_chat.send_message(
                "Passphrase cannot be empty. Try again."
            )
            return True

        chat = update.effective_chat

        if step == 1:
            # Verify current passphrase
            if not self._core._km.verify_passphrase(text):
                self._rekey_awaiting.pop(user_id, None)
                await chat.send_message(
                    "Current passphrase is incorrect. Re-key aborted.\n"
                    "Send /rekey to try again."
                )
                return True
            # Move to step 2
            self._rekey_awaiting[user_id] = 2
            await chat.send_message(
                "Current passphrase verified.\n\n"
                "Step 2/2: Send your NEW passphrase.\n"
                "(Your message will be deleted immediately.)"
            )
            return True

        if step == 2:
            self._rekey_awaiting.pop(user_id, None)
            # Check vault still unlocked
            if not self._core._km.is_unlocked:
                await chat.send_message(
                    "Vault locked during re-key. Aborted.\n"
                    "Unlock and try /rekey again."
                )
                return True
            await chat.send_message(
                "Re-encrypting vault... this may take a moment."
            )
            try:
                async with TypingIndicator(chat):
                    from healthbot.vault_ops.rekey import VaultRekey
                    vr = VaultRekey(self._core._config, self._core._km)
                    backup_path = vr.rotate(text)
                await chat.send_message(
                    f"Vault re-encrypted with new passphrase.\n"
                    f"Safety backup: {backup_path.name}\n"
                    "Use the new passphrase to unlock from now on."
                )
            except Exception as e:
                logger.error("Rekey error: %s", e)
                await chat.send_message(f"Rekey failed: {type(e).__name__}")
            return True

        return False

    @require_auth
    async def version(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /version — show build version, commit hash, and model."""
        import subprocess

        from healthbot._version import __build_date__, __version__

        # Read git short hash at runtime (from repo, not vault)
        git_hash = "unknown"
        try:
            repo_root = Path(__file__).resolve().parents[3]
            import os
            _env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True, text=True, timeout=5,
                cwd=str(repo_root), env=_env,
            )
            if result.returncode == 0:
                git_hash = result.stdout.strip()
        except Exception:
            pass

        lines = [
            f"HealthBot v{__version__}",
            f"Built: {__build_date__}",
            f"Commit: {git_hash}",
            "Engine: Claude CLI",
        ]
        await update.message.reply_text("\n".join(lines))

    @require_unlocked
    async def digest(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /digest — show or configure daily health digest.

        Usage:
            /digest         — send digest now
            /digest HH:MM   — set daily digest time
            /digest off     — disable daily digest
        """
        from healthbot.bot.formatters import paginate

        args = context.args
        if args:
            arg = args[0].lower()
            if arg == "off":
                self._core._config.digest_time = ""
                await update.message.reply_text("Daily digest disabled.")
                return
            # Try to parse as HH:MM
            try:
                parts = arg.split(":")
                hour, minute = int(parts[0]), int(parts[1])
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    self._core._config.digest_time = f"{hour:02d}:{minute:02d}"
                    await update.message.reply_text(
                        f"Daily digest set for {hour:02d}:{minute:02d}."
                    )
                    return
            except (ValueError, IndexError):
                pass
            await update.message.reply_text(
                "Usage: /digest [HH:MM|off]\n"
                "  /digest         — send digest now\n"
                "  /digest 08:00   — set daily time\n"
                "  /digest off     — disable"
            )
            return

        # No args — send digest now
        from healthbot.reasoning.digest import build_daily_digest, format_digest

        db = self._core._get_db()
        uid = update.effective_user.id
        report = build_daily_digest(db, uid)
        text = format_digest(report)
        for page in paginate(text):
            await update.message.reply_text(page)

    @require_unlocked
    async def ingest(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /ingest command — toggle ingestion mode."""
        args = context.args
        if not args:
            status = "ON" if self._core._ingestion_mode else "OFF"
            count = self._core._ingestion_count
            await update.message.reply_text(
                f"Ingestion mode: {status}"
                + (f" ({count} docs ingested)" if count else "")
                + "\n\nUsage:\n"
                "/ingest on — mute notifications, bulk upload\n"
                "/ingest done — run full analysis and report"
            )
            return

        action = args[0].lower()
        if action in ("on", "start"):
            if self._core._upload_mode:
                await update.message.reply_text(
                    "Upload mode is active. Use /finish first, "
                    "then /ingest on for bulk ingestion."
                )
                return
            self._core._ingestion_mode = True
            self._core._ingestion_count = 0
            self._core._router.ingestion_mode = True
            self._core._router._ingestion_count_cb = self._increment_ingest
            if self._core._scheduler:
                self._core._scheduler.ingestion_mode = True
            await update.message.reply_text(
                "Ingestion mode ON. Upload your PDFs/ZIPs — "
                "notifications muted until you send /ingest done."
            )
        elif action in ("done", "off", "stop"):
            if not self._core._ingestion_mode:
                await update.message.reply_text("Ingestion mode is already off.")
                return

            count = self._core._ingestion_count
            self._core._ingestion_mode = False
            self._core._ingestion_count = 0
            self._core._router.ingestion_mode = False
            self._core._router._ingestion_count_cb = None
            if self._core._scheduler:
                self._core._scheduler.ingestion_mode = False

            await update.message.reply_text(
                f"Ingestion mode OFF. {count} doc{'s' if count != 1 else ''} "
                f"ingested. Running analysis..."
            )

            # Batch post-ingest analysis
            async with TypingIndicator(update.effective_chat):
                await self._run_batch_analysis(update, context)
        else:
            await update.message.reply_text(
                "Usage: /ingest on | /ingest done"
            )

    def _increment_ingest(self) -> None:
        """Increment ingestion counter."""
        self._core._ingestion_count += 1

    @require_unlocked
    async def upload(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /upload command — enter secure upload mode."""
        if self._core._upload_mode:
            count = self._core._upload_count
            await update.message.reply_text(
                "Upload mode already active"
                + (f" ({count} docs uploaded)" if count else "")
                + ".\nSend documents or /finish when done."
            )
            return
        if self._core._ingestion_mode:
            await update.message.reply_text(
                "Ingestion mode is active. Use /ingest done first, "
                "then /upload for secure upload mode."
            )
            return
        self._core._upload_mode = True
        self._core._upload_count = 0
        self._core._router.upload_mode = True
        self._core._router._upload_count_cb = self._increment_upload
        if self._core._scheduler:
            self._core._scheduler.upload_mode = True
        # Schedule 10-min auto-finish timeout
        if context.job_queue:
            existing = context.job_queue.get_jobs_by_name("upload_timeout")
            for job in existing:
                job.schedule_removal()
            context.job_queue.run_once(
                self._upload_timeout, when=600, name="upload_timeout",
                chat_id=update.effective_chat.id,
                data={"chat_id": update.effective_chat.id},
            )
        await update.message.reply_text(
            "Secure upload mode active.\n"
            "Nothing leaves your machine while this is on.\n\n"
            "Upload PDFs, ZIPs, photos, or genetic data.\n"
            "Everything is encrypted immediately.\n\n"
            "Send /finish when done."
        )

    @require_unlocked
    async def finish(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /finish command — end secure upload mode."""
        if not self._core._upload_mode:
            await update.message.reply_text("No upload session active.")
            return

        count = self._core._upload_count
        self._core._upload_mode = False
        self._core._upload_count = 0
        self._core._router.upload_mode = False
        self._core._router._upload_count_cb = None
        if self._core._scheduler:
            self._core._scheduler.upload_mode = False
        # Cancel timeout job
        if context.job_queue:
            for job in context.job_queue.get_jobs_by_name("upload_timeout"):
                job.schedule_removal()

        s = "s" if count != 1 else ""
        await update.message.reply_text(
            f"{count} document{s} encrypted and stored."
        )

        # Batch analysis on uploaded data
        if count > 0:
            await update.message.reply_text("Running analysis...")
            async with TypingIndicator(update.effective_chat):
                await self._run_batch_analysis(update, context)

            # Sync clean DB so Claude gets new data context
            try:
                self._core._trigger_clean_sync()
            except Exception as e:
                logger.warning("Clean sync after upload failed: %s", e)

    def _increment_upload(self) -> None:
        """Increment upload counter."""
        self._core._upload_count += 1

    async def _upload_timeout(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Auto-finish upload mode after 10 minutes of inactivity."""
        if not self._core._upload_mode:
            return
        count = self._core._upload_count
        self._core._upload_mode = False
        self._core._upload_count = 0
        self._core._router.upload_mode = False
        self._core._router._upload_count_cb = None
        if self._core._scheduler:
            self._core._scheduler.upload_mode = False

        chat_id = context.job.data.get("chat_id") if context.job.data else None
        if chat_id:
            s = "s" if count != 1 else ""
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"Upload mode timed out (10 min).\n"
                    f"{count} document{s} encrypted and stored."
                ),
            )

    async def _run_batch_analysis(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Run deferred analysis after bulk ingestion."""
        from healthbot.bot.formatters import paginate

        user_id = update.effective_user.id if update.effective_user else 0
        parts = []

        # 1. Rebuild search index
        try:
            from healthbot.retrieval.search import SearchEngine
            from healthbot.security.vault import Vault

            db = self._core._get_db()
            vault = Vault(self._core._config.blobs_dir, self._core._km)
            engine = SearchEngine(self._core._config, db, vault)
            doc_count = engine.build_index()
            parts.append(f"Search index rebuilt ({doc_count} documents).")
        except Exception as e:
            logger.warning("Batch search index rebuild failed: %s", e)

        # 2. Alert check
        try:
            from healthbot.reasoning.watcher import HealthWatcher

            db = self._core._get_db()
            watcher = HealthWatcher(db, user_id=user_id)
            alerts = watcher.check_all()
            if alerts:
                for alert in alerts:
                    icon = {"urgent": "!", "watch": "~", "info": ""}.get(alert.severity, "")
                    parts.append(f"{icon} {alert.title}\n{alert.body}")
        except Exception as e:
            logger.warning("Batch alert check failed: %s", e)

        # 3. Overdue screening check (skip if paused)
        try:
            from healthbot.bot.overdue_pause import is_overdue_paused
            from healthbot.reasoning.overdue import OverdueDetector

            if not is_overdue_paused(self._core._config):
                db = self._core._get_db()
                detector = OverdueDetector(db)
                overdue = detector.check_overdue(user_id=user_id)
                if overdue:
                    lines = [f"  {o.test_name} ({o.days_overdue} days)" for o in overdue[:5]]
                    parts.append(
                        "Overdue screenings:\n" + "\n".join(lines)
                        + "\n  Use /snooze 2w to pause these reminders."
                    )
        except Exception as e:
            logger.warning("Batch overdue check failed: %s", e)

        # 5. Hypothesis scan
        try:
            from healthbot.reasoning.hypothesis_generator import HypothesisGenerator

            db = self._core._get_db()
            demographics = db.get_user_demographics(user_id)
            gen = HypothesisGenerator(db)
            new_hyps = gen.scan_all(
                user_id,
                sex=demographics.get("sex"),
                age=demographics.get("age"),
            )
            if new_hyps:
                lines = []
                for h in new_hyps[:3]:
                    evidence = ", ".join(h.evidence_for[:2])
                    lines.append(
                        f"  {h.title} ({h.confidence:.0%}, based on {evidence})"
                    )
                parts.append("Pattern detection:\n" + "\n".join(lines))
        except Exception as e:
            logger.warning("Batch hypothesis scan failed: %s", e)

        # 6. Panel gap detection
        try:
            from healthbot.reasoning.panel_gaps import PanelGapDetector

            db = self._core._get_db()
            detector = PanelGapDetector(db)
            report = detector.detect(user_id=user_id)
            if report.has_gaps:
                lines = []
                for pg in report.panel_gaps[:3]:
                    lines.append(f"  {pg.panel_name}: missing {', '.join(pg.missing)}")
                for cg in report.conditional_gaps[:3]:
                    lines.append(f"  Consider: {', '.join(cg.missing_tests)}")
                parts.append("Panel gaps:\n" + "\n".join(lines))
        except Exception as e:
            logger.warning("Batch panel gap check failed: %s", e)

        if parts:
            for page in paginate("\n\n".join(parts)):
                await update.message.reply_text(page)
        else:
            await update.message.reply_text("Analysis complete. No alerts or insights.")

    @require_auth
    async def pause_overdue(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        duration_text: str | None = None,
    ) -> None:
        """Handle natural-language 'pause overdue notifications for X'."""
        from datetime import timedelta

        from healthbot.bot.overdue_pause import (
            get_pause_until,
            is_overdue_paused,
            parse_duration,
        )
        from healthbot.bot.overdue_pause import pause_overdue as do_pause

        if is_overdue_paused(self._core._config):
            deadline = get_pause_until(self._core._config)
            if deadline:
                await update.message.reply_text(
                    f"Overdue notifications are already paused "
                    f"until {deadline.strftime('%b %d, %Y %H:%M UTC')}.\n"
                    "Say 'unpause notifications' to resume."
                )
                return

        if not duration_text:
            duration = timedelta(weeks=2)
        else:
            duration = parse_duration(duration_text)
            if duration is None:
                await update.message.reply_text(
                    "I didn't understand that duration.\n"
                    "Try: 'pause notifications for 2 weeks' or "
                    "'pause notifications for 3 days'."
                )
                return

        deadline = do_pause(self._core._config, duration)
        local = deadline.astimezone()
        await update.message.reply_text(
            f"Overdue notifications paused until "
            f"{local.strftime('%b %d, %Y %H:%M %Z')}.\n"
            "Say 'unpause notifications' or 'resume notifications' "
            "to turn them back on."
        )

    @require_auth
    async def unpause_overdue(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle natural-language 'unpause/resume overdue notifications'."""
        from healthbot.bot.overdue_pause import unpause_overdue as do_unpause

        was_paused = do_unpause(self._core._config)
        if was_paused:
            await update.message.reply_text(
                "Overdue notifications resumed. "
                "You'll see alerts at the next check."
            )
        else:
            await update.message.reply_text(
                "Overdue notifications were not paused."
            )

    @require_auth
    async def restart(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /restart command — restart the bot process remotely."""
        import os
        import subprocess

        # Locate botctl script relative to the source tree
        src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        repo_root = os.path.dirname(src_dir)
        botctl = os.path.join(repo_root, "scripts", "botctl")

        if not os.path.isfile(botctl):
            await update.message.reply_text("Cannot restart: botctl script not found.")
            return

        await update.message.reply_text("Restarting bot...")

        # Spawn detached botctl restart (it will SIGTERM this process, then start fresh)
        import os
        _env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(Path.home())}
        subprocess.Popen(
            [botctl, "restart"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            env=_env,
        )

    @require_auth
    async def audit(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /audit -- vault security audit + data integrity check."""
        await update.message.reply_text("Running vault security audit...")
        try:
            async with TypingIndicator(update.effective_chat):
                from healthbot.security.audit import VaultAuditor
                auditor = VaultAuditor(self._core._config)
                report = auditor.run_all()
            await update.message.reply_text(report.format())

            # Data integrity check (requires vault unlock)
            if self._core._km.is_unlocked:
                from healthbot.vault_ops.integrity_check import IntegrityChecker
                db = self._core._get_db()
                uid = update.effective_user.id if update.effective_user else 0
                checker = IntegrityChecker(db)
                integrity = checker.check_all(user_id=uid)
                await update.message.reply_text(checker.format_report(integrity))
        except Exception as e:
            logger.error("Audit error: %s", e)
            await update.message.reply_text(f"Audit failed: {type(e).__name__}")

    @require_unlocked
    async def integrity(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /integrity — vault data integrity check."""
        async with TypingIndicator(update.effective_chat):
            from healthbot.vault_ops.integrity_check import IntegrityChecker
            db = self._core._get_db()
            uid = update.effective_user.id if update.effective_user else 0
            checker = IntegrityChecker(db)
            report = checker.check_all(user_id=uid)
        await update.message.reply_text(checker.format_report(report))

    @require_unlocked
    async def refresh(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /refresh — re-export health data to Claude CLI context."""
        conv = self._core._get_claude_conversation()
        if conv is None:
            await update.message.reply_text(
                "Claude CLI not available.\n"
                "Install: brew install claude-code"
            )
            return

        await update.message.reply_text("Refreshing health data export...")
        async with TypingIndicator(update.effective_chat):
            try:
                # Prefer CleanDB (pre-anonymized, faster)
                clean = self._core._get_clean_db()
                if clean:
                    summary = conv.refresh_data_from_clean_db(clean)
                else:
                    from healthbot.llm.anonymizer import Anonymizer

                    db = self._core._get_db()
                    fw = self._core._fw
                    anon = Anonymizer(phi_firewall=fw, use_ner=False)
                    summary = conv.refresh_data(db, anon, fw)
            except Exception as e:
                logger.error("Data refresh failed: %s", e)
                await update.message.reply_text(
                    f"Refresh failed: {type(e).__name__}"
                )
                return

        await update.message.reply_text(f"Health data updated.\n\n{summary}")

    # ── PII alerts ────────────────────────────────────────────────

    @require_auth
    async def pii_alerts(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /pii_alerts — show PII detection alert history."""
        from healthbot.security.pii_alert import PiiAlertService

        svc = PiiAlertService.get_instance(
            log_dir=self._core._config.log_dir,
        )
        await update.message.reply_text(svc.format_report())

    # ── Integration status ─────────────────────────────────────────

    @require_auth
    async def auth_status(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /auth_status — show which integrations are configured."""
        import shutil

        from healthbot.security.keychain import Keychain

        keychain = Keychain()
        lines = ["Integration Status\n"]

        # Telegram (always configured if bot is running)
        lines.append("  Telegram: connected")

        # WHOOP
        whoop_id = keychain.retrieve("whoop_client_id")
        lines.append(
            "  WHOOP: connected" if whoop_id else "  WHOOP: not configured"
        )

        # Oura Ring
        oura_id = keychain.retrieve("oura_client_id")
        lines.append(
            "  Oura Ring: connected" if oura_id else "  Oura Ring: not configured"
        )

        # Claude API key (optional — CLI login is preferred)
        claude_key = keychain.retrieve("claude_api_key")
        if claude_key:
            lines.append("  Claude API key: set (Keychain)")
        else:
            lines.append("  Claude API key: not set (using CLI login)")

        # Claude CLI
        claude_path = shutil.which("claude")
        lines.append(
            "  Claude CLI: installed" if claude_path
            else "  Claude CLI: not found"
        )

        # Ollama
        ollama_path = shutil.which("ollama")
        if ollama_path:
            import os
            import subprocess
            _env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
            try:
                result = subprocess.run(
                    ["ollama", "list"],
                    capture_output=True, text=True, timeout=5, env=_env,
                )
                lines.append(
                    "  Ollama: running" if result.returncode == 0
                    else "  Ollama: installed (not running)"
                )
            except Exception:
                lines.append("  Ollama: installed (status unknown)")
        else:
            lines.append("  Ollama: not installed (optional)")

        # GLiNER NER
        try:
            from healthbot.security.ner_layer import NerLayer
            ner = NerLayer()
            lines.append(
                "  GLiNER NER: loaded" if ner.available
                else "  GLiNER NER: not available"
            )
        except Exception:
            lines.append("  GLiNER NER: not installed (optional)")

        # Vault status
        lines.append("")
        lines.append(
            "  Vault: unlocked" if self._km.is_unlocked
            else "  Vault: locked"
        )

        await update.message.reply_text("\n".join(lines))

    # ── Claude CLI authentication ───────────────────────────────────

    def is_awaiting_claude_auth(self, user_id: int) -> bool:
        """Check if user is in Claude CLI auth setup flow."""
        return user_id in self._claude_auth_awaiting

    async def handle_claude_auth_input(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> bool:
        """Process API key input during Claude auth setup.

        Returns True if the message was consumed.
        """
        user_id = update.effective_user.id if update.effective_user else 0
        if user_id not in self._claude_auth_awaiting:
            return False

        text = (update.message.text or "").strip()
        if not text:
            await update.message.reply_text(
                "Please send your Anthropic API key."
            )
            return True

        # Delete the message containing the API key immediately
        try:
            await update.message.delete()
        except Exception:
            pass

        self._claude_auth_awaiting.discard(user_id)
        await self._store_claude_key(update, text)
        return True

    @require_auth
    async def claude_auth(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /claude_auth — manage Claude CLI authentication.

        /claude_auth          -> show status and options
        /claude_auth setup    -> enter API key input flow (fallback)
        /claude_auth <key>    -> store API key (message deleted)
        /claude_auth remove   -> remove stored API key
        /claude_auth check    -> verify authentication works
        """
        from healthbot.security.keychain import Keychain

        args = context.args
        keychain = Keychain()
        user_id = update.effective_user.id if update.effective_user else 0

        # Inline API key: /claude_auth sk-ant-...
        if args and args[0].startswith("sk-ant-"):
            api_key = " ".join(args)
            try:
                await update.message.delete()
            except Exception:
                pass
            await self._store_claude_key(update, api_key)
            return

        # /claude_auth remove
        if args and args[0].lower() == "remove":
            deleted = keychain.delete("claude_api_key")
            # Reset Claude conversation so it picks up the change
            if self._core._claude_conversation:
                self._core._claude_conversation = None
            if deleted:
                await update.message.reply_text(
                    "Claude API key removed from Keychain.\n"
                    "Claude CLI will use your CLI login instead."
                )
            else:
                await update.message.reply_text(
                    "No Claude API key was stored."
                )
            return

        # /claude_auth check
        if args and args[0].lower() == "check":
            await update.message.reply_text("Checking Claude CLI...")
            async with TypingIndicator(update.effective_chat):
                from healthbot.llm.claude_client import ClaudeClient

                api_key = keychain.retrieve("claude_api_key")
                claude = ClaudeClient(
                    cli_path=self._core._config.claude_cli_path,
                    timeout=30,
                    api_key=api_key,
                )
                ok, msg = claude.diagnose()
            if ok:
                source = "API key" if api_key else "CLI login"
                await update.message.reply_text(
                    f"Claude CLI: OK ({source})\n{msg}"
                )
            else:
                await update.message.reply_text(
                    f"Claude CLI: PROBLEM\n\n{msg}"
                )
            return

        # /claude_auth setup — enter API key input flow
        if args and args[0].lower() == "setup":
            self._claude_auth_awaiting.add(user_id)
            await update.message.reply_text(
                "Send your Anthropic API key as the next message.\n"
                "(It will be deleted immediately and stored "
                "securely in macOS Keychain.)\n\n"
                "Get a key at: console.anthropic.com/settings/keys\n\n"
                "Note: this is optional — 'claude login' in terminal "
                "uses your existing subscription at no extra cost."
            )
            return

        # Unknown subcommand
        if args:
            await update.message.reply_text(
                "Unknown option. Commands:\n"
                "  /claude_auth — show status\n"
                "  /claude_auth check — test authentication\n"
                "  /claude_auth setup — enter API key\n"
                "  /claude_auth remove — remove stored key"
            )
            return

        # /claude_auth (no args) — show status and options
        existing = keychain.retrieve("claude_api_key")
        if existing:
            if len(existing) > 16:
                masked = existing[:7] + "..." + existing[-4:]
            else:
                masked = existing[:4] + "..." + existing[-2:]
            await update.message.reply_text(
                f"Claude API key configured: {masked}\n\n"
                "Commands:\n"
                "  /claude_auth check — verify it works\n"
                "  /claude_auth remove — remove key (use CLI login instead)\n"
                "  /claude_auth <key> — replace with new key"
            )
        else:
            await update.message.reply_text(
                "Claude CLI Authentication\n\n"
                "Recommended (free, uses your subscription):\n"
                "  Run in terminal: claude login\n\n"
                "Commands:\n"
                "  /claude_auth check — test if already authenticated\n"
                "  /claude_auth setup — use API key instead (costs money)"
            )

    async def _store_claude_key(
        self, update: Update, api_key: str,
    ) -> None:
        """Validate and store a Claude API key in Keychain."""
        from healthbot.security.keychain import Keychain

        # Format validation — Anthropic keys start with sk-ant-
        if not api_key.startswith("sk-ant-") or len(api_key) < 20:
            await update.effective_chat.send_message(
                "That doesn't look like a valid Anthropic API key.\n"
                "Keys start with 'sk-ant-' and are ~100+ characters.\n"
                "Get one at: console.anthropic.com/settings/keys\n"
                "Try again with /claude_auth"
            )
            return

        keychain = Keychain()
        keychain.store("claude_api_key", api_key)

        # Reset Claude conversation so it picks up the new key
        if self._core._claude_conversation:
            self._core._claude_conversation = None

        await update.effective_chat.send_message(
            "Claude API key stored in Keychain.\n"
            "Use /claude_auth check to verify.\n"
            "Send any message to start chatting."
        )

    @require_unlocked
    async def privacy(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /privacy command — view or set PDF extraction privacy mode."""
        args = context.args
        if not args:
            mode = self._core._config.privacy_mode
            desc = {
                "relaxed": (
                    "Relaxed — redacted PDF sent to Claude "
                    "(vision, better accuracy)"
                ),
                "strict": (
                    "Strict — only extracted text sent to Claude "
                    "(no files)"
                ),
            }
            await update.message.reply_text(
                f"Privacy mode: {desc.get(mode, mode)}\n\n"
                "Change with /privacy strict or /privacy relaxed"
            )
            return

        mode = args[0].lower()
        if mode not in ("strict", "relaxed"):
            await update.message.reply_text(
                "Usage: /privacy strict or /privacy relaxed"
            )
            return

        self._core._config.set_privacy_mode(mode)
        if mode == "strict":
            await update.message.reply_text(
                "Privacy: strict\n"
                "Only extracted text sent to Claude. "
                "No PDF files leave your machine."
            )
        else:
            await update.message.reply_text(
                "Privacy: relaxed\n"
                "Redacted PDFs sent to Claude for vision analysis.\n"
                "All PII is blacked out before sending. "
                "Better accuracy on complex reports."
            )

    @require_unlocked
    async def redacted(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Toggle sending redacted PDF back after ingestion. /redacted on|off"""
        args = context.args
        current = self._core._config.send_redacted_pdf
        if not args:
            state = "enabled" if current else "disabled"
            await update.message.reply_text(
                f"Redacted PDF send-back: {state}\n"
                "Usage: /redacted on|off"
            )
            return
        choice = args[0].lower()
        if choice not in ("on", "off"):
            await update.message.reply_text("Usage: /redacted on|off")
            return
        enabled = choice == "on"
        self._core._config.set_send_redacted_pdf(enabled)
        if enabled:
            await update.message.reply_text(
                "Redacted PDF send-back enabled. "
                "After ingestion, you'll receive the PII-stripped copy.\n"
                "Note: The redacted file will be sent via Telegram "
                "(stored on Telegram servers)."
            )
        else:
            await update.message.reply_text(
                "Redacted PDF send-back disabled."
            )

    @require_unlocked
    async def preferences(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /preferences — show or clear learned communication preferences.

        /preferences       — list all saved preferences
        /preferences clear — delete all preference entries
        """
        args = context.args or []

        clean_db = self._core._get_clean_db()
        if not clean_db:
            await update.message.reply_text("Clean DB not available.")
            return

        try:
            memories = clean_db.get_user_memory()
            prefs = [m for m in (memories or []) if m.get("category") == "preference"]

            if args and args[0].lower() == "clear":
                if not prefs:
                    await update.message.reply_text("No preferences to clear.")
                    return
                for pref in prefs:
                    clean_db.delete_user_memory(pref["key"])
                await update.message.reply_text(
                    f"Cleared {len(prefs)} preference(s). "
                    "Responses will use default style."
                )
                return

            if not prefs:
                await update.message.reply_text(
                    "No communication preferences saved yet.\n\n"
                    "Tell me how you'd like me to communicate:\n"
                    '  "Be more concise"\n'
                    '  "Use bullet points"\n'
                    '  "Give me more detail"\n\n'
                    "I'll remember and apply your preferences."
                )
                return

            lines = ["Communication Preferences:", ""]
            for p in prefs:
                lines.append(f"  - {p['key']}: {p['value']}")
            lines.append("")
            lines.append("/preferences clear — reset all preferences")
            await update.message.reply_text("\n".join(lines))
        finally:
            clean_db.close()

    @require_unlocked
    async def snooze(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Snooze overdue alerts. /snooze 2w | /snooze off"""
        args = context.args
        if not args:
            from healthbot.bot.overdue_pause import get_pause_until, is_overdue_paused
            if is_overdue_paused(self._core._config):
                deadline = get_pause_until(self._core._config)
                if deadline:
                    await update.message.reply_text(
                        f"Overdue alerts paused until "
                        f"{deadline:%Y-%m-%d}.\n"
                        "Use /snooze off to resume."
                    )
                else:
                    await update.message.reply_text(
                        "Overdue alerts are paused.\n"
                        "Use /snooze off to resume."
                    )
            else:
                await update.message.reply_text(
                    "Overdue alerts active.\n"
                    "Usage: /snooze 2w | /snooze 30d | /snooze off"
                )
            return
        text = " ".join(args)
        if text.lower() == "off":
            await self.unpause_overdue(update, context)
        else:
            await self.pause_overdue(update, context, duration_text=text)
