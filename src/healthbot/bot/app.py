"""Telegram bot Application factory."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    TypeHandler,
    filters,
)

from healthbot.bot.handlers import Handlers
from healthbot.bot.handlers_identity import IdentityHandlers
from healthbot.bot.handlers_onboard import OnboardHandlers
from healthbot.bot.handlers_reset import ResetHandlers
from healthbot.config import Config
from healthbot.security.key_manager import KeyManager
from healthbot.security.keychain import Keychain
from healthbot.security.phi_firewall import PhiFirewall
from healthbot.security.vault import Vault

logger = logging.getLogger("healthbot")


def create_application(
    config: Config,
    phi_firewall: PhiFirewall | None = None,
) -> Application:
    """Build and configure the Telegram Application.

    Parameters
    ----------
    phi_firewall:
        Shared PhiFirewall instance.  When provided, the same object is
        used by all components (handlers, sync, log scrubber) so that
        identity patterns added at unlock propagate everywhere.
    """
    keychain = Keychain()
    token = keychain.retrieve("telegram_bot_token")
    if not token:
        raise RuntimeError(
            "Telegram bot token not found in Keychain.\n"
            "Store it with: healthbot --setup"
        )

    km = KeyManager(config)
    fw = phi_firewall or PhiFirewall()

    handlers = Handlers(config, km, fw)

    def _get_vault() -> Vault:
        return Vault(config.blobs_dir, km)

    core = handlers._core
    reset_handlers = ResetHandlers(
        config=config, key_manager=km,
        get_db=core._get_db, get_vault=_get_vault,
        check_auth=core._check_auth,
    )
    onboard_handlers = OnboardHandlers(
        config=config, key_manager=km,
        get_db=core._get_db, check_auth=core._check_auth,
    )
    identity_handlers = IdentityHandlers(
        config=config, key_manager=km,
        get_db=core._get_db, check_auth=core._check_auth,
    )
    core._router.set_reset_handlers(reset_handlers)
    core._router.set_onboard_handlers(onboard_handlers)
    core._router.set_identity_handlers(identity_handlers)
    core._router.set_data_handlers(handlers._data)
    core._router.set_session_handlers(handlers._session)

    # Post-reset hooks: wipe chat + auto-onboard
    async def _post_reset_onboard(update: Update, user_id: int) -> None:
        engine = onboard_handlers._get_engine()
        first_q = engine.start(user_id)
        await update.effective_chat.send_message(
            "Welcome back! Let's rebuild your health profile.\n"
            "I'll ask a few quick questions.\n\n" + first_q
        )

    reset_handlers.set_post_reset_hooks(
        wipe_chat=core.wipe_session_chat,
        start_onboard=_post_reset_onboard,
    )

    # Nuke in-memory Claude state after /delete_labs
    def _clear_claude_state() -> None:
        if core._claude_conversation:
            core._claude_conversation._history.clear()
            core._claude_conversation._memory.clear()
            core._claude_conversation = None

    reset_handlers.set_clear_claude_state(_clear_claude_state)

    # Reload firewall patterns immediately after identity profile changes
    identity_handlers.set_on_identity_updated(
        lambda user_id: core._router._load_identity_profile(user_id, core._get_db())
    )

    # Post-onboarding: show next steps + trigger aboutme narrative refresh
    async def _post_onboard_summary(update: Update, user_id: int) -> None:
        try:
            await update.effective_chat.send_message(
                "Profile saved. Use /aboutme to see your health summary.\n\n"
                "Next steps:\n"
                "- Upload a lab PDF to start tracking\n"
                "- /sync to connect a wearable (WHOOP, Oura)\n"
                "- Ask any health question"
            )
            # Auto-generate aboutme AI narrative in background
            import asyncio
            conv = core._get_claude_conversation()
            if conv:
                db = core._get_db()

                async def _refresh_narrative() -> None:
                    try:
                        prompt = (
                            "Based on everything you know about me, write a brief "
                            "health narrative (3-5 sentences). Focus on:\n"
                            "- Key trends or changes in my labs\n"
                            "- Active concerns or patterns you've noticed\n"
                            "- What to watch or test next\n\n"
                            "Be direct, no hedging. Plain text only."
                        )
                        response, _ = await asyncio.to_thread(
                            conv.handle_message, prompt, user_id,
                        )
                        if response:
                            from healthbot.bot.formatters import strip_markdown
                            from healthbot.bot.handlers_health import HealthHandlers
                            narrative = strip_markdown(response).strip()
                            HealthHandlers._store_aboutme_summary(
                                db, user_id, narrative,
                            )
                    except Exception as e:
                        logger.debug("Post-onboard narrative refresh: %s", e)

                asyncio.create_task(_refresh_narrative())
        except Exception as e:
            logger.debug("Post-onboard summary skipped: %s", e)

    onboard_handlers.set_on_complete(_post_onboard_summary)

    # Initialize PII alert service singleton
    from healthbot.security.pii_alert import PiiAlertService
    PiiAlertService.get_instance(log_dir=config.log_dir)

    builder = ApplicationBuilder().token(token)

    # Use local Bot API server if available (lifts 20 MB file limit)
    port = config.telegram_local_api_port
    try:
        import json as _json
        import urllib.request
        resp = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/bot{token}/getMe", timeout=3,
        )
        body = _json.loads(resp.read())
        resp.close()
        if body.get("ok"):
            from telegram.request import HTTPXRequest
            request = HTTPXRequest(
                read_timeout=300,
                write_timeout=300,
                connect_timeout=30,
            )
            builder = (
                builder
                .base_url(f"http://127.0.0.1:{port}/bot")
                .base_file_url(f"http://127.0.0.1:{port}/file/bot")
                .local_mode(True)
                .request(request)
            )
            logger.info("Using local Bot API server on port %d", port)
        else:
            raise ValueError("getMe not ok")
    except Exception:
        logger.info(
            "Local Bot API not available on port %d; "
            "using default (20 MB file limit)", port,
        )

    app = builder.build()

    # Command handlers
    app.add_handler(CommandHandler("start", handlers.start))
    app.add_handler(CommandHandler("help", handlers.help_cmd))
    app.add_handler(CommandHandler("unlock", handlers.unlock))
    app.add_handler(CommandHandler("lock", handlers.lock))
    app.add_handler(CommandHandler("memory", handlers.memory))
    app.add_handler(CommandHandler("insights", handlers.insights))
    app.add_handler(CommandHandler("dashboard", handlers.insights))
    app.add_handler(CommandHandler("summary", handlers.summary))
    app.add_handler(CommandHandler("trend", handlers.trend))
    app.add_handler(CommandHandler("ask", handlers.ask))
    app.add_handler(CommandHandler("overdue", handlers.overdue))
    app.add_handler(CommandHandler("correlate", handlers.correlate))
    app.add_handler(CommandHandler("doctorprep", handlers.doctorprep))
    app.add_handler(CommandHandler("research_cloud", handlers.research_cloud))
    app.add_handler(CommandHandler("gaps", handlers.gaps))
    app.add_handler(CommandHandler("healthreview", handlers.healthreview))
    app.add_handler(CommandHandler("feedback", handlers.feedback))
    app.add_handler(CommandHandler("backup", handlers.backup))
    app.add_handler(CommandHandler("rekey", handlers.rekey))
    app.add_handler(CommandHandler("doctorpacket", handlers.doctorpacket))
    app.add_handler(CommandHandler("version", handlers.version))
    app.add_handler(CommandHandler("audit", handlers.audit))
    app.add_handler(CommandHandler("interactions", handlers.interactions))
    app.add_handler(CommandHandler("hypotheses", handlers.hypotheses))
    app.add_handler(CommandHandler("profile", handlers.profile))
    app.add_handler(CommandHandler("aboutme", handlers.aboutme))
    app.add_handler(CommandHandler("labs", handlers.labs))
    app.add_handler(CommandHandler("recommend", handlers.recommend))
    app.add_handler(CommandHandler("template", handlers.template))
    app.add_handler(CommandHandler("evidence", handlers.evidence))
    app.add_handler(CommandHandler("oura_auth", handlers.oura_auth))
    app.add_handler(CommandHandler("oura", handlers.sync_oura))
    app.add_handler(CommandHandler("log", handlers.log_event))
    app.add_handler(CommandHandler("effectiveness", handlers.effectiveness))
    app.add_handler(CommandHandler("sideeffects", handlers.sideeffects))
    app.add_handler(CommandHandler("retests", handlers.retests))
    app.add_handler(CommandHandler("supplements", handlers.supplements))
    app.add_handler(CommandHandler("screenings", handlers.screenings))
    app.add_handler(CommandHandler("comorbidity", handlers.comorbidity))
    app.add_handler(CommandHandler("stress", handlers.stress))
    app.add_handler(CommandHandler("sleeprec", handlers.sleeprec))
    app.add_handler(CommandHandler("symptoms", handlers.symptoms))
    app.add_handler(CommandHandler("goals", handlers.goals))
    app.add_handler(CommandHandler("timeline", handlers.timeline))
    app.add_handler(CommandHandler("report", handlers.report))
    app.add_handler(CommandHandler("emergency", handlers.emergency))
    app.add_handler(CommandHandler("undo", handlers.undo))
    app.add_handler(CommandHandler("remind", handlers.remind))
    app.add_handler(CommandHandler("reminders", handlers.reminders))
    app.add_handler(CommandHandler("wearable_status", handlers.wearable_status))
    app.add_handler(CommandHandler("whoop_auth", handlers.whoop_auth))
    app.add_handler(CommandHandler("sync", handlers.sync_all))
    app.add_handler(CommandHandler("connectors", handlers.connectors))
    app.add_handler(CommandHandler("apple_sync", handlers.apple_sync))
    app.add_handler(CommandHandler("export", handlers.export_fhir))
    app.add_handler(CommandHandler("ai_export", handlers.ai_export))
    app.add_handler(CommandHandler("refresh", handlers.refresh))
    app.add_handler(CommandHandler("claude_auth", handlers.claude_auth))
    app.add_handler(CommandHandler("auth_status", handlers.auth_status))
    app.add_handler(CommandHandler("pii_alerts", handlers.pii_alerts))
    app.add_handler(CommandHandler("privacy", handlers.privacy))
    app.add_handler(CommandHandler("redacted", handlers.redacted))
    app.add_handler(CommandHandler("snooze", handlers.snooze))
    app.add_handler(CommandHandler("preferences", handlers.preferences))
    app.add_handler(CommandHandler("docs", handlers.docs))
    app.add_handler(CommandHandler("import", handlers.import_health))
    app.add_handler(CommandHandler("mychart", handlers.import_mychart))
    app.add_handler(CommandHandler("fasten", handlers.import_fasten))
    app.add_handler(CommandHandler("scrub_pii", handlers.scrub_pii))
    app.add_handler(CommandHandler("cleansync", handlers.cleansync))
    app.add_handler(CommandHandler("reset", reset_handlers.reset))
    app.add_handler(CommandHandler("delete", reset_handlers.delete))
    app.add_handler(CommandHandler("delete_labs", reset_handlers.delete_labs))
    app.add_handler(CommandHandler("delete_doc", reset_handlers.delete_doc))
    app.add_handler(CommandHandler("rescan", handlers.rescan))
    app.add_handler(CommandHandler("onboard", onboard_handlers.onboard))
    app.add_handler(CommandHandler("onboarding", onboard_handlers.onboard))
    app.add_handler(CommandHandler("identity", identity_handlers.identity))
    app.add_handler(CommandHandler("identity_check", identity_handlers.identity_check))
    app.add_handler(CommandHandler("identity_clear", identity_handlers.identity_clear))
    app.add_handler(CommandHandler("restart", handlers.restart))
    app.add_handler(CommandHandler("ingest", handlers.ingest))
    app.add_handler(CommandHandler("upload", handlers.upload))
    app.add_handler(CommandHandler("finish", handlers.finish))
    app.add_handler(CommandHandler("digest", handlers.digest))
    app.add_handler(CommandHandler("doctors", handlers.doctors))
    app.add_handler(CommandHandler("appointments", handlers.appointments))
    app.add_handler(CommandHandler("debug", handlers.debug))
    app.add_handler(CommandHandler("genetics", handlers.genetics))
    app.add_handler(CommandHandler("integrity", handlers.integrity))
    app.add_handler(CommandHandler("workouts", handlers.workouts))
    app.add_handler(CommandHandler("weeklyreport", handlers.weeklyreport))
    app.add_handler(CommandHandler("monthlyreport", handlers.monthlyreport))
    app.add_handler(CommandHandler("analyze", handlers.analyze))
    app.add_handler(CommandHandler("score", handlers.score))
    app.add_handler(CommandHandler("wearable_chart", handlers.wearable_chart))
    app.add_handler(CommandHandler("sleep_chart", handlers.sleep_chart))
    app.add_handler(CommandHandler("lab_heatmap", handlers.lab_heatmap))
    app.add_handler(CommandHandler("scatter", handlers.scatter))
    app.add_handler(CommandHandler("trends_chart", handlers.trends_chart))

    # Callback query handlers (inline keyboard buttons)
    app.add_handler(CallbackQueryHandler(
        onboard_handlers.handle_onboard_callback,
        pattern=r"^onboard:",
    ))
    app.add_handler(CallbackQueryHandler(
        handlers._health.handle_improvement_callback,
        pattern=r"^si:",
    ))
    app.add_handler(CallbackQueryHandler(
        handlers._data.handle_cleansync_callback,
        pattern=r"^cleansync:",
    ))

    # Message handlers (passphrase entry + document upload + photo + free text)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, handlers.handle_message
    ))
    app.add_handler(MessageHandler(
        filters.PHOTO, handlers.handle_message
    ))
    app.add_handler(MessageHandler(
        filters.Document.ALL, handlers.handle_message
    ))

    # Track ALL incoming messages for session chat wipe on lock
    async def _track_messages(update: Update, context) -> None:
        if update.message:
            core.track_message(update.effective_chat.id, update.message.message_id)

    app.add_handler(TypeHandler(Update, _track_messages), group=-1)

    # Proactive alert scheduler (periodic + incoming folder + on-unlock)
    if app.job_queue is not None:
        handlers.wire_scheduler(app.job_queue)
    else:
        logger.warning("JobQueue not available; proactive alerts disabled.")

    # Always register unlock callback (clean sync + context refresh)
    # This must run even without job queue — it's the data pipeline.
    handlers.wire_unlock_callback()

    return app
