"""Telegram handlers for /identity commands — encrypted identity profile survey.

Multi-step interactive survey collecting PII identifiers (name, email, DOB,
family names, custom terms) for smarter anonymization. Data is encrypted
with AES-256-GCM and stored in the RAW VAULT ONLY.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.config import Config
from healthbot.security.key_manager import KeyManager

logger = logging.getLogger("healthbot")

SESSION_TIMEOUT = 1800  # 30 minutes

_SKIP_WORDS = frozenset({"skip", "none", "n/a", "na", "no", "-"})
_CANCEL_WORDS = frozenset({"cancel", "quit", "stop", "exit"})


@dataclass
class IdentityQuestion:
    """A single question in the identity survey."""

    key: str
    prompt: str
    field_type: str  # "name", "email", "dob", "custom"
    multi_value: bool = False


IDENTITY_QUESTIONS: list[IdentityQuestion] = [
    IdentityQuestion(
        key="full_name",
        prompt=(
            "What is your full legal name?\n"
            "(This will be used to detect your name in documents.)"
        ),
        field_type="name",
    ),
    IdentityQuestion(
        key="email",
        prompt=(
            "What is your email address?\n"
            "(Type 'skip' if you don't want to add one.)"
        ),
        field_type="email",
    ),
    IdentityQuestion(
        key="dob",
        prompt=(
            "What is your date of birth?\n"
            "(e.g., 1990-03-15, 03/15/1990, March 15 1990)"
        ),
        field_type="dob",
    ),
    IdentityQuestion(
        key="family",
        prompt=(
            "Family member names to watch for?\n"
            "(Comma-separated, e.g., 'Sarah Smith, Mike Johnson')\n"
            "Type 'skip' if none."
        ),
        field_type="name",
        multi_value=True,
    ),
    IdentityQuestion(
        key="custom",
        prompt=(
            "Any other PII to watch for?\n"
            "(Addresses, employer names, doctor names — comma-separated)\n"
            "Type 'skip' if none."
        ),
        field_type="custom",
        multi_value=True,
    ),
]


@dataclass
class IdentitySurveySession:
    """Tracks progress through the identity survey."""

    user_id: int
    current_index: int = 0
    answers: dict[str, str] = field(default_factory=dict)
    started_at: float = field(default_factory=time.time)

    @property
    def is_complete(self) -> bool:
        return self.current_index >= len(IDENTITY_QUESTIONS)

    @property
    def current_question(self) -> IdentityQuestion | None:
        if self.is_complete:
            return None
        return IDENTITY_QUESTIONS[self.current_index]

    def is_expired(self) -> bool:
        return (time.time() - self.started_at) > SESSION_TIMEOUT


class IdentityHandlers:
    """Handlers for /identity, /identity_check, /identity_clear."""

    def __init__(
        self,
        config: Config,
        key_manager: KeyManager,
        get_db: callable,
        check_auth: callable,
    ) -> None:
        self._config = config
        self._km = key_manager
        self._get_db = get_db
        self._check_auth = check_auth
        self._sessions: dict[int, IdentitySurveySession] = {}
        self._on_identity_updated: callable | None = None

    def set_on_identity_updated(self, callback: callable) -> None:
        """Register callback to reload firewall patterns after identity update."""
        self._on_identity_updated = callback

    def is_active(self, user_id: int) -> bool:
        """Check if user has an active identity survey session."""
        session = self._sessions.get(user_id)
        if session is None:
            return False
        if session.is_complete or session.is_expired():
            self._sessions.pop(user_id, None)
            return False
        return True

    async def handle_answer(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> bool:
        """Handle text response during active identity survey.

        Returns True if consumed (caller should stop routing).
        """
        user_id = update.effective_user.id if update.effective_user else 0
        if not self.is_active(user_id):
            return False

        text = update.message.text or ""
        normalized = text.strip().lower()

        # Cancel
        if normalized in _CANCEL_WORDS:
            self._sessions.pop(user_id, None)
            await update.message.reply_text(
                "Identity survey cancelled. Use /identity to start again."
            )
            return True

        session = self._sessions[user_id]
        question = session.current_question
        if question is None:
            return False

        # Skip
        if normalized in _SKIP_WORDS:
            session.current_index += 1
            if session.is_complete:
                await update.message.reply_text(self._finish(session))
            else:
                await update.message.reply_text(
                    self._format_question(session.current_index)
                )
            return True

        # Store answer
        self._store_answer(session, question, text.strip())
        session.answers[question.key] = text.strip()
        session.current_index += 1

        if session.is_complete:
            await update.message.reply_text(self._finish(session))
        else:
            await update.message.reply_text(
                self._format_question(session.current_index)
            )
        return True

    async def identity(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /identity — start survey or show current profile."""
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        user_id = update.effective_user.id

        # Check if profile already exists
        from healthbot.security.identity_profile import IdentityProfile

        db = self._get_db()
        profile = IdentityProfile(db=db)
        fields = profile.get_all_fields(user_id)

        if fields:
            # Show current profile and ask about updating
            lines = ["IDENTITY PROFILE", "=" * 25, ""]
            for f in fields:
                key = f["field_key"].replace("_", " ").title()
                if f["field_key"].startswith("family:"):
                    key = f"Family {int(f['field_key'].split(':')[1]) + 1}"
                elif f["field_key"].startswith("custom:"):
                    key = f"Custom {int(f['field_key'].split(':')[1]) + 1}"
                lines.append(f"  {key}: {f['value']}")
            lines.append("")
            lines.append("Patterns compiled for PII detection.")
            n_patterns = len(profile.compile_phi_patterns(user_id))
            n_names = len(profile.compile_ner_known_names(user_id))
            lines.append(f"  {n_patterns} regex patterns active")
            lines.append(f"  {n_names} known names for NER boosting")
            lines.append("")
            lines.append("Send /identity_clear to reset, or /identity again to update.")
            await update.message.reply_text("\n".join(lines))

            # Start survey to update
            self._sessions[user_id] = IdentitySurveySession(user_id=user_id)
            await update.message.reply_text(
                "Updating profile...\n\n"
                + self._format_question(0)
            )
        else:
            # First time — start survey
            self._sessions[user_id] = IdentitySurveySession(user_id=user_id)
            await update.message.reply_text(
                "Let's set up your identity profile for smarter PII detection.\n"
                "This data is encrypted and stored locally in the raw vault only.\n"
                "It is NEVER sent to any AI or cloud service.\n\n"
                + self._format_question(0)
            )

    async def identity_check(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /identity_check <text> — test detection against sample text."""
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        user_id = update.effective_user.id
        args = context.args
        if not args:
            await update.message.reply_text(
                "Usage: /identity_check <sample text>\n"
                "Example: /identity_check John Smith saw Dr. Jones on 03/15/1990"
            )
            return

        sample_text = " ".join(args)

        from healthbot.security.identity_profile import IdentityProfile

        db = self._get_db()
        profile = IdentityProfile(db=db)
        results = profile.test_anonymization(user_id, sample_text)

        if results:
            lines = [f"Identity detection found {len(results)} match(es):", ""]
            for r in results:
                lines.append(
                    f"  [{r.field_key}] \"{r.matched_text}\" "
                    f"(pattern: {r.pattern_name})"
                )
            lines.append("")
            lines.append("These would be redacted in outbound data.")
        else:
            lines = [
                "No identity-based matches found.",
                "Standard PII patterns (SSN, phone, etc.) still apply.",
            ]
            fields = profile.get_all_fields(user_id)
            if not fields:
                lines.append("Tip: Set up your profile with /identity first.")

        await update.message.reply_text("\n".join(lines))

    async def identity_clear(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /identity_clear — delete all identity data."""
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        user_id = update.effective_user.id

        from healthbot.security.identity_profile import IdentityProfile

        db = self._get_db()
        profile = IdentityProfile(db=db)
        count = profile.delete_all(user_id)

        if count:
            await update.message.reply_text(
                f"Identity profile cleared ({count} field(s) deleted).\n"
                "PII detection reverted to standard patterns.\n"
                "Use /identity to set up a new profile."
            )
        else:
            await update.message.reply_text(
                "No identity profile to clear.\n"
                "Use /identity to set one up."
            )

    def on_vault_lock(self) -> None:
        """Clear survey sessions on vault lock."""
        self._sessions.clear()

    # --- Private helpers ---

    def _store_answer(
        self,
        session: IdentitySurveySession,
        question: IdentityQuestion,
        text: str,
    ) -> None:
        """Store answer as encrypted identity field(s)."""
        from healthbot.security.identity_profile import IdentityProfile

        db = self._get_db()
        profile = IdentityProfile(db=db)

        if question.multi_value:
            import re
            parts = re.split(r"[;,]", text)
            values = [p.strip() for p in parts if p.strip()]
            for i, value in enumerate(values):
                field_key = f"{question.key}:{i}"
                profile.store_field(
                    session.user_id, field_key, value, question.field_type,
                )
        else:
            # Normalize DOB to ISO format if possible
            value = text
            if question.field_type == "dob":
                value = self._normalize_dob(text)
            profile.store_field(
                session.user_id, question.key, value, question.field_type,
            )

    @staticmethod
    def _normalize_dob(text: str) -> str:
        """Try to normalize DOB to YYYY-MM-DD format."""
        from datetime import datetime

        formats = [
            "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d",
            "%B %d %Y", "%B %d, %Y", "%b %d %Y", "%b %d, %Y",
            "%d %B %Y", "%d %b %Y",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(text.strip(), fmt).date()
                return dt.isoformat()
            except ValueError:
                continue
        return text.strip()

    def _format_question(self, index: int) -> str:
        """Format a question with progress indicator."""
        q = IDENTITY_QUESTIONS[index]
        total = len(IDENTITY_QUESTIONS)
        return (
            f"[{index + 1}/{total}] {q.prompt}\n\n"
            "(Type 'skip' to skip, 'cancel' to stop)"
        )

    def _finish(self, session: IdentitySurveySession) -> str:
        """Generate summary and clean up session."""
        self._sessions.pop(session.user_id, None)

        from healthbot.security.identity_profile import IdentityProfile

        db = self._get_db()
        profile = IdentityProfile(db=db)
        n_patterns = len(profile.compile_phi_patterns(session.user_id))
        n_names = len(profile.compile_ner_known_names(session.user_id))

        lines = [
            "IDENTITY PROFILE SAVED",
            "=" * 25,
            "",
        ]
        for q in IDENTITY_QUESTIONS:
            answer = session.answers.get(q.key)
            if answer:
                label = q.key.replace("_", " ").title()
                lines.append(f"  {label}: {answer}")
        lines.append("")
        lines.append(f"  {n_patterns} regex patterns compiled for PII detection")
        lines.append(f"  {n_names} known names for NER boosting")
        lines.append("")
        lines.append("All data encrypted (AES-256-GCM) in raw vault only.")
        lines.append("Use /identity_check <text> to test detection.")

        # Reload firewall patterns so new identity is active immediately
        if self._on_identity_updated:
            try:
                self._on_identity_updated(session.user_id)
            except Exception as e:
                logger.debug("Identity update callback failed: %s", e)

        return "\n".join(lines)
