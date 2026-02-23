"""Background Claude analysis engine.

Runs Claude synthesis in the background (every 4h/12h) to cross-reference
new data against the patient's full profile. Uses watermarks in vault_meta
to track what has already been analyzed, so no work is repeated.
"""
from __future__ import annotations

import json
import logging
import re

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")

# Watermark keys stored in vault_meta
_WM_LAB_COUNT = "bg_last_lab_count"
_WM_WEARABLE_DATE = "bg_last_wearable_date"
_WM_RESEARCH_COUNT = "bg_last_research_count"


class BackgroundAnalysisEngine:
    """Build targeted prompts for background Claude synthesis.

    Watermarks are staged when prompts are built but only committed
    after a successful Claude response via commit_*_watermarks().
    """

    def __init__(self, db: HealthDB, config: object) -> None:
        self._db = db
        self._config = config
        # Staged watermarks — committed only after successful response
        self._pending_health_wm: dict[str, str] = {}
        self._pending_research_wm: dict[str, str] = {}

    # -- Watermark helpers (vault_meta k/v store) --

    def get_watermark(self, key: str) -> str:
        """Read a watermark value from vault_meta."""
        row = self._db.conn.execute(
            "SELECT value FROM vault_meta WHERE key = ?", (key,),
        ).fetchone()
        return row["value"] if row else ""

    def set_watermark(self, key: str, value: str) -> None:
        """Write a watermark value to vault_meta."""
        self._db.conn.execute(
            "INSERT OR REPLACE INTO vault_meta (key, value) VALUES (?, ?)",
            (key, value),
        )
        self._db.conn.commit()

    def commit_health_watermarks(self) -> None:
        """Commit staged health watermarks after successful synthesis."""
        for key, value in self._pending_health_wm.items():
            self.set_watermark(key, value)
        self._pending_health_wm.clear()

    def commit_research_watermarks(self) -> None:
        """Commit staged research watermarks after successful synthesis."""
        for key, value in self._pending_research_wm.items():
            self.set_watermark(key, value)
        self._pending_research_wm.clear()

    # -- Prompt builders --

    def build_health_synthesis_prompt(
        self, user_id: int, *, force: bool = False,
    ) -> str | None:
        """Build a prompt for background health synthesis.

        Checks watermarks to detect new data since last run.
        Returns None if nothing new (zero tokens).
        The caller feeds this prompt to handle_message(), which
        prepends the full patient context via _build_prompt().

        Watermarks are staged but not committed — call
        commit_health_watermarks() after a successful response.

        Args:
            user_id: Patient user ID.
            force: If True, skip watermark check (used by /analyze).
        """
        # Count current lab results
        try:
            row = self._db.conn.execute(
                "SELECT COUNT(*) AS cnt FROM observations "
                "WHERE user_id = ? AND record_type = 'lab_result'",
                (user_id,),
            ).fetchone()
            current_lab_count = row["cnt"] if row else 0
        except Exception:
            current_lab_count = 0

        # Latest wearable date
        try:
            row = self._db.conn.execute(
                "SELECT MAX(date) AS d FROM wearable_daily WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            current_wearable_date = row["d"] if row and row["d"] else ""
        except Exception:
            current_wearable_date = ""

        # Active hypotheses count
        try:
            hyps = self._db.get_active_hypotheses(user_id)
            current_hyp_count = len(hyps) if hyps else 0
        except Exception:
            current_hyp_count = 0

        if not force:
            last_lab = int(self.get_watermark(_WM_LAB_COUNT) or "0")
            last_wearable = self.get_watermark(_WM_WEARABLE_DATE)

            data_changed = (
                current_lab_count != last_lab
                or current_wearable_date != last_wearable
            )
            if not data_changed:
                return None

            delta_labs = max(current_lab_count - last_lab, 0)
        else:
            delta_labs = current_lab_count

        # Stage watermarks (committed only after successful response)
        self._pending_health_wm = {
            _WM_LAB_COUNT: str(current_lab_count),
        }
        if current_wearable_date:
            self._pending_health_wm[_WM_WEARABLE_DATE] = current_wearable_date

        # For /analyze (force), commit immediately since user sees response
        if force:
            self.commit_health_watermarks()

        wearable_line = (
            f"- Wearable data updated through {current_wearable_date}"
            if current_wearable_date
            else "- No wearable data available"
        )

        lab_word = "result" if delta_labs == 1 else "results"

        return (
            "Background health review. You have my complete medical profile "
            "in context.\n\n"
            "SINCE LAST ANALYSIS:\n"
            f"- {delta_labs} new lab {lab_word} added\n"
            f"{wearable_line}\n"
            f"- {current_hyp_count} active hypotheses tracked by pattern "
            "engine\n\n"
            "TASKS:\n"
            "1. Review new data against my full history. Cross-reference "
            "everything.\n"
            "2. Check if new results support or contradict existing "
            "hypotheses — update confidence.\n"
            "3. Check medication interactions against current labs. "
            "Flag timing/dosing issues.\n"
            "4. If any finding is clinically significant, start your response "
            'with "ALERT:" followed by a one-line summary suitable for a '
            "notification.\n"
            "5. Emit structured blocks: HYPOTHESIS (updates), INSIGHT (new "
            "connections), RESEARCH (if you need to look something up), "
            "ACTION (if I need a test/follow-up).\n"
            "6. If previous advice should change based on new evidence, "
            "emit a CORRECTION block.\n\n"
            "Be concise. This is automated analysis, not a conversation."
        )

    def build_research_synthesis_prompt(
        self, user_id: int,
    ) -> str | None:
        """Build a prompt for background research synthesis.

        Checks watermark to detect new PubMed articles since last run.
        Returns None if no new articles.

        Watermarks are staged but not committed — call
        commit_research_watermarks() after a successful response.
        """
        try:
            from healthbot.research.external_evidence_store import (
                ExternalEvidenceStore,
            )

            store = ExternalEvidenceStore(self._db)
            entries = store.list_evidence(limit=20)
            # Filter non-expired
            entries = [e for e in entries if not e.get("expired", False)]
            current_count = len(entries)
        except Exception:
            return None

        last_count = int(self.get_watermark(_WM_RESEARCH_COUNT) or "0")
        new_articles = current_count - last_count

        if new_articles <= 0:
            # Reset watermark if count decreased (expiry/cleanup)
            if current_count < last_count:
                self.set_watermark(_WM_RESEARCH_COUNT, str(current_count))
            return None

        # Stage watermark (committed only after successful response)
        self._pending_research_wm = {
            _WM_RESEARCH_COUNT: str(current_count),
        }

        article_word = "article was" if new_articles == 1 else "articles were"

        return (
            "Background research synthesis. You have my complete medical "
            "profile in context, including a RESEARCH LIBRARY section with "
            "recent PubMed articles.\n\n"
            f"{new_articles} new {article_word} found related to my "
            "conditions.\n\n"
            "TASKS:\n"
            "1. For each article in the research library, cross-reference "
            "against my:\n"
            "   - Current lab values (do any markers match what the article "
            "discusses?)\n"
            "   - Active hypotheses (does this support/contradict any?)\n"
            "   - Current medications (interactions, timing, efficacy data?)\n"
            "   - Symptoms or conditions I've reported\n"
            "2. If an article is directly relevant to me, emit an INSIGHT "
            "block with the PMID.\n"
            "3. If an article changes confidence in an existing hypothesis, "
            "emit a HYPOTHESIS update.\n"
            "4. If an article suggests I should get a specific test, emit an "
            "ACTION block.\n"
            "5. If previous advice should change based on new evidence, "
            "emit a CORRECTION block.\n"
            '6. If a finding is urgent or clinically significant, start with '
            '"ALERT:".\n\n'
            "Cite PMIDs. Be specific about connections. Skip articles that "
            "aren't relevant to me."
        )

    @staticmethod
    def extract_alert(response: str) -> str | None:
        """Extract alert message from Claude response.

        Scans for "ALERT:" prefix or urgent ACTION blocks.
        Returns short Telegram-friendly message (max 200 chars) or None.
        """
        if not response:
            return None

        for line in response.splitlines():
            stripped = line.strip()
            if stripped.upper().startswith("ALERT:"):
                alert_text = stripped[6:].strip()
                if alert_text:
                    return alert_text[:200]

        # Check for urgent ACTION blocks
        for match in re.finditer(
            r"ACTION:\s*(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})", response,
        ):
            try:
                data = json.loads(match.group(1))
                if data.get("urgency") == "urgent":
                    reason = data.get("reason", data.get("test", ""))
                    if reason:
                        return f"Urgent: {reason}"[:200]
            except (ValueError, KeyError):
                continue

        return None
