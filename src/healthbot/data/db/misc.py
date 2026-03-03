"""Miscellaneous methods — health goals, external evidence, health records ext,
substance knowledge, saved messages, redaction log, and remaining methods.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from healthbot.data.models import ExternalEvidence

logger = logging.getLogger("healthbot")


class MiscMixin:
    """Mixin for miscellaneous database operations."""

    # --- Health Records Ext (extensible storage) ---

    def insert_health_record_ext(
        self, user_id: int, data_type: str, label: str, data_dict: dict,
    ) -> str:
        """Insert or update an extensible health record.

        Encrypts data_dict with AAD `health_records_ext.encrypted_data.{id}`.
        Deduplicates on (user_id, data_type, label).
        Returns the record ID.
        """
        # Check for existing record with same (user_id, data_type, label)
        row = self.conn.execute(
            "SELECT id FROM health_records_ext WHERE user_id = ? AND data_type = ? AND label = ?",
            (user_id, data_type, label),
        ).fetchone()
        if row:
            rec_id = row["id"]
        else:
            rec_id = uuid.uuid4().hex

        aad = f"health_records_ext.encrypted_data.{rec_id}"
        enc = self._encrypt(data_dict, aad)
        now = self._now()

        self.conn.execute(
            """INSERT OR REPLACE INTO health_records_ext
               (id, user_id, data_type, label, created_at, updated_at, encrypted_data)
               VALUES (?, ?, ?, ?, COALESCE(
                   (SELECT created_at FROM health_records_ext WHERE id = ?), ?
               ), ?, ?)""",
            (rec_id, user_id, data_type, label, rec_id, now, now, enc),
        )
        self.conn.commit()
        return rec_id

    def get_health_records_ext(
        self, user_id: int, data_type: str | None = None, since: str | None = None,
    ) -> list[dict]:
        """Query extensible health records, decrypting each."""
        sql = "SELECT * FROM health_records_ext WHERE user_id = ?"
        params: list[Any] = [user_id]
        if data_type:
            sql += " AND data_type = ?"
            params.append(data_type)
        if since:
            sql += " AND updated_at >= ?"
            params.append(since)
        sql += " ORDER BY updated_at DESC"
        rows = self.conn.execute(sql, params).fetchall()

        results: list[dict] = []
        for r in rows:
            d = dict(r)
            if d.get("encrypted_data"):
                aad = f"health_records_ext.encrypted_data.{d['id']}"
                try:
                    d["data"] = self._decrypt(d.pop("encrypted_data"), aad)
                except Exception:
                    d["data"] = {}
                    d.pop("encrypted_data", None)
            else:
                d["data"] = {}
                d.pop("encrypted_data", None)
            results.append(d)
        return results

    # --- Substance Knowledge ---

    def insert_substance_knowledge(
        self,
        user_id: int,
        name: str,
        data: dict,
        quality_score: float = 0.0,
    ) -> str:
        """Insert a substance knowledge profile with AES-256-GCM encryption."""
        sk_id = uuid.uuid4().hex
        aad = f"substance_knowledge.encrypted_data.{sk_id}"
        enc_data = self._encrypt(data, aad)
        now = self._now()
        self.conn.execute(
            """INSERT INTO substance_knowledge
               (id, user_id, name, created_at, updated_at, quality_score,
                encrypted_data)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id, name) DO UPDATE SET
                 updated_at = excluded.updated_at,
                 quality_score = excluded.quality_score,
                 encrypted_data = excluded.encrypted_data""",
            (sk_id, user_id, name.lower(), now, now, quality_score, enc_data),
        )
        self.conn.commit()
        return sk_id

    def get_substance_knowledge(
        self, user_id: int, name: str,
    ) -> dict | None:
        """Get a substance knowledge profile by name."""
        row = self.conn.execute(
            "SELECT * FROM substance_knowledge WHERE user_id = ? AND name = ?",
            (user_id, name.lower()),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        aad = f"substance_knowledge.encrypted_data.{d['id']}"
        try:
            d["data"] = self._decrypt(d.pop("encrypted_data"), aad)
        except Exception:
            d["data"] = {}
            d.pop("encrypted_data", None)
        return d

    def get_all_substance_knowledge(self, user_id: int) -> list[dict]:
        """Get all substance knowledge profiles for a user."""
        rows = self.conn.execute(
            "SELECT * FROM substance_knowledge WHERE user_id = ? ORDER BY name",
            (user_id,),
        ).fetchall()
        results: list[dict] = []
        for row in rows:
            d = dict(row)
            aad = f"substance_knowledge.encrypted_data.{d['id']}"
            try:
                d["data"] = self._decrypt(d.pop("encrypted_data"), aad)
            except Exception:
                d["data"] = {}
                d.pop("encrypted_data", None)
            results.append(d)
        return results

    def update_substance_knowledge(
        self,
        user_id: int,
        name: str,
        data: dict,
        quality_score: float | None = None,
    ) -> bool:
        """Update an existing substance knowledge profile."""
        row = self.conn.execute(
            "SELECT id FROM substance_knowledge WHERE user_id = ? AND name = ?",
            (user_id, name.lower()),
        ).fetchone()
        if not row:
            return False
        sk_id = row["id"]
        aad = f"substance_knowledge.encrypted_data.{sk_id}"
        enc_data = self._encrypt(data, aad)
        sql = "UPDATE substance_knowledge SET encrypted_data = ?, updated_at = ?"
        params: list = [enc_data, self._now()]
        if quality_score is not None:
            sql += ", quality_score = ?"
            params.append(quality_score)
        sql += " WHERE id = ?"
        params.append(sk_id)
        self.conn.execute(sql, params)
        self.conn.commit()
        return True

    # --- Health goals ---

    def insert_health_goal(self, user_id: int, goal_data: dict) -> str:
        """Insert a health goal (encrypted)."""
        goal_id = uuid.uuid4().hex
        aad = f"health_goals.encrypted_data.{goal_id}"
        enc_data = self._encrypt(goal_data, aad)
        self.conn.execute(
            """INSERT INTO health_goals (id, user_id, created_at, encrypted_data)
               VALUES (?, ?, ?, ?)""",
            (goal_id, user_id, self._now(), enc_data),
        )
        self.conn.commit()
        return goal_id

    def get_health_goals(self, user_id: int) -> list[dict]:
        """Get all health goals for a user (decrypted)."""
        try:
            rows = self.conn.execute(
                "SELECT * FROM health_goals WHERE user_id = ?",
                (user_id,),
            ).fetchall()
        except Exception:
            return []  # Table might not exist yet
        results = []
        for row in rows:
            aad = f"health_goals.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
                data["_id"] = row["id"]
                data["_created_at"] = row["created_at"]
                results.append(data)
            except Exception as e:
                logger.warning("Decrypt failed for health_goals row %s: %s", row["id"], e)
                continue
        return results

    def delete_health_goal(self, goal_id: str) -> bool:
        """Delete a health goal by ID."""
        cursor = self.conn.execute(
            "DELETE FROM health_goals WHERE id = ?", (goal_id,),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    # --- External evidence ---

    def insert_external_evidence(self, ev: ExternalEvidence) -> str:
        """Insert external evidence from research."""
        ev_id = ev.id or uuid.uuid4().hex
        aad = f"external_evidence.encrypted_data.{ev_id}"
        enc_data = self._encrypt(ev, aad)
        self.conn.execute(
            """INSERT INTO external_evidence (evidence_id, source, query_hash,
               created_at, encrypted_data) VALUES (?, ?, ?, ?, ?)""",
            (ev_id, ev.source, ev.query_hash, self._now(), enc_data),
        )
        self.conn.commit()
        return ev_id

    # --- Redaction log ---

    def store_redaction_log(
        self, entries: list[dict], destination: str,
    ) -> None:
        """Store encrypted redaction diff log entry.

        Only stores category + position (what was removed), NOT the
        original PII text. Encrypted in Tier 1 for audit trail.
        """
        if not entries:
            return

        log_id = str(uuid.uuid4())
        ts = datetime.now(UTC).isoformat()
        data = json.dumps(entries, ensure_ascii=False)
        encrypted = self._encrypt(data, f"redaction_log.data.{log_id}")

        try:
            self.conn.execute(
                "INSERT INTO redaction_log (id, destination, redaction_count, "
                "timestamp, encrypted_data) VALUES (?, ?, ?, ?, ?)",
                (log_id, destination, len(entries), ts, encrypted),
            )
            self.conn.commit()
        except Exception as e:
            if "no such table" not in str(e).lower():
                logger.warning("Failed to store redaction log: %s", e)

    # --- Saved Messages ---

    def save_message(
        self, user_id: int, text: str, context: str | None = None,
    ) -> str:
        """Save a bookmarked message. Returns the saved message ID."""
        msg_id = str(uuid.uuid4())
        # Extract preview: first sentence, truncated to 50 chars
        preview = text.split(". ")[0].split("\n")[0]
        if len(preview) > 50:
            preview = preview[:47] + "..."
        data = {"text": text, "preview": preview, "context": context}
        encrypted = self._encrypt(
            data, f"saved_messages.encrypted_data.{msg_id}",
        )
        self.conn.execute(
            "INSERT INTO saved_messages (id, user_id, saved_at, encrypted_data)"
            " VALUES (?, ?, ?, ?)",
            (msg_id, user_id, self._now(), encrypted),
        )
        self.conn.commit()
        return msg_id

    def get_saved_messages(
        self, user_id: int, limit: int = 100, offset: int = 0,
    ) -> list[dict]:
        """Fetch saved messages for a user, newest first."""
        rows = self.conn.execute(
            "SELECT id, saved_at, encrypted_data FROM saved_messages"
            " WHERE user_id = ? ORDER BY saved_at DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        ).fetchall()
        results = []
        for row in rows:
            try:
                data = self._decrypt(
                    row["encrypted_data"],
                    f"saved_messages.encrypted_data.{row['id']}",
                )
                data["id"] = row["id"]
                data["saved_at"] = row["saved_at"]
                results.append(data)
            except Exception as e:
                logger.warning("Failed to decrypt saved message %s: %s", row["id"], e)
        return results

    def count_saved_messages(self, user_id: int) -> int:
        """Count saved messages for a user."""
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM saved_messages WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return row["cnt"] if row else 0

    def delete_saved_message(self, user_id: int, msg_id: str) -> bool:
        """Delete a saved message. Returns True if deleted."""
        cur = self.conn.execute(
            "DELETE FROM saved_messages WHERE id = ? AND user_id = ?",
            (msg_id, user_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def delete_all_saved_messages(self, user_id: int) -> int:
        """Delete all saved messages for a user. Returns count deleted."""
        cur = self.conn.execute(
            "DELETE FROM saved_messages WHERE user_id = ?", (user_id,),
        )
        self.conn.commit()
        return cur.rowcount

    def search_saved_messages(self, user_id: int, query: str) -> list[dict]:
        """Search saved messages by text content (decrypts and filters in Python)."""
        all_msgs = self.get_saved_messages(user_id, limit=1000)
        q = query.lower()
        return [
            msg for msg in all_msgs
            if q in msg.get("text", "").lower()
            or q in (msg.get("context") or "").lower()
        ]
