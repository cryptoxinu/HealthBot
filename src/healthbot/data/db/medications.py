"""Medication and medication reminder methods."""
from __future__ import annotations

import logging
import uuid
from typing import Any

from healthbot.data.models import Medication

logger = logging.getLogger("healthbot")


class MedicationsMixin:
    """Mixin for medication and reminder database operations."""

    def insert_medication(self, med: Medication, user_id: int = 0) -> str:
        """Insert a medication record."""
        med_id = med.id or uuid.uuid4().hex
        aad = f"medications.encrypted_data.{med_id}"
        enc_data = self._encrypt(med, aad)
        try:
            self.conn.execute(
                """INSERT INTO medications (med_id, status, start_date, end_date,
                   source_doc_id, created_at, encrypted_data, user_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (med_id, med.status,
                 med.start_date.isoformat() if med.start_date else None,
                 med.end_date.isoformat() if med.end_date else None,
                 med.source_blob_id, self._now(), enc_data, user_id),
            )
        except Exception:
            # Fallback for pre-migration schema without user_id column
            self.conn.execute(
                """INSERT INTO medications (med_id, status, start_date, end_date,
                   source_doc_id, created_at, encrypted_data)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (med_id, med.status,
                 med.start_date.isoformat() if med.start_date else None,
                 med.end_date.isoformat() if med.end_date else None,
                 med.source_blob_id, self._now(), enc_data),
            )
        self.conn.commit()
        return med_id

    def get_active_medications(
        self, user_id: int | None = None, since: str | None = None,
    ) -> list[dict]:
        """Return all active medications (decrypted)."""
        sql = "SELECT * FROM medications WHERE status = 'active'"
        params: list[Any] = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        if since:
            sql += " AND created_at > ?"
            params.append(since)
        rows = self.conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            aad = f"medications.encrypted_data.{row['med_id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
                results.append(data)
            except Exception as exc:
                logger.warning(
                    "Skipping corrupt medication row %s: %s",
                    row["med_id"], exc,
                )
        return results

    # --- Medication Reminders ---

    def upsert_med_reminder(
        self, user_id: int, med_name: str, time: str, notes: str = "",
    ) -> str:
        """Insert or update a medication reminder (encrypted)."""
        data = {"med_name": med_name, "notes": notes}

        # Check if reminder already exists for this med
        existing = self.get_med_reminders(user_id)
        for r in existing:
            if r.get("med_name", "").lower() == med_name.lower():
                # Update existing — encrypt once with the existing row's AAD
                old_id = r.get("_id", "")
                old_aad = f"med_reminders.encrypted_data.{old_id}"
                enc_data = self._encrypt(data, old_aad)
                self.conn.execute(
                    "UPDATE med_reminders SET time = ?, encrypted_data = ? WHERE id = ?",
                    (time, enc_data, old_id),
                )
                self.conn.commit()
                return old_id

        # Insert new — encrypt once with the new row's AAD
        reminder_id = uuid.uuid4().hex
        aad = f"med_reminders.encrypted_data.{reminder_id}"
        enc_data = self._encrypt(data, aad)
        self.conn.execute(
            """INSERT INTO med_reminders (id, user_id, time, enabled, created_at, encrypted_data)
               VALUES (?, ?, ?, 1, ?, ?)""",
            (reminder_id, user_id, time, self._now(), enc_data),
        )
        self.conn.commit()
        return reminder_id

    def get_med_reminders(self, user_id: int) -> list[dict]:
        """Get all medication reminders for a user (decrypted)."""
        try:
            rows = self.conn.execute(
                "SELECT * FROM med_reminders WHERE user_id = ? AND enabled = 1",
                (user_id,),
            ).fetchall()
        except Exception:
            return []  # Table might not exist yet
        results = []
        for row in rows:
            aad = f"med_reminders.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
                data["_id"] = row["id"]
                data["_time"] = row["time"]
                data["_enabled"] = bool(row["enabled"])
                results.append(data)
            except Exception as e:
                logger.warning("Decrypt failed for med_reminders row %s: %s", row["id"], e)
                continue
        return results

    def disable_med_reminder(self, user_id: int, med_name: str) -> bool:
        """Disable a medication reminder by name. Returns True if found."""
        reminders = self.get_med_reminders(user_id)
        for r in reminders:
            if r.get("med_name", "").lower() == med_name.lower():
                self.conn.execute(
                    "UPDATE med_reminders SET enabled = 0 WHERE id = ?",
                    (r["_id"],),
                )
                self.conn.commit()
                return True
        return False

    def pause_med_reminder(
        self, user_id: int, med_name: str,
        paused_reason: str, resume_after: str,
    ) -> bool:
        """Pause a medication reminder with reason and resume date.

        Stores paused_reason and resume_after in the encrypted JSON blob.
        Returns True if a matching reminder was found and paused.
        """
        reminders = self.get_med_reminders(user_id)
        for r in reminders:
            if r.get("med_name", "").lower() == med_name.lower():
                rid = r["_id"]
                aad = f"med_reminders.encrypted_data.{rid}"
                data = {
                    "med_name": r.get("med_name", ""),
                    "notes": r.get("notes", ""),
                    "paused_reason": paused_reason,
                    "resume_after": resume_after,
                }
                enc_data = self._encrypt(data, aad)
                self.conn.execute(
                    "UPDATE med_reminders SET encrypted_data = ? WHERE id = ?",
                    (enc_data, rid),
                )
                self.conn.commit()
                return True
        return False

    def resume_med_reminder(self, user_id: int, med_name: str) -> bool:
        """Resume a paused medication reminder. Returns True if found and resumed."""
        reminders = self.get_med_reminders(user_id)
        for r in reminders:
            if (
                r.get("med_name", "").lower() == med_name.lower()
                and r.get("paused_reason")
            ):
                rid = r["_id"]
                aad = f"med_reminders.encrypted_data.{rid}"
                data = {
                    "med_name": r.get("med_name", ""),
                    "notes": r.get("notes", ""),
                }
                enc_data = self._encrypt(data, aad)
                self.conn.execute(
                    "UPDATE med_reminders SET encrypted_data = ? WHERE id = ?",
                    (enc_data, rid),
                )
                self.conn.commit()
                return True
        return False

    def get_paused_reminders(self, user_id: int) -> list[dict]:
        """Get all paused medication reminders (those with paused_reason set)."""
        all_reminders = self.get_med_reminders(user_id)
        return [r for r in all_reminders if r.get("paused_reason")]
