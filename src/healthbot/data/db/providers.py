"""Doctors and appointments methods."""
from __future__ import annotations

import logging
import uuid
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger("healthbot")


class ProvidersMixin:
    """Mixin for provider and appointment database operations."""

    def insert_provider(self, user_id: int, data: dict) -> str:
        """Insert a healthcare provider. Returns provider ID."""
        prov_id = uuid.uuid4().hex
        aad = f"providers.encrypted_data.{prov_id}"
        enc_data = self._encrypt(data, aad)
        self.conn.execute(
            """INSERT INTO providers (id, user_id, created_at, encrypted_data)
               VALUES (?, ?, ?, ?)""",
            (prov_id, user_id, self._now(), enc_data),
        )
        self.conn.commit()
        return prov_id

    def get_providers(self, user_id: int) -> list[dict]:
        """Get all providers for a user."""
        rows = self.conn.execute(
            "SELECT * FROM providers WHERE user_id = ? ORDER BY created_at",
            (user_id,),
        ).fetchall()
        results = []
        for row in rows:
            aad = f"providers.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for providers row %s: %s", row["id"], e)
                data = {}
            data["_id"] = row["id"]
            data["_created_at"] = row["created_at"]
            results.append(data)
        return results

    def delete_provider(self, provider_id: str) -> bool:
        """Delete a provider by ID."""
        cur = self.conn.execute(
            "DELETE FROM providers WHERE id = ?", (provider_id,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    # --- Appointments ---

    def insert_appointment(self, user_id: int, provider_id: str, data: dict) -> str:
        """Insert an appointment. Returns appointment ID."""
        appt_id = uuid.uuid4().hex
        aad = f"appointments.encrypted_data.{appt_id}"
        appt_date = data.get("date", "")
        enc_data = self._encrypt(data, aad)
        self.conn.execute(
            """INSERT INTO appointments
               (id, user_id, provider_id, appt_date, status, created_at, encrypted_data)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (appt_id, user_id, provider_id, appt_date, "scheduled",
             self._now(), enc_data),
        )
        self.conn.commit()
        return appt_id

    def get_appointments(
        self, user_id: int, status: str | None = None,
    ) -> list[dict]:
        """Get appointments for a user, optionally filtered by status."""
        sql = "SELECT * FROM appointments WHERE user_id = ?"
        params: list[Any] = [user_id]
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY appt_date ASC"
        rows = self.conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            aad = f"appointments.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for appointments row %s: %s", row["id"], e)
                data = {}
            data["_id"] = row["id"]
            data["_provider_id"] = row["provider_id"]
            data["_appt_date"] = row["appt_date"]
            data["_status"] = row["status"]
            data["_prep_sent"] = bool(row["prep_sent"])
            results.append(data)
        return results

    def get_upcoming_appointments(
        self, user_id: int, within_days: int = 2,
    ) -> list[dict]:
        """Get appointments within the next N days."""
        today = date.today().isoformat()
        future = (date.today() + timedelta(days=within_days)).isoformat()
        sql = """SELECT * FROM appointments
                 WHERE user_id = ? AND appt_date >= ? AND appt_date <= ?
                 AND status = 'scheduled'
                 ORDER BY appt_date ASC"""
        rows = self.conn.execute(sql, (user_id, today, future)).fetchall()
        results = []
        for row in rows:
            aad = f"appointments.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for appointments row %s: %s", row["id"], e)
                data = {}
            data["_id"] = row["id"]
            data["_provider_id"] = row["provider_id"]
            data["_appt_date"] = row["appt_date"]
            data["_status"] = row["status"]
            data["_prep_sent"] = bool(row["prep_sent"])
            results.append(data)
        return results

    def mark_appointment_prep_sent(self, appt_id: str) -> None:
        """Mark appointment as having had prep sent."""
        self.conn.execute(
            "UPDATE appointments SET prep_sent = 1 WHERE id = ?",
            (appt_id,),
        )
        self.conn.commit()

    def update_appointment_status(self, appt_id: str, status: str) -> None:
        """Update appointment status (scheduled, completed, cancelled)."""
        self.conn.execute(
            "UPDATE appointments SET status = ? WHERE id = ?",
            (status, appt_id),
        )
        self.conn.commit()

    def delete_appointment(self, appt_id: str) -> bool:
        """Delete an appointment by ID."""
        cur = self.conn.execute(
            "DELETE FROM appointments WHERE id = ?", (appt_id,),
        )
        self.conn.commit()
        return cur.rowcount > 0
