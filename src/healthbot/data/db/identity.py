"""Identity profile field methods."""
from __future__ import annotations

import logging
import sqlite3
import uuid

logger = logging.getLogger("healthbot")


class IdentityMixin:
    """Mixin for user identity (encrypted PII for smarter anonymization)."""

    def upsert_identity_field(
        self, user_id: int, field_key: str, value: str, field_type: str,
    ) -> str:
        """Insert or update an encrypted identity field.

        field_key: "full_name", "email", "dob", "family:0", "custom:0", etc.
        field_type: "name", "email", "dob", "custom"

        Returns field ID.
        """
        field_id = uuid.uuid4().hex
        aad = f"user_identity.encrypted_data.{field_id}"
        enc_data = self._encrypt({"value": value, "type": field_type}, aad)
        try:
            self.conn.execute(
                """INSERT INTO user_identity
                   (id, user_id, field_key, created_at, updated_at, encrypted_data)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (field_id, user_id, field_key, self._now(), self._now(), enc_data),
            )
        except sqlite3.IntegrityError:
            # Unique constraint on (user_id, field_key) — update existing
            row = self.conn.execute(
                "SELECT id FROM user_identity WHERE user_id = ? AND field_key = ?",
                (user_id, field_key),
            ).fetchone()
            if row:
                field_id = row["id"]
                aad = f"user_identity.encrypted_data.{field_id}"
                enc_data = self._encrypt(
                    {"value": value, "type": field_type}, aad,
                )
                self.conn.execute(
                    "UPDATE user_identity SET encrypted_data = ?, updated_at = ? "
                    "WHERE id = ?",
                    (enc_data, self._now(), field_id),
                )
        self.conn.commit()
        return field_id

    def get_identity_fields(self, user_id: int) -> list[dict]:
        """Retrieve and decrypt all identity fields for a user."""
        rows = self.conn.execute(
            "SELECT id, field_key, encrypted_data FROM user_identity "
            "WHERE user_id = ? ORDER BY field_key",
            (user_id,),
        ).fetchall()
        results = []
        for row in rows:
            aad = f"user_identity.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for user_identity row %s: %s", row["id"], e)
                continue  # Skip corrupted entries
            results.append({
                "field_key": row["field_key"],
                "value": data.get("value", ""),
                "type": data.get("type", ""),
            })
        return results

    def delete_identity_field(self, user_id: int, field_key: str) -> bool:
        """Delete a specific identity field."""
        cur = self.conn.execute(
            "DELETE FROM user_identity WHERE user_id = ? AND field_key = ?",
            (user_id, field_key),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def delete_all_identity_fields(self, user_id: int) -> int:
        """Delete all identity fields for a user. Returns count deleted."""
        cur = self.conn.execute(
            "DELETE FROM user_identity WHERE user_id = ?", (user_id,),
        )
        self.conn.commit()
        return cur.rowcount
