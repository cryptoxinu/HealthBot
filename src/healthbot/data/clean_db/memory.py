"""Clean DB memory mixin — user memory, audit log, corrections, improvements."""
from __future__ import annotations


class MemoryMixin:
    """Mixin providing user memory, audit, corrections, and improvements for CleanDB."""

    # ── User memory methods ──────────────────────────────

    def upsert_user_memory(
        self,
        key: str,
        value: str,
        category: str = "",
        confidence: float = 1.0,
        source: str = "claude_inferred",
    ) -> None:
        """PII-validated upsert of a user memory entry."""
        self._validate_text_fields(
            {"key": key, "value": value, "category": category},
            f"user_memory.{key}",
        )
        now = self._now()
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_user_memory
               (key, value, category, confidence, source, superseded_by,
                created_at, updated_at, synced_at)
               VALUES (?, ?, ?, ?, ?,
                COALESCE((SELECT superseded_by FROM clean_user_memory WHERE key = ?), ''),
                COALESCE((SELECT created_at FROM clean_user_memory WHERE key = ?), ?),
                ?, ?)""",
            (key, value, category, confidence, source, key, key, now, now, now),
        )
        self._auto_commit()

    def get_user_memory(self, category: str | None = None) -> list[dict]:
        """Return active memory entries (where superseded_by is empty)."""
        if category:
            rows = self.conn.execute(
                """SELECT * FROM clean_user_memory
                   WHERE superseded_by = '' AND category = ?
                   ORDER BY updated_at DESC LIMIT 200""",
                (category,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT * FROM clean_user_memory
                   WHERE superseded_by = ''
                   ORDER BY updated_at DESC LIMIT 200""",
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_user_memory(self, key: str) -> bool:
        """Delete a single user memory entry. Returns True if deleted."""
        cursor = self.conn.execute(
            "DELETE FROM clean_user_memory WHERE key = ?", (key,),
        )
        self._auto_commit()
        return cursor.rowcount > 0

    def clear_all_user_memory(self) -> int:
        """Delete all user memory entries. Returns count deleted."""
        cursor = self.conn.execute("DELETE FROM clean_user_memory")
        self._auto_commit()
        return cursor.rowcount

    def mark_memory_superseded(self, old_key: str, new_key: str) -> None:
        """Mark an old memory entry as superseded by a new key."""
        self.conn.execute(
            """UPDATE clean_user_memory SET superseded_by = ?, updated_at = ?
               WHERE key = ? AND superseded_by = ''""",
            (new_key, self._now(), old_key),
        )
        self._auto_commit()

    # ── Memory audit log ─────────────────────────────────

    def log_memory_change(
        self,
        key: str,
        old_value: str,
        new_value: str,
        source_type: str = "",
        source_ref: str = "",
    ) -> None:
        """Record a memory write event in the audit log."""
        self.conn.execute(
            """INSERT INTO memory_audit_log
               (key, old_value, new_value, source_type, source_ref, changed_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (key, old_value, new_value, source_type, source_ref, self._now()),
        )
        self._auto_commit()

    def get_memory_audit_log(self, limit: int = 50) -> list[dict]:
        """Return recent memory audit entries, newest first."""
        rows = self.conn.execute(
            """SELECT * FROM memory_audit_log
               ORDER BY changed_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Correction + system improvement methods ────────

    def insert_correction(
        self,
        correction_id: str,
        original_claim: str,
        correction: str,
        source: str = "user",
    ) -> None:
        """PII-validated insert of a correction entry."""
        self._validate_text_fields(
            {"original_claim": original_claim, "correction": correction},
            f"correction.{correction_id}",
        )
        now = self._now()
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_corrections
               (id, original_claim, correction, source, created_at, synced_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (correction_id, original_claim, correction, source, now, now),
        )
        self._auto_commit()

    def insert_system_improvement(
        self,
        area: str = "",
        suggestion: str = "",
        priority: str = "low",
    ) -> str:
        """PII-validated insert of a system improvement suggestion.

        Returns the generated improvement ID.
        """
        self._validate_text_fields(
            {"area": area, "suggestion": suggestion},
            "system_improvement",
        )
        import uuid
        imp_id = uuid.uuid4().hex
        now = self._now()
        self.conn.execute(
            """INSERT INTO clean_system_improvements
               (id, area, suggestion, priority, status, created_at, synced_at)
               VALUES (?, ?, ?, ?, 'open', ?, ?)""",
            (imp_id, area, suggestion, priority, now, now),
        )
        self._auto_commit()
        return imp_id

    def get_corrections(self, limit: int = 50) -> list[dict]:
        """Return recent corrections ordered by creation date."""
        rows = self.conn.execute(
            """SELECT * FROM clean_corrections
               ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_system_improvements(
        self, status: str | None = None, limit: int = 50,
    ) -> list[dict]:
        """Return system improvement suggestions, optionally filtered by status."""
        if status:
            rows = self.conn.execute(
                """SELECT * FROM clean_system_improvements
                   WHERE status = ? ORDER BY created_at DESC LIMIT ?""",
                (status, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT * FROM clean_system_improvements
                   ORDER BY created_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def update_system_improvement_status(
        self, improvement_id: str, status: str,
    ) -> bool:
        """Update the status of a system improvement. Returns True if updated."""
        cursor = self.conn.execute(
            """UPDATE clean_system_improvements SET status = ?, synced_at = ?
               WHERE id = ?""",
            (status, self._now(), improvement_id),
        )
        self._auto_commit()
        return cursor.rowcount > 0
