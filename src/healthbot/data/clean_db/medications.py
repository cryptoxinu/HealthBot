"""Clean DB medications mixin — medication methods."""
from __future__ import annotations


class MedicationsMixin:
    """Mixin providing medication methods for CleanDB."""

    def upsert_medication(
        self,
        med_id: str,
        *,
        name: str = "",
        dose: str = "",
        unit: str = "",
        frequency: str = "",
        status: str = "active",
        start_date: str = "",
        end_date: str = "",
    ) -> None:
        self._validate_text_fields(
            {"name": name, "dose": dose, "frequency": frequency},
            f"medication.{med_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_medications
               (med_id, name, dose, unit, frequency, status, start_date, end_date, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (med_id, name, dose, unit, frequency, status, start_date, end_date,
             self._now()),
        )
        self._auto_commit()

    def get_medications(self, status: str = "active") -> list[dict]:
        if status == "all":
            rows = self.conn.execute(
                "SELECT * FROM clean_medications ORDER BY start_date DESC",
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM clean_medications WHERE status = ? ORDER BY start_date DESC",
                (status,),
            ).fetchall()
        return [dict(r) for r in rows]
