"""Clean DB demographics mixin — user profile methods."""
from __future__ import annotations


class DemographicsMixin:
    """Mixin providing demographics/user profile methods for CleanDB."""

    def upsert_demographics(
        self,
        user_id: int,
        *,
        age: int | None = None,
        sex: str = "",
        ethnicity: str = "",
        height_m: float | None = None,
        weight_kg: float | None = None,
        bmi: float | None = None,
    ) -> None:
        # Demographics are not PII when stripped of name/DOB/address
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_demographics
               (user_id, age, sex, ethnicity, height_m, weight_kg, bmi, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, age, sex, ethnicity, height_m, weight_kg, bmi, self._now()),
        )
        self._auto_commit()

    def get_demographics(self, user_id: int | None = None) -> dict | None:
        if user_id is not None:
            row = self.conn.execute(
                "SELECT * FROM clean_demographics WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT * FROM clean_demographics LIMIT 1",
            ).fetchone()
        return dict(row) if row else None
