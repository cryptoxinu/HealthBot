"""Clean DB observations mixin — lab results methods."""
from __future__ import annotations


class ObservationsMixin:
    """Mixin providing lab result / observation methods for CleanDB."""

    def upsert_observation(
        self,
        obs_id: str,
        *,
        record_type: str = "lab_result",
        canonical_name: str = "",
        date_effective: str = "",
        triage_level: str = "normal",
        flag: str = "",
        test_name: str = "",
        value: str = "",
        unit: str = "",
        reference_low: float | None = None,
        reference_high: float | None = None,
        reference_text: str = "",
        age_at_collection: int | None = None,
        source_lab: str = "",
    ) -> None:
        self._validate_text_fields(
            {"test_name": test_name, "canonical_name": canonical_name,
             "value": value, "unit": unit, "reference_text": reference_text,
             "source_lab": source_lab},
            f"observation.{obs_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_observations
               (obs_id, record_type, canonical_name, date_effective, triage_level,
                flag, test_name, value, unit, reference_low, reference_high,
                reference_text, age_at_collection, source_lab, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (obs_id, record_type, canonical_name, date_effective, triage_level,
             flag, test_name, value, unit, reference_low, reference_high,
             reference_text, age_at_collection, source_lab, self._now()),
        )
        self._auto_commit()

    def get_lab_results(
        self,
        *,
        test_name: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        flag: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        sql = "SELECT * FROM clean_observations WHERE record_type = 'lab_result'"
        params: list = []
        if test_name:
            sql += " AND (canonical_name LIKE ? OR test_name LIKE ?)"
            pat = f"%{test_name}%"
            params.extend([pat, pat])
        if start_date:
            sql += " AND date_effective >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date_effective <= ?"
            params.append(end_date)
        if flag:
            sql += " AND flag = ?"
            params.append(flag)
        sql += " ORDER BY date_effective DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def _get_latest_per_test(self, *, limit: int = 200) -> list[dict]:
        """Return the most recent result for each unique test.

        Uses a SQL window function to deduplicate by canonical_name,
        ensuring qualitative tests (JAK2, CALR, HBsAg, etc.) are always
        visible even when newer numeric panels have many more rows.
        """
        sql = """
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY canonical_name
                    ORDER BY date_effective DESC
                ) AS rn
                FROM clean_observations
                WHERE record_type = 'lab_result'
            ) WHERE rn = 1
            ORDER BY date_effective DESC
            LIMIT ?
        """
        rows = self.conn.execute(sql, (limit,)).fetchall()
        # Strip the synthetic rn column from results
        return [{k: v for k, v in dict(r).items() if k != "rn"} for r in rows]
