"""Clean DB wearables mixin — wearable data methods."""
from __future__ import annotations


class WearablesMixin:
    """Mixin providing wearable data methods for CleanDB."""

    def upsert_wearable(
        self,
        wearable_id: str,
        *,
        date: str,
        provider: str = "whoop",
        hrv: float | None = None,
        rhr: float | None = None,
        resp_rate: float | None = None,
        spo2: float | None = None,
        sleep_score: float | None = None,
        recovery_score: float | None = None,
        strain: float | None = None,
        sleep_duration_min: int | None = None,
        rem_min: int | None = None,
        deep_min: int | None = None,
        light_min: int | None = None,
        calories: float | None = None,
        sleep_latency_min: float | None = None,
        wake_episodes: int | None = None,
        sleep_efficiency_pct: float | None = None,
        workout_sport_name: str | None = None,
        workout_avg_hr: float | None = None,
        workout_max_hr: float | None = None,
        skin_temp: float | None = None,
    ) -> None:
        # Wearable data is purely numeric (+ sport name from API presets) — no PII
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_wearable_daily
               (id, date, provider, hrv, rhr, resp_rate, spo2, sleep_score,
                recovery_score, strain, sleep_duration_min, rem_min, deep_min,
                light_min, calories, sleep_latency_min, wake_episodes,
                sleep_efficiency_pct, workout_sport_name, workout_avg_hr,
                workout_max_hr, skin_temp, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (wearable_id, date, provider, hrv, rhr, resp_rate, spo2,
             sleep_score, recovery_score, strain, sleep_duration_min,
             rem_min, deep_min, light_min, calories, sleep_latency_min,
             wake_episodes, sleep_efficiency_pct, workout_sport_name,
             workout_avg_hr, workout_max_hr, skin_temp, self._now()),
        )
        self._auto_commit()

    def get_wearable_data(
        self,
        *,
        days: int = 7,
        provider: str = "whoop",
    ) -> list[dict]:
        rows = self.conn.execute(
            """SELECT * FROM clean_wearable_daily
               WHERE provider = ?
               ORDER BY date DESC LIMIT ?""",
            (provider, days),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_wearable_daily(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        provider: str = "whoop",
        limit: int = 365,
        user_id: int | None = None,
    ) -> list[dict]:
        """Query wearable data — same interface as HealthDB for duck typing.

        Wearable data is purely numeric (no PII), so reasoning modules
        can read directly from Clean DB without decrypt overhead.
        """
        sql = "SELECT * FROM clean_wearable_daily WHERE provider = ?"
        params: list = [provider]
        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)
        sql += " ORDER BY date DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
