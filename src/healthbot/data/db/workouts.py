"""Exercise tracking methods."""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from healthbot.data.models import Workout

logger = logging.getLogger("healthbot")


class WorkoutsMixin:
    """Mixin for workout/exercise database operations."""

    def insert_workout(self, wo: Workout, user_id: int = 0) -> str:
        """Insert a workout record with AES-256-GCM encryption."""
        wo_id = wo.id or uuid.uuid4().hex
        aad = f"workouts.encrypted_data.{wo_id}"
        enc_data = self._encrypt(wo, aad)
        start_date = wo.start_time.isoformat() if wo.start_time else ""
        try:
            self.conn.execute(
                """INSERT INTO workouts
                   (id, user_id, sport_type, start_date, source, created_at,
                    encrypted_data)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (wo_id, user_id, wo.sport_type, start_date,
                 wo.source, self._now(), enc_data),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return wo_id

    def get_existing_workout_keys(
        self, user_id: int = 0,
    ) -> set[tuple[str, str | None]]:
        """Return (sport_type, start_date) pairs for dedup checks."""
        rows = self.conn.execute(
            "SELECT sport_type, start_date FROM workouts WHERE user_id = ?",
            (user_id,),
        ).fetchall()
        return {(r["sport_type"], r["start_date"]) for r in rows}

    def query_workouts(
        self,
        sport_type: str | None = None,
        start_after: str | None = None,
        user_id: int | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Query and decrypt workouts with optional filters."""
        sql = "SELECT * FROM workouts WHERE 1=1"
        params: list[Any] = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        if sport_type:
            sql += " AND sport_type = ?"
            params.append(sport_type)
        if start_after:
            sql += " AND start_date >= ?"
            params.append(start_after)
        sql += " ORDER BY start_date DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(sql, params).fetchall()
        results: list[dict] = []
        for row in rows:
            aad = f"workouts.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
                data["_id"] = row["id"]
                data["_sport_type"] = row["sport_type"]
                data["_start_date"] = row["start_date"]
                results.append(data)
            except Exception as e:
                logger.warning("Decrypt failed for workouts row %s: %s", row["id"], e)
                continue
        return results

    def get_workout_summary(
        self,
        days: int = 30,
        user_id: int | None = None,
    ) -> dict:
        """Aggregate workout stats over a time period.

        Returns dict with:
            total_workouts, total_minutes, total_calories,
            by_sport: {sport: {count, minutes, calories}},
            streak_days: consecutive days with at least one workout.
        """
        start_after = (
            datetime.now(UTC) - timedelta(days=days)
        ).strftime("%Y-%m-%d")
        rows = self.query_workouts(
            start_after=start_after, user_id=user_id, limit=500,
        )

        total_mins = 0.0
        total_cal = 0.0
        by_sport: dict[str, dict] = {}
        workout_dates: set[str] = set()

        for row in rows:
            sport = row.get("sport_type", row.get("_sport_type", "other"))
            dur = float(row.get("duration_minutes", 0) or 0)
            cal = float(row.get("calories_burned", 0) or 0)
            total_mins += dur
            total_cal += cal

            if sport not in by_sport:
                by_sport[sport] = {"count": 0, "minutes": 0.0, "calories": 0.0}
            by_sport[sport]["count"] += 1
            by_sport[sport]["minutes"] += dur
            by_sport[sport]["calories"] += cal

            dt = row.get("_start_date", "")[:10]
            if dt:
                workout_dates.add(dt)

        # Calculate streak: consecutive days ending today (or most recent)
        streak = 0
        if workout_dates:
            sorted_dates = sorted(workout_dates, reverse=True)
            today = datetime.now(UTC).strftime("%Y-%m-%d")
            # Start from today or most recent workout date
            check = today if today in workout_dates else sorted_dates[0]
            check_date = datetime.strptime(check, "%Y-%m-%d").date()
            for _ in range(len(sorted_dates)):
                if check_date.isoformat() in workout_dates:
                    streak += 1
                    check_date -= timedelta(days=1)
                else:
                    break

        return {
            "total_workouts": len(rows),
            "total_minutes": total_mins,
            "total_calories": total_cal,
            "by_sport": by_sport,
            "streak_days": streak,
        }
