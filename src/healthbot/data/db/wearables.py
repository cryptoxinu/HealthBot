"""WHOOP/Oura wearable daily data methods."""
from __future__ import annotations

import logging
from typing import Any

from healthbot.data.models import WhoopDaily

logger = logging.getLogger("healthbot")


class WearablesMixin:
    """Mixin for wearable daily data database operations."""

    def insert_wearable_daily(self, wd: WhoopDaily, user_id: int = 0) -> str:
        """Insert a wearable daily record."""
        import uuid

        wd_id = wd.id or uuid.uuid4().hex
        aad = f"wearable_daily.encrypted_data.{wd_id}"
        enc_data = self._encrypt(wd, aad)
        self.conn.execute(
            """INSERT OR IGNORE INTO wearable_daily (id, date, provider,
               created_at, encrypted_data, user_id)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (wd_id, wd.date.isoformat(), wd.provider, self._now(), enc_data, user_id),
        )
        self.conn.commit()
        return wd_id

    def query_wearable_daily(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        provider: str = "whoop",
        limit: int = 365,
        user_id: int | None = None,
        since: str | None = None,
    ) -> list[dict]:
        """Query wearable daily data."""
        sql = "SELECT * FROM wearable_daily WHERE provider = ?"
        params: list[Any] = [provider]
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)
        if since:
            sql += " AND created_at > ?"
            params.append(since)
        sql += " ORDER BY date DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            aad = f"wearable_daily.encrypted_data.{row['id']}"
            data = self._decrypt(row["encrypted_data"], aad)
            data["_id"] = row["id"]
            data["_date"] = row["date"]
            results.append(data)
        return results

    def query_wearable_stats(self, provider: str) -> dict | None:
        """Return aggregate stats for a wearable provider.

        Returns dict with keys: count, first_date, last_date.
        Returns None if no records found.
        """
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt, MIN(date) as first, MAX(date) as last "
            "FROM wearable_daily WHERE provider = ?",
            (provider,),
        ).fetchone()
        if not row or not row["cnt"]:
            return None
        return {
            "count": row["cnt"],
            "first_date": row["first"],
            "last_date": row["last"],
        }
