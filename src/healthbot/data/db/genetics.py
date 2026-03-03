"""Genetic variant methods."""
from __future__ import annotations

import logging
import sqlite3
import uuid

logger = logging.getLogger("healthbot")


class GeneticsMixin:
    """Mixin for genetic variant database operations."""

    def insert_genetic_variant(
        self, user_id: int, rsid: str, chromosome: str,
        position: int, variant_data: dict,
    ) -> str:
        """Insert a genetic variant. Returns variant ID.

        Upserts on (user_id, rsid) — updates if already exists.
        """
        vid = uuid.uuid4().hex
        aad = f"genetic_variants.encrypted_data.{vid}"
        enc_data = self._encrypt(variant_data, aad)
        try:
            self.conn.execute(
                """INSERT INTO genetic_variants
                   (id, user_id, rsid, chromosome, position, source,
                    created_at, encrypted_data)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (vid, user_id, rsid, chromosome, position,
                 variant_data.get("source", "tellmegen"),
                 self._now(), enc_data),
            )
        except sqlite3.IntegrityError:
            # Unique constraint on (user_id, rsid) — update existing
            row = self.conn.execute(
                "SELECT id FROM genetic_variants WHERE user_id = ? AND rsid = ?",
                (user_id, rsid),
            ).fetchone()
            if row:
                vid = row["id"]
                aad = f"genetic_variants.encrypted_data.{vid}"
                enc_data = self._encrypt(variant_data, aad)
                self.conn.execute(
                    "UPDATE genetic_variants SET encrypted_data = ? WHERE id = ?",
                    (enc_data, vid),
                )
        self.conn.commit()
        return vid

    def get_genetic_variants(
        self, user_id: int, rsids: list[str] | None = None,
    ) -> list[dict]:
        """Get genetic variants for a user, optionally filtered by rsid list."""
        if rsids:
            placeholders = ",".join("?" * len(rsids))
            rows = self.conn.execute(
                f"SELECT * FROM genetic_variants WHERE user_id = ? AND rsid IN ({placeholders})",
                [user_id, *rsids],
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM genetic_variants WHERE user_id = ? ORDER BY chromosome, position",
                (user_id,),
            ).fetchall()
        results = []
        for row in rows:
            aad = f"genetic_variants.encrypted_data.{row['id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for genetic_variants row %s: %s", row["id"], e)
                data = {}
            data["_id"] = row["id"]
            data["_rsid"] = row["rsid"]
            data["_chromosome"] = row["chromosome"]
            data["_position"] = row["position"]
            data["_source"] = row["source"]
            results.append(data)
        return results

    def get_genetic_variant_count(self, user_id: int) -> int:
        """Count total variants stored for a user."""
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM genetic_variants WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return row["cnt"] if row else 0

    def delete_genetic_variants(self, user_id: int) -> int:
        """Delete all genetic variants for a user. Returns count deleted."""
        cur = self.conn.execute(
            "DELETE FROM genetic_variants WHERE user_id = ?", (user_id,),
        )
        self.conn.commit()
        return cur.rowcount
