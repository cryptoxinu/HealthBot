"""Permanent, growing clinical knowledge base.

Unlike the TTL-based evidence cache, this stores knowledge permanently:
- Research findings relevant to user's conditions (never expires)
- User corrections ("my doctor said X, not Y")
- Answered questions and their sources

Stored in health.db (included in encrypted backups automatically).
"""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


class KnowledgeBase:
    """Permanent clinical knowledge store with corrections tracking."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def store_finding(
        self,
        topic: str,
        finding: str,
        source: str,
        relevance_score: float = 0.5,
        user_confirmed: bool = False,
    ) -> str | None:
        """Store a research finding permanently.

        Returns the knowledge entry ID, or None if storage failed.
        """
        try:
            kb_id = uuid.uuid4().hex
            now = datetime.now(UTC).isoformat()
            aad = f"knowledge_base.encrypted_data.{kb_id}"
            data = {
                "topic": topic,
                "finding": finding,
                "source": source,
            }
            enc_data = self._db._encrypt(data, aad)

            self._db.conn.execute(
                """INSERT INTO knowledge_base
                   (id, topic, finding, source, relevance_score,
                    user_confirmed, category, created_at, encrypted_data)
                   VALUES (?, ?, ?, ?, ?, ?, 'research', ?, ?)""",
                (
                    kb_id, topic, finding[:200], source,
                    relevance_score, int(user_confirmed),
                    now, enc_data,
                ),
            )
            self._db.conn.commit()
            return kb_id
        except Exception as e:
            logger.debug("Knowledge base store failed: %s", e)
            return None

    def store_correction(
        self,
        original_claim: str,
        correction: str,
        source: str = "user",
    ) -> str | None:
        """Store a user correction — system was wrong about something.

        Returns the correction entry ID.
        """
        try:
            kb_id = uuid.uuid4().hex
            now = datetime.now(UTC).isoformat()
            aad = f"knowledge_base.encrypted_data.{kb_id}"
            data = {
                "topic": "correction",
                "original_claim": original_claim[:500],
                "correction": correction,
                "source": source,
            }
            enc_data = self._db._encrypt(data, aad)

            self._db.conn.execute(
                """INSERT INTO knowledge_base
                   (id, topic, finding, source, relevance_score,
                    user_confirmed, category, created_at, encrypted_data)
                   VALUES (?, ?, ?, ?, 1.0, 1, 'correction', ?, ?)""",
                (
                    kb_id, "correction",
                    correction[:200], source, now, enc_data,
                ),
            )
            self._db.conn.commit()
            return kb_id
        except Exception as e:
            logger.debug("Knowledge base correction failed: %s", e)
            return None

    def query(self, topic: str, top_k: int = 5) -> list[dict]:
        """Retrieve relevant knowledge for a topic.

        Uses simple keyword matching on the topic field.
        """
        try:
            # Search by topic keyword match
            rows = self._db.conn.execute(
                """SELECT id, topic, finding, source, relevance_score,
                          category, created_at
                   FROM knowledge_base
                   WHERE topic LIKE ? OR finding LIKE ?
                   ORDER BY relevance_score DESC, created_at DESC
                   LIMIT ?""",
                (f"%{topic}%", f"%{topic}%", top_k),
            ).fetchall()

            results = []
            for row in rows:
                kb_id = row["id"]
                aad = f"knowledge_base.encrypted_data.{kb_id}"
                try:
                    data = self._db._decrypt(row["encrypted_data"], aad)
                except Exception:
                    data = {}

                results.append({
                    "id": kb_id,
                    "topic": row["topic"],
                    "finding": data.get("finding", row["finding"]),
                    "source": row["source"],
                    "relevance_score": row["relevance_score"],
                    "category": row["category"],
                    "created_at": row["created_at"],
                })
            return results
        except Exception as e:
            logger.debug("Knowledge base query failed: %s", e)
            return []

    def get_corrections(self, top_k: int = 10) -> list[dict]:
        """Get all user corrections."""
        try:
            rows = self._db.conn.execute(
                """SELECT id, finding, source, created_at
                   FROM knowledge_base
                   WHERE category = 'correction'
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (top_k,),
            ).fetchall()

            results = []
            for row in rows:
                kb_id = row["id"]
                aad = f"knowledge_base.encrypted_data.{kb_id}"
                try:
                    data = self._db._decrypt(row["encrypted_data"], aad)
                except Exception:
                    data = {}
                results.append({
                    "original_claim": data.get("original_claim", ""),
                    "correction": data.get("correction", row["finding"]),
                    "source": row["source"],
                    "created_at": row["created_at"],
                })
            return results
        except Exception as e:
            logger.debug("Knowledge base corrections query failed: %s", e)
            return []

    def find_similar(
        self,
        topic: str,
        finding: str,
        source: str,
        threshold: float = 0.80,
    ) -> bool:
        """Check if a similar entry already exists (for dedup).

        Uses SequenceMatcher on topic+finding against entries
        with the same source. Returns True if a match is found.
        """
        from difflib import SequenceMatcher

        try:
            rows = self._db.conn.execute(
                """SELECT topic, finding FROM knowledge_base
                   WHERE source = ? LIMIT 200""",
                (source,),
            ).fetchall()
            candidate = f"{topic}|{finding}".lower()
            for row in rows:
                existing = f"{row['topic']}|{row['finding']}".lower()
                ratio = SequenceMatcher(None, candidate, existing).ratio()
                if ratio >= threshold:
                    return True
        except Exception as e:
            logger.debug("KB find_similar failed: %s", e)
        return False

    def delete_stale(
        self, source: str, max_age_days: int = 90,
    ) -> int:
        """Remove auto-generated entries older than max_age_days.

        Never removes user_confirmed or claude_research entries.
        Returns count of deleted entries.
        """
        try:
            cutoff = datetime.now(UTC) - timedelta(days=max_age_days)
            cutoff_str = cutoff.isoformat()
            cursor = self._db.conn.execute(
                """DELETE FROM knowledge_base
                   WHERE source = ? AND user_confirmed = 0
                   AND created_at < ?""",
                (source, cutoff_str),
            )
            self._db.conn.commit()
            return cursor.rowcount
        except Exception as e:
            logger.debug("KB delete_stale failed: %s", e)
            return 0

    def count(self) -> int:
        """Return total number of knowledge entries."""
        try:
            row = self._db.conn.execute(
                "SELECT COUNT(*) as cnt FROM knowledge_base",
            ).fetchone()
            return row["cnt"] if row else 0
        except Exception:
            return 0
