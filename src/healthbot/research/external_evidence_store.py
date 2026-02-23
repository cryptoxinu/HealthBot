"""External evidence store.

Stores research results as external evidence, never mixing
them into patient-record truth. Supports TTL-based expiry.
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime, timedelta

from healthbot.data.db import HealthDB
from healthbot.data.models import ExternalEvidence


class ExternalEvidenceStore:
    """Manage external evidence from research queries."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def store(
        self,
        source: str,
        query: str,
        result: dict | str,
        ttl_days: int = 30,
        condition_related: bool = False,
    ) -> str:
        """Store research evidence with optional TTL.

        Args:
            condition_related: If True, evidence never expires (permanent).
                Research directly related to user's conditions should persist.
        """
        query_hash = hashlib.sha256(query.encode()).hexdigest()[:16]
        result_json = result if isinstance(result, dict) else {"text": result}
        if condition_related:
            expires_at = ""  # Never expires
        else:
            expires_at = (datetime.now(UTC) + timedelta(days=ttl_days)).isoformat()

        ev = ExternalEvidence(
            id=uuid.uuid4().hex,
            source=source,
            query_hash=query_hash,
            prompt_sanitized=query,
            result_json=result_json,
            created_at=datetime.now(UTC),
        )
        ev_id = self._db.insert_external_evidence(ev)

        # Set expires_at (migration 4 adds the column)
        try:
            self._db.conn.execute(
                "UPDATE external_evidence SET expires_at = ? WHERE evidence_id = ?",
                (expires_at, ev_id),
            )
            self._db.conn.commit()
        except Exception:
            pass  # Column may not exist if migration not yet applied

        return ev_id

    def lookup_cached(self, query: str) -> dict | None:
        """Check if we have non-expired evidence for this query."""
        query_hash = hashlib.sha256(query.encode()).hexdigest()[:16]
        row = self._db.conn.execute(
            "SELECT evidence_id FROM external_evidence WHERE query_hash = ?",
            (query_hash,),
        ).fetchone()
        if not row:
            return None

        ev_id = row["evidence_id"]

        # Check expiry
        if self._is_expired(ev_id):
            return None

        ev = self._db.conn.execute(
            "SELECT encrypted_data FROM external_evidence WHERE evidence_id = ?",
            (ev_id,),
        ).fetchone()
        if ev:
            aad = f"external_evidence.encrypted_data.{ev_id}"
            return self._db._decrypt(ev["encrypted_data"], aad)
        return None

    def list_evidence(self, limit: int = 20) -> list[dict]:
        """List all cached evidence entries (metadata only)."""
        rows = self._db.conn.execute(
            """SELECT evidence_id, source, query_hash, created_at, expires_at
               FROM external_evidence
               ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()

        results = []
        for row in rows:
            ev_id = row["evidence_id"]
            aad = f"external_evidence.encrypted_data.{ev_id}"
            try:
                ev_row = self._db.conn.execute(
                    "SELECT encrypted_data FROM external_evidence WHERE evidence_id = ?",
                    (ev_id,),
                ).fetchone()
                data = self._db._decrypt(ev_row["encrypted_data"], aad) if ev_row else {}
            except Exception:
                data = {}

            results.append({
                "evidence_id": ev_id,
                "source": row["source"],
                "query": data.get("prompt_sanitized", ""),
                "summary": _truncate(data.get("text", str(data.get("result_json", ""))), 100),
                "created_at": row["created_at"],
                "expired": self._is_expired(ev_id),
            })

        return results

    def get_evidence_detail(self, evidence_id: str) -> dict | None:
        """Get full detail for a specific evidence entry."""
        row = self._db.conn.execute(
            "SELECT * FROM external_evidence WHERE evidence_id = ?",
            (evidence_id,),
        ).fetchone()
        if not row:
            return None

        aad = f"external_evidence.encrypted_data.{evidence_id}"
        data = self._db._decrypt(row["encrypted_data"], aad)
        data["_evidence_id"] = evidence_id
        data["_source"] = row["source"]
        data["_created_at"] = row["created_at"]
        return data

    def cleanup_expired(self) -> int:
        """Delete all expired evidence entries. Returns count deleted."""
        now = datetime.now(UTC).isoformat()
        cursor = self._db.conn.execute(
            "DELETE FROM external_evidence WHERE expires_at != '' AND expires_at < ?",
            (now,),
        )
        self._db.conn.commit()
        return cursor.rowcount

    def _is_expired(self, evidence_id: str) -> bool:
        """Check if a specific evidence entry has expired."""
        try:
            row = self._db.conn.execute(
                "SELECT expires_at FROM external_evidence WHERE evidence_id = ?",
                (evidence_id,),
            ).fetchone()
            if not row or not row["expires_at"]:
                return False
            return row["expires_at"] < datetime.now(UTC).isoformat()
        except Exception:
            return False


def _truncate(text: str, max_len: int) -> str:
    """Truncate text with ellipsis."""
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."
