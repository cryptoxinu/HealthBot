"""Clean DB hypotheses mixin — medical hypothesis methods."""
from __future__ import annotations


class HypothesesMixin:
    """Mixin providing hypothesis methods for CleanDB."""

    def upsert_hypothesis(
        self,
        hyp_id: str,
        *,
        title: str,
        confidence: float = 0.0,
        evidence_for: str = "[]",
        evidence_against: str = "[]",
        missing_tests: str = "[]",
        status: str = "active",
    ) -> None:
        self._validate_text_fields(
            {"title": title, "evidence_for": evidence_for,
             "evidence_against": evidence_against,
             "missing_tests": missing_tests},
            f"hypothesis.{hyp_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_hypotheses
               (id, title, confidence, evidence_for, evidence_against,
                missing_tests, status, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (hyp_id, title, confidence, evidence_for, evidence_against,
             missing_tests, status, self._now()),
        )
        self._auto_commit()

    def get_hypotheses(self, status: str = "active") -> list[dict]:
        rows = self.conn.execute(
            """SELECT * FROM clean_hypotheses WHERE status = ?
               ORDER BY confidence DESC""",
            (status,),
        ).fetchall()
        return [dict(r) for r in rows]
