"""Clean DB health context mixin — facts + context methods."""
from __future__ import annotations


class HealthContextMixin:
    """Mixin providing health context and facts methods for CleanDB."""

    def upsert_health_context(
        self,
        ctx_id: str,
        *,
        category: str = "",
        fact: str,
    ) -> None:
        self._assert_no_phi(fact, f"health_context.{ctx_id}")
        # Encrypt the fact text (may contain borderline data)
        encrypted = self._encrypt(fact, f"clean_health_context.fact.{ctx_id}")
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_health_context
               (id, category, fact, synced_at)
               VALUES (?, ?, ?, ?)""",
            (ctx_id, category, encrypted, self._now()),
        )
        self._auto_commit()

    def get_health_context(self, category: str | None = None) -> list[dict]:
        if category:
            rows = self.conn.execute(
                "SELECT * FROM clean_health_context WHERE category = ?",
                (category,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM clean_health_context",
            ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            if isinstance(d["fact"], bytes):
                d["fact"] = self._decrypt(d["fact"], f"clean_health_context.fact.{d['id']}")
            results.append(d)
        return results

    # ── Exact-fact lookup (patient constants) ────────────

    _FACT_CATEGORIES: frozenset[str] = frozenset({
        "allergy", "medication", "demographic", "baseline_metric",
        "medical_context", "supplement", "preference",
        "lifestyle", "goal",
    })

    def get_facts(self, category: str | None = None) -> dict[str, str]:
        """Return high-confidence user-stated facts as a key->value dict.

        Only returns active (non-superseded), user-stated memories with
        confidence >= 0.9. These are deterministic constants that Claude
        should never contradict.

        Args:
            category: Optional category filter. Must be one of the
                      recognized fact categories if provided.

        Returns:
            Dict mapping fact key to value string.
        """
        if category and category not in self._FACT_CATEGORIES:
            return {}
        if category:
            rows = self.conn.execute(
                """SELECT key, value FROM clean_user_memory
                   WHERE superseded_by = ''
                     AND confidence >= 0.9
                     AND source = 'user_stated'
                     AND category = ?
                   ORDER BY key""",
                (category,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT key, value, category FROM clean_user_memory
                   WHERE superseded_by = ''
                     AND confidence >= 0.9
                     AND source = 'user_stated'
                   ORDER BY category, key""",
            ).fetchall()
        return {r["key"]: r["value"] for r in rows}
