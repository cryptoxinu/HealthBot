"""Clean DB search mixin — full-text search methods."""
from __future__ import annotations


class SearchMixin:
    """Mixin providing full-text search across all clean data."""

    def search(self, query: str, limit: int = 20) -> list[dict]:
        """Search across all clean data by keyword."""
        pat = f"%{query}%"
        results: list[dict] = []

        # Search observations
        rows = self.conn.execute(
            """SELECT 'lab' as source, obs_id as id, test_name, value, unit,
                      date_effective as date, flag
               FROM clean_observations
               WHERE test_name LIKE ? OR canonical_name LIKE ? OR value LIKE ?
               LIMIT ?""",
            (pat, pat, pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        # Search medications
        rows = self.conn.execute(
            """SELECT 'medication' as source, med_id as id, name, dose, frequency,
                      start_date as date, status
               FROM clean_medications
               WHERE name LIKE ? OR dose LIKE ?
               LIMIT ?""",
            (pat, pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        # Search workouts
        rows = self.conn.execute(
            """SELECT 'workout' as source, id, sport_type, start_date as date,
                      duration_minutes, calories_burned
               FROM clean_workouts
               WHERE sport_type LIKE ?
               LIMIT ?""",
            (pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        # Search genetic variants
        rows = self.conn.execute(
            """SELECT 'genetic' as source, id, rsid, genotype, phenotype,
                      '' as date
               FROM clean_genetic_variants
               WHERE rsid LIKE ? OR phenotype LIKE ?
               LIMIT ?""",
            (pat, pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        # Search health goals
        rows = self.conn.execute(
            """SELECT 'goal' as source, id, goal_text, created_at as date
               FROM clean_health_goals
               WHERE goal_text LIKE ?
               LIMIT ?""",
            (pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        # Search hypotheses
        rows = self.conn.execute(
            """SELECT 'hypothesis' as source, id, title, confidence, status,
                      '' as date
               FROM clean_hypotheses
               WHERE title LIKE ? OR evidence_for LIKE ?
               LIMIT ?""",
            (pat, pat, limit),
        ).fetchall()
        results.extend(dict(r) for r in rows)

        return results[:limit]
