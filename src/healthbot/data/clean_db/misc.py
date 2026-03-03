"""Clean DB misc mixin — genetics, goals, reminders, providers, appointments, etc."""
from __future__ import annotations


class MiscMixin:
    """Mixin providing genetics, goals, reminders, providers, appointments,
    extended records, substances, analysis rules, and remaining methods.
    """

    # ── Workouts ──────────────────────────────────────────

    def upsert_workout(
        self,
        workout_id: str,
        *,
        sport_type: str = "",
        start_date: str = "",
        source: str = "",
        duration_minutes: float | None = None,
        calories_burned: float | None = None,
        avg_heart_rate: float | None = None,
        max_heart_rate: float | None = None,
        min_heart_rate: float | None = None,
        distance_km: float | None = None,
    ) -> None:
        # Workouts are purely numeric + API preset sport names — no PII
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_workouts
               (id, sport_type, start_date, source, duration_minutes,
                calories_burned, avg_heart_rate, max_heart_rate,
                min_heart_rate, distance_km, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (workout_id, sport_type, start_date, source, duration_minutes,
             calories_burned, avg_heart_rate, max_heart_rate,
             min_heart_rate, distance_km, self._now()),
        )
        self._auto_commit()

    def get_workouts(
        self,
        *,
        sport_type: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        if sport_type:
            rows = self.conn.execute(
                """SELECT * FROM clean_workouts WHERE sport_type = ?
                   ORDER BY start_date DESC LIMIT ?""",
                (sport_type, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM clean_workouts ORDER BY start_date DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Genetics ──────────────────────────────────────────

    def upsert_genetic_variant(
        self,
        variant_id: str,
        *,
        rsid: str = "",
        chromosome: str = "",
        position: int | None = None,
        source: str = "",
        genotype: str = "",
        risk_allele: str = "",
        phenotype: str = "",
    ) -> None:
        # rsIDs are public scientific identifiers — no PII
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_genetic_variants
               (id, rsid, chromosome, position, source, genotype,
                risk_allele, phenotype, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (variant_id, rsid, chromosome, position, source, genotype,
             risk_allele, phenotype, self._now()),
        )
        self._auto_commit()

    def get_genetic_variants(
        self,
        *,
        rsid: str | None = None,
        limit: int = 200,
    ) -> list[dict]:
        if rsid:
            rows = self.conn.execute(
                "SELECT * FROM clean_genetic_variants WHERE rsid = ? LIMIT ?",
                (rsid, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT * FROM clean_genetic_variants
                   ORDER BY chromosome, position LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Health goals ──────────────────────────────────────

    def upsert_health_goal(
        self,
        goal_id: str,
        *,
        created_at: str = "",
        goal_text: str,
    ) -> None:
        self._assert_no_phi(goal_text, f"health_goal.{goal_id}")
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_health_goals
               (id, created_at, goal_text, synced_at)
               VALUES (?, ?, ?, ?)""",
            (goal_id, created_at, goal_text, self._now()),
        )
        self._auto_commit()

    def get_health_goals(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM clean_health_goals ORDER BY created_at DESC",
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Med reminders ─────────────────────────────────────

    def upsert_med_reminder(
        self,
        reminder_id: str,
        *,
        time: str = "",
        enabled: bool = True,
        med_name: str = "",
        notes: str = "",
    ) -> None:
        self._validate_text_fields(
            {"med_name": med_name, "notes": notes},
            f"med_reminder.{reminder_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_med_reminders
               (id, time, enabled, med_name, notes, synced_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (reminder_id, time, int(enabled), med_name, notes, self._now()),
        )
        self._auto_commit()

    def get_med_reminders(self, enabled_only: bool = True) -> list[dict]:
        if enabled_only:
            rows = self.conn.execute(
                "SELECT * FROM clean_med_reminders WHERE enabled = 1 ORDER BY time",
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM clean_med_reminders ORDER BY time",
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Providers ─────────────────────────────────────────

    def upsert_provider(
        self,
        provider_id: str,
        *,
        specialty: str = "",
        notes: str = "",
    ) -> None:
        self._validate_text_fields(
            {"specialty": specialty, "notes": notes},
            f"provider.{provider_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_providers
               (id, specialty, notes, synced_at)
               VALUES (?, ?, ?, ?)""",
            (provider_id, specialty, notes, self._now()),
        )
        self._auto_commit()

    def get_providers(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM clean_providers ORDER BY specialty",
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Appointments ──────────────────────────────────────

    def upsert_appointment(
        self,
        appt_id: str,
        *,
        provider_id: str = "",
        appt_date: str = "",
        status: str = "",
        reason: str = "",
    ) -> None:
        self._assert_no_phi(reason, f"appointment.{appt_id}")
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_appointments
               (id, provider_id, appt_date, status, reason, synced_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (appt_id, provider_id, appt_date, status, reason, self._now()),
        )
        self._auto_commit()

    def get_appointments(
        self,
        *,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        if status:
            rows = self.conn.execute(
                """SELECT * FROM clean_appointments WHERE status = ?
                   ORDER BY appt_date DESC LIMIT ?""",
                (status, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM clean_appointments ORDER BY appt_date DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Health records ext methods ────────────────────────

    def upsert_health_record_ext(
        self,
        record_id: str,
        *,
        data_type: str,
        label: str,
        value: str = "",
        unit: str = "",
        date_effective: str = "",
        source: str = "",
        details: str = "",
        tags: str = "",
    ) -> None:
        self._validate_text_fields(
            {"label": label, "value": value, "source": source,
             "details": details, "tags": tags},
            f"health_record_ext.{record_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_health_records_ext
               (id, data_type, label, value, unit, date_effective, source,
                details, tags, synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (record_id, data_type, label, value, unit, date_effective,
             source, details, tags, self._now()),
        )
        self._auto_commit()

    def get_health_records_ext(
        self, data_type: str | None = None, limit: int = 200,
    ) -> list[dict]:
        if data_type:
            rows = self.conn.execute(
                """SELECT * FROM clean_health_records_ext
                   WHERE data_type = ? ORDER BY date_effective DESC LIMIT ?""",
                (data_type, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT * FROM clean_health_records_ext
                   ORDER BY data_type, date_effective DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Substance knowledge methods ───────────────────

    def upsert_substance_knowledge(
        self,
        substance_id: str,
        *,
        name: str,
        quality_score: float = 0.0,
        mechanism: str = "",
        half_life: str = "",
        cyp_interactions: str = "",
        pathway_effects: str = "",
        aliases: str = "",
        clinical_summary: str = "",
        research_sources: str = "",
    ) -> None:
        self._validate_text_fields(
            {"name": name, "mechanism": mechanism, "half_life": half_life,
             "clinical_summary": clinical_summary,
             "cyp_interactions": cyp_interactions,
             "pathway_effects": pathway_effects,
             "aliases": aliases,
             "research_sources": research_sources},
            f"substance_knowledge.{substance_id}",
        )
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_substance_knowledge
               (id, name, quality_score, mechanism, half_life, cyp_interactions,
                pathway_effects, aliases, clinical_summary, research_sources,
                synced_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (substance_id, name.lower(), quality_score, mechanism, half_life,
             cyp_interactions, pathway_effects, aliases, clinical_summary,
             research_sources, self._now()),
        )
        self._auto_commit()

    def get_substance_knowledge(self, name: str) -> dict | None:
        """Get substance knowledge profile by name."""
        row = self.conn.execute(
            "SELECT * FROM clean_substance_knowledge WHERE name = ?",
            (name.lower(),),
        ).fetchone()
        return dict(row) if row else None

    def get_all_substance_knowledge(self) -> list[dict]:
        """Get all substance knowledge profiles."""
        rows = self.conn.execute(
            "SELECT * FROM clean_substance_knowledge ORDER BY name",
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Analysis rule methods ──────────────────────────

    def upsert_analysis_rule(
        self,
        name: str,
        scope: str,
        rule: str,
        priority: str = "medium",
        active: bool = True,
    ) -> str:
        """PII-validated upsert of an analysis rule. Returns rule ID."""
        self._validate_text_fields(
            {"name": name, "scope": scope, "rule": rule},
            f"analysis_rule.{name}",
        )
        import uuid as _uuid
        now = self._now()
        # Check for existing by name
        row = self.conn.execute(
            "SELECT id FROM clean_analysis_rules WHERE name = ?", (name,),
        ).fetchone()
        rule_id = row["id"] if row else _uuid.uuid4().hex
        self.conn.execute(
            """INSERT OR REPLACE INTO clean_analysis_rules
               (id, name, scope, rule, priority, active, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?,
                COALESCE((SELECT created_at FROM clean_analysis_rules WHERE id = ?), ?),
                ?)""",
            (rule_id, name, scope, rule, priority, int(active),
             rule_id, now, now),
        )
        self._auto_commit()
        return rule_id

    def get_active_analysis_rules(self) -> list[dict]:
        rows = self.conn.execute(
            """SELECT * FROM clean_analysis_rules WHERE active = 1
               ORDER BY CASE priority
                   WHEN 'high' THEN 1
                   WHEN 'medium' THEN 2
                   WHEN 'low' THEN 3
                   ELSE 4
               END, updated_at DESC""",
        ).fetchall()
        return [dict(r) for r in rows]

    def deactivate_analysis_rule(self, name: str) -> bool:
        cursor = self.conn.execute(
            """UPDATE clean_analysis_rules SET active = 0, updated_at = ?
               WHERE name = ? AND active = 1""",
            (self._now(), name),
        )
        self._auto_commit()
        return cursor.rowcount > 0
