"""Extended sync workers for additional raw vault data types.

Covers: workouts, genetic variants, health goals, med reminders,
providers, and appointments. Follows the same pattern as
clean_sync_workers.py — iterate records, anonymize text fields,
upsert to clean DB, catch PhiDetectedError.
"""
from __future__ import annotations

import logging
from collections.abc import Callable

from healthbot.data.clean_db import CleanDB, PhiDetectedError
from healthbot.data.clean_sync_workers import SyncReport, _record_pii_alert

logger = logging.getLogger("healthbot")


def sync_workouts(
    raw_db: object,
    clean: CleanDB,
    report: SyncReport,
    user_id: int,
    *,
    since: str | None = None,
) -> set[str] | None:
    """Sync workouts — purely numeric, no PII. Returns synced IDs."""
    synced_ids: set[str] = set()

    try:
        kwargs: dict = {"user_id": user_id, "limit": 500}
        if since:
            kwargs["start_after"] = since
        data = raw_db.query_workouts(**kwargs)
    except Exception as e:
        report.errors.append(f"workouts query: {e}")
        return None

    for wo in data:
        wid = wo.get("_id", wo.get("id", ""))
        if not wid:
            continue
        synced_ids.add(str(wid))
        try:
            clean.upsert_workout(
                workout_id=wid,
                sport_type=wo.get("_sport_type", wo.get("sport_type", "")),
                start_date=wo.get("_start_date", wo.get("start_date", "")),
                source=wo.get("_source", wo.get("source", "")),
                duration_minutes=wo.get("duration_minutes"),
                calories_burned=wo.get("calories_burned"),
                avg_heart_rate=wo.get("avg_heart_rate"),
                max_heart_rate=wo.get("max_heart_rate"),
                min_heart_rate=wo.get("min_heart_rate"),
                distance_km=wo.get("distance_km"),
            )
            report.workouts_synced += 1
        except Exception as e:
            report.errors.append(f"workout {wid}: {e}")

    return None if since else synced_ids


def sync_genetic_variants(
    raw_db: object,
    clean: CleanDB,
    report: SyncReport,
    user_id: int,
) -> set[str] | None:
    """Sync genetic variants — public scientific data, no PII. Returns synced IDs."""
    synced_ids: set[str] = set()

    try:
        data = raw_db.get_genetic_variants(user_id)
    except Exception as e:
        report.errors.append(f"genetic_variants query: {e}")
        return None

    for v in data:
        vid = v.get("_id", v.get("id", ""))
        if not vid:
            continue
        synced_ids.add(str(vid))
        try:
            clean.upsert_genetic_variant(
                variant_id=vid,
                rsid=v.get("_rsid", v.get("rsid", "")),
                chromosome=v.get("_chromosome", v.get("chromosome", "")),
                position=v.get("_position", v.get("position")),
                source=v.get("_source", v.get("source", "")),
                genotype=v.get("genotype", ""),
                risk_allele=v.get("risk_allele", ""),
                phenotype=v.get("phenotype", ""),
            )
            report.genetic_variants_synced += 1
        except Exception as e:
            report.errors.append(f"genetic_variant {vid}: {e}")

    return synced_ids


def sync_health_goals(
    records: list[dict],
    anonymize: Callable[[str], str],
    clean: CleanDB,
    report: SyncReport,
    *,
    incremental: bool = False,
) -> set[str] | None:
    """Sync health goals — anonymizes goal_text. Returns synced IDs or None."""
    synced_ids: set[str] = set()

    for g in records:
        gid = g.get("_id", g.get("id", ""))
        if not gid:
            continue
        synced_ids.add(str(gid))

        try:
            goal_text = anonymize(g.get("goal_text", ""))
            if not goal_text:
                continue
            clean.upsert_health_goal(
                goal_id=gid,
                created_at=g.get("_created_at", g.get("created_at", "")),
                goal_text=goal_text,
            )
            report.health_goals_synced += 1
        except PhiDetectedError:
            report.pii_blocked += 1
            report.pii_blocked_details.append(f"Health goal ({gid})")
            logger.warning("PII blocked in health_goal %s", gid)
            _record_pii_alert("PHI_in_health_goal")
        except Exception as e:
            report.errors.append(f"health_goal {gid}: {e}")

    return None if incremental else synced_ids


def sync_med_reminders(
    records: list[dict],
    anonymize: Callable[[str], str],
    clean: CleanDB,
    report: SyncReport,
    *,
    incremental: bool = False,
) -> set[str] | None:
    """Sync med reminders — anonymizes med_name + notes. Returns synced IDs or None."""
    synced_ids: set[str] = set()

    for r in records:
        rid = r.get("_id", r.get("id", ""))
        if not rid:
            continue
        synced_ids.add(str(rid))

        try:
            med_name = anonymize(r.get("med_name", ""))
            notes = anonymize(r.get("notes", ""))
            clean.upsert_med_reminder(
                reminder_id=rid,
                time=r.get("_time", r.get("time", "")),
                enabled=r.get("_enabled", r.get("enabled", True)),
                med_name=med_name,
                notes=notes,
            )
            report.med_reminders_synced += 1
        except PhiDetectedError:
            report.pii_blocked += 1
            display = r.get("med_name", rid)
            report.pii_blocked_details.append(f"Med reminder '{display}'")
            logger.warning("PII blocked in med_reminder %s", rid)
            _record_pii_alert("PHI_in_med_reminder")
        except Exception as e:
            report.errors.append(f"med_reminder {rid}: {e}")

    return None if incremental else synced_ids


def sync_providers(
    records: list[dict],
    anonymize: Callable[[str], str],
    clean: CleanDB,
    report: SyncReport,
    *,
    incremental: bool = False,
) -> set[str] | None:
    """Sync providers — specialty + notes only (omits name/address/phone)."""
    synced_ids: set[str] = set()

    for p in records:
        pid = p.get("_id", p.get("id", ""))
        if not pid:
            continue
        synced_ids.add(str(pid))

        try:
            specialty = anonymize(p.get("specialty", ""))
            notes = anonymize(p.get("notes", ""))
            clean.upsert_provider(
                provider_id=pid,
                specialty=specialty,
                notes=notes,
            )
            report.providers_synced += 1
        except PhiDetectedError:
            report.pii_blocked += 1
            report.pii_blocked_details.append(f"Provider ({pid})")
            logger.warning("PII blocked in provider %s", pid)
            _record_pii_alert("PHI_in_provider")
        except Exception as e:
            report.errors.append(f"provider {pid}: {e}")

    return None if incremental else synced_ids


def sync_health_records_ext(
    raw_db: object,
    anonymize: Callable[[str], str],
    clean: CleanDB,
    report: SyncReport,
    user_id: int,
    *,
    since: str | None = None,
) -> set[str] | None:
    """Sync extensible health records — anonymize text fields. Returns synced IDs."""
    synced_ids: set[str] = set()

    try:
        data = raw_db.get_health_records_ext(user_id, since=since)
    except Exception as e:
        report.errors.append(f"health_records_ext query: {e}")
        return None

    for rec in data:
        rid = rec.get("id", "")
        if not rid:
            continue
        synced_ids.add(str(rid))

        inner = rec.get("data", {})
        if isinstance(inner, str):
            import json
            try:
                inner = json.loads(inner)
            except Exception:
                inner = {}

        try:
            label = anonymize(inner.get("label", rec.get("label", "")))
            value = anonymize(str(inner.get("value", "")))
            source = anonymize(str(inner.get("source", "")))
            details = anonymize(str(inner.get("details", "")))
            tags = anonymize(str(inner.get("tags", "")))

            clean.upsert_health_record_ext(
                record_id=rid,
                data_type=rec.get("data_type", ""),
                label=label,
                value=value,
                unit=str(inner.get("unit", "")),
                date_effective=str(inner.get("date", "")),
                source=source,
                details=details,
                tags=tags,
            )
            report.health_records_ext_synced += 1
        except PhiDetectedError:
            report.pii_blocked += 1
            display = rec.get("label", rid)
            report.pii_blocked_details.append(f"Health record ext '{display}'")
            logger.warning("PII blocked in health_record_ext %s", rid)
            _record_pii_alert("PHI_in_health_record_ext")
        except Exception as e:
            report.errors.append(f"health_record_ext {rid}: {e}")

    return None if since else synced_ids


def sync_substance_knowledge(
    raw_db: object,
    anonymize: Callable[[str], str],
    clean: CleanDB,
    report: SyncReport,
    user_id: int,
) -> set[str] | None:
    """Sync substance knowledge — anonymizes text fields. Returns synced IDs."""
    synced_ids: set[str] = set()

    try:
        data = raw_db.get_all_substance_knowledge(user_id)
    except Exception as e:
        report.errors.append(f"substance_knowledge query: {e}")
        return None

    for rec in data:
        rid = rec.get("id", "")
        if not rid:
            continue
        synced_ids.add(str(rid))
        inner = rec.get("data", {})
        if isinstance(inner, str):
            import json
            try:
                inner = json.loads(inner)
            except Exception:
                inner = {}

        try:
            import json as _json
            mechanism = anonymize(str(inner.get("mechanism_of_action", "")))
            half_life = str(inner.get("half_life", ""))
            cyp = _json.dumps(inner.get("cyp_interactions", {}))
            pathways = _json.dumps(inner.get("pathway_effects", {}))
            aliases = ",".join(inner.get("aliases", []))
            summary = anonymize(str(inner.get("clinical_evidence_summary", "")))
            sources = ",".join(str(s) for s in inner.get("research_sources", []))

            clean.upsert_substance_knowledge(
                substance_id=rid,
                name=rec.get("name", ""),
                quality_score=rec.get("quality_score", 0.0),
                mechanism=mechanism,
                half_life=half_life,
                cyp_interactions=cyp,
                pathway_effects=pathways,
                aliases=aliases,
                clinical_summary=summary,
                research_sources=sources,
            )
            report.substance_knowledge_synced = getattr(
                report, "substance_knowledge_synced", 0,
            ) + 1
        except PhiDetectedError:
            report.pii_blocked += 1
            report.pii_blocked_details.append(f"Substance knowledge ({rid})")
            logger.warning("PII blocked in substance_knowledge %s", rid)
            _record_pii_alert("PHI_in_substance_knowledge")
        except Exception as e:
            report.errors.append(f"substance_knowledge {rid}: {e}")

    return synced_ids


def sync_appointments(
    records: list[dict],
    anonymize: Callable[[str], str],
    clean: CleanDB,
    report: SyncReport,
    *,
    incremental: bool = False,
) -> set[str] | None:
    """Sync appointments — anonymizes reason, omits location."""
    synced_ids: set[str] = set()

    for a in records:
        aid = a.get("_id", a.get("id", ""))
        if not aid:
            continue
        synced_ids.add(str(aid))

        try:
            reason = anonymize(a.get("reason", ""))
            clean.upsert_appointment(
                appt_id=aid,
                provider_id=a.get("_provider_id", a.get("provider_id", "")),
                appt_date=a.get("_appt_date", a.get("appt_date", "")),
                status=a.get("_status", a.get("status", "")),
                reason=reason,
            )
            report.appointments_synced += 1
        except PhiDetectedError:
            report.pii_blocked += 1
            report.pii_blocked_details.append(f"Appointment ({aid})")
            logger.warning("PII blocked in appointment %s", aid)
            _record_pii_alert("PHI_in_appointment")
        except Exception as e:
            report.errors.append(f"appointment {aid}: {e}")

    return None if incremental else synced_ids
