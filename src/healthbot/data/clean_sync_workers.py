"""Per-type sync worker functions and shared types for clean_sync engine.

Each function takes pre-fetched records + anonymize callback + clean DB + report,
and writes anonymized data to the clean DB. Extracted from CleanSyncEngine to
keep the orchestrator under 400 lines.

SyncReport and _normalize_lab_brand live here to avoid circular imports.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from healthbot.data.clean_db import CleanDB, PhiDetectedError

logger = logging.getLogger("healthbot")

# Normalize lab brand names for standardization.
_LAB_BRAND_MAP: dict[str, str] = {
    "labcorp": "LabCorp",
    "laboratory corporation": "LabCorp",
    "laboratory corporation of america": "LabCorp",
    "quest diagnostics": "Quest Diagnostics",
    "quest": "Quest Diagnostics",
    "mychart": "MyChart",
    "epic": "Epic",
    "bioreference": "BioReference",
    "bioreference laboratories": "BioReference",
    "sonora quest": "Sonora Quest",
    "aegis": "Aegis",
}


def _normalize_lab_brand(raw_name: str) -> str:
    """Normalize a raw lab name to a canonical brand."""
    if not raw_name:
        return ""
    key = raw_name.strip().lower()
    if key in _LAB_BRAND_MAP:
        return _LAB_BRAND_MAP[key]
    for prefix, brand in _LAB_BRAND_MAP.items():
        if key.startswith(prefix):
            return brand
    return raw_name.strip()


@dataclass
class SyncReport:
    """Results from a clean sync run."""

    observations_synced: int = 0
    medications_synced: int = 0
    wearables_synced: int = 0
    demographics_synced: bool = False
    hypotheses_synced: int = 0
    health_context_synced: int = 0
    workouts_synced: int = 0
    genetic_variants_synced: int = 0
    health_goals_synced: int = 0
    med_reminders_synced: int = 0
    providers_synced: int = 0
    appointments_synced: int = 0
    health_records_ext_synced: int = 0
    stale_deleted: int = 0
    pii_blocked: int = 0
    pii_blocked_details: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    incremental: bool = False

    def summary(self) -> str:
        prefix = "[incremental] " if self.incremental else ""
        parts = [
            f"Observations: {self.observations_synced}",
            f"Medications: {self.medications_synced}",
            f"Wearables: {self.wearables_synced}",
            f"Demographics: {'yes' if self.demographics_synced else 'no'}",
            f"Hypotheses: {self.hypotheses_synced}",
            f"Health context: {self.health_context_synced}",
            f"Workouts: {self.workouts_synced}",
            f"Genetics: {self.genetic_variants_synced}",
            f"Goals: {self.health_goals_synced}",
            f"Reminders: {self.med_reminders_synced}",
            f"Providers: {self.providers_synced}",
            f"Appointments: {self.appointments_synced}",
            f"Extended records: {self.health_records_ext_synced}",
        ]
        if self.stale_deleted:
            parts.append(f"Stale deleted: {self.stale_deleted}")
        if self.pii_blocked:
            parts.append(f"PII blocked: {self.pii_blocked}")
        if self.errors:
            parts.append(f"Errors: {len(self.errors)}")
        return prefix + " | ".join(parts)



def _record_pii_alert(category: str) -> None:
    """Best-effort PII alert recording."""
    try:
        from healthbot.security.pii_alert import PiiAlertService
        PiiAlertService.get_instance().record(
            category=category, destination="clean_db",
        )
    except Exception:
        pass


def sync_observations(
    records: list[dict],
    anonymize: Callable[[str], str],
    clean: CleanDB,
    report: SyncReport,
    *,
    incremental: bool = False,
    on_progress: Callable[[str], None] | None = None,
) -> set[str] | None:
    """Sync lab results/vitals to clean DB. Returns synced IDs (full) or None (incremental)."""
    synced_ids: set[str] = set()
    total = len(records)

    for idx, rec in enumerate(records):
        meta = rec.get("_meta", {})
        obs_id = meta.get("obs_id", "")
        if not obs_id:
            continue
        synced_ids.add(obs_id)

        try:
            test_name = rec.get("test_name") or rec.get("canonical_name", "")
            value = str(rec.get("value", ""))
            unit = rec.get("unit", "")
            ref_text = rec.get("reference_text", "")
            source_lab = _normalize_lab_brand(rec.get("lab_name", ""))

            test_name = anonymize(test_name)
            ref_text = anonymize(ref_text)
            value = anonymize(value)

            clean.upsert_observation(
                obs_id=obs_id,
                record_type=meta.get("record_type", "lab_result"),
                canonical_name=anonymize(rec.get("canonical_name", "")),
                date_effective=meta.get("date_effective", ""),
                triage_level=meta.get("triage_level", "normal"),
                flag=rec.get("flag", ""),
                test_name=test_name,
                value=value,
                unit=unit,
                reference_low=rec.get("reference_low"),
                reference_high=rec.get("reference_high"),
                reference_text=ref_text,
                age_at_collection=rec.get("age_at_collection"),
                source_lab=source_lab,
            )
            report.observations_synced += 1
            if on_progress and (report.observations_synced % 25 == 0 or idx == total - 1):
                try:
                    on_progress(
                        f"Observations: {report.observations_synced}/{total}"
                    )
                except Exception:
                    pass
        except PhiDetectedError:
            report.pii_blocked += 1
            display = rec.get("canonical_name") or test_name or obs_id
            date = meta.get("date_effective", "")
            report.pii_blocked_details.append(f"Lab '{display}' ({date})")
            logger.warning("PII blocked in observation %s", obs_id)
            _record_pii_alert("PHI_in_observation")
        except Exception as e:
            report.errors.append(f"observation {obs_id}: {e}")

    return None if incremental else synced_ids


def sync_medications(
    records: list[dict],
    anonymize: Callable[[str], str],
    clean: CleanDB,
    report: SyncReport,
    *,
    incremental: bool = False,
) -> set[str] | None:
    """Sync medications to clean DB. Returns synced IDs (full) or None (incremental)."""
    synced_ids: set[str] = set()

    for med in records:
        med_id = med.get("id", "")
        if not med_id:
            continue
        synced_ids.add(str(med_id))

        try:
            name = anonymize(med.get("name", ""))
            dose = str(med.get("dose", ""))
            frequency = med.get("frequency", "")

            clean.upsert_medication(
                med_id=med_id,
                name=name,
                dose=dose,
                unit=med.get("unit", ""),
                frequency=frequency,
                status=med.get("status", "active"),
                start_date=med.get("start_date", ""),
                end_date=med.get("end_date", ""),
            )
            report.medications_synced += 1
        except PhiDetectedError:
            report.pii_blocked += 1
            display = med.get("name", med_id)
            report.pii_blocked_details.append(f"Medication '{display}'")
            logger.warning("PII blocked in medication %s", med_id)
            _record_pii_alert("PHI_in_medication")
        except Exception as e:
            report.errors.append(f"medication {med_id}: {e}")

    return None if incremental else synced_ids


def sync_wearables(
    raw_db: object,
    clean: CleanDB,
    report: SyncReport,
    user_id: int,
    *,
    since: str | None = None,
) -> set[str] | None:
    """Sync wearable data — purely numeric, no PII. Returns synced IDs."""
    synced_ids: set[str] = set()

    for provider in ("whoop", "oura"):
        try:
            data = raw_db.query_wearable_daily(
                provider=provider, limit=365, user_id=user_id, since=since,
            )
        except Exception as e:
            report.errors.append(f"wearable {provider} query: {e}")
            continue

        for day in data:
            wid = day.get("_id", day.get("id", ""))
            if not wid:
                continue
            synced_ids.add(str(wid))
            try:
                clean.upsert_wearable(
                    wearable_id=wid,
                    date=day.get("_date", day.get("date", "")),
                    provider=provider,
                    hrv=day.get("hrv"),
                    rhr=day.get("rhr"),
                    resp_rate=day.get("resp_rate"),
                    spo2=day.get("spo2"),
                    sleep_score=day.get("sleep_score"),
                    recovery_score=day.get("recovery_score"),
                    strain=day.get("strain"),
                    sleep_duration_min=day.get("sleep_duration_min"),
                    rem_min=day.get("rem_min"),
                    deep_min=day.get("deep_min"),
                    light_min=day.get("light_min"),
                    calories=day.get("calories"),
                    sleep_latency_min=day.get("sleep_latency_min"),
                    wake_episodes=day.get("wake_episodes"),
                    sleep_efficiency_pct=day.get("sleep_efficiency_pct"),
                    workout_sport_name=day.get("workout_sport_name"),
                    workout_avg_hr=day.get("workout_avg_hr"),
                    workout_max_hr=day.get("workout_max_hr"),
                    skin_temp=day.get("skin_temp"),
                )
                report.wearables_synced += 1
            except Exception as e:
                report.errors.append(f"wearable {wid}: {e}")

    return None if since else synced_ids


def sync_demographics(
    raw_db: object,
    clean: CleanDB,
    report: SyncReport,
    user_id: int,
) -> None:
    """Sync demographics — age/sex/ethnicity/height/weight/BMI only."""
    try:
        demo = raw_db.get_user_demographics(user_id)
    except Exception as e:
        report.errors.append(f"demographics query: {e}")
        return

    if not demo or not any(demo.values()):
        return

    try:
        clean.upsert_demographics(
            user_id=user_id,
            age=demo.get("age"),
            sex=demo.get("sex", ""),
            ethnicity=demo.get("ethnicity", ""),
            height_m=demo.get("height_m"),
            weight_kg=demo.get("weight_kg"),
            bmi=demo.get("bmi"),
        )
        report.demographics_synced = True
    except Exception as e:
        report.errors.append(f"demographics: {e}")


def sync_hypotheses(
    records: list[dict],
    anonymize: Callable[[str], str],
    clean: CleanDB,
    report: SyncReport,
    *,
    incremental: bool = False,
) -> set[str] | None:
    """Sync active hypotheses to clean DB. Returns synced IDs (full) or None."""
    synced_ids: set[str] = set()

    for h in records:
        hyp_id = h.get("_id", h.get("id", ""))
        if not hyp_id:
            continue
        synced_ids.add(str(hyp_id))

        try:
            title = anonymize(h.get("title", ""))
            evidence_for = json.dumps(h.get("evidence_for", []))
            evidence_against = json.dumps(h.get("evidence_against", []))
            missing_tests = json.dumps(h.get("missing_tests", []))

            clean.upsert_hypothesis(
                hyp_id=hyp_id,
                title=title,
                confidence=h.get("_confidence", h.get("confidence", 0.0)),
                evidence_for=evidence_for,
                evidence_against=evidence_against,
                missing_tests=missing_tests,
                status=h.get("_status", h.get("status", "active")),
            )
            report.hypotheses_synced += 1
        except PhiDetectedError:
            report.pii_blocked += 1
            display = h.get("title", hyp_id)
            report.pii_blocked_details.append(f"Hypothesis '{display[:60]}'")
            logger.warning("PII blocked in hypothesis %s", hyp_id)
            _record_pii_alert("PHI_in_hypothesis")
        except Exception as e:
            report.errors.append(f"hypothesis {hyp_id}: {e}")

    return None if incremental else synced_ids


def sync_health_context(
    records: list[dict],
    anonymize: Callable[[str], str],
    clean: CleanDB,
    report: SyncReport,
    *,
    incremental: bool = False,
) -> set[str] | None:
    """Sync LTM health context facts (non-demographic). Returns synced IDs or None."""
    synced_ids: set[str] = set()

    for fact in records:
        cat = fact.get("_category", "").lower()
        if cat == "demographic":
            continue

        fact_id = fact.get("_id", fact.get("id", ""))
        if not fact_id:
            continue
        synced_ids.add(str(fact_id))

        text = fact.get("fact", "")
        if not text:
            continue

        try:
            cleaned = anonymize(text)
            clean.upsert_health_context(
                ctx_id=fact_id,
                category=cat,
                fact=cleaned,
            )
            report.health_context_synced += 1
        except PhiDetectedError:
            report.pii_blocked += 1
            report.pii_blocked_details.append(f"Health fact ({cat})")
            logger.warning("PII blocked in health_context %s", fact_id)
            _record_pii_alert("PHI_in_health_context")
        except Exception as e:
            report.errors.append(f"health_context {fact_id}: {e}")

    return None if incremental else synced_ids
