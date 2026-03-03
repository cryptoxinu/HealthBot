"""Shared helper functions for the handlers_data package."""
from __future__ import annotations

_DEVELOPER_URLS: dict[str, str] = {
    "WHOOP": "https://developer.whoop.com",
    "Oura Ring": "https://developer.ouraring.com",
}


def _fmt_duration(seconds: float) -> str:
    """Format seconds as human-readable duration."""
    if seconds < 60:
        return f"{int(seconds)} sec"
    if seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}:{s:02d}"
    h, remainder = divmod(int(seconds), 3600)
    m, _ = divmod(remainder, 60)
    return f"{h}h {m}m"


def _format_estimate(est) -> str:
    """Format a SyncEstimate as a Telegram-friendly preview message."""
    lines = ["Clean Sync Preview\n"]
    record_parts = []
    for label, count in [
        ("obs", est.obs_count), ("meds", est.meds_count),
        ("hypotheses", est.hyps_count), ("context", est.ctx_count),
        ("goals", est.goals_count), ("reminders", est.reminders_count),
        ("providers", est.providers_count), ("appts", est.appointments_count),
        ("wearable days", est.wearable_count), ("genetics", est.genetics_count),
        ("ext records", est.ext_count),
    ]:
        if count:
            record_parts.append(f"{count} {label}")
    if record_parts:
        lines.append("Records: " + ", ".join(record_parts))

    lines.append(f"Text fields to anonymize: ~{est.total_text_fields:,}")
    if est.cache_size:
        cache_pct = min(100, int(est.cache_size / max(est.total_text_fields, 1) * 100))
        lines.append(f"Cache: {est.cache_size:,} already cached ({cache_pct}%)")

    uncached = max(0, est.total_text_fields - est.estimated_safe_skip - est.cache_size)
    if uncached:
        lines.append(f"Uncached fields to process: ~{uncached}")

    lines.append("\nChoose mode:\n")
    lines.append(
        f"[Fast] Regex + NER + identity -- ~{_fmt_duration(est.estimated_fast_sec)}\n"
        "  Catches known names/DOB/patterns. No LLM.\n"
    )
    lines.append(
        f"[Hybrid] Smart -- ~{_fmt_duration(est.estimated_hybrid_sec)}\n"
        f"  Fast pass first, Ollama reviews ~{est.hybrid_ollama_fields:,} uncertain fields.\n"
    )
    lines.append(
        f"[Full] All layers -- ~{_fmt_duration(est.estimated_full_sec)}\n"
        "  Ollama on every field. Most thorough.\n"
    )
    lines.append(
        f"[Rebuild] -- ~{_fmt_duration(est.estimated_rebuild_sec)}\n"
        "  Clear cache + full re-anonymize."
    )
    return "\n".join(lines)


def _format_progress(prog, elapsed: float) -> str:
    """Format live SyncProgress as a Telegram message."""
    lines = [f"Syncing... ({_fmt_duration(elapsed)} elapsed)\n"]

    all_phases = [
        "Observations", "Medications", "Wearables", "Demographics",
        "Hypotheses", "Health context", "Workouts", "Genetics",
        "Goals", "Reminders", "Providers", "Appointments", "Extended records",
        "Ollama review",
    ]
    for phase in all_phases:
        if phase in prog.phases_completed:
            lines.append(f"  [done] {phase}")
        elif phase == prog.current_phase:
            if prog.phase_total:
                lines.append(f"  [....] {phase}: {prog.phase_done}/{prog.phase_total}")
            else:
                lines.append(f"  [....] {phase}")
        # Don't show Ollama review if not in hybrid mode
        elif phase == "Ollama review" and not prog.hybrid_queued:
            continue
        else:
            lines.append(f"  [    ] {phase}")

    anon_parts = [
        f"{prog.safe_skipped} safe-skipped",
        f"{prog.cache_hits} cached",
    ]
    if prog.hybrid_queued:
        anon_parts.append(f"{prog.hybrid_reviewed}/{prog.hybrid_queued} Ollama-reviewed")
    elif prog.ollama_calls:
        anon_parts.append(f"{prog.ollama_calls} Ollama")
    lines.append(f"\nAnonymization: {', '.join(anon_parts)}")
    return "\n".join(lines)


def _format_final(report, prog, elapsed: float) -> str:
    """Format the final sync report."""
    lines = [f"Clean sync complete ({_fmt_duration(elapsed)})\n"]

    field_labels = [
        ("observations_synced", "Observations"),
        ("medications_synced", "Medications"),
        ("wearables_synced", "Wearables"),
        ("demographics_synced", "Demographics"),
        ("hypotheses_synced", "Hypotheses"),
        ("health_context_synced", "Health context"),
        ("workouts_synced", "Workouts"),
        ("genetic_variants_synced", "Genetics"),
        ("health_goals_synced", "Goals"),
        ("med_reminders_synced", "Reminders"),
        ("providers_synced", "Providers"),
        ("appointments_synced", "Appointments"),
        ("health_records_ext_synced", "Extended records"),
    ]
    parts = []
    for attr, label in field_labels:
        val = getattr(report, attr, 0)
        if isinstance(val, bool):
            parts.append(f"{label}: {'yes' if val else 'no'}")
        elif val:
            parts.append(f"{label}: {val}")
    if parts:
        lines.append(" | ".join(parts))

    extras = []
    if report.stale_deleted:
        extras.append(f"Stale removed: {report.stale_deleted}")
    if report.pii_blocked:
        extras.append(f"PII blocked: {report.pii_blocked}")
    if report.errors:
        extras.append(f"Errors: {len(report.errors)}")
    if extras:
        lines.append(" | ".join(extras))

    if prog:
        speed_parts = [
            f"{prog.safe_skipped} safe-skipped",
            f"{prog.cache_hits} cached",
        ]
        if prog.hybrid_queued:
            speed_parts.append(
                f"{prog.hybrid_reviewed}/{prog.hybrid_queued} Ollama-reviewed"
            )
        elif prog.ollama_calls:
            speed_parts.append(f"{prog.ollama_calls} full-pipeline")
        lines.append(f"\nSpeed: {', '.join(speed_parts)}")
    return "\n".join(lines)
