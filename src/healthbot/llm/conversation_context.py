"""Prompt/context building for ClaudeConversationManager.

Builds the (system, prompt) tuple for Claude CLI. Appends health data,
hypotheses, KB findings, research evidence, and user memory to the prompt.
Split from claude_conversation.py to stay under 400 lines per file.
"""
from __future__ import annotations

import json
import logging
import re
import time

logger = logging.getLogger("healthbot")

# Cache TTL for user memory (seconds)
_MEMORY_CACHE_TTL = 60

# Max conversation history entries to include in prompt
_MAX_HISTORY = 20

# Max memory entries to include in prompt
_MAX_MEMORY = 50

# Categories where full wearable daily detail is included
_WEARABLE_DETAIL_CATEGORIES = frozenset({
    "wearable_query", "symptom_report", "doctor_visit", "general",
})
# Categories where full lab table is included
_LABS_DETAIL_CATEGORIES = frozenset({
    "lab_discussion", "medication_change", "symptom_report",
    "doctor_visit", "general",
})


def _measure_parts(parts: list[str]) -> int:
    """Return total character count of parts joined with newlines."""
    return sum(len(p) for p in parts) + max(len(parts) - 1, 0)  # newline joins


def measure_prompt_sections(mgr) -> dict[str, int]:
    """Measure character count per prompt section without sending anything.

    Mirrors build_prompt() logic but collects per-section sizes.
    Uses 'general' category so all detail sections are included.
    Returns dict mapping section label to character count.
    """
    sections: dict[str, int] = {}

    # System prompt
    sections["System prompt"] = len(mgr._context_prompt or "")

    # Patient constants
    parts: list[str] = []
    append_patient_constants(mgr, parts)
    if parts:
        sections["Patient constants"] = _measure_parts(parts)

    # Health data
    parts: list[str] = []
    if mgr._health_sections:
        append_health_sections(mgr, parts, "general health overview")
    elif mgr._health_data:
        parts.append("## HEALTH DATA\n")
        parts.append(mgr._health_data)
        parts.append("")
    if parts:
        sections["Health data"] = _measure_parts(parts)

    # Integration status
    parts = []
    status = ""
    if mgr._status_builder:
        try:
            status = mgr._status_builder()
        except Exception:
            pass
    if not status:
        status = mgr._integration_status
    if status:
        parts.append("## INTEGRATION STATUS\n")
        parts.append(status)
        parts.append("")
    if parts:
        sections["Integration status"] = _measure_parts(parts)

    # Hypotheses
    parts = []
    append_hypotheses(mgr, parts)
    if parts:
        sections["Hypotheses"] = _measure_parts(parts)

    # KB findings
    parts = []
    append_kb_findings(mgr, parts, "general health overview")
    if parts:
        sections["KB findings"] = _measure_parts(parts)

    # Research library
    parts = []
    append_research_evidence(mgr, parts, "general health overview")
    if parts:
        sections["Research library"] = _measure_parts(parts)

    # User memory
    parts = []
    append_user_memory(mgr, parts)
    if parts:
        sections["User memory"] = _measure_parts(parts)

    # Analysis rules
    parts = []
    append_analysis_rules(mgr, parts)
    if parts:
        sections["Analysis rules"] = _measure_parts(parts)

    # Health records ext
    parts = []
    append_health_records_ext(mgr, parts)
    if parts:
        sections["Health records ext"] = _measure_parts(parts)

    # Previous insights (persistent memory)
    if mgr._memory:
        parts = []
        recent_memory = mgr._memory[-_MAX_MEMORY:]
        parts.append("## PREVIOUS INSIGHTS\n")
        for mem in recent_memory:
            cat = mem.get("category", "")
            fact = mem.get("fact", "")
            ts = (mem.get("timestamp") or "")[:10]
            parts.append(f"- [{cat}] {fact} ({ts})")
        parts.append("")
        sections["Previous insights"] = _measure_parts(parts)

    # History
    if mgr._history:
        parts = []
        parts.append("## CONVERSATION HISTORY\n")
        hist = mgr._history[-_MAX_HISTORY * 2:]
        for msg in hist:
            role = "User" if msg["role"] == "user" else "Assistant"
            content = msg["content"]
            if len(content) > 500:
                content = content[:500] + "..."
            parts.append(f"{role}: {content}")
        parts.append("")
        n_msgs = len(hist)
        sections[f"History ({n_msgs} msgs)"] = _measure_parts(parts)

    return sections


# ── Source request detection ────────────────────────────────────

# Short bare keywords — only match if message is short (<40 chars)
_SOURCE_SHORT_RE = re.compile(
    r"^(sources?|citations?|evidence|proof|study|studies|references?)\??!?$",
    re.IGNORECASE,
)

# Explicit longer phrases — match regardless of length
_SOURCE_LONG_RE = re.compile(
    r"^.*(where did you get that|show me the evidence|back that up|"
    r"what stud(?:y|ies) .+|cite your sources?|what's the evidence|"
    r"what is the evidence|how do you know that|"
    r"what are your sources?).*$",
    re.IGNORECASE,
)


def _is_source_request(text: str) -> bool:
    """Detect if user is asking about sources from the previous response.

    Uses length-gated matching: short bare keywords (< 40 chars) like
    "source?" are matched, but longer messages must use explicit citation-
    request phrasing to avoid false positives like "what are the best
    sources of magnesium?"
    """
    stripped = text.strip()
    if len(stripped) < 40 and _SOURCE_SHORT_RE.match(stripped):
        return True
    return bool(_SOURCE_LONG_RE.match(stripped))


def build_prompt(mgr, user_text: str) -> tuple[str, str]:
    """Build (system, prompt) for ClaudeClient.send().

    system = context.md content
    prompt = health_data + memory + history + user message

    Uses query-aware section selection when health sections are
    available: wearable detail is included only for wearable/symptom/
    general queries; full lab table only for lab/symptom/general queries.

    Opens a single CleanDB connection and shares it across sub-methods
    to avoid redundant connection overhead.
    """
    parts: list[str] = []

    # Open a single CleanDB connection for all sub-methods that need it
    from healthbot.llm.conversation_routing import get_clean_db

    shared_clean_db = get_clean_db(mgr)
    try:
        # Patient constants (deterministic facts Claude must not contradict)
        append_patient_constants(mgr, parts, clean_db=shared_clean_db)

        # User memory from Clean DB (preferences near top for maximum effect)
        append_user_memory(mgr, parts)

        # Health data — query-aware if sections available
        if mgr._health_sections:
            append_health_sections(mgr, parts, user_text)
        elif mgr._health_data:
            parts.append("## HEALTH DATA\n")
            parts.append(mgr._health_data)
            parts.append("")

        # Integration status (wearable connections) — prefer live callback
        status = ""
        if mgr._status_builder:
            try:
                status = mgr._status_builder()
            except Exception:
                pass
        if not status:
            status = mgr._integration_status
        if status:
            parts.append("## INTEGRATION STATUS\n")
            parts.append(status)
            parts.append("")

        # Active hypotheses from tracker
        append_hypotheses(mgr, parts)

        # Knowledge base findings relevant to this query
        append_kb_findings(mgr, parts, user_text)

        # Cached research articles (PubMed evidence bridge)
        append_research_evidence(mgr, parts, user_text)

        # Analysis rules from Clean DB
        append_analysis_rules(mgr, parts, clean_db=shared_clean_db)

        # Additional health records from Clean DB
        append_health_records_ext(mgr, parts, clean_db=shared_clean_db)
    finally:
        if shared_clean_db:
            shared_clean_db.close()

    # Substance knowledge profiles relevant to this query
    try:
        from healthbot.llm.conversation_context_ext import (
            append_active_interactions_summary,
            append_medication_timelines,
            append_substance_knowledge,
        )
        append_substance_knowledge(mgr, parts, user_text)
        append_active_interactions_summary(mgr, parts)
        append_medication_timelines(mgr, parts)
    except Exception as exc:
        logger.debug("Substance knowledge lookup failed: %s", exc)

    # Persistent memory
    if mgr._memory:
        recent_memory = mgr._memory[-_MAX_MEMORY:]
        parts.append("## PREVIOUS INSIGHTS\n")
        for mem in recent_memory:
            cat = mem.get("category", "")
            fact = mem.get("fact", "")
            ts = (mem.get("timestamp") or "")[:10]
            parts.append(f"- [{cat}] {fact} ({ts})")
        parts.append("")

    # Conversation history
    if mgr._history:
        parts.append("## CONVERSATION HISTORY\n")
        for msg in mgr._history[-_MAX_HISTORY * 2:]:
            role = "User" if msg["role"] == "user" else "Assistant"
            content = msg["content"]
            if len(content) > 500:
                content = content[:500] + "..."
            parts.append(f"{role}: {content}")
        parts.append("")

    # Inject citation context for source follow-up requests
    if _is_source_request(user_text) and getattr(mgr, "_last_citations", None):
        parts.append("## CITATION CONTEXT (sources from your previous response)\n")
        for cit in mgr._last_citations:
            cit_id = cit.get("id", "?")
            # Strip internal _type key before serializing
            clean = {k: v for k, v in cit.items() if k != "_type"}
            parts.append(f"Source [{cit_id}]: {json.dumps(clean, ensure_ascii=False)}")
        parts.append("")

    # Current message
    parts.append(f"User: {user_text}")

    return mgr._context_prompt, "\n".join(parts)


def append_health_sections(mgr, parts: list[str], user_text: str) -> None:
    """Append health data sections with query-aware selection.

    When the query contains a temporal phrase (e.g. "last month",
    "since January"), labs are filtered to that date range via
    a fresh CleanDB query instead of using the pre-built section.
    """
    from healthbot.nlu.medical_classifier import classify_medical_category

    category = classify_medical_category(user_text)
    s = mgr._health_sections

    # Check for temporal filter
    temporal = _resolve_query_temporal(user_text)

    parts.append("## HEALTH DATA\n")

    # Always include header + demographics
    if s.get("header"):
        parts.append(s["header"])
    if s.get("demographics"):
        parts.append(s["demographics"])

    # Labs: temporal-filtered, full table, or flagged-only summary
    if temporal and temporal.get("start"):
        filtered_labs = _build_temporal_labs(mgr, temporal)
        if filtered_labs:
            parts.append(filtered_labs)
        elif s.get("labs_summary"):
            parts.append(s["labs_summary"])
    elif category in _LABS_DETAIL_CATEGORIES:
        if s.get("labs"):
            parts.append(s["labs"])
    else:
        if s.get("labs_summary"):
            parts.append(s["labs_summary"])

    # Medications: always full
    if s.get("medications"):
        parts.append(s["medications"])

    # Wearable: full detail or compact summary
    if category in _WEARABLE_DETAIL_CATEGORIES:
        if s.get("wearable_detail"):
            parts.append(s["wearable_detail"])
    else:
        if s.get("wearable_summary"):
            parts.append(s["wearable_summary"])
            parts.append("")

    # Always include hypotheses, context, memory, and extended sections
    if s.get("hypotheses"):
        parts.append(s["hypotheses"])
    if s.get("health_context"):
        parts.append(s["health_context"])
    for key in ("workouts", "genetics", "goals", "med_reminders",
                "providers", "appointments"):
        if s.get(key):
            parts.append(s[key])
    if s.get("health_records_ext"):
        parts.append(s["health_records_ext"])
    if s.get("analysis_rules"):
        parts.append(s["analysis_rules"])
    if s.get("user_memory"):
        parts.append(s["user_memory"])

    parts.append("")


def safe_anonymize(anon, text: str) -> str:
    """Anonymize text via pipeline with retry. Returns '[REDACTED]' only as last resort."""
    from healthbot.llm.anonymize_pipeline import AnonymizePipeline

    # Build a minimal but useful fallback that preserves structure context
    # instead of replacing the entire text with an opaque "[REDACTED]".
    text_len = len(text) if text else 0
    fallback = (
        f"[Content redacted for privacy — {text_len} chars, "
        f"contained potentially sensitive data that could not be fully anonymized]"
    )
    pipeline = AnonymizePipeline(
        anon, max_passes=2, fallback="fallback_text",
        fallback_text=fallback,
    )
    result = pipeline.process(text)
    return result.text


def append_hypotheses(mgr, parts: list[str]) -> None:
    """Add active hypotheses to the prompt context.

    All text fields are passed through anonymize() + assert_safe()
    since hypotheses come from raw Tier 1 DB.
    """
    if not mgr._db or not mgr._user_id:
        return
    try:
        hyps = mgr._db.get_active_hypotheses(mgr._user_id)
    except Exception:
        return
    if not hyps:
        return
    anon = mgr._get_anonymizer()
    parts.append("## ACTIVE HYPOTHESES (your diagnostic workup)\n")
    for h in hyps[:15]:
        title = safe_anonymize(anon, h.get("title", "?"))
        conf = h.get("confidence") or h.get("_confidence", "?")
        parts.append(f"- {title} (confidence: {conf})")
        ev_for = h.get("evidence_for")
        if ev_for and isinstance(ev_for, list):
            ev_text = safe_anonymize(anon, ", ".join(ev_for))
            parts.append(f"  Evidence for: {ev_text}")
        ev_against = h.get("evidence_against")
        if ev_against and isinstance(ev_against, list):
            ev_text = safe_anonymize(anon, ", ".join(ev_against))
            parts.append(f"  Against: {ev_text}")
        missing = h.get("missing_tests")
        if missing and isinstance(missing, list):
            missing_text = safe_anonymize(anon, ", ".join(missing))
            parts.append(f"  Missing tests: {missing_text}")
    parts.append("")


def append_kb_findings(mgr, parts: list[str], query: str) -> None:
    """Add relevant knowledge base findings to the prompt context."""
    if not mgr._db:
        return
    try:
        from healthbot.research.knowledge_base import KnowledgeBase
        kb = KnowledgeBase(mgr._db)
        findings = kb.query(topic=query, top_k=15)
        corrections = kb.get_corrections(top_k=5)
    except Exception:
        return
    if not findings and not corrections:
        return
    anon = mgr._get_anonymizer()
    parts.append("## KNOWLEDGE BASE\n")
    if findings:
        parts.append("What you've learned about me so far:")
        for f in findings:
            source = f.get("source", "?")
            finding = safe_anonymize(anon, f.get("finding", ""))
            ts = (f.get("created_at") or "")[:10]
            parts.append(f"- [{source}] {finding} ({ts})")
    if corrections:
        parts.append("\nCorrections (do NOT repeat these mistakes):")
        for c in corrections:
            original = safe_anonymize(anon, c.get("original_claim", ""))
            corrected = safe_anonymize(anon, c.get("correction", ""))
            parts.append(f"- Wrong: {original} → Right: {corrected}")
    parts.append("")


def append_research_evidence(mgr, parts: list[str], query: str) -> None:
    """Add cached PubMed articles to the prompt as a RESEARCH LIBRARY."""
    if not mgr._db:
        return
    try:
        from healthbot.research.external_evidence_store import (
            ExternalEvidenceStore,
        )

        store = ExternalEvidenceStore(mgr._db)
        entries = store.list_evidence(limit=10)
    except Exception:
        return
    # Filter out expired entries
    entries = [e for e in entries if not e.get("expired", False)]
    if not entries:
        return

    anon = mgr._get_anonymizer()
    parts.append("## RESEARCH LIBRARY\n")
    parts.append("Cached PubMed articles relevant to my conditions:")
    for entry in entries:
        ev_id = entry.get("evidence_id", "")
        try:
            detail = store.get_evidence_detail(ev_id)
        except Exception:
            continue
        if not detail:
            continue

        # Extract article metadata from stored result
        result = detail.get("result_json", detail)
        if isinstance(result, str):
            result = {"text": result}
        title = result.get("title", entry.get("query", "Unknown"))
        journal = result.get("journal", "")
        year = result.get("year", "")
        pmid = result.get("pmid", "")
        abstract = result.get("abstract", result.get("text", ""))
        condition = entry.get("query", "")

        # Truncate abstract to save tokens
        if len(abstract) > 300:
            abstract = abstract[:297] + "..."

        title = safe_anonymize(anon, title)
        abstract = safe_anonymize(anon, abstract)
        condition = safe_anonymize(anon, condition)

        line = f"- {title}"
        if journal or year:
            cite_parts = " ".join(p for p in (journal, year) if p)
            line += f" ({cite_parts})"
        if pmid:
            line += f" [PMID:{pmid}]"
        parts.append(line)
        if abstract:
            parts.append(f"  Abstract: {abstract}")
        if condition:
            parts.append(f"  Related to: {condition}")
    parts.append("")


def _apply_confidence_decay(mem: dict) -> float:
    """Apply age-based confidence decay for claude_inferred memories.

    Display-only — does not modify the stored confidence.
    >90 days old: confidence * 0.8
    >180 days old: confidence * 0.6
    """
    conf = mem.get("confidence", 1.0)
    if mem.get("source") != "claude_inferred":
        return conf

    created = mem.get("created_at", "")
    if not created:
        return conf

    try:
        from datetime import UTC, datetime

        dt = datetime.fromisoformat(created)
        # Handle naive timestamps (no timezone info) by assuming UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        age_days = (datetime.now(UTC) - dt).days
        if age_days > 180:
            return conf * 0.6
        if age_days > 90:
            return conf * 0.8
    except Exception:
        pass
    return conf


def append_patient_constants(mgr, parts: list[str], *, clean_db=None) -> None:
    """Prepend verified patient constants that Claude must never contradict.

    Pulls high-confidence (>=0.9) user-stated memories from Clean DB
    and presents them as immutable facts. This prevents hallucinated
    constants (wrong allergies, wrong meds, etc.).
    """
    from healthbot.llm.conversation_routing import get_clean_db

    owns_db = clean_db is None
    if owns_db:
        clean_db = get_clean_db(mgr)
    if not clean_db:
        return
    try:
        facts = clean_db.get_facts()
    except Exception:
        return
    finally:
        if owns_db:
            clean_db.close()
    if not facts:
        return

    parts.append("## PATIENT CONSTANTS (verified — do NOT contradict)\n")
    for key, value in facts.items():
        label = key.replace("_", " ").title()
        parts.append(f"- {label}: {value}")
    parts.append("")


def append_user_memory(mgr, parts: list[str]) -> None:
    """Add user memory from Clean DB to the prompt context.

    Preferences are extracted into their own prominent section so Claude
    applies them to every response. Memories are cached per-session
    with a TTL and invalidated when a MEMORY block writes new data.
    Inferred memories older than 90/180 days show decayed confidence.
    """
    now = time.monotonic()
    cache_expired = (now - mgr._memory_cache_ts) > _MEMORY_CACHE_TTL
    if mgr._cached_user_memory is None or cache_expired:
        clean_db = mgr._get_clean_db()
        if not clean_db:
            return
        try:
            mgr._cached_user_memory = clean_db.get_user_memory() or []
            mgr._memory_cache_ts = now
        except Exception:
            return
        finally:
            clean_db.close()

    memories = mgr._cached_user_memory
    if not memories:
        return

    by_cat: dict[str, list[dict]] = {}
    for mem in memories:
        by_cat.setdefault(mem.get("category", "general"), []).append(mem)

    # Preferences get their own prominent section
    prefs = by_cat.pop("preference", [])
    if prefs:
        parts.append("## COMMUNICATION PREFERENCES (follow these exactly)\n")
        for mem in prefs:
            parts.append(f"  - {mem['key']}: {mem['value']}")
        parts.append("")

    # Response styles get their own section
    styles = by_cat.pop("response_style", [])
    if styles:
        parts.append("## RESPONSE STYLE PATTERNS (follow these for matching query types)\n")
        for mem in styles:
            parts.append(f"  - {mem['key']}: {mem['value']}")
        parts.append("")

    # Rest of user memory
    if by_cat:
        parts.append("## WHAT I KNOW ABOUT YOU\n")
        for cat in sorted(by_cat.keys()):
            parts.append(f"  {cat.replace('_', ' ').title()}:")
            for mem in by_cat[cat]:
                conf = _apply_confidence_decay(mem)
                marker = "" if conf >= 0.9 else f" (~{conf:.0%} confidence)"
                parts.append(f"  - {mem['key']}: {mem['value']}{marker}")
        parts.append("")


def append_analysis_rules(mgr, parts: list[str], *, clean_db=None) -> None:
    """Add active analysis rules from Clean DB to the prompt context."""
    from healthbot.llm.conversation_routing import get_clean_db

    owns_db = clean_db is None
    if owns_db:
        clean_db = get_clean_db(mgr)
    if not clean_db:
        return
    try:
        rules = clean_db.get_active_analysis_rules()
    except Exception:
        return
    finally:
        if owns_db:
            clean_db.close()
    if not rules:
        return

    parts.append("## ACTIVE ANALYSIS RULES\n")
    parts.append("Rules you defined for cross-referencing (apply these proactively):")
    for r in rules:
        priority = r.get("priority", "medium").upper()
        parts.append(f"- [{priority}] {r.get('name', '')} (scope: {r.get('scope', '')})")
        parts.append(f"  {r.get('rule', '')}")
    parts.append("")


def append_health_records_ext(mgr, parts: list[str], *, clean_db=None) -> None:
    """Add extended health records from Clean DB to the prompt context."""
    from healthbot.llm.conversation_routing import get_clean_db

    owns_db = clean_db is None
    if owns_db:
        clean_db = get_clean_db(mgr)
    if not clean_db:
        return
    try:
        records = clean_db.get_health_records_ext()
    except Exception:
        return
    finally:
        if owns_db:
            clean_db.close()
    if not records:
        return

    by_type: dict[str, list[dict]] = {}
    for r in records:
        by_type.setdefault(r.get("data_type", "other"), []).append(r)

    parts.append("## ADDITIONAL HEALTH RECORDS\n")
    for dtype in sorted(by_type.keys()):
        parts.append(f"### {dtype.replace('_', ' ').title()}")
        for r in by_type[dtype]:
            line = f"- {r.get('label', '')}"
            if r.get("value"):
                line += f": {r['value']}"
            if r.get("unit"):
                line += f" {r['unit']}"
            if r.get("date_effective"):
                line += f" ({r['date_effective']})"
            parts.append(line)
    parts.append("")


# ── Temporal query helpers ────────────────────────────────────


def _resolve_query_temporal(user_text: str) -> dict | None:
    """Extract temporal range from user query. Returns None if none found."""
    try:
        from healthbot.nlu.date_parse import resolve_temporal
        return resolve_temporal(user_text)
    except Exception:
        return None


def _build_temporal_labs(mgr, temporal: dict) -> str:
    """Query CleanDB for labs within a temporal range and format as markdown."""
    from healthbot.llm.conversation_routing import get_clean_db

    clean_db = get_clean_db(mgr)
    if not clean_db:
        return ""
    try:
        start = temporal.get("start", "")
        end = temporal.get("end", "")
        labs = clean_db.get_lab_results(start_date=start, end_date=end, limit=100)
    except Exception:
        return ""
    finally:
        clean_db.close()

    if not labs:
        return f"## Lab Results ({start} to {end})\n\nNo lab results in this period.\n"

    parts: list[str] = [f"## Lab Results ({start} to {end}) — {len(labs)} results\n"]
    has_lab = any(lab.get("source_lab") for lab in labs)
    if has_lab:
        parts.append("| Date | Test | Value | Unit | Reference | Flag | Lab |")
        parts.append("|------|------|-------|------|-----------|------|-----|")
    else:
        parts.append("| Date | Test | Value | Unit | Reference | Flag |")
        parts.append("|------|------|-------|------|-----------|------|")
    for lab in labs:
        ref = ""
        if lab.get("reference_low") is not None and lab.get("reference_high") is not None:
            ref = f"{lab['reference_low']}-{lab['reference_high']}"
        elif lab.get("reference_text"):
            ref = lab["reference_text"]
        row = (
            f"| {lab.get('date_effective', '')} "
            f"| {lab.get('test_name') or lab.get('canonical_name', '')} "
            f"| {lab.get('value', '')} "
            f"| {lab.get('unit', '')} "
            f"| {ref} "
            f"| {lab.get('flag', '')} "
        )
        if has_lab:
            row += f"| {lab.get('source_lab', '')} |"
        else:
            row += "|"
        parts.append(row)
    parts.append("")
    return "\n".join(parts)
