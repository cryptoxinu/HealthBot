"""Block routing and memory sync for ClaudeConversationManager.

Routes structured blocks (HYPOTHESIS, MEMORY, CORRECTION, etc.) to the
appropriate subsystems. Handles MEMORY → demographics/LTM sync.
Split from claude_conversation.py to stay under 400 lines per file.
"""
from __future__ import annotations

import logging
import re
import uuid

logger = logging.getLogger("healthbot")

# LTM fact format patterns (MEMORY key → onboarding format)
_LTM_DEMOGRAPHIC_MAP: dict[str, tuple[str, re.Pattern[str]]] = {
    "height":    ("Height: {value}",         re.compile(r"height\s*[:=]", re.IGNORECASE)),
    "height_m":  ("Height: {value}",         re.compile(r"height\s*[:=]", re.IGNORECASE)),
    "weight":    ("Weight: {value}",         re.compile(r"weight\s*[:=]", re.IGNORECASE)),
    "weight_kg": ("Weight: {value}",         re.compile(r"weight\s*[:=]", re.IGNORECASE)),
    "age":       ("Age: {value}",            re.compile(r"age\s*[:=]", re.IGNORECASE)),
    "sex": (
        "Biological sex: {value}",
        re.compile(r"(biological\s*)?sex\s*[:=]", re.IGNORECASE),
    ),
    "ethnicity": ("Ethnicity: {value}",      re.compile(r"ethnicity\s*[:=]", re.IGNORECASE)),
    "nickname":  ("Nickname: {value}",       re.compile(r"nickname\s*[:=]", re.IGNORECASE)),
}

# MEMORY category → LTM category mapping for non-demographic keys
_MEMORY_CATEGORY_TO_LTM: dict[str, str] = {
    "medical_context": "condition",
    "supplement": "medication",
    "lifestyle": "lifestyle",
    "preference": "preference",
    "goal": "goal",
    "demographic": "demographic",
    "general": "user_memory",
}


def get_clean_db(mgr):
    """Create a fresh CleanDB connection.

    Callers must close after use. Returns None if unavailable.
    """
    if not mgr._clean_db_available or not mgr._km:
        return None
    try:
        from healthbot.data.clean_db import CleanDB

        path = mgr._config.clean_db_path
        if not path.exists():
            return None
        clean = CleanDB(path, phi_firewall=mgr._fw)
        clean.open(clean_key=mgr._km.get_clean_key())
        return clean
    except Exception as e:
        logger.warning("CleanDB unavailable in conversation: %s", e)
        return None


def route_block(mgr, block_type: str, block: dict) -> None:
    """Route a structured block to the appropriate subsystem."""
    if block_type == "HYPOTHESIS" and mgr._db and block.get("title"):
        from healthbot.reasoning.hypothesis_tracker import HypothesisTracker
        tracker = HypothesisTracker(mgr._db)
        tracker.upsert_hypothesis(mgr._user_id, {
            "title": block["title"],
            "confidence": block.get("confidence", 0.5),
            "evidence_for": block.get("evidence_for", []),
            "evidence_against": block.get("evidence_against", []),
            "missing_tests": block.get("missing_tests", []),
        })

    elif block_type == "RESEARCH" and mgr._db and block.get("finding"):
        from healthbot.research.knowledge_base import KnowledgeBase
        kb = KnowledgeBase(mgr._db)
        kb.store_finding(
            topic=block.get("topic", "general"),
            finding=block["finding"],
            source=block.get("source", "claude_research"),
            relevance_score=0.9,
        )

    elif block_type == "INSIGHT" and mgr._db and block.get("fact"):
        from healthbot.research.knowledge_base import KnowledgeBase
        kb = KnowledgeBase(mgr._db)
        kb.store_finding(
            topic=block.get("category", "general"),
            finding=block["fact"],
            source="claude_insight",
            relevance_score=0.8,
        )

    elif block_type == "CONDITION" and mgr._db and block.get("name"):
        from healthbot.research.knowledge_base import KnowledgeBase
        kb = KnowledgeBase(mgr._db)
        kb.store_finding(
            topic=block["name"],
            finding=(
                f"Status: {block.get('status', 'unknown')}. "
                f"Evidence: {block.get('evidence', '')}"
            ),
            source="claude_diagnosis",
            relevance_score=1.0,
        )

    elif block_type == "ACTION" and mgr._db:
        test = block.get("test", "").strip()
        reason = block.get("reason", "").strip()
        if not test and reason:
            test = reason[:80]
            logger.info("ACTION block missing 'test', using reason: %s", test)
        if not test:
            logger.warning(
                "ACTION block dropped — no test or reason: %s",
                str(block)[:100],
            )
            return
        from healthbot.research.knowledge_base import KnowledgeBase
        kb = KnowledgeBase(mgr._db)
        kb.store_finding(
            topic=f"action:{test}",
            finding=(
                f"{reason} "
                f"(urgency: {block.get('urgency', 'routine')})"
            ),
            source="claude_action",
            relevance_score=0.9,
        )

    elif block_type == "MEMORY" and block.get("key") and block.get("value"):
        handle_memory_block(mgr, block)

    elif block_type == "CORRECTION" and block.get("correction"):
        # Tier 1 KB (for prompt context in _append_kb_findings)
        if mgr._db:
            from healthbot.research.knowledge_base import KnowledgeBase
            kb = KnowledgeBase(mgr._db)
            kb.store_correction(
                original_claim=block.get("original_claim", ""),
                correction=block["correction"],
                source=block.get("source", "user"),
            )
        # Tier 2 Clean DB (for /memory corrections command)
        clean_db = mgr._get_clean_db()
        if clean_db:
            try:
                clean_db.insert_correction(
                    correction_id=uuid.uuid4().hex,
                    original_claim=block.get("original_claim", ""),
                    correction=block["correction"],
                    source=block.get("source", "user"),
                )
            finally:
                clean_db.close()

    elif block_type == "SYSTEM_IMPROVEMENT" and block.get("suggestion"):
        handle_system_improvement(mgr, block)

    elif block_type == "HEALTH_DATA" and block.get("type"):
        handle_health_data_block(mgr, block)

    elif block_type == "ANALYSIS_RULE" and block.get("name") and block.get("rule"):
        handle_analysis_rule_block(mgr, block)

    elif block_type == "DATA_QUALITY" and mgr._db:
        handle_data_quality(mgr, block)


def handle_memory_block(mgr, block: dict) -> str | None:
    """Route a MEMORY block to CleanDB user memory.

    Also updates clean_demographics when demographic keys are detected
    (height, weight, age, sex) so /aboutme reflects the latest values.

    Returns a feedback string describing what happened, or None on failure.
    """
    clean_db = mgr._get_clean_db()
    if not clean_db:
        return None
    try:
        key = block["key"].strip().lower().replace(" ", "_")
        value = block["value"]
        category = block.get("category", "general")
        if mgr._fw.contains_phi(value):
            logger.warning("MEMORY block contains PHI, blocked: %s", key)
            return f"[Could not remember '{key}' — contains sensitive data]"

        # Contradiction detection: check existing value before upsert
        old_value = None
        try:
            existing = clean_db.get_user_memory()
            for mem in (existing or []):
                if mem.get("key") == key:
                    old_value = mem.get("value")
                    break
        except Exception:
            pass

        supersedes = block.get("supersedes", "")
        if supersedes:
            clean_db.mark_memory_superseded(supersedes.strip().lower(), key)
        clean_db.upsert_user_memory(
            key=key,
            value=value,
            category=category,
            confidence=block.get("confidence", 1.0),
            source=block.get("source") or (
                "user_stated" if block.get("confidence", 1.0) >= 1.0
                else "claude_inferred"
            ),
        )
        # Audit log: record what changed
        try:
            clean_db.log_memory_change(
                key=key,
                old_value=old_value or "",
                new_value=value,
                source_type=block.get("source") or (
                    "user_stated" if block.get("confidence", 1.0) >= 1.0
                    else "claude_inferred"
                ),
                source_ref=supersedes,
            )
        except Exception as exc:
            logger.debug("Audit log write failed: %s", exc)

        # Update clean_demographics for demographic keys
        sync_memory_to_demographics(mgr, clean_db, key, value)
        # Update Raw Vault LTM so /aboutme reads the latest value
        sync_memory_to_ltm(mgr, key, value, category=category)

        # Medication-specific: sync to clean_medications for temporal tracking
        if category in ("medication", "supplement"):
            _sync_medication_memory(clean_db, key, value)

        # Build feedback
        if old_value and old_value != value:
            feedback = f"Updated: {key} (was: {old_value})"
            logger.info(
                "MEMORY contradiction resolved: %s changed from %r to %r",
                key, old_value, value,
            )
        else:
            feedback = f"Remembered: {key}"
        mgr.invalidate_memory_cache()
        return feedback
    except Exception as exc:
        logger.warning("Failed to store MEMORY block '%s': %s", block.get("key", "?"), exc)
        return f"[Failed to remember '{block.get('key', '?')}': {exc}]"
    finally:
        clean_db.close()


def sync_memory_to_demographics(mgr, clean_db, key: str, value: str) -> None:
    """If a MEMORY key is a demographic field, update clean_demographics."""
    demo_update: dict = {}
    key_lower = key.lower()

    if key_lower in ("height", "height_m"):
        m = re.match(r"(\d+)\s*(?:feet|ft|')['\s]*(\d+)?", value, re.IGNORECASE)
        if m:
            feet = int(m.group(1))
            inches = int(m.group(2)) if m.group(2) else 0
            demo_update["height_m"] = round((feet * 12 + inches) * 0.0254, 4)
        else:
            m = re.match(r"([\d.]+)\s*(?:m|meters?)", value, re.IGNORECASE)
            if m:
                demo_update["height_m"] = float(m.group(1))

    elif key_lower in ("weight", "weight_kg"):
        m = re.match(r"([\d.]+)\s*(?:lbs?|pounds?)", value, re.IGNORECASE)
        if m:
            demo_update["weight_kg"] = round(float(m.group(1)) / 2.20462, 2)
        else:
            m = re.match(r"([\d.]+)\s*(?:kg|kilos?)", value, re.IGNORECASE)
            if m:
                demo_update["weight_kg"] = float(m.group(1))
            else:
                # Bare number without units — assume lbs (US default)
                m = re.match(r"([\d.]+)", value)
                if m:
                    demo_update["weight_kg"] = round(float(m.group(1)) / 2.20462, 2)

    elif key_lower == "age":
        m = re.match(r"(\d+)", value)
        if m:
            demo_update["age"] = int(m.group(1))

    elif key_lower == "sex":
        demo_update["sex"] = value.strip().lower()

    if demo_update:
        try:
            user_id = mgr._user_id or 0
            clean_db.upsert_demographics(user_id, **demo_update)
            logger.info("Updated demographics from MEMORY block: %s", demo_update)
        except Exception as exc:
            logger.warning("Failed to sync memory to demographics: %s", exc)


def _sync_medication_memory(clean_db, key: str, value: str) -> None:
    """Sync medication MEMORY blocks to clean_medications for temporal tracking.

    Parses the value for dose, timing, and started/stopped intents.
    Updates start_date on new medications so week number tracking works.
    """
    import re as _re
    from datetime import UTC as _UTC
    from datetime import datetime as _dt

    value_lower = value.lower()
    med_name = key.replace("_", " ").strip()

    # Detect intent
    stopped = any(w in value_lower for w in ("stopped", "discontinued", "quit", "no longer"))
    started = any(w in value_lower for w in ("started", "began", "beginning", "starting"))

    # Extract dose if present (e.g., "5mg", "1.5 mg/week", "200mg daily")
    dose_match = _re.search(
        r"(\d+(?:\.\d+)?)\s*(mg|mcg|iu|ml|g|units?)(?:\s*/\s*(?:day|week|month))?",
        value_lower,
    )
    dose = dose_match.group(0) if dose_match else ""

    try:
        meds = clean_db.get_medications(status="all")
        existing = None
        for m in meds:
            if m.get("name", "").lower() == med_name.lower():
                existing = m
                break

        now = _dt.now(_UTC).strftime("%Y-%m-%d")

        if stopped and existing:
            # Mark as discontinued
            clean_db.conn.execute(
                "UPDATE clean_medications SET status = ?, end_date = ? WHERE med_id = ?",
                ("discontinued", now, existing["med_id"]),
            )
            clean_db.conn.commit()
            logger.info("Medication stopped via MEMORY: %s", med_name)
        elif started or not existing:
            # New medication or explicit start
            if existing:
                # Update existing — set dose and ensure start_date
                updates = []
                params = []
                if dose:
                    updates.append("dose = ?")
                    params.append(dose)
                if not existing.get("start_date"):
                    updates.append("start_date = ?")
                    params.append(now)
                updates.append("status = ?")
                params.append("active")
                if updates:
                    params.append(existing["med_id"])
                    clean_db.conn.execute(
                        f"UPDATE clean_medications SET {', '.join(updates)} WHERE med_id = ?",
                        params,
                    )
                    clean_db.conn.commit()
            else:
                # Insert new medication
                import uuid
                med_id = uuid.uuid4().hex
                clean_db.conn.execute(
                    """INSERT INTO clean_medications
                       (med_id, name, dose, unit, frequency, status, start_date, synced_at)
                       VALUES (?, ?, ?, '', '', 'active', ?, ?)""",
                    (med_id, med_name, dose, now, now),
                )
                clean_db.conn.commit()
                logger.info("New medication via MEMORY: %s", med_name)
        elif dose and existing:
            # Dose update on existing med
            clean_db.conn.execute(
                "UPDATE clean_medications SET dose = ? WHERE med_id = ?",
                (dose, existing["med_id"]),
            )
            clean_db.conn.commit()
            logger.info("Medication dose updated via MEMORY: %s → %s", med_name, dose)
    except Exception as exc:
        logger.debug("_sync_medication_memory failed: %s", exc)


def sync_memory_to_ltm(
    mgr, key: str, value: str, category: str = "demographic",
) -> None:
    """Update Raw Vault LTM fact to match a MEMORY block.

    Handles both demographic keys (with format patterns) and general
    MEMORY keys (stored as "Key Title: value" in the appropriate LTM category).
    """
    user_id = mgr._user_id or 0
    db = mgr._db
    if db is None:
        return

    # Demographic keys use format patterns
    entry = _LTM_DEMOGRAPHIC_MAP.get(key.lower())
    if entry:
        fmt, pattern = entry
        new_fact = fmt.format(value=value)
        try:
            facts = db.get_ltm_by_category(user_id, "demographic")
            for fact in facts:
                if pattern.search(fact.get("fact", "")):
                    db.update_ltm(fact["_id"], new_fact)
                    logger.info("Updated LTM demographic fact %s → %r", fact["_id"], new_fact)
                    return
            # No existing fact found — create one
            fact_id = db.insert_ltm(user_id, "demographic", new_fact, source="claude_memory")
            logger.info("Inserted new LTM demographic fact %s → %r", fact_id, new_fact)
        except Exception as exc:
            logger.warning("Failed to sync memory to LTM: %s", exc)
        return

    # Non-demographic keys: store in appropriate LTM category
    ltm_category = _MEMORY_CATEGORY_TO_LTM.get(category, "user_memory")
    key_title = key.replace("_", " ").title()
    new_fact = f"{key_title}: {value}"
    # Build a pattern to find existing LTM entries with the same key prefix
    key_pattern = re.compile(rf"^{re.escape(key_title)}\s*:", re.IGNORECASE)

    try:
        facts = db.get_ltm_by_category(user_id, ltm_category)
        for fact in facts:
            if key_pattern.search(fact.get("fact", "")):
                db.update_ltm(fact["_id"], new_fact)
                logger.info("Updated LTM %s fact %s → %r", ltm_category, fact["_id"], new_fact)
                return
        # No existing fact — create one
        fact_id = db.insert_ltm(user_id, ltm_category, new_fact, source="claude_memory")
        logger.info("Inserted new LTM %s fact %s → %r", ltm_category, fact_id, new_fact)
    except Exception as exc:
        logger.warning("Failed to sync memory to LTM (%s): %s", ltm_category, exc)


def reconcile_demographics_to_ltm(mgr) -> None:
    """Push clean_demographics values into LTM.

    Fixes stale onboarding LTM facts when clean_demographics was
    updated (via MEMORY blocks) but LTM was not.
    """
    clean_db = mgr._get_clean_db()
    if not clean_db:
        return
    try:
        cd = clean_db.get_demographics(mgr._user_id)
        if not cd:
            return
        pairs: list[tuple[str, str]] = []
        if cd.get("height_m"):
            total_in = cd["height_m"] / 0.0254
            feet = int(total_in // 12)
            inches = int(total_in % 12 + 0.5)
            pairs.append(("height", f"{feet}'{inches}\""))
        if cd.get("weight_kg"):
            lbs = int(cd["weight_kg"] * 2.205 + 0.5)
            pairs.append(("weight", f"{lbs} lbs"))
        if cd.get("age"):
            pairs.append(("age", str(cd["age"])))
        if cd.get("sex"):
            pairs.append(("sex", cd["sex"]))
        if cd.get("ethnicity"):
            pairs.append(("ethnicity", cd["ethnicity"]))
        for key, value in pairs:
            sync_memory_to_ltm(mgr, key, value)
    except Exception as exc:
        logger.warning("Demographics→LTM reconciliation failed: %s", exc)
    finally:
        clean_db.close()


def handle_health_data_block(mgr, block: dict) -> None:
    """Route a HEALTH_DATA block to raw vault + clean DB."""
    data_type = block.get("type", "other")
    label = block.get("label", "")
    if not label:
        return

    # Store in raw vault
    record_id = uuid.uuid4().hex
    if mgr._db:
        try:
            record_id = mgr._db.insert_health_record_ext(
                user_id=mgr._user_id or 0,
                data_type=data_type,
                label=label,
                data_dict={
                    "value": block.get("value", ""),
                    "unit": block.get("unit", ""),
                    "date": block.get("date", ""),
                    "source": block.get("source", "claude_conversation"),
                    "details": block.get("details", {}),
                    "tags": block.get("tags", []),
                    "label": label,
                },
            )
        except Exception as exc:
            logger.warning("Failed to store HEALTH_DATA in raw vault: %s", exc)

    # Store in clean DB (anonymized) — reuse raw vault ID for consistency
    clean_db = get_clean_db(mgr)
    if not clean_db:
        return
    try:
        import json as _json
        details = block.get("details", {})
        if isinstance(details, dict):
            details = _json.dumps(details)
        tags = block.get("tags", [])
        if isinstance(tags, list):
            tags = ",".join(str(t) for t in tags)

        clean_db.upsert_health_record_ext(
            record_id=record_id,
            data_type=data_type,
            label=label,
            value=str(block.get("value", "")),
            unit=str(block.get("unit", "")),
            date_effective=str(block.get("date", "")),
            source=str(block.get("source", "claude_conversation")),
            details=str(details),
            tags=str(tags),
        )
    except Exception as exc:
        logger.warning("Failed to store HEALTH_DATA in clean DB: %s", exc)
    finally:
        clean_db.close()


def handle_analysis_rule_block(mgr, block: dict) -> None:
    """Route an ANALYSIS_RULE block to clean DB."""
    clean_db = get_clean_db(mgr)
    if not clean_db:
        return
    try:
        name = block["name"]
        rule_text = block["rule"]
        if mgr._fw.contains_phi(rule_text) or mgr._fw.contains_phi(name):
            logger.warning("ANALYSIS_RULE block contains PHI, blocked: %s", name)
            return

        # Deactivate superseded rule if specified
        supersedes = block.get("supersedes", "")
        if supersedes:
            clean_db.deactivate_analysis_rule(supersedes)

        clean_db.upsert_analysis_rule(
            name=name,
            scope=block.get("scope", ""),
            rule=rule_text,
            priority=block.get("priority", "medium"),
            active=block.get("active", True),
        )
    except Exception as exc:
        logger.warning("Failed to store ANALYSIS_RULE: %s", exc)
    finally:
        clean_db.close()


def handle_system_improvement(mgr, block: dict) -> None:
    """Route a SYSTEM_IMPROVEMENT block to CleanDB."""
    clean_db = mgr._get_clean_db()
    if not clean_db:
        return
    imp_id = None
    try:
        suggestion = block["suggestion"]
        if mgr._fw.contains_phi(suggestion):
            logger.warning("SYSTEM_IMPROVEMENT block contains PHI, blocked")
            return
        imp_id = clean_db.insert_system_improvement(
            area=block.get("area", ""),
            suggestion=suggestion,
            priority=block.get("priority", "low"),
        )
    finally:
        clean_db.close()
    # Fire notification callback (Telegram push with inline buttons)
    if mgr._on_system_improvement and imp_id:
        try:
            mgr._on_system_improvement({
                "id": imp_id,
                "area": block.get("area", ""),
                "suggestion": suggestion,
                "priority": block.get("priority", "low"),
            })
        except Exception as exc:
            logger.warning("System improvement callback failed: %s", exc)


def handle_data_quality(mgr, block: dict) -> None:
    """Process a DATA_QUALITY block by triggering re-extraction."""
    from healthbot.reasoning.feedback_loop import FeedbackLoop

    vault = mgr._get_vault()
    loop = FeedbackLoop(db=mgr._db, vault=vault)
    result = loop.handle_quality_issue(
        user_id=mgr._user_id,
        issue_type=block.get("issue", "unknown"),
        test_name=block.get("test", ""),
        details=block.get("details", ""),
        page=block.get("page"),
    )
    mgr._pending_quality_notifications.append({
        "test": block.get("test", "unknown"),
        "issue": block.get("issue", "unknown"),
        **result,
    })


def format_quality_notifications(mgr) -> str:
    """Format pending quality notifications as user-facing text."""
    if not mgr._pending_quality_notifications:
        return ""

    parts: list[str] = []
    for notif in mgr._pending_quality_notifications:
        test = notif.get("test", "unknown")
        if notif.get("rescan_attempted") and notif.get("rescan_count", 0) > 0:
            count = notif["rescan_count"]
            parts.append(
                f"Re-scanned for {test}: found {count} additional "
                f"result(s). Run /labs to see updated data.",
            )
        elif notif.get("rescan_attempted"):
            parts.append(
                f"Re-scanned for {test}: no new results found. "
                f"The data may already be complete.",
            )
        else:
            parts.append(
                f"Noted: {test} data may be incomplete. "
                f"Consider re-uploading the lab PDF.",
            )

    mgr._pending_quality_notifications.clear()
    return "\n".join(parts)
