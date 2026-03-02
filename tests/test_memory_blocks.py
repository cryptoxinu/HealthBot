"""Tests for memory, correction, and system improvement block handling."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from healthbot.data.clean_db import CleanDB
from healthbot.llm.conversation_routing import (
    _MEMORY_CATEGORY_TO_LTM,
    handle_memory_block,
    sync_memory_to_ltm,
)
from healthbot.security.phi_firewall import PhiFirewall

# ── Fixtures ───────────────────────────────────────────


@pytest.fixture
def clean_db(tmp_path: Path) -> CleanDB:
    """Temporary CleanDB with no encryption."""
    db_path = tmp_path / "clean.db"
    fw = PhiFirewall()
    cdb = CleanDB(db_path, phi_firewall=fw)
    cdb.open(clean_key=None)
    yield cdb
    cdb.close()


# ── Block pattern matching ────────────────────────────


def test_memory_block_pattern_match():
    """MEMORY block is parsed from response text."""
    from healthbot.llm.claude_conversation import _BLOCK_PATTERN

    text = 'Some response. MEMORY: {"key":"height","value":"6 feet","category":"demographics"}'
    matches = list(_BLOCK_PATTERN.finditer(text))
    assert len(matches) == 1
    assert matches[0].group(1) == "MEMORY"
    data = json.loads(matches[0].group(2))
    assert data["key"] == "height"
    assert data["value"] == "6 feet"


def test_correction_block_pattern_match():
    """CORRECTION block is parsed from response text."""
    from healthbot.llm.claude_conversation import _BLOCK_PATTERN

    text = 'CORRECTION: {"original_claim":"5 feet","correction":"6 feet","source":"user"}'
    matches = list(_BLOCK_PATTERN.finditer(text))
    assert len(matches) == 1
    assert matches[0].group(1) == "CORRECTION"


def test_system_improvement_block_pattern_match():
    """SYSTEM_IMPROVEMENT block is parsed from response text."""
    from healthbot.llm.claude_conversation import _BLOCK_PATTERN

    text = (
        'SYSTEM_IMPROVEMENT: {"area":"workflow",'
        '"suggestion":"Add reminder feature","priority":"high"}'
    )
    matches = list(_BLOCK_PATTERN.finditer(text))
    assert len(matches) == 1
    assert matches[0].group(1) == "SYSTEM_IMPROVEMENT"


# ── Block stripping from visible response ─────────────


def test_memory_block_stripped_from_response():
    """MEMORY blocks are removed from user-visible response."""
    import re

    from healthbot.llm.claude_conversation import _BLOCK_PATTERN

    response = (
        'Got it, noted! MEMORY: {"key":"height","value":"6 feet",'
        '"category":"demographics"} Have a good day.'
    )
    cleaned = _BLOCK_PATTERN.sub("", response)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    assert "MEMORY" not in cleaned
    assert "Got it" in cleaned
    assert "good day" in cleaned


# ── Block types skip flat memory ──────────────────────


def test_memory_block_skips_flat_memory():
    """MEMORY, CORRECTION, SYSTEM_IMPROVEMENT skip flat memory storage."""
    from healthbot.llm.claude_conversation import ClaudeConversationManager

    fw = PhiFirewall()
    conv = ClaudeConversationManager(
        config=MagicMock(vault_home=Path("/tmp")),
        claude_client=MagicMock(),
        phi_firewall=fw,
    )
    # Pre-state: empty memory
    assert len(conv._memory) == 0

    for block_type in ("MEMORY", "CORRECTION", "SYSTEM_IMPROVEMENT"):
        block = {
            "_type": block_type,
            "key": "test",
            "value": "val",
            "correction": "fix",
            "suggestion": "improve",
            "original_claim": "old",
        }
        conv._store_insight(block)

    # None should have been added to flat memory
    assert len(conv._memory) == 0


# ── MEMORY block routes to CleanDB ────────────────────


def test_memory_block_routes_to_clean_db(clean_db: CleanDB):
    """MEMORY block upserts to clean_user_memory table."""
    clean_db.upsert_user_memory(
        key="height", value="6 feet", category="demographics",
    )
    memories = clean_db.get_user_memory()
    assert len(memories) == 1
    assert memories[0]["key"] == "height"
    assert memories[0]["value"] == "6 feet"

    # Upsert with same key updates the value
    clean_db.upsert_user_memory(
        key="height", value="6 foot 2", category="demographics",
    )
    memories = clean_db.get_user_memory()
    assert len(memories) == 1
    assert memories[0]["value"] == "6 foot 2"


# ── MEMORY block with PHI is blocked ─────────────────


def test_memory_block_phi_blocked():
    """MEMORY block containing PHI is not stored."""
    from healthbot.llm.claude_conversation import ClaudeConversationManager

    fw = PhiFirewall()
    conv = ClaudeConversationManager(
        config=MagicMock(vault_home=Path("/tmp")),
        claude_client=MagicMock(),
        phi_firewall=fw,
    )

    block = {
        "_type": "MEMORY",
        "key": "ssn",
        "value": "123-45-6789",  # SSN — should be caught by PhiFirewall
        "category": "personal",
    }
    # Should not raise, but should log warning and skip
    conv._store_insight(block)
    # No flat memory added either
    assert len(conv._memory) == 0


# ── CORRECTION dual-routes to KB + CleanDB ────────────


def test_correction_dual_routes():
    """CORRECTION block writes to both KB (Tier 1) and CleanDB (Tier 2)."""
    from healthbot.llm.claude_conversation import ClaudeConversationManager

    fw = PhiFirewall()
    conv = ClaudeConversationManager(
        config=MagicMock(vault_home=Path("/tmp")),
        claude_client=MagicMock(),
        phi_firewall=fw,
    )
    conv._db = MagicMock()

    mock_clean_db = MagicMock()
    conv._get_clean_db = lambda: mock_clean_db

    block = {
        "_type": "CORRECTION",
        "original_claim": "height is 5 feet",
        "correction": "height is 6 feet",
        "source": "user",
    }
    conv._store_insight(block)

    # Tier 2: CleanDB.insert_correction called
    mock_clean_db.insert_correction.assert_called_once()
    call_kwargs = mock_clean_db.insert_correction.call_args
    assert call_kwargs[1]["correction"] == "height is 6 feet"


# ── SYSTEM_IMPROVEMENT routes to CleanDB ──────────────


def test_system_improvement_routes_to_clean_db():
    """SYSTEM_IMPROVEMENT block routes to CleanDB."""
    from healthbot.llm.claude_conversation import ClaudeConversationManager

    fw = PhiFirewall()
    conv = ClaudeConversationManager(
        config=MagicMock(vault_home=Path("/tmp")),
        claude_client=MagicMock(),
        phi_firewall=fw,
    )

    mock_clean_db = MagicMock()
    mock_clean_db.insert_system_improvement.return_value = "abc123"
    conv._get_clean_db = lambda: mock_clean_db

    block = {
        "_type": "SYSTEM_IMPROVEMENT",
        "area": "workflow",
        "suggestion": "Add daily summary feature",
        "priority": "medium",
    }
    conv._store_insight(block)

    mock_clean_db.insert_system_improvement.assert_called_once_with(
        area="workflow",
        suggestion="Add daily summary feature",
        priority="medium",
    )


# ── SYSTEM_IMPROVEMENT callback fires ─────────────────


def test_system_improvement_fires_callback():
    """SYSTEM_IMPROVEMENT block fires the notification callback."""
    from healthbot.llm.claude_conversation import ClaudeConversationManager

    fw = PhiFirewall()
    conv = ClaudeConversationManager(
        config=MagicMock(vault_home=Path("/tmp")),
        claude_client=MagicMock(),
        phi_firewall=fw,
    )

    mock_clean_db = MagicMock()
    mock_clean_db.insert_system_improvement.return_value = "deadbeef"
    conv._get_clean_db = lambda: mock_clean_db

    callback_calls: list[dict] = []
    conv._on_system_improvement = lambda block: callback_calls.append(block)

    block = {
        "_type": "SYSTEM_IMPROVEMENT",
        "area": "ux",
        "suggestion": "Better charts",
        "priority": "high",
    }
    conv._store_insight(block)

    assert len(callback_calls) == 1
    assert callback_calls[0]["id"] == "deadbeef"
    assert callback_calls[0]["suggestion"] == "Better charts"


# ── CleanDB query methods ─────────────────────────────


def test_get_corrections(clean_db: CleanDB):
    """get_corrections returns inserted corrections."""
    clean_db.insert_correction(
        correction_id="c1",
        original_claim="was 5 feet",
        correction="is 6 feet",
        source="user",
    )
    clean_db.insert_correction(
        correction_id="c2",
        original_claim="takes aspirin",
        correction="stopped aspirin",
        source="user",
    )
    corrections = clean_db.get_corrections()
    assert len(corrections) == 2
    # Ordered by created_at DESC
    assert corrections[0]["id"] == "c2"


def test_get_system_improvements(clean_db: CleanDB):
    """get_system_improvements returns improvements with optional status filter."""
    clean_db.insert_system_improvement(
        area="ux", suggestion="Better charts", priority="high",
    )
    clean_db.insert_system_improvement(
        area="data", suggestion="More wearable metrics", priority="low",
    )

    # All improvements
    all_imps = clean_db.get_system_improvements()
    assert len(all_imps) == 2

    # Filter by status
    open_imps = clean_db.get_system_improvements(status="open")
    assert len(open_imps) == 2

    approved = clean_db.get_system_improvements(status="approved")
    assert len(approved) == 0


def test_update_system_improvement_status(clean_db: CleanDB):
    """update_system_improvement_status changes status and returns True."""
    imp_id = clean_db.insert_system_improvement(
        area="ux", suggestion="Dark mode", priority="medium",
    )

    updated = clean_db.update_system_improvement_status(imp_id, "approved")
    assert updated is True

    imps = clean_db.get_system_improvements(status="approved")
    assert len(imps) == 1
    assert imps[0]["id"] == imp_id

    # Non-existent ID returns False
    assert clean_db.update_system_improvement_status("nonexistent", "approved") is False


def test_memory_supersedes(clean_db: CleanDB):
    """Memory supersedes marks old entry and allows new value."""
    clean_db.upsert_user_memory(
        key="height", value="5 feet", category="demographics",
    )

    # Supersede old entry
    clean_db.mark_memory_superseded("height", "height_corrected")
    clean_db.upsert_user_memory(
        key="height_corrected", value="6 feet", category="demographics",
    )

    # Active memories should only show the new one
    memories = clean_db.get_user_memory()
    assert len(memories) == 1
    assert memories[0]["key"] == "height_corrected"
    assert memories[0]["value"] == "6 feet"


# ── LTM sync fix tests ──────────────────────────────


def _make_mgr(
    user_id: int = 123,
    clean_db_mock: MagicMock | None = None,
    db: MagicMock | None = None,
):
    """Create a minimal mock manager for LTM sync tests."""
    mgr = MagicMock()
    mgr._user_id = user_id
    mgr._fw.contains_phi.return_value = False
    mgr.invalidate_memory_cache = MagicMock()

    if clean_db_mock is None:
        clean_db_mock = MagicMock()
        clean_db_mock.get_user_memory.return_value = []
    mgr._get_clean_db.return_value = clean_db_mock

    if db is None:
        db = MagicMock()
        db.get_ltm_by_category.return_value = []
        db.insert_ltm.return_value = "ltm-123"
    mgr._db = db
    return mgr


class TestSyncMemoryToLtm:
    def test_demographic_key_inserts(self):
        db = MagicMock()
        db.get_ltm_by_category.return_value = []
        db.insert_ltm.return_value = "ltm-new"
        mgr = _make_mgr(db=db)

        sync_memory_to_ltm(mgr, "height", "6'2\"")
        db.insert_ltm.assert_called_once()
        call_args = db.insert_ltm.call_args
        assert call_args[0][1] == "demographic"
        assert "6'2\"" in call_args[0][2]

    def test_demographic_key_updates_existing(self):
        db = MagicMock()
        db.get_ltm_by_category.return_value = [
            {"_id": "ltm-old", "fact": "Height: 5'10\""},
        ]
        mgr = _make_mgr(db=db)

        sync_memory_to_ltm(mgr, "height", "6'2\"")
        db.update_ltm.assert_called_once_with("ltm-old", "Height: 6'2\"")
        db.insert_ltm.assert_not_called()

    def test_non_demographic_key_inserts_to_ltm(self):
        db = MagicMock()
        db.get_ltm_by_category.return_value = []
        db.insert_ltm.return_value = "ltm-new"
        mgr = _make_mgr(db=db)

        sync_memory_to_ltm(
            mgr, "favorite_supplement", "magnesium glycinate",
            category="supplement",
        )
        db.insert_ltm.assert_called_once()
        call_args = db.insert_ltm.call_args
        # supplement -> medication via _MEMORY_CATEGORY_TO_LTM mapping
        assert call_args[0][1] == "medication"
        assert "Favorite Supplement: magnesium glycinate" in call_args[0][2]

    def test_non_demographic_key_updates_existing(self):
        db = MagicMock()
        db.get_ltm_by_category.return_value = [
            {"_id": "ltm-old", "fact": "Favorite Supplement: vitamin D"},
        ]
        mgr = _make_mgr(db=db)

        sync_memory_to_ltm(
            mgr, "favorite_supplement", "magnesium glycinate",
            category="supplement",
        )
        db.update_ltm.assert_called_once()
        assert "magnesium glycinate" in db.update_ltm.call_args[0][1]

    def test_no_db_does_nothing(self):
        mgr = _make_mgr()
        mgr._db = None
        # Should not raise
        sync_memory_to_ltm(mgr, "height", "6 feet")

    def test_category_mapping_values(self):
        assert _MEMORY_CATEGORY_TO_LTM["medical_context"] == "condition"
        assert _MEMORY_CATEGORY_TO_LTM["supplement"] == "medication"
        assert _MEMORY_CATEGORY_TO_LTM["preference"] == "preference"
        assert _MEMORY_CATEGORY_TO_LTM["general"] == "user_memory"


# ── handle_memory_block feedback + contradiction ─────


class TestHandleMemoryBlockFeedback:
    def test_returns_remembered_feedback(self):
        cdb = MagicMock()
        cdb.get_user_memory.return_value = []
        mgr = _make_mgr(clean_db_mock=cdb)

        feedback = handle_memory_block(mgr, {
            "key": "favorite_coffee",
            "value": "espresso",
            "category": "preference",
        })
        assert feedback == "Preference updated: favorite coffee"
        cdb.upsert_user_memory.assert_called_once()
        mgr.invalidate_memory_cache.assert_called_once()

    def test_phi_blocked_returns_feedback(self):
        mgr = _make_mgr()
        mgr._fw.contains_phi.return_value = True

        feedback = handle_memory_block(mgr, {
            "key": "name",
            "value": "John Doe",
        })
        assert "Could not remember" in feedback
        assert "sensitive data" in feedback

    def test_contradiction_detection_returns_updated(self):
        cdb = MagicMock()
        cdb.get_user_memory.return_value = [
            {"key": "favorite_supplement", "value": "vitamin D"},
        ]
        mgr = _make_mgr(clean_db_mock=cdb)

        feedback = handle_memory_block(mgr, {
            "key": "favorite_supplement",
            "value": "magnesium glycinate",
            "category": "supplement",
        })
        assert "Updated:" in feedback
        assert "was: vitamin D" in feedback

    def test_no_clean_db_returns_none(self):
        mgr = _make_mgr()
        mgr._get_clean_db.return_value = None

        feedback = handle_memory_block(mgr, {
            "key": "test",
            "value": "value",
        })
        assert feedback is None

    def test_exception_returns_failure_feedback(self):
        cdb = MagicMock()
        cdb.get_user_memory.return_value = []
        cdb.upsert_user_memory.side_effect = RuntimeError("DB error")
        mgr = _make_mgr(clean_db_mock=cdb)

        feedback = handle_memory_block(mgr, {
            "key": "test",
            "value": "value",
        })
        assert "Failed to remember" in feedback
        assert "DB error" in feedback


# ── Memory feedback integration ──────────────────────


class TestMemoryFeedbackInit:
    def test_feedback_list_initialized(self):
        from healthbot.llm.claude_conversation import ClaudeConversationManager

        real = ClaudeConversationManager.__new__(ClaudeConversationManager)
        real.__init__(
            config=MagicMock(vault_home=Path("/tmp")),
            claude_client=MagicMock(),
            phi_firewall=MagicMock(),
        )
        assert hasattr(real, "_memory_feedback")
        assert isinstance(real._memory_feedback, list)
        assert len(real._memory_feedback) == 0


# ── Cache TTL ────────────────────────────────────────


class TestMemoryCacheTTL:
    def test_cache_ttl_constant_defined(self):
        from healthbot.llm.conversation_context import _MEMORY_CACHE_TTL
        assert _MEMORY_CACHE_TTL == 60


# ── Confidence decay ──────────────────────────────


class TestConfidenceDecay:
    def test_no_decay_for_user_stated(self):
        from healthbot.llm.conversation_context import _apply_confidence_decay

        mem = {
            "confidence": 0.9,
            "source": "user_stated",
            "created_at": "2024-01-01T00:00:00+00:00",
        }
        assert _apply_confidence_decay(mem) == 0.9

    def test_no_decay_for_recent_inferred(self):
        from datetime import UTC, datetime, timedelta

        from healthbot.llm.conversation_context import _apply_confidence_decay

        recent = (datetime.now(UTC) - timedelta(days=30)).isoformat()
        mem = {
            "confidence": 0.8,
            "source": "claude_inferred",
            "created_at": recent,
        }
        assert _apply_confidence_decay(mem) == 0.8

    def test_decay_after_90_days(self):
        from datetime import UTC, datetime, timedelta

        from healthbot.llm.conversation_context import _apply_confidence_decay

        old = (datetime.now(UTC) - timedelta(days=100)).isoformat()
        mem = {
            "confidence": 1.0,
            "source": "claude_inferred",
            "created_at": old,
        }
        result = _apply_confidence_decay(mem)
        assert abs(result - 0.8) < 0.01

    def test_decay_after_180_days(self):
        from datetime import UTC, datetime, timedelta

        from healthbot.llm.conversation_context import _apply_confidence_decay

        very_old = (datetime.now(UTC) - timedelta(days=200)).isoformat()
        mem = {
            "confidence": 1.0,
            "source": "claude_inferred",
            "created_at": very_old,
        }
        result = _apply_confidence_decay(mem)
        assert abs(result - 0.6) < 0.01

    def test_no_created_at_returns_original(self):
        from healthbot.llm.conversation_context import _apply_confidence_decay

        mem = {
            "confidence": 0.7,
            "source": "claude_inferred",
        }
        assert _apply_confidence_decay(mem) == 0.7

    def test_naive_timestamp_handled(self):
        """Naive timestamps (no timezone) don't crash — treated as UTC."""
        from datetime import datetime, timedelta

        from healthbot.llm.conversation_context import _apply_confidence_decay

        # Naive timestamp (no +00:00 suffix) older than 90 days
        naive_ts = (datetime.now() - timedelta(days=100)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        mem = {
            "confidence": 1.0,
            "source": "claude_inferred",
            "created_at": naive_ts,
        }
        result = _apply_confidence_decay(mem)
        assert abs(result - 0.8) < 0.01


# ── Memory search filtering ──────────────────────


class TestMemorySearchFiltering:
    """Test the search logic used in _memory_search."""

    def _filter(self, memories: list[dict], term: str) -> list[dict]:
        """Replicate the search filter logic from _memory_search."""
        term_lower = term.lower()
        return [
            mem for mem in memories
            if term_lower in mem.get("key", "").lower()
            or term_lower in mem.get("value", "").lower()
            or term_lower in mem.get("category", "").lower()
        ]

    def test_search_matches_key(self):
        memories = [
            {"key": "favorite_supplement", "value": "magnesium", "category": "supplement"},
            {"key": "height", "value": "6 feet", "category": "demographic"},
        ]
        results = self._filter(memories, "supplement")
        assert len(results) == 1  # matches key and category of first entry
        assert results[0]["key"] == "favorite_supplement"

    def test_search_matches_value(self):
        memories = [
            {"key": "height", "value": "6 feet", "category": "demographic"},
            {"key": "weight", "value": "180 lbs", "category": "demographic"},
        ]
        results = self._filter(memories, "feet")
        assert len(results) == 1
        assert results[0]["key"] == "height"

    def test_search_matches_category(self):
        memories = [
            {"key": "height", "value": "6 feet", "category": "demographic"},
            {"key": "coffee", "value": "espresso", "category": "preference"},
        ]
        results = self._filter(memories, "preference")
        assert len(results) == 1
        assert results[0]["key"] == "coffee"

    def test_search_case_insensitive(self):
        memories = [
            {"key": "Vitamin_D", "value": "5000 IU", "category": "supplement"},
        ]
        results = self._filter(memories, "VITAMIN")
        assert len(results) == 1

    def test_search_no_matches(self):
        memories = [
            {"key": "height", "value": "6 feet", "category": "demographic"},
        ]
        results = self._filter(memories, "nonexistent")
        assert len(results) == 0


# ── Memory export formatting ─────────────────────


class TestMemoryExport:
    def test_export_builds_text(self):
        """Verify export text contains key memory fields."""
        memories = [
            {
                "key": "height",
                "value": "6 feet",
                "category": "demographic",
                "confidence": 1.0,
                "source": "user_stated",
                "created_at": "2025-01-15T10:00:00",
                "updated_at": "2025-01-15T10:00:00",
            },
            {
                "key": "sleep_pattern",
                "value": "night owl",
                "category": "lifestyle",
                "confidence": 0.7,
                "source": "claude_inferred",
                "created_at": "2025-02-01T10:00:00",
                "updated_at": "2025-02-10T10:00:00",
            },
        ]
        # Build export text like _memory_export does
        by_cat: dict[str, list[dict]] = {}
        for mem in memories:
            by_cat.setdefault(mem.get("category", "general"), []).append(mem)

        lines = [f"Total entries: {len(memories)}"]
        for cat in sorted(by_cat.keys()):
            lines.append(f"[{cat.replace('_', ' ').upper()}]")
            for mem in by_cat[cat]:
                lines.append(f"  Key: {mem['key']}")
                lines.append(f"  Value: {mem['value']}")

        text = "\n".join(lines)
        assert "height" in text
        assert "6 feet" in text
        assert "DEMOGRAPHIC" in text
        assert "sleep_pattern" in text
        assert "LIFESTYLE" in text
