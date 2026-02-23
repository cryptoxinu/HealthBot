"""Tests for memory, correction, and system improvement block handling."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from healthbot.data.clean_db import CleanDB
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
