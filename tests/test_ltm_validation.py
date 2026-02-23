"""Tests for LTM consolidation validation."""
from __future__ import annotations

from healthbot.llm.memory_store import MemoryStore


class TestIsDuplicate:
    """Dedup logic for LTM facts."""

    def test_exact_duplicate(self) -> None:
        assert MemoryStore._is_duplicate(
            "User is 28 years old",
            ["User is 28 years old"],
        ) is True

    def test_case_insensitive_duplicate(self) -> None:
        assert MemoryStore._is_duplicate(
            "user is 28 years old",
            ["User Is 28 Years Old"],
        ) is True

    def test_fuzzy_duplicate(self) -> None:
        assert MemoryStore._is_duplicate(
            "User is 28 years old, male",
            ["User is 28 years old, male, height 6'0\""],
            threshold=0.7,
        ) is True

    def test_not_duplicate(self) -> None:
        assert MemoryStore._is_duplicate(
            "Takes metformin 500mg twice daily",
            ["User is 28 years old"],
        ) is False

    def test_empty_existing(self) -> None:
        assert MemoryStore._is_duplicate("any fact", []) is False


class TestContainsPhi:
    """PHI detection in LTM facts."""

    def _store(self):
        from unittest.mock import MagicMock
        return MemoryStore(db=MagicMock())

    def test_clean_fact(self) -> None:
        assert self._store()._contains_phi("User has type 2 diabetes") is False

    def test_fact_with_ssn(self) -> None:
        assert self._store()._contains_phi("SSN is 123-45-6789") is True

    def test_fact_with_phone(self) -> None:
        assert self._store()._contains_phi("Call me at (555) 123-4567") is True


class TestValidateFacts:
    """Full validation pipeline."""

    def _make_store(self, existing_facts=None):
        """Create a MemoryStore with mocked DB."""
        from unittest.mock import MagicMock
        db = MagicMock()
        db.get_ltm_by_user.return_value = existing_facts or []
        store = MemoryStore(db)
        return store

    def test_empty_input(self) -> None:
        store = self._make_store()
        assert store._validate_facts([], user_id=1) == []

    def test_duplicate_removed(self) -> None:
        existing = [{"fact": "User is 28 years old", "category": "demographic"}]
        store = self._make_store(existing)
        facts = [
            {"category": "demographic", "fact": "User is 28 years old"},
            {"category": "medication", "fact": "Takes metformin 500mg"},
        ]
        result = store._validate_facts(facts, user_id=1)
        assert len(result) == 1
        assert result[0]["fact"] == "Takes metformin 500mg"

    def test_phi_blocked(self) -> None:
        store = self._make_store()
        facts = [
            {"category": "demographic", "fact": "Name is John Smith, SSN 123-45-6789"},
            {"category": "condition", "fact": "Has type 2 diabetes"},
        ]
        result = store._validate_facts(facts, user_id=1)
        assert len(result) == 1
        assert result[0]["fact"] == "Has type 2 diabetes"

    def test_short_fact_skipped(self) -> None:
        store = self._make_store()
        facts = [{"category": "other", "fact": "yes"}]
        result = store._validate_facts(facts, user_id=1)
        assert len(result) == 0

    def test_batch_dedup(self) -> None:
        store = self._make_store()
        facts = [
            {"category": "condition", "fact": "User has diabetes"},
            {"category": "condition", "fact": "User has diabetes"},
        ]
        result = store._validate_facts(facts, user_id=1)
        assert len(result) == 1
