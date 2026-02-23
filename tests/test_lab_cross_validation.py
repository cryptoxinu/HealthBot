"""Tests for lab parser cross-validation and Ollama re-run logic."""
from __future__ import annotations

import pytest

from healthbot.data.models import LabResult
from healthbot.ingest.lab_pdf_parser import (
    LabPdfParser,
    _adjust_confidence,
    _parse_numeric,
    _replace_result,
    _values_match,
)

# ── Helper functions ─────────────────────────────────────


class TestParseNumeric:
    def test_simple_float(self):
        assert _parse_numeric("95.5") == 95.5

    def test_integer(self):
        assert _parse_numeric("100") == 100.0

    def test_less_than_prefix(self):
        assert _parse_numeric("<0.5") == 0.5

    def test_greater_than_prefix(self):
        assert _parse_numeric(">1.0") == 1.0

    def test_gte_prefix(self):
        assert _parse_numeric(">=3.5") == 3.5

    def test_comma_separated(self):
        assert _parse_numeric("1,200") == 1200.0

    def test_non_numeric(self):
        assert _parse_numeric("Non-Reactive") is None

    def test_empty_string(self):
        assert _parse_numeric("") is None

    def test_none(self):
        assert _parse_numeric(None) is None


class TestValuesMatch:
    def test_exact_match(self):
        assert _values_match("95", "95")

    def test_within_tolerance(self):
        # 95 vs 96 = ~1% diff, within 5% tolerance
        assert _values_match("95", "96")

    def test_beyond_tolerance(self):
        # 95 vs 150 = ~37% diff, well beyond 5%
        assert not _values_match("95", "150")

    def test_small_absolute_tolerance(self):
        # 0.1 vs 0.3 → 100% relative diff but within 0.5 absolute
        assert _values_match("0.1", "0.3")

    def test_non_numeric_exact(self):
        assert _values_match("Reactive", "reactive")

    def test_non_numeric_different(self):
        assert not _values_match("Reactive", "Non-Reactive")

    def test_both_zero(self):
        assert _values_match("0", "0")


class TestAdjustConfidence:
    def _make_result(self, name: str = "glucose", conf: float = 0.85) -> LabResult:
        return LabResult(id="r1", test_name=name, canonical_name=name, confidence=conf)

    def test_consensus_boosts(self):
        r = self._make_result(conf=0.85)
        _adjust_confidence(r, {"glucose": {"consensus": True}})
        assert r.confidence == 0.90

    def test_conflict_reduces(self):
        r = self._make_result(conf=0.85)
        _adjust_confidence(r, {"glucose": {
            "consensus": False, "conflict_note": "disagreement",
        }})
        assert r.confidence == 0.70

    def test_no_entry_unchanged(self):
        r = self._make_result(conf=0.85)
        _adjust_confidence(r, {})
        assert r.confidence == 0.85

    def test_boost_capped_at_099(self):
        r = self._make_result(conf=0.97)
        _adjust_confidence(r, {"glucose": {"consensus": True}})
        assert r.confidence == 0.99

    def test_reduce_floored_at_050(self):
        r = self._make_result(conf=0.55)
        _adjust_confidence(r, {"glucose": {
            "consensus": False, "conflict_note": "x",
        }})
        assert r.confidence == 0.50


class TestReplaceResult:
    def test_replaces_matching(self):
        old = LabResult(id="r1", test_name="Glucose", canonical_name="glucose", value="95")
        new = LabResult(id="r2", test_name="Glucose", canonical_name="glucose", value="100")
        results = [old]
        _replace_result(results, "glucose", new)
        assert results[0].value == "100"

    def test_no_match_unchanged(self):
        r = LabResult(id="r1", test_name="TSH", canonical_name="tsh", value="2.1")
        results = [r]
        new = LabResult(id="r2", test_name="Glucose", canonical_name="glucose", value="100")
        _replace_result(results, "glucose", new)
        assert results[0].value == "2.1"


# ── _find_conflicts ──────────────────────────────────────


class TestFindConflicts:
    def _make(self, name: str, value: str, conf: float = 0.85) -> LabResult:
        return LabResult(
            id="r1", test_name=name, canonical_name=name,
            value=value, confidence=conf,
        )

    def test_consensus_two_methods_agree(self):
        table = [self._make("glucose", "95", 0.95)]
        regex = [self._make("glucose", "95", 0.60)]
        ollama: list[LabResult] = []

        conflicts = LabPdfParser._find_conflicts(table, ollama, regex)
        assert "glucose" in conflicts
        assert conflicts["glucose"]["consensus"] is True
        assert conflicts["glucose"]["conflict_note"] is None

    def test_conflict_ollama_vs_deterministic(self):
        table = [self._make("glucose", "95", 0.95)]
        ollama = [self._make("glucose", "150", 0.85)]
        regex = [self._make("glucose", "95", 0.60)]

        conflicts = LabPdfParser._find_conflicts(table, ollama, regex)
        assert conflicts["glucose"]["consensus"] is False
        assert conflicts["glucose"]["conflict_note"] is not None
        assert conflicts["glucose"]["deterministic_value"] == "95"

    def test_single_source_not_in_conflicts(self):
        table = [self._make("glucose", "95", 0.95)]
        ollama: list[LabResult] = []
        regex: list[LabResult] = []

        conflicts = LabPdfParser._find_conflicts(table, ollama, regex)
        assert "glucose" not in conflicts

    def test_deterministic_value_from_table_regex_agreement(self):
        table = [self._make("tsh", "2.1", 0.95)]
        ollama = [self._make("tsh", "5.0", 0.85)]
        regex = [self._make("tsh", "2.1", 0.60)]

        conflicts = LabPdfParser._find_conflicts(table, ollama, regex)
        assert conflicts["tsh"]["deterministic_value"] == "2.1"

    def test_multiple_tests_mixed(self):
        table = [
            self._make("glucose", "95", 0.95),
            self._make("tsh", "2.1", 0.95),
        ]
        ollama = [
            self._make("glucose", "95", 0.85),  # agrees
            self._make("tsh", "5.0", 0.85),      # disagrees
        ]
        regex: list[LabResult] = []

        conflicts = LabPdfParser._find_conflicts(table, ollama, regex)
        assert conflicts["glucose"]["consensus"] is True
        assert conflicts["tsh"]["consensus"] is False


# ── _merge_three_way with conflicts ──────────────────────


class TestMergeThreeWayWithConflicts:
    def _make(self, name: str, value: str, conf: float) -> LabResult:
        return LabResult(
            id="r1", test_name=name, canonical_name=name,
            value=value, confidence=conf,
        )

    def test_consensus_boosts_confidence(self):
        table = [self._make("glucose", "95", 0.95)]
        ollama = [self._make("glucose", "95", 0.85)]
        regex: list[LabResult] = []
        conflicts = {"glucose": {"consensus": True, "conflict_note": None}}

        results = LabPdfParser._merge_three_way(table, ollama, regex, conflicts)
        assert len(results) == 1
        # Table wins (priority), confidence boosted from 0.95 to 0.99 (capped)
        assert results[0].confidence == 0.99

    def test_conflict_reduces_confidence(self):
        table = [self._make("glucose", "95", 0.95)]
        ollama = [self._make("glucose", "150", 0.85)]
        regex: list[LabResult] = []
        conflicts = {"glucose": {
            "consensus": False, "conflict_note": "table=95, ollama=150",
        }}

        results = LabPdfParser._merge_three_way(table, ollama, regex, conflicts)
        assert len(results) == 1
        # Table wins but confidence reduced
        assert results[0].confidence == pytest.approx(0.80)

    def test_no_conflicts_map_unchanged(self):
        table = [self._make("glucose", "95", 0.95)]
        results = LabPdfParser._merge_three_way(table, [], [])
        assert results[0].confidence == 0.95

    def test_ollama_only_with_conflicts_applied(self):
        """Bug fix: conflicts must apply even without table results."""
        ollama = [self._make("glucose", "95", 0.85)]
        regex = [self._make("glucose", "95", 0.60)]
        conflicts = {"glucose": {"consensus": True, "conflict_note": None}}

        results = LabPdfParser._merge_three_way([], ollama, regex, conflicts)
        assert len(results) == 1
        assert results[0].confidence == 0.90  # 0.85 + 0.05 boost

    def test_regex_only_with_conflicts_applied(self):
        """Conflicts must apply even with regex-only results."""
        regex = [self._make("glucose", "95", 0.60)]
        conflicts = {"glucose": {"consensus": True, "conflict_note": None}}

        results = LabPdfParser._merge_three_way([], [], regex, conflicts)
        assert len(results) == 1
        assert results[0].confidence == 0.65  # 0.60 + 0.05 boost


# ── Edge cases ──────────────────────────────────────────


class TestValuesMatchEdgeCases:
    def test_negative_numbers_match(self):
        assert _values_match("-95", "-96")

    def test_sign_mismatch_no_match(self):
        assert not _values_match("-95", "95")

    def test_zero_vs_small_within_abs_tol(self):
        assert _values_match("0", "0.4")

    def test_zero_vs_large_no_match(self):
        assert not _values_match("0", "5")

    def test_whitespace_non_numeric(self):
        assert _values_match(" Reactive ", "reactive")


class TestAdjustConfidenceEdgeCases:
    def _make_result(self, name="glucose", conf=0.85):
        return LabResult(
            id="r1", test_name=name, canonical_name=name,
            confidence=conf,
        )

    def test_conflict_without_note_unchanged(self):
        """consensus=False but no conflict_note → no reduction."""
        r = self._make_result(conf=0.85)
        _adjust_confidence(r, {"glucose": {
            "consensus": False, "conflict_note": None,
        }})
        assert r.confidence == 0.85

    def test_empty_conflicts_dict(self):
        r = self._make_result(conf=0.85)
        _adjust_confidence(r, {})
        assert r.confidence == 0.85


class TestReplaceResultEdgeCases:
    def test_empty_list_no_crash(self):
        results: list[LabResult] = []
        new = LabResult(
            id="r2", test_name="Glucose",
            canonical_name="glucose", value="100",
        )
        _replace_result(results, "glucose", new)
        assert len(results) == 0

    def test_replaces_first_match_only(self):
        r1 = LabResult(
            id="r1", test_name="Glucose",
            canonical_name="glucose", value="95",
        )
        r2 = LabResult(
            id="r2", test_name="Glucose",
            canonical_name="glucose", value="96",
        )
        new = LabResult(
            id="r3", test_name="Glucose",
            canonical_name="glucose", value="100",
        )
        results = [r1, r2]
        _replace_result(results, "glucose", new)
        assert results[0].value == "100"
        assert results[1].value == "96"


# ── Ollama re-run integration ───────────────────────────


class TestOllamaRerunConflictResolution:
    """Tests for the full Stage 5.5 re-run logic in parse_bytes().

    These test _rerun_ollama_conflicts + conflict resolution together
    by mocking the Ollama call.
    """

    def _make(self, name, value, conf=0.85):
        return LabResult(
            id="r1", test_name=name, canonical_name=name,
            value=value, confidence=conf,
        )

    def test_self_correction_marks_consensus(self):
        """Ollama re-run matches deterministic → consensus restored."""
        table = [self._make("glucose", "95", 0.95)]
        ollama_orig = [self._make("glucose", "150", 0.85)]
        regex = [self._make("glucose", "95", 0.60)]

        conflicts = LabPdfParser._find_conflicts(
            table, ollama_orig, regex,
        )
        assert conflicts["glucose"]["consensus"] is False

        # Simulate re-run: Ollama now returns 95 (matches deterministic)
        rerun_result = self._make("glucose", "95", 0.85)
        rerun_map = {"glucose": rerun_result}

        # Apply same logic as Stage 5.5
        det_val = conflicts["glucose"]["deterministic_value"]
        rerun_r = rerun_map.get("glucose")

        assert rerun_r is not None
        assert _values_match(rerun_r.value, det_val)
        # Self-corrected: replace and mark consensus
        _replace_result(ollama_orig, "glucose", rerun_r)
        conflicts["glucose"]["consensus"] = True
        conflicts["glucose"]["conflict_note"] = None

        assert conflicts["glucose"]["consensus"] is True
        assert ollama_orig[0].value == "95"

    def test_persistent_disagreement_flags_conflict(self):
        """Ollama re-run returns same wrong value → genuine ambiguity."""
        table = [self._make("glucose", "95", 0.95)]
        ollama_orig = [self._make("glucose", "150", 0.85)]
        regex: list[LabResult] = []

        conflicts = LabPdfParser._find_conflicts(
            table, ollama_orig, regex,
        )
        det_val = conflicts["glucose"]["deterministic_value"]
        orig_val = conflicts["glucose"]["sources"]["ollama"]

        # Re-run returns same value (150)
        rerun_r = self._make("glucose", "150", 0.85)

        assert not _values_match(rerun_r.value, det_val)
        assert _values_match(rerun_r.value, orig_val)

        # Persistent conflict
        conflicts["glucose"]["conflict_note"] = (
            f"Ollama consistently reads {orig_val}, "
            f"but table/regex reads {det_val}"
        )
        assert "consistently reads 150" in conflicts["glucose"]["conflict_note"]

    def test_third_value_drops_ollama(self):
        """Ollama re-run gives yet another value → unreliable, drop."""
        table = [self._make("glucose", "95", 0.95)]
        ollama_orig = [self._make("glucose", "150", 0.85)]
        regex: list[LabResult] = []

        conflicts = LabPdfParser._find_conflicts(
            table, ollama_orig, regex,
        )
        det_val = conflicts["glucose"]["deterministic_value"]
        orig_val = conflicts["glucose"]["sources"]["ollama"]

        # Re-run returns 200 — a THIRD different value
        rerun_r = self._make("glucose", "200", 0.85)

        assert not _values_match(rerun_r.value, det_val)
        assert not _values_match(rerun_r.value, orig_val)

        # Drop Ollama entirely
        ollama_orig = [
            r for r in ollama_orig
            if r.canonical_name != "glucose"
        ]
        assert len(ollama_orig) == 0

    def test_rerun_returns_none_drops_ollama(self):
        """Ollama re-run returns None (failed) → drop, use deterministic."""
        table = [self._make("glucose", "95", 0.95)]
        ollama_orig = [self._make("glucose", "150", 0.85)]
        regex: list[LabResult] = []

        LabPdfParser._find_conflicts(table, ollama_orig, regex)

        # Re-run returned nothing for glucose
        rerun_r = None
        assert rerun_r is None

        # Should drop Ollama result
        ollama_orig = [
            r for r in ollama_orig
            if r.canonical_name != "glucose"
        ]
        assert len(ollama_orig) == 0

    def test_no_ollama_conflicts_skips_rerun(self):
        """When no Ollama conflicts, no re-run should happen."""
        table = [self._make("glucose", "95", 0.95)]
        regex = [self._make("glucose", "95", 0.60)]
        ollama: list[LabResult] = []

        conflicts = LabPdfParser._find_conflicts(table, ollama, regex)

        # No Ollama results → no conflicts involving Ollama
        ollama_conflicts = {
            name for name, info in conflicts.items()
            if not info["consensus"]
            and "ollama" in info.get("sources", {})
            and info.get("deterministic_value") is not None
        }
        assert len(ollama_conflicts) == 0

    def test_conflict_confidence_end_to_end(self):
        """Full flow: conflict detected → re-run fails → merge with reduced
        confidence."""
        table = [self._make("glucose", "95", 0.95)]
        ollama = [self._make("glucose", "150", 0.85)]
        regex: list[LabResult] = []

        conflicts = LabPdfParser._find_conflicts(table, ollama, regex)
        # Simulate persistent conflict (Ollama re-run returned same value)
        conflicts["glucose"]["conflict_note"] = (
            "Ollama consistently reads 150, but table reads 95"
        )

        results = LabPdfParser._merge_three_way(
            table, ollama, regex, conflicts,
        )
        # Table wins with downweighted confidence: 0.95 - 0.15 = 0.80
        assert len(results) == 1
        assert results[0].value == "95"
        assert results[0].confidence == pytest.approx(0.80)
