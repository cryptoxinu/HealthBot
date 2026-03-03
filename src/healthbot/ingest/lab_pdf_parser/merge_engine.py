"""Three-way merge and conflict resolution for lab extraction.

Merges results from table parsing, Ollama LLM, and regex extraction.
Cross-validates values across sources, re-runs Ollama on conflicts,
and adjusts confidence scores based on agreement.
"""
from __future__ import annotations

import logging

from healthbot.data.models import LabResult
from healthbot.ingest.lab_pdf_parser.helpers import (
    _adjust_confidence,
    _values_match,
)

logger = logging.getLogger("healthbot")


class MergeEngineMixin:
    """Mixin providing three-way merge and conflict resolution."""

    @staticmethod
    def _merge_three_way(
        table_results: list[LabResult],
        ollama_results: list[LabResult],
        regex_results: list[LabResult],
        conflicts: dict[str, dict] | None = None,
    ) -> list[LabResult]:
        """Merge results from three extraction methods.

        Priority: table (0.95) > Ollama (0.85) > regex (0.60).
        Deduplication by canonical_name.
        Confidence adjusted by cross-validation conflicts map.
        """
        seen: set[str] = set()
        merged: list[LabResult] = []
        conflicts = conflicts or {}

        # Table results first (highest confidence)
        for r in table_results:
            if r.canonical_name not in seen:
                _adjust_confidence(r, conflicts)
                merged.append(r)
                seen.add(r.canonical_name)

        # Ollama supplements
        for r in ollama_results:
            if r.canonical_name not in seen:
                _adjust_confidence(r, conflicts)
                merged.append(r)
                seen.add(r.canonical_name)

        # Regex fills remaining gaps
        for r in regex_results:
            if r.canonical_name not in seen:
                r.confidence = 0.60
                _adjust_confidence(r, conflicts)
                merged.append(r)
                seen.add(r.canonical_name)

        return merged

    @staticmethod
    def _merge_results(
        primary: list[LabResult], supplement: list[LabResult],
    ) -> list[LabResult]:
        """Merge regex supplement into Ollama primary results.

        Deduplicates by canonical_name only (not page) because Ollama
        returns all results with source_page=0 while regex has real page
        numbers. A lab report has one result per test — page doesn't matter.
        """
        seen = {r.canonical_name for r in primary}
        merged = list(primary)
        for r in supplement:
            if r.canonical_name not in seen:
                r.confidence = 0.6  # lower confidence for regex-only
                merged.append(r)
                seen.add(r.canonical_name)
        return merged

    @staticmethod
    def _find_conflicts(
        table_results: list[LabResult],
        ollama_results: list[LabResult],
        regex_results: list[LabResult],
    ) -> dict[str, dict]:
        """Compare values across extraction methods for same test.

        Returns: {canonical_name: {
            "sources": {"table": val, "ollama": val, "regex": val},
            "consensus": bool,
            "conflict_note": str | None,
            "deterministic_value": str | None,
        }}
        """
        # Build lookups by canonical_name
        table_map = {r.canonical_name: r.value for r in table_results}
        ollama_map = {r.canonical_name: r.value for r in ollama_results}
        regex_map = {r.canonical_name: r.value for r in regex_results}

        all_names = set(table_map) | set(ollama_map) | set(regex_map)
        result: dict[str, dict] = {}

        for name in all_names:
            sources: dict[str, str] = {}
            if name in table_map:
                sources["table"] = table_map[name]
            if name in ollama_map:
                sources["ollama"] = ollama_map[name]
            if name in regex_map:
                sources["regex"] = regex_map[name]

            if len(sources) < 2:
                # Single source — no cross-validation possible
                continue

            # Compare all pairs
            vals = list(sources.values())
            consensus = True
            for i in range(len(vals)):
                for j in range(i + 1, len(vals)):
                    if not _values_match(vals[i], vals[j]):
                        consensus = False
                        break
                if not consensus:
                    break

            # Deterministic value: table and regex agree
            det_val = None
            if "table" in sources and "regex" in sources:
                if _values_match(sources["table"], sources["regex"]):
                    det_val = sources["table"]
            elif "table" in sources:
                det_val = sources["table"]
            elif "regex" in sources:
                det_val = sources["regex"]

            conflict_note = None
            if not consensus:
                conflict_note = (
                    "Extraction conflict: "
                    + ", ".join(f"{k}={v}" for k, v in sources.items())
                )

            result[name] = {
                "sources": sources,
                "consensus": consensus,
                "conflict_note": conflict_note,
                "deterministic_value": det_val,
            }

        return result

    def _rerun_ollama_conflicts(
        self,
        ollama_pages: list[str],
        blob_id: str,
        conflict_names: set[str],
    ) -> dict[str, LabResult]:
        """Re-run Ollama parse for conflicting tests only.

        One additional Ollama call (not per-test). Returns only results
        whose canonical_name is in conflict_names.
        """
        logger.info(
            "Re-running Ollama for %d conflicting tests", len(conflict_names),
        )
        rerun_results = self._ollama_parse_pages(ollama_pages, blob_id)
        return {
            r.canonical_name: r
            for r in rerun_results
            if r.canonical_name in conflict_names
        }
