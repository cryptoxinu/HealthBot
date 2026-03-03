"""Demographics validation and lab result filtering.

Validates parsed lab results against demographic expectations and
filters out results that are clearly not valid lab tests.
"""
from __future__ import annotations

import logging

from healthbot.data.models import LabResult

logger = logging.getLogger("healthbot")


class ValidationMixin:
    """Mixin providing demographics validation and lab filtering."""

    @staticmethod
    def _validate_with_demographics(
        labs: list[LabResult], demographics: dict,
    ) -> list[LabResult]:
        """Flag results that are implausibly far from expected ranges.

        Catches likely parse errors (e.g., decimal point missed ->
        glucose 1000 instead of 100).
        """
        from healthbot.reasoning.reference_ranges import get_range

        sex = demographics.get("sex")
        age = demographics.get("age")

        for lab in labs:
            if not isinstance(lab.value, (int, float)):
                continue
            ref = get_range(
                lab.canonical_name or lab.test_name.lower(),
                sex=sex, age=age,
            )
            if not ref:
                continue
            high = ref.get("high")
            if high and lab.value > high * 10:
                lab.confidence *= 0.3
                lab.flag = f"{lab.flag} SUSPECT" if lab.flag else "SUSPECT"
                logger.warning(
                    "Suspect value: %s = %s (>10x high ref %s)",
                    lab.test_name, lab.value, high,
                )
        return labs

    @staticmethod
    def _filter_valid_results(labs: list[LabResult]) -> list[LabResult]:
        """Drop results that are clearly not valid lab tests.

        Safety net after parsing -- catches anything the parser's blocklist missed.
        Three acceptance paths:
        1. Numeric -- known canonical OR has ref ranges
        2. Inequality string -- "<0.5", ">1.0" (clinically valid)
        3. Qualitative string -- canonical in QUALITATIVE_TESTS, OR value in
           VALID_QUALITATIVE_VALUES, OR result has reference_text
        """
        from healthbot.normalize.lab_normalizer import (
            QUALITATIVE_TESTS,
            TEST_NAME_MAP,
            VALID_QUALITATIVE_VALUES,
        )

        known_canonical = set(TEST_NAME_MAP.values())

        filtered = []
        for lab in labs:
            name = lab.test_name.strip()
            if len(name) < 2:
                continue

            canonical = lab.canonical_name or ""

            # Path 1: Numeric values
            val = lab.value
            if isinstance(val, (int, float)):
                # Must be known OR have ref ranges
                if (
                    canonical in known_canonical
                    or lab.reference_low is not None
                    or lab.reference_high is not None
                ):
                    filtered.append(lab)
                else:
                    logger.info(
                        "Dropping unrecognized numeric test without ref "
                        "range: %s (canonical: %s)", name, canonical,
                    )
                continue

            if not isinstance(val, str):
                continue

            # Path 2: Inequality-prefixed numeric strings (<0.5, >1.0)
            stripped = val.lstrip("<>≤≥= ")
            try:
                float(stripped)
                if (
                    canonical in known_canonical
                    or lab.reference_low is not None
                    or lab.reference_high is not None
                ):
                    filtered.append(lab)
                else:
                    logger.info(
                        "Dropping unrecognized inequality test without "
                        "ref range: %s (canonical: %s)", name, canonical,
                    )
                continue
            except (ValueError, TypeError):
                pass

            # Path 3a: Trust Claude -- high-confidence extraction of string values.
            # Claude reads the PDF with vision (confidence 0.92). If it
            # extracted a string value with high confidence, it's a real
            # result -- accept it even if it's not in our hardcoded sets.
            # The hardcoded sets remain as safety nets for the Ollama path
            # (confidence 0.85).
            if lab.confidence >= 0.90 and val.strip():
                filtered.append(lab)
                continue

            # Path 3b: Qualitative string values (hardcoded safety net)
            if (
                canonical in QUALITATIVE_TESTS
                or val.strip().lower() in VALID_QUALITATIVE_VALUES
                or (hasattr(lab, "reference_text") and lab.reference_text)
            ):
                filtered.append(lab)
            else:
                logger.info(
                    "Dropping unrecognized qualitative result: %s = %r "
                    "(canonical: %s)", name, val, canonical,
                )
        return filtered
