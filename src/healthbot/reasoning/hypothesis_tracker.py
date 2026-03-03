"""Hypothesis tracking with fuzzy matching and evidence management.

Prevents duplicate hypotheses by matching incoming titles against existing ones
using fuzzy string matching. Merges evidence when a match is found.
Also detects when missing tests become available in lab data.
"""
from __future__ import annotations

import logging
from difflib import SequenceMatcher

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")

# Similarity threshold for matching hypothesis titles
_MATCH_THRESHOLD = 0.90

# Pairs of conditions that are lexically similar but clinically opposite.
# Fuzzy matching must NEVER merge these, regardless of similarity score.
_CONFUSABLE_PAIRS = {
    frozenset({"hypothyroidism", "hyperthyroidism"}),
    frozenset({"hypoglycemia", "hyperglycemia"}),
    frozenset({"hypokalemia", "hyperkalemia"}),
    frozenset({"hyponatremia", "hypernatremia"}),
    frozenset({"hypocalcemia", "hypercalcemia"}),
    frozenset({"hypotension", "hypertension"}),
    frozenset({"type 1 diabetes", "type 2 diabetes"}),
}


def _is_confusable_pair(title_a: str, title_b: str) -> bool:
    """Return True if the two titles form a confusable medical pair.

    Uses substring matching so that e.g. "type 2 diabetes mellitus" still
    matches the confusable pair {"type 1 diabetes", "type 2 diabetes"}.
    """
    a_lower = title_a.lower().strip()
    b_lower = title_b.lower().strip()
    for pair in _CONFUSABLE_PAIRS:
        terms = list(pair)
        # Check if each title contains a different term from the pair
        for i in range(len(terms)):
            other = 1 - i
            if (terms[i] in a_lower and terms[other] in b_lower):
                return True
    return False


class HypothesisTracker:
    """Track, deduplicate, and update medical hypotheses."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def find_matching_hypothesis(
        self, title: str, user_id: int
    ) -> dict | None:
        """Find an existing hypothesis with a similar title.

        Uses SequenceMatcher for fuzzy matching against all active hypotheses.
        Returns the best match above the threshold, or None.
        """
        hypotheses = self._db.get_active_hypotheses(user_id)
        if not hypotheses:
            return None

        title_lower = title.lower().strip()
        best_match: dict | None = None
        best_score = 0.0

        for hyp in hypotheses:
            existing_title = hyp.get("title", "").lower().strip()
            # Block merges between clinically confusable pairs
            if _is_confusable_pair(title_lower, existing_title):
                continue
            score = SequenceMatcher(None, title_lower, existing_title).ratio()
            if score > best_score and score >= _MATCH_THRESHOLD:
                best_score = score
                best_match = hyp

        return best_match

    def upsert_hypothesis(self, user_id: int, incoming: dict) -> str:
        """Insert or merge a hypothesis.

        If a matching hypothesis exists, merges evidence lists (deduplicated)
        and updates confidence. Otherwise, inserts a new hypothesis.

        Returns the hypothesis ID.
        """
        title = incoming.get("title", "")
        if not title:
            raise ValueError("Hypothesis must have a title")

        existing = self.find_matching_hypothesis(title, user_id)

        if existing:
            return self._merge(existing, incoming)

        hyp_id = self._db.insert_hypothesis(user_id, incoming)
        logger.info("New hypothesis: %s (id=%s)", title, hyp_id)
        return hyp_id

    def _merge(self, existing: dict, incoming: dict) -> str:
        """Merge incoming evidence into an existing hypothesis."""
        hyp_id = existing["_id"]

        # Merge evidence_for (deduplicated)
        ev_for = list(existing.get("evidence_for", []))
        for item in incoming.get("evidence_for", []):
            if item not in ev_for:
                ev_for.append(item)

        # Merge evidence_against (deduplicated)
        ev_against = list(existing.get("evidence_against", []))
        for item in incoming.get("evidence_against", []):
            if item not in ev_against:
                ev_against.append(item)

        # Merge missing_tests (deduplicated)
        missing = list(existing.get("missing_tests", []))
        for item in incoming.get("missing_tests", []):
            if item not in missing:
                missing.append(item)

        # Weighted average: existing evidence weighs more (0.7) to prevent
        # wild swings, but new evidence (0.3) can still decrease confidence
        old_conf = existing.get("confidence", existing.get("_confidence", 0.0))
        new_conf = incoming.get("confidence", 0.0)
        confidence = old_conf * 0.7 + new_conf * 0.3

        updated = {
            "title": existing.get("title", incoming.get("title", "")),
            "confidence": confidence,
            "evidence_for": ev_for,
            "evidence_against": ev_against,
            "missing_tests": missing,
            "notes": incoming.get("notes", existing.get("notes", "")),
            "status": existing.get("_status", "active"),
        }

        self._db.update_hypothesis(hyp_id, updated)
        logger.info("Merged hypothesis: %s (id=%s)", updated["title"], hyp_id)
        return hyp_id

    def check_fulfilled_tests(self, user_id: int) -> list[dict]:
        """Check if any missing tests now have lab data.

        For each active hypothesis, checks if lab results exist for tests
        listed in missing_tests. If found, removes from missing_tests and
        adds a note to evidence_for.

        Returns list of hypotheses that were updated.
        """
        hypotheses = self._db.get_active_hypotheses(user_id)
        updated = []

        for hyp in hypotheses:
            missing = hyp.get("missing_tests", [])
            if not missing:
                continue

            still_missing = []
            newly_found = []

            for test_name in missing:
                if self._has_lab_data(test_name, user_id=user_id):
                    newly_found.append(test_name)
                else:
                    still_missing.append(test_name)

            if newly_found:
                ev_for = list(hyp.get("evidence_for", []))
                for test in newly_found:
                    note = f"Test now available: {test}"
                    if note not in ev_for:
                        ev_for.append(note)

                data = {
                    "title": hyp.get("title", ""),
                    "confidence": hyp.get("confidence", hyp.get("_confidence", 0.0)),
                    "evidence_for": ev_for,
                    "evidence_against": hyp.get("evidence_against", []),
                    "missing_tests": still_missing,
                    "notes": hyp.get("notes", ""),
                    "status": hyp.get("_status", "active"),
                }
                self._db.update_hypothesis(hyp["_id"], data)
                data["_id"] = hyp["_id"]
                updated.append(data)
                logger.info(
                    "Fulfilled tests for '%s': %s",
                    hyp.get("title"),
                    ", ".join(newly_found),
                )

        return updated

    def _has_lab_data(self, test_name: str, user_id: int | None = None) -> bool:
        """Check if we have any lab results for the given test name."""
        from healthbot.normalize.lab_normalizer import normalize_test_name

        canonical = normalize_test_name(test_name)
        results = self._db.query_observations(
            canonical_name=canonical, limit=1, user_id=user_id,
        )
        return len(results) > 0

    def ruleout(self, hyp_id: str, reason: str = "") -> None:
        """Mark a hypothesis as ruled out."""
        hyp = self._db.get_hypothesis(hyp_id)
        if not hyp:
            raise ValueError(f"Hypothesis {hyp_id} not found")

        ev_against = list(hyp.get("evidence_against", []))
        if reason and reason not in ev_against:
            ev_against.append(reason)

        data = {
            "title": hyp.get("title", ""),
            "confidence": 0.0,
            "evidence_for": hyp.get("evidence_for", []),
            "evidence_against": ev_against,
            "missing_tests": hyp.get("missing_tests", []),
            "notes": hyp.get("notes", ""),
            "status": "ruled_out",
        }
        self._db.update_hypothesis(hyp_id, data)
        logger.info("Ruled out hypothesis: %s", hyp.get("title"))

    def validate_against_new_data(
        self, user_id: int, new_canonical_names: set[str],
    ) -> list[dict]:
        """Validate active hypotheses against newly ingested lab data.

        For each active hypothesis with a pattern_id:
        - If a new lab matches a trigger/optional and IS abnormal
          in the expected direction -> boost confidence +0.10
        - If a new lab matches but is NORMAL -> reduce confidence -0.15
        - If confidence drops below 0.10 -> status='ruled_out'
        - Removes fulfilled tests from missing_tests

        Returns list of dicts describing what changed.
        """
        from healthbot.reasoning.hypothesis_generator import PATTERN_RULES
        from healthbot.reasoning.reference_ranges import get_range

        rules_by_id = {r["id"]: r for r in PATTERN_RULES}

        hypotheses = self._db.get_active_hypotheses(user_id)
        demographics = self._db.get_user_demographics(user_id)
        sex = demographics.get("sex") if demographics else None
        age = demographics.get("age") if demographics else None

        updates: list[dict] = []

        for hyp in hypotheses:
            status = hyp.get("_status", hyp.get("status", "active"))
            if status not in ("active", "investigating"):
                continue

            pattern_id = hyp.get("pattern_id", "")
            if not pattern_id or pattern_id not in rules_by_id:
                continue

            rule = rules_by_id[pattern_id]
            expected: dict[str, str] = {}
            expected.update(rule.get("triggers", {}))
            expected.update(rule.get("optional", {}))

            relevant = new_canonical_names & set(expected.keys())
            if not relevant:
                continue

            confidence = hyp.get(
                "confidence", hyp.get("_confidence", 0.5),
            )
            ev_for = list(hyp.get("evidence_for", []))
            ev_against = list(hyp.get("evidence_against", []))
            missing = list(hyp.get("missing_tests", []))
            changed = False

            for test_name in relevant:
                direction = expected[test_name]

                obs = self._db.query_observations(
                    record_type="lab_result",
                    canonical_name=test_name,
                    limit=1,
                    user_id=user_id,
                )
                if not obs:
                    continue

                try:
                    value = float(obs[0].get("value"))
                except (ValueError, TypeError):
                    continue

                ref = get_range(test_name, sex=sex, age=age)
                if not ref:
                    continue

                is_abnormal = False
                if direction == "low":
                    low = ref.get("low")
                    is_abnormal = low is not None and value < low
                elif direction == "high":
                    high = ref.get("high")
                    is_abnormal = high is not None and value > high

                if is_abnormal:
                    confidence += 0.10
                    note = (
                        f"{test_name} is {direction} ({value})"
                        " — supports hypothesis"
                    )
                    if note not in ev_for:
                        ev_for.append(note)
                    changed = True
                else:
                    confidence -= 0.15
                    note = (
                        f"{test_name} is normal ({value})"
                        f" — contradicts expected {direction}"
                    )
                    if note not in ev_against:
                        ev_against.append(note)
                    changed = True

                # Remove from missing_tests if present
                test_lower = test_name.lower().replace(" ", "_")
                missing = [
                    m for m in missing
                    if m.lower().replace(" ", "_") != test_lower
                    and m != test_name
                ]

            if not changed:
                continue

            confidence = max(0.0, min(confidence, 0.95))

            new_status = status
            if confidence < 0.10:
                new_status = "ruled_out"

            data = {
                "title": hyp.get("title", ""),
                "confidence": confidence,
                "evidence_for": ev_for,
                "evidence_against": ev_against,
                "missing_tests": missing,
                "notes": hyp.get("notes", ""),
                "status": new_status,
            }

            self._db.update_hypothesis(hyp["_id"], data)
            updates.append({
                "hyp_id": hyp["_id"],
                "title": hyp.get("title", ""),
                "confidence": confidence,
                "status": new_status,
            })

            logger.info(
                "Hypothesis validation: '%s' -> confidence=%.2f status=%s",
                hyp.get("title"), confidence, new_status,
            )

        return updates

    def confirm(self, hyp_id: str, reason: str = "") -> None:
        """Mark a hypothesis as confirmed."""
        hyp = self._db.get_hypothesis(hyp_id)
        if not hyp:
            raise ValueError(f"Hypothesis {hyp_id} not found")

        ev_for = list(hyp.get("evidence_for", []))
        if reason and reason not in ev_for:
            ev_for.append(reason)

        data = {
            "title": hyp.get("title", ""),
            "confidence": 1.0,
            "evidence_for": ev_for,
            "evidence_against": hyp.get("evidence_against", []),
            "missing_tests": [],
            "notes": hyp.get("notes", ""),
            "status": "confirmed",
        }
        self._db.update_hypothesis(hyp_id, data)
        logger.info("Confirmed hypothesis: %s", hyp.get("title"))
