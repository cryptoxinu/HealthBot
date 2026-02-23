"""Failure capture — writes failed cases to the vault directory.

Used by the /feedback command and the eval pipeline.
Runtime output goes to ~/.healthbot/eval/ to keep the source tree clean.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

FAILING_CASES_PATH = Path.home() / ".healthbot" / "eval" / "failing_cases.jsonl"


def capture_failure(
    user_input: str,
    bot_response: str,
    user_feedback: str = "",
    category: str = "user_report",
) -> None:
    """Append a failing case to the failure log."""
    entry = {
        "id": f"user_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}",
        "category": category,
        "input": user_input,
        "bot_response": bot_response,
        "user_feedback": user_feedback,
        "timestamp": datetime.now(UTC).isoformat(),
        "status": "new",
    }
    FAILING_CASES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(FAILING_CASES_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")


def promote_to_golden(case_id: str, golden_path: str | Path) -> bool:
    """Move a fixed case from failing_cases.jsonl to a golden suite.

    The case must have been manually edited to include check_type + expected.
    """
    if not FAILING_CASES_PATH.exists():
        return False

    lines = FAILING_CASES_PATH.read_text().strip().splitlines()
    remaining = []
    promoted = None

    for line in lines:
        data = json.loads(line)
        if data.get("id") == case_id:
            if "check_type" not in data or "expected" not in data:
                return False  # Not ready for promotion
            data["status"] = "promoted"
            promoted = data
        else:
            remaining.append(line)

    if not promoted:
        return False

    # Append to golden suite
    golden = Path(golden_path)
    golden_entry = {
        "id": promoted["id"],
        "category": promoted.get("category", "user_report"),
        "input": promoted["input"],
        "check_type": promoted["check_type"],
        "expected": promoted["expected"],
        "description": promoted.get("user_feedback", ""),
    }
    with open(golden, "a") as f:
        f.write(json.dumps(golden_entry) + "\n")

    # Rewrite failing_cases without the promoted one
    FAILING_CASES_PATH.write_text("\n".join(remaining) + "\n" if remaining else "")
    return True
