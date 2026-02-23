"""Improvement pipeline — runs golden + failing cases, reports summary.

Gate: if any regression in golden cases, blocks the change.
"""
from __future__ import annotations

import sys
from pathlib import Path

from eval.runner import EvalRunner

EVAL_DIR = Path(__file__).parent
GOLDEN_PATH = EVAL_DIR / "golden_cases.jsonl"
PRIVACY_PATH = EVAL_DIR / "privacy_cases.jsonl"
UNIT_PATH = EVAL_DIR / "unit_cases.jsonl"
FAILING_PATH = EVAL_DIR / "failing_cases.jsonl"


def run_pipeline(evaluator=None, strict: bool = True) -> bool:
    """Run the full eval pipeline.

    Args:
        evaluator: Optional callable(input) -> str for LLM-based checks.
        strict: If True, exit with error on any golden test failure.

    Returns:
        True if all golden tests pass.
    """
    runner = EvalRunner()
    all_passed = True

    # 1. Golden test suite
    print("=" * 60)
    print("GOLDEN TEST SUITE")
    print("=" * 60)
    golden_cases = runner.load_cases(GOLDEN_PATH)
    if golden_cases:
        report = runner.run(golden_cases, evaluator)
        print(runner.format_report(report))
        if report.failed > 0:
            all_passed = False
    else:
        print("No golden test cases found.")

    # 2. Privacy test suite
    print()
    print("=" * 60)
    print("PRIVACY TEST SUITE")
    print("=" * 60)
    privacy_cases = runner.load_cases(PRIVACY_PATH)
    if privacy_cases:
        report = runner.run(privacy_cases, evaluator)
        print(runner.format_report(report))
        if report.failed > 0:
            all_passed = False
    else:
        print("No privacy test cases found.")

    # 3. Unit normalization test suite
    print()
    print("=" * 60)
    print("UNIT NORMALIZATION TEST SUITE")
    print("=" * 60)
    unit_cases = runner.load_cases(UNIT_PATH)
    if unit_cases:
        report = runner.run(unit_cases, evaluator)
        print(runner.format_report(report))
        if report.failed > 0:
            all_passed = False
    else:
        print("No unit normalization test cases found.")

    # 4. Failing cases (informational only — doesn't block)
    print()
    print("=" * 60)
    print("FAILING CASES (informational)")
    print("=" * 60)
    failing_cases = runner.load_cases(FAILING_PATH)
    if failing_cases:
        report = runner.run(failing_cases, evaluator)
        print(runner.format_report(report))
        print(f"\n  ({report.failed} known failing cases remaining)")
    else:
        print("No failing cases. Clean slate!")

    # Summary
    print()
    print("=" * 60)
    status = "PASS" if all_passed else "FAIL"
    print(f"PIPELINE RESULT: {status}")
    print("=" * 60)

    if strict and not all_passed:
        sys.exit(1)

    return all_passed


if __name__ == "__main__":
    run_pipeline()
