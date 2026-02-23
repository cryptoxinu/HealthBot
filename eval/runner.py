"""Eval runner — loads JSONL test cases, runs checks, reports pass/fail.

Each test case has:
    - id: unique test case ID
    - category: "citation", "trend", "timeline", "privacy", "unit_norm", "performance"
    - input: the test input (varies by check type)
    - check_type: "contains", "not_contains", "citation_count_min", "max_ms", "equals"
    - expected: the expected value
    - description: human-readable description
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class EvalCase:
    """A single test case."""

    id: str
    category: str
    input: str | dict
    check_type: str
    expected: str | int | float
    description: str = ""


@dataclass
class EvalResult:
    """Result of running a single test case."""

    case_id: str
    passed: bool
    actual: str = ""
    message: str = ""
    duration_ms: float = 0.0


@dataclass
class EvalReport:
    """Full eval run report."""

    total: int = 0
    passed: int = 0
    failed: int = 0
    results: list[EvalResult] = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        return self.passed / self.total if self.total > 0 else 0.0


class EvalRunner:
    """Load and execute JSONL test cases against the system."""

    def __init__(self) -> None:
        self._checkers: dict[str, callable] = {
            "contains": self._check_contains,
            "not_contains": self._check_not_contains,
            "citation_count_min": self._check_citation_count_min,
            "max_ms": self._check_max_ms,
            "equals": self._check_equals,
        }

    def load_cases(self, jsonl_path: str | Path) -> list[EvalCase]:
        """Load test cases from a JSONL file."""
        cases = []
        path = Path(jsonl_path)
        if not path.exists():
            return cases

        for line in path.read_text().strip().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            data = json.loads(line)
            cases.append(EvalCase(
                id=data["id"],
                category=data.get("category", ""),
                input=data["input"],
                check_type=data["check_type"],
                expected=data["expected"],
                description=data.get("description", ""),
            ))
        return cases

    def run(self, cases: list[EvalCase], evaluator=None) -> EvalReport:
        """Run all test cases and produce a report.

        Args:
            cases: Test cases to run.
            evaluator: Optional callable(input) -> str that produces output.
                       If None, uses the input directly as the output (for
                       deterministic checks).
        """
        report = EvalReport(total=len(cases))

        for case in cases:
            start = time.perf_counter()

            if evaluator:
                try:
                    actual = evaluator(case.input)
                except Exception as e:
                    elapsed = (time.perf_counter() - start) * 1000
                    report.results.append(EvalResult(
                        case_id=case.id,
                        passed=False,
                        message=f"Evaluator error: {e}",
                        duration_ms=elapsed,
                    ))
                    report.failed += 1
                    continue
            else:
                actual = case.input if isinstance(case.input, str) else json.dumps(case.input)

            elapsed = (time.perf_counter() - start) * 1000

            checker = self._checkers.get(case.check_type)
            if not checker:
                report.results.append(EvalResult(
                    case_id=case.id,
                    passed=False,
                    message=f"Unknown check_type: {case.check_type}",
                    duration_ms=elapsed,
                ))
                report.failed += 1
                continue

            passed, message = checker(actual, case.expected, elapsed)
            report.results.append(EvalResult(
                case_id=case.id,
                passed=passed,
                actual=str(actual)[:200],
                message=message,
                duration_ms=elapsed,
            ))
            if passed:
                report.passed += 1
            else:
                report.failed += 1

        return report

    def format_report(self, report: EvalReport) -> str:
        """Format report for console output."""
        lines = [
            "EVAL REPORT",
            "=" * 50,
            f"Total: {report.total} | Passed: {report.passed} | Failed: {report.failed}",
            f"Pass rate: {report.pass_rate:.1%}",
            "",
        ]

        if report.failed > 0:
            lines.append("FAILURES:")
            lines.append("-" * 40)
            for r in report.results:
                if not r.passed:
                    lines.append(f"  FAIL {r.case_id}: {r.message}")
                    if r.actual:
                        lines.append(f"        actual: {r.actual[:100]}")

        return "\n".join(lines)

    # --- Checkers ---

    @staticmethod
    def _check_contains(actual: str, expected: str, _ms: float) -> tuple[bool, str]:
        if expected.lower() in actual.lower():
            return True, "OK"
        return False, f"Expected to contain '{expected}'"

    @staticmethod
    def _check_not_contains(actual: str, expected: str, _ms: float) -> tuple[bool, str]:
        if expected.lower() not in actual.lower():
            return True, "OK"
        return False, f"Should NOT contain '{expected}'"

    @staticmethod
    def _check_citation_count_min(actual: str, expected: int, _ms: float) -> tuple[bool, str]:
        count = actual.count("[") // 2  # rough count: each citation has [ and ]
        # More precise: count [xxx, yyy] style citations
        import re
        citations = re.findall(r"\[[^\]]*,\s*[^\]]*\]", actual)
        count = len(citations)
        if count >= expected:
            return True, f"OK ({count} citations)"
        return False, f"Expected >= {expected} citations, found {count}"

    @staticmethod
    def _check_max_ms(actual: str, expected: float, elapsed_ms: float) -> tuple[bool, str]:
        if elapsed_ms <= expected:
            return True, f"OK ({elapsed_ms:.1f}ms)"
        return False, f"Too slow: {elapsed_ms:.1f}ms > {expected}ms"

    @staticmethod
    def _check_equals(actual: str, expected: str, _ms: float) -> tuple[bool, str]:
        if actual.strip() == str(expected).strip():
            return True, "OK"
        return False, f"Expected '{expected}', got '{actual[:100]}'"
