"""Tests for healthbot.bot.formatters — Telegram message formatting."""
from __future__ import annotations

from healthbot.bot.formatters import (
    escape_md2,
    format_lab_result,
    format_score_bar,
    paginate,
)


class TestEscapeMd2:
    def test_escapes_special_chars(self) -> None:
        result = escape_md2("Hello *world* [test]")
        assert "\\*" in result
        assert "\\[" in result
        assert "\\]" in result

    def test_plain_text_unchanged(self) -> None:
        assert escape_md2("hello") == "hello"

    def test_escapes_underscore(self) -> None:
        result = escape_md2("snake_case")
        assert "\\_" in result


class TestPaginate:
    def test_short_text_single_page(self) -> None:
        pages = paginate("short text")
        assert len(pages) == 1
        assert pages[0] == "short text"

    def test_long_text_splits(self) -> None:
        text = "\n".join(f"line {i}" for i in range(200))
        pages = paginate(text, max_len=100)
        assert len(pages) > 1
        # All content should be preserved
        combined = "\n".join(pages)
        assert "line 0" in combined
        assert "line 199" in combined

    def test_respects_max_len(self) -> None:
        text = "\n".join(f"line {i}" for i in range(100))
        pages = paginate(text, max_len=50)
        for page in pages:
            assert len(page) <= 50 or "\n" not in page  # Single line may exceed

    def test_empty_text(self) -> None:
        pages = paginate("")
        assert pages == [""]


class TestFormatScoreBar:
    def test_full_bar(self) -> None:
        bar = format_score_bar(100, width=10)
        assert bar == "[##########]"

    def test_empty_bar(self) -> None:
        bar = format_score_bar(0, width=10)
        assert bar == "[..........]"

    def test_half_bar(self) -> None:
        bar = format_score_bar(50, width=10)
        assert bar == "[#####.....]"


class TestFormatLabResult:
    def test_normal_result(self) -> None:
        result = format_lab_result("Glucose", "100", "mg/dL", "normal")
        assert "Glucose" in result
        assert "100" in result
        assert "mg/dL" in result

    def test_urgent_result_has_marker(self) -> None:
        result = format_lab_result("LDL", "200", "mg/dL", "urgent")
        assert "!" in result

    def test_with_reference(self) -> None:
        result = format_lab_result("TSH", "2.5", "mIU/L", "normal", ref_text="0.5-4.5")
        assert "0.5-4.5" in result

    def test_with_citation(self) -> None:
        result = format_lab_result("HbA1c", "5.7", "%", "normal", citation="[L1]")
        assert "[L1]" in result
