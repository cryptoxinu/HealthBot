"""Tests for bot/formatters.py — pagination, escaping, formatting."""
from __future__ import annotations

import pytest

from healthbot.bot.formatters import (
    escape_md2,
    format_lab_result,
    format_score_bar,
    paginate,
)


class TestPaginate:
    def test_short_text_single_page(self):
        result = paginate("hello world")
        assert result == ["hello world"]

    def test_long_text_splits(self):
        text = "\n".join(f"Line {i}" for i in range(1000))
        pages = paginate(text, max_len=200)
        assert len(pages) > 1
        for page in pages:
            assert len(page) <= 200

    def test_respects_line_boundaries(self):
        text = "AAAA\nBBBB\nCCCC\nDDDD"
        pages = paginate(text, max_len=10)
        for page in pages:
            # Each page should end at a line boundary
            assert not page.endswith("\n")

    def test_empty_string(self):
        result = paginate("")
        assert result == [""]

    def test_single_long_line_preserved(self):
        text = "A" * 100
        pages = paginate(text, max_len=200)
        assert pages == [text]

    def test_exact_max_len(self):
        text = "A" * 4096
        pages = paginate(text, max_len=4096)
        assert pages == [text]


class TestEscapeMd2:
    def test_special_chars_escaped(self):
        result = escape_md2("hello_world*bold*")
        assert result == r"hello\_world\*bold\*"

    def test_plain_text_unchanged(self):
        result = escape_md2("hello world")
        assert result == "hello world"

    def test_brackets_escaped(self):
        result = escape_md2("[link](url)")
        assert r"\[link\]\(url\)" == result

    def test_empty_string(self):
        assert escape_md2("") == ""


class TestFormatScoreBar:
    def test_full_score(self):
        bar = format_score_bar(100.0, width=10)
        assert bar == "[##########]"

    def test_zero_score(self):
        bar = format_score_bar(0.0, width=10)
        assert bar == "[..........]"

    def test_half_score(self):
        bar = format_score_bar(50.0, width=10)
        assert bar == "[#####.....]"


class TestFormatLabResult:
    @pytest.mark.parametrize("value,status,expected", [
        ("95", "normal", "mg/dL"),
        ("250", "urgent", "!"),
        ("500", "critical", "!!"),
    ])
    def test_format_lab_result_status(self, value, status, expected):
        result = format_lab_result("Glucose", value, "mg/dL", status)
        assert "Glucose" in result
        assert expected in result

    def test_with_ref_text(self):
        result = format_lab_result("Glucose", "95", "mg/dL", "normal", ref_text="70-100")
        assert "70-100" in result

    def test_with_citation(self):
        result = format_lab_result(
            "Glucose", "95", "mg/dL", "normal", citation="[doc:abc123]"
        )
        assert "[doc:abc123]" in result
