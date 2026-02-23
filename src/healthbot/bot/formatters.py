"""Telegram message formatting utilities.

Handles MarkdownV2 escaping and message pagination.
"""
from __future__ import annotations

import re

MAX_MESSAGE_LENGTH = 4096


def strip_markdown(text: str) -> str:
    """Convert markdown to plain text for Telegram display.

    Strips: bold, italic, headers, code fences, horizontal rules,
    and converts markdown tables to aligned plain text.
    """
    # Remove code fences
    text = re.sub(r"```[\s\S]*?```", "", text)
    # Headers → plain text (remove # prefix)
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    # Bold/italic → plain text
    text = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", text)
    text = re.sub(r"_{1,3}(.+?)_{1,3}", r"\1", text)
    # Strikethrough
    text = re.sub(r"~~(.+?)~~", r"\1", text)
    # Links: [text](url) → text
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    # Inline code
    text = re.sub(r"`([^`]+)`", r"\1", text)
    # Blockquotes
    text = re.sub(r"^>\s?", "", text, flags=re.MULTILINE)
    # Horizontal rules
    text = re.sub(r"^-{3,}$", "", text, flags=re.MULTILINE)
    # Markdown table rows: strip pipes and dashes
    text = re.sub(r"^\|?[-:]+\|[-:|\s]+$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\|(.+)\|$", lambda m: m.group(1).replace("|", "  "), text, flags=re.MULTILINE)
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def escape_md2(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    special = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", text)


def paginate(text: str, max_len: int = MAX_MESSAGE_LENGTH) -> list[str]:
    """Split text into pages that fit Telegram's message limit."""
    if len(text) <= max_len:
        return [text]

    pages: list[str] = []
    lines = text.split("\n")
    current = ""

    for line in lines:
        if len(current) + len(line) + 1 > max_len:
            if current:
                pages.append(current)
            current = line
        else:
            current = f"{current}\n{line}" if current else line

    if current:
        pages.append(current)

    return pages


def format_score_bar(score: float, width: int = 10) -> str:
    """Generate a visual score bar."""
    filled = int(score / 100 * width)
    return "[" + "#" * filled + "." * (width - filled) + "]"


def format_lab_result(
    test_name: str,
    value: str,
    unit: str,
    triage: str,
    ref_text: str = "",
    citation: str = "",
) -> str:
    """Format a single lab result for display."""
    triage_emoji = {
        "normal": "",
        "watch": "~",
        "urgent": "!",
        "critical": "!!",
        "emergency": "!!!",
    }
    marker = triage_emoji.get(triage, "")
    line = f"{marker} {test_name}: {value} {unit}"
    if ref_text:
        line += f" (ref: {ref_text})"
    if citation:
        line += f" {citation}"
    return line.strip()
