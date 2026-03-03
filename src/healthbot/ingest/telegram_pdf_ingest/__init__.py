"""Telegram PDF ingestion pipeline.

Receives a PDF from Telegram as bytes, validates, encrypts original,
parses lab results, and stores everything in the vault.
Non-lab documents (doctor's notes, after-visit summaries, etc.) are
analyzed by Ollama to extract medical facts into LTM.
PDFs are NEVER saved to disk unencrypted.
"""
from healthbot.ingest.telegram_pdf_ingest.claude_extractor import (
    _CLAUDE_LAB_SYSTEM,
)
from healthbot.ingest.telegram_pdf_ingest.models import IngestResult
from healthbot.ingest.telegram_pdf_ingest.pipeline import TelegramPdfIngest

__all__ = [
    "IngestResult",
    "TelegramPdfIngest",
    "_CLAUDE_LAB_SYSTEM",
]
