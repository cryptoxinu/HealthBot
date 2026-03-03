"""Lab report PDF parsing.

Extracts structured lab results from PDF lab reports using pdfminer.six.
Dual extraction: regex patterns (fast, deterministic) + Ollama LLM (catches
non-standard formats). Results are merged — union of both, deduped by
canonical name + page. This maximizes accuracy at the cost of one LLM call.
"""
from pdfminer.high_level import extract_text  # noqa: F401 — patched by tests

from healthbot.ingest.lab_pdf_parser.helpers import (
    _DATE_PATTERNS,
    _DOB_LABELS,
    _LAB_NAME_PATTERNS,
    _MONTH_NAMES,
    ParsedPage,
    _adjust_confidence,
    _parse_numeric,
    _replace_result,
    _values_match,
)
from healthbot.ingest.lab_pdf_parser.ollama_parser import (
    _LAB_PARSE_SYSTEM,
    _MED_MODEL,
)
from healthbot.ingest.lab_pdf_parser.parser_core import LabPdfParser
from healthbot.ingest.lab_pdf_parser.regex_parser import (
    _BAD_TEST_NAMES,
    _COMMA_TEST_NAMES,
    _HAS_LETTER,
    _REF_RANGE,
    _RESULT_PATTERNS,
    _VALID_UNIT,
)

__all__ = [
    "LabPdfParser",
    "ParsedPage",
    # helpers
    "_adjust_confidence",
    "_parse_numeric",
    "_replace_result",
    "_values_match",
    "_DATE_PATTERNS",
    "_DOB_LABELS",
    "_LAB_NAME_PATTERNS",
    "_MONTH_NAMES",
    # regex_parser
    "_BAD_TEST_NAMES",
    "_COMMA_TEST_NAMES",
    "_HAS_LETTER",
    "_REF_RANGE",
    "_RESULT_PATTERNS",
    "_VALID_UNIT",
    # ollama_parser
    "_LAB_PARSE_SYSTEM",
    "_MED_MODEL",
]
