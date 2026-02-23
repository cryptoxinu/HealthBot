"""Tests for PDF safety validation."""
from __future__ import annotations

import pytest

from healthbot.config import Config
from healthbot.security.pdf_safety import PdfSafety, PdfSafetyError


@pytest.fixture
def pdf_safety() -> PdfSafety:
    config = Config()
    config.max_pdf_size_bytes = 1024 * 1024  # 1 MB for tests
    config.max_pdf_pages = 10
    return PdfSafety(config)


# Minimal valid PDF (1 blank page)
MINIMAL_PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R>>endobj\n"
    b"xref\n0 4\n"
    b"0000000000 65535 f \n"
    b"0000000009 00000 n \n"
    b"0000000058 00000 n \n"
    b"0000000115 00000 n \n"
    b"trailer<</Size 4/Root 1 0 R>>\n"
    b"startxref\n183\n%%EOF"
)


class TestPdfSafety:
    """Test PDF safety checks."""

    def test_rejects_non_pdf(self, pdf_safety: PdfSafety) -> None:
        with pytest.raises(PdfSafetyError, match="Not a valid PDF"):
            pdf_safety.validate_bytes(b"This is not a PDF file")

    def test_rejects_empty(self, pdf_safety: PdfSafety) -> None:
        with pytest.raises(PdfSafetyError, match="Not a valid PDF"):
            pdf_safety.validate_bytes(b"")

    def test_rejects_oversized(self, pdf_safety: PdfSafety) -> None:
        # Create data that exceeds max size (1MB for tests)
        large_data = b"%PDF-" + b"A" * (1024 * 1024 + 1)
        with pytest.raises(PdfSafetyError, match="too large"):
            pdf_safety.validate_bytes(large_data)

    def test_rejects_encrypted_pdf(self, pdf_safety: PdfSafety) -> None:
        data = b"%PDF-1.4\n/Encrypt some_encryption_dict"
        with pytest.raises(PdfSafetyError, match="Encrypted"):
            pdf_safety.validate_bytes(data)

    def test_rejects_javascript(self, pdf_safety: PdfSafety) -> None:
        data = b"%PDF-1.4\n/JavaScript (alert('xss'))"
        with pytest.raises(PdfSafetyError, match="dangerous action"):
            pdf_safety.validate_bytes(data)

    def test_rejects_launch_action(self, pdf_safety: PdfSafety) -> None:
        data = b"%PDF-1.4\n/Launch /some/command"
        with pytest.raises(PdfSafetyError, match="dangerous action"):
            pdf_safety.validate_bytes(data)

    def test_rejects_js_action(self, pdf_safety: PdfSafety) -> None:
        data = b"%PDF-1.4\n/JS (malicious)"
        with pytest.raises(PdfSafetyError, match="dangerous action"):
            pdf_safety.validate_bytes(data)

    def test_rejects_openaction(self, pdf_safety: PdfSafety) -> None:
        data = b"%PDF-1.4\n/OpenAction something"
        with pytest.raises(PdfSafetyError, match="dangerous action"):
            pdf_safety.validate_bytes(data)

    def test_encrypted_blob_not_pdf(self, pdf_safety: PdfSafety) -> None:
        """Encrypted blob should NOT begin with %PDF-."""
        import tempfile
        from pathlib import Path

        from healthbot.config import Config
        from healthbot.security.key_manager import KeyManager
        from healthbot.security.vault import Vault

        with tempfile.TemporaryDirectory() as td:
            cfg = Config(vault_home=Path(td))
            cfg.ensure_dirs()
            km = KeyManager(cfg)
            km.setup("testpass")
            v = Vault(cfg.blobs_dir, km)
            blob_id = v.store_blob(MINIMAL_PDF)
            enc_path = cfg.blobs_dir / f"{blob_id}.enc"
            enc_data = enc_path.read_bytes()
            assert not enc_data.startswith(b"%PDF-"), "Encrypted blob must not start with %PDF-"
