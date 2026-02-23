"""Encrypted numpy vector storage.

Stores TF-IDF vectors and dense embedding matrices as encrypted arrays via the vault.
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import numpy as np
from scipy import sparse

from healthbot.security.vault import Vault


class VectorStore:
    """Encrypted numpy vector persistence."""

    def __init__(self, vault: Vault, vectors_dir: Path) -> None:
        self._vault = vault
        self._dir = vectors_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    def save_sparse_matrix(
        self, name: str, matrix: sparse.csr_matrix, doc_ids: list[str]
    ) -> None:
        """Serialize and encrypt a sparse matrix with its document ID mapping."""
        buf = io.BytesIO()
        sparse.save_npz(buf, matrix)
        matrix_bytes = buf.getvalue()

        meta = json.dumps({"doc_ids": doc_ids}).encode("utf-8")

        # Combine: 4-byte length prefix for meta, then meta, then matrix
        meta_len = len(meta).to_bytes(4, "big")
        combined = meta_len + meta + matrix_bytes

        self._vault.store_blob(combined, blob_id=f"vec_{name}")

    def load_sparse_matrix(
        self, name: str
    ) -> tuple[sparse.csr_matrix, list[str]] | None:
        """Load and decrypt a sparse matrix."""
        blob_id = f"vec_{name}"
        if not self._vault.blob_exists(blob_id):
            return None

        combined = self._vault.retrieve_blob(blob_id)
        meta_len = int.from_bytes(combined[:4], "big")
        meta = json.loads(combined[4 : 4 + meta_len].decode("utf-8"))
        matrix_bytes = combined[4 + meta_len :]

        buf = io.BytesIO(matrix_bytes)
        matrix = sparse.load_npz(buf)
        return matrix, meta["doc_ids"]

    def save_vocabulary(
        self, name: str, vocabulary: dict[str, int],
        idf: list[float] | None = None,
    ) -> None:
        """Save TF-IDF vocabulary and IDF weights."""
        # Convert numpy int64 to Python int for JSON serialization
        clean_vocab = {k: int(v) for k, v in vocabulary.items()}
        payload = {"vocabulary": clean_vocab}
        if idf is not None:
            payload["idf"] = idf
        data = json.dumps(payload).encode("utf-8")
        self._vault.store_blob(data, blob_id=f"vocab_{name}")

    def load_vocabulary(
        self, name: str
    ) -> tuple[dict[str, int], list[float] | None] | None:
        """Load TF-IDF vocabulary and IDF weights."""
        blob_id = f"vocab_{name}"
        if not self._vault.blob_exists(blob_id):
            return None
        raw = self._vault.retrieve_blob(blob_id)
        payload = json.loads(raw.decode("utf-8"))
        # Handle both old (dict-only) and new (dict+idf) formats
        if isinstance(payload, dict) and "vocabulary" in payload:
            return payload["vocabulary"], payload.get("idf")
        # Legacy: raw vocabulary dict
        return payload, None

    def save_dense_matrix(
        self, name: str, matrix: np.ndarray, doc_ids: list[str]
    ) -> None:
        """Serialize and encrypt a dense numpy matrix with doc ID mapping."""
        buf = io.BytesIO()
        np.save(buf, matrix)
        matrix_bytes = buf.getvalue()

        meta = json.dumps({"doc_ids": doc_ids}).encode("utf-8")
        meta_len = len(meta).to_bytes(4, "big")
        combined = meta_len + meta + matrix_bytes

        self._vault.store_blob(combined, blob_id=f"dense_{name}")

    def load_dense_matrix(
        self, name: str
    ) -> tuple[np.ndarray, list[str]] | None:
        """Load and decrypt a dense numpy matrix."""
        blob_id = f"dense_{name}"
        if not self._vault.blob_exists(blob_id):
            return None

        combined = self._vault.retrieve_blob(blob_id)
        meta_len = int.from_bytes(combined[:4], "big")
        meta = json.loads(combined[4 : 4 + meta_len].decode("utf-8"))
        matrix_bytes = combined[4 + meta_len :]

        buf = io.BytesIO(matrix_bytes)
        matrix = np.load(buf)
        return matrix, meta["doc_ids"]
