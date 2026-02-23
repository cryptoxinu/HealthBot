"""Tests for encrypted blob storage."""
from __future__ import annotations

from unittest.mock import MagicMock

import cryptography.exceptions
import pytest

from healthbot.security.vault import Vault


def _mock_km(key: bytes = b"\x01" * 32):
    km = MagicMock()
    km.get_key.return_value = key
    return km


class TestVaultStoreRetrieve:
    def test_store_and_retrieve(self, tmp_path):
        vault = Vault(tmp_path, _mock_km())
        data = b"Hello, encrypted world!"
        blob_id = vault.store_blob(data)
        assert vault.blob_exists(blob_id)
        retrieved = vault.retrieve_blob(blob_id)
        assert retrieved == data

    def test_store_with_custom_id(self, tmp_path):
        vault = Vault(tmp_path, _mock_km())
        blob_id = vault.store_blob(b"test", blob_id="custom123")
        assert blob_id == "custom123"
        assert vault.blob_exists("custom123")

    def test_store_creates_enc_file(self, tmp_path):
        vault = Vault(tmp_path, _mock_km())
        blob_id = vault.store_blob(b"test")
        assert (tmp_path / f"{blob_id}.enc").exists()

    def test_retrieve_wrong_key_fails(self, tmp_path):
        key1 = b"\x01" * 32
        key2 = b"\x02" * 32
        vault1 = Vault(tmp_path, _mock_km(key1))
        blob_id = vault1.store_blob(b"secret data")
        vault2 = Vault(tmp_path, _mock_km(key2))
        with pytest.raises(cryptography.exceptions.InvalidTag):
            vault2.retrieve_blob(blob_id)

    def test_large_blob(self, tmp_path):
        vault = Vault(tmp_path, _mock_km())
        data = b"x" * 1_000_000
        blob_id = vault.store_blob(data)
        assert vault.retrieve_blob(blob_id) == data


class TestVaultDelete:
    def test_delete_removes_file(self, tmp_path):
        vault = Vault(tmp_path, _mock_km())
        blob_id = vault.store_blob(b"test")
        vault.delete_blob(blob_id)
        assert not vault.blob_exists(blob_id)
        assert not (tmp_path / f"{blob_id}.enc").exists()

    def test_delete_nonexistent_noop(self, tmp_path):
        vault = Vault(tmp_path, _mock_km())
        vault.delete_blob("nonexistent")  # Should not raise


class TestVaultList:
    def test_list_empty(self, tmp_path):
        vault = Vault(tmp_path, _mock_km())
        assert vault.list_blobs() == []

    def test_list_multiple(self, tmp_path):
        vault = Vault(tmp_path, _mock_km())
        ids = set()
        for i in range(3):
            ids.add(vault.store_blob(f"blob{i}".encode()))
        assert set(vault.list_blobs()) == ids

    def test_blob_exists_false(self, tmp_path):
        vault = Vault(tmp_path, _mock_km())
        assert vault.blob_exists("nonexistent") is False
