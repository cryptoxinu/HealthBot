"""Tests for importers/oura_client.py — OAuth flow and data sync (mocked)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.importers.oura_client import OuraAuthError, OuraClient


@pytest.fixture
def oura(config, vault) -> OuraClient:
    keychain = MagicMock()
    keychain.retrieve.side_effect = lambda key: {
        "oura_client_id": "test_oura_id",
        "oura_client_secret": "test_oura_secret",
    }.get(key)
    return OuraClient(config, keychain, vault)


class TestAuthUrl:
    def test_generates_url(self, oura):
        url, state = oura.get_authorization_url()
        assert "test_oura_id" in url
        assert "response_type=code" in url
        assert "daily" in url or "sleep" in url
        assert len(state) > 0

    def test_missing_client_id_raises(self, config, vault):
        keychain = MagicMock()
        keychain.retrieve.return_value = None
        client = OuraClient(config, keychain, vault)
        with pytest.raises(OuraAuthError, match="client_id"):
            client.get_authorization_url()


class TestExchangeCode:
    @pytest.mark.asyncio
    async def test_exchange_stores_tokens(self, oura):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "access_token": "oura_at_123",
            "refresh_token": "oura_rt_456",
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("healthbot.importers.oura_client.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client

            await oura.exchange_code("auth_code_123")

        assert oura._access_token == "oura_at_123"
        assert oura._refresh_token == "oura_rt_456"

    @pytest.mark.asyncio
    async def test_missing_credentials_raises(self, config, vault):
        keychain = MagicMock()
        keychain.retrieve.return_value = None
        client = OuraClient(config, keychain, vault)
        with pytest.raises(OuraAuthError, match="credentials"):
            await client.exchange_code("code")


class TestRefreshToken:
    @pytest.mark.asyncio
    async def test_refresh_updates_access_token(self, oura):
        oura._refresh_token = "oura_rt_old"

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"access_token": "oura_at_new"}
        mock_resp.raise_for_status = MagicMock()

        with patch("healthbot.importers.oura_client.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client

            await oura.refresh_access_token()

        assert oura._access_token == "oura_at_new"

    @pytest.mark.asyncio
    async def test_no_refresh_token_raises(self, oura):
        oura._refresh_token = None
        oura._vault.retrieve_blob = MagicMock(side_effect=Exception("not found"))
        with pytest.raises(OuraAuthError, match="refresh token"):
            await oura.refresh_access_token()


class TestSyncDaily:
    @pytest.mark.asyncio
    async def test_sync_inserts_records(self, oura, db):
        db.run_migrations()
        oura._access_token = "test_token"

        readiness_data = [
            {
                "day": "2025-01-15",
                "score": 82,
                "contributors": {
                    "resting_heart_rate": 58,
                    "hrv_balance": 42,
                },
            }
        ]
        sleep_data = [
            {
                "day": "2025-01-15",
                "score": 88,
                "contributors": {
                    "rem_sleep": 5400,
                    "deep_sleep": 7200,
                },
            }
        ]
        activity_data = [
            {
                "day": "2025-01-15",
                "active_calories": 450,
                "steps": 8500,
            }
        ]

        with patch.object(oura, "fetch_readiness", return_value=readiness_data):
            with patch.object(oura, "fetch_sleep", return_value=sleep_data):
                with patch.object(oura, "fetch_activity", return_value=activity_data):
                    count = await oura.sync_daily(db, days=7)

        assert count == 1
        records = db.query_wearable_daily(provider="oura", limit=10)
        assert len(records) == 1
        assert records[0].get("recovery_score") == 82


class TestPaginatedGet:
    @pytest.mark.asyncio
    async def test_handles_pagination(self, oura):
        oura._access_token = "test_token"

        page1 = {"data": [{"id": 1}, {"id": 2}], "next_token": "cursor_abc"}
        page2 = {"data": [{"id": 3}], "next_token": None}

        responses = [MagicMock(json=MagicMock(return_value=p)) for p in [page1, page2]]
        for r in responses:
            r.status_code = 200
            r.raise_for_status = MagicMock()

        with patch("healthbot.importers.oura_client.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.request = AsyncMock(side_effect=responses)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client

            results = await oura._paginated_get(
                "/v2/usercollection/daily_readiness", {"start_date": "2025-01-01"}
            )

        assert len(results) == 3
