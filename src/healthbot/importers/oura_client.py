"""Oura Ring API client with OAuth 2.0.

Follows the same pattern as whoop_client.py:
- OAuth ONLY (no password scraping)
- Credentials in macOS Keychain
- Async httpx with auto-refresh on 401
- Maps to WhoopDaily model with provider="oura"
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
from datetime import date, datetime, timedelta
from urllib.parse import urlencode

import httpx

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import WhoopDaily
from healthbot.security.keychain import Keychain
from healthbot.security.vault import Vault

logger = logging.getLogger("healthbot")


class OuraAuthError(Exception):
    """Raised when Oura authentication fails."""


class OuraAPIError(Exception):
    """Raised when an Oura API request fails."""


class OuraClient:
    """Oura Ring API async client with OAuth 2.0."""

    SCOPES = "daily heartrate personal sleep workout"

    def __init__(self, config: Config, keychain: Keychain, vault: Vault) -> None:
        self._config = config
        self._keychain = keychain
        self._vault = vault
        self._access_token: str | None = None
        self._refresh_token: str | None = None

    def get_authorization_url(
        self, redirect_uri: str = "http://localhost:8765/callback"
    ) -> tuple[str, str]:
        """Generate OAuth authorization URL.

        Returns:
            (authorization_url, state_token) for CSRF validation.
        """
        client_id = self._keychain.retrieve("oura_client_id")
        if not client_id:
            raise OuraAuthError(
                "Oura client_id not found in Keychain. "
                "Run: healthbot --setup and configure Oura credentials."
            )
        state = secrets.token_urlsafe(32)
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": self.SCOPES,
            "state": state,
        }
        return f"{self._config.oura_auth_url}?{urlencode(params)}", state

    async def exchange_code(
        self, code: str, redirect_uri: str = "http://localhost:8765/callback"
    ) -> None:
        """Exchange authorization code for tokens."""
        client_id = self._keychain.retrieve("oura_client_id")
        client_secret = self._keychain.retrieve("oura_client_secret")
        if not client_id or not client_secret:
            raise OuraAuthError("Oura credentials not found in Keychain.")

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                self._config.oura_token_url,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        access_token = data.get("access_token")
        if not access_token:
            raise OuraAuthError(
                "Oura token response missing access_token. "
                "Check your Oura credentials and try again."
            )
        self._access_token = access_token
        self._refresh_token = data.get("refresh_token")
        if self._refresh_token:
            self._vault.store_blob(
                self._refresh_token.encode(), blob_id="oura_refresh_token"
            )

    async def refresh_access_token(self) -> None:
        """Refresh the access token."""
        if not self._refresh_token:
            try:
                rt_bytes = self._vault.retrieve_blob("oura_refresh_token")
                self._refresh_token = rt_bytes.decode()
            except Exception as exc:
                raise OuraAuthError(
                    "No refresh token available. Re-authorize."
                ) from exc

        client_id = self._keychain.retrieve("oura_client_id")
        client_secret = self._keychain.retrieve("oura_client_secret")

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                self._config.oura_token_url,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
            )
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise OuraAPIError(
                    f"Token refresh failed (HTTP {resp.status_code})"
                ) from exc
            data = resp.json()

        self._access_token = data["access_token"]
        if "refresh_token" in data:
            self._refresh_token = data["refresh_token"]
            self._vault.store_blob(
                self._refresh_token.encode(), blob_id="oura_refresh_token"
            )

    async def fetch_readiness(self, start: str, end: str) -> list[dict]:
        """Fetch daily readiness data."""
        return await self._paginated_get(
            "/v2/usercollection/daily_readiness",
            {"start_date": start, "end_date": end},
        )

    async def fetch_sleep(self, start: str, end: str) -> list[dict]:
        """Fetch daily sleep data."""
        return await self._paginated_get(
            "/v2/usercollection/daily_sleep",
            {"start_date": start, "end_date": end},
        )

    async def fetch_activity(self, start: str, end: str) -> list[dict]:
        """Fetch daily activity data."""
        return await self._paginated_get(
            "/v2/usercollection/daily_activity",
            {"start_date": start, "end_date": end},
        )

    async def sync_daily(
        self,
        db: HealthDB,
        days: int = 30,
        user_id: int = 0,
        clean_db: object | None = None,
    ) -> int:
        """Sync Oura readiness, sleep, and activity data. Returns records synced.

        If clean_db is provided, writes directly to Clean DB (wearable data
        is purely numeric, no PII — skips encrypt/decrypt overhead).
        """
        end = date.today().isoformat()
        start = (date.today() - timedelta(days=days)).isoformat()

        readiness_data = await self.fetch_readiness(start, end)
        sleep_data = await self.fetch_sleep(start, end)
        activity_data = await self.fetch_activity(start, end)

        # Index by date for merging
        sleep_by_date: dict[str, dict] = {}
        for s in sleep_data:
            d = s.get("day", "")
            contributors = s.get("contributors", {})
            ts_end = s.get("timestamp_end")
            ts_start = s.get("timestamp_start")
            dur = None
            if ts_end and ts_start:
                try:
                    if isinstance(ts_end, (int, float)):
                        dur = _sec_to_min(ts_end - ts_start)
                    else:
                        dt_end = datetime.fromisoformat(str(ts_end))
                        dt_start = datetime.fromisoformat(str(ts_start))
                        dur = int((dt_end - dt_start).total_seconds() / 60)
                except (ValueError, TypeError):
                    dur = None
            # Use actual physiological values, not contributor scores (0-100).
            # Oura API provides actual minutes in total_rem_sleep_duration etc.
            rem_sec = s.get("rem_sleep_duration") or s.get("total_rem_sleep_duration")
            deep_sec = s.get("deep_sleep_duration") or s.get("total_deep_sleep_duration")
            light_sec = s.get("light_sleep_duration") or s.get("total_light_sleep_duration")
            sleep_by_date[d] = {
                "sleep_score": s.get("score"),
                "sleep_duration_min": dur,
                "rem_min": _sec_to_min(rem_sec) if rem_sec else None,
                "deep_min": _sec_to_min(deep_sec) if deep_sec else None,
                "light_min": _sec_to_min(light_sec) if light_sec else None,
                "lowest_rhr": s.get("lowest_heart_rate"),
                "avg_hrv": s.get("average_hrv"),  # actual HRV in ms
                "avg_resp_rate": s.get("average_breath"),
            }

        activity_by_date: dict[str, dict] = {}
        for a in activity_data:
            d = a.get("day", "")
            activity_by_date[d] = {
                "calories": a.get("active_calories"),
                "steps": a.get("steps"),
            }

        count = 0
        for item in readiness_data:
            d = item.get("day") or end
            try:
                parsed_date = date.fromisoformat(d)
            except (ValueError, TypeError):
                logger.warning("Oura: skipping record with invalid date: %s", d)
                continue
            contributors = item.get("contributors", {})
            sleep = sleep_by_date.get(d, {})
            activity = activity_by_date.get(d, {})

            wd = WhoopDaily(
                id=hashlib.sha256(f"oura-{d}".encode()).hexdigest(),
                date=parsed_date,
                recovery_score=item.get("score"),
                rhr=sleep.get("lowest_rhr"),  # actual BPM, not contributor score
                hrv=sleep.get("avg_hrv"),  # actual ms, not contributor score
                spo2=None,
                skin_temp=contributors.get("body_temperature"),
                sleep_score=sleep.get("sleep_score"),
                sleep_duration_min=sleep.get("sleep_duration_min"),
                rem_min=sleep.get("rem_min"),  # actual minutes, not score
                deep_min=sleep.get("deep_min"),  # actual minutes, not score
                light_min=sleep.get("light_min"),
                resp_rate=sleep.get("avg_resp_rate"),
                strain=None,
                calories=activity.get("calories"),
                provider="oura",
            )
            # Always write to raw vault (Tier 1 source of truth)
            db.insert_wearable_daily(wd, user_id=user_id)
            # Also write to Clean DB if available
            if clean_db is not None:
                clean_db.upsert_wearable(
                    wearable_id=wd.id,
                    date=wd.date.isoformat(),
                    provider="oura",
                    hrv=wd.hrv,
                    rhr=wd.rhr,
                    resp_rate=wd.resp_rate,
                    spo2=wd.spo2,
                    sleep_score=wd.sleep_score,
                    recovery_score=wd.recovery_score,
                    strain=wd.strain,
                    sleep_duration_min=wd.sleep_duration_min,
                    rem_min=wd.rem_min,
                    deep_min=wd.deep_min,
                    light_min=wd.light_min,
                    calories=wd.calories,
                    skin_temp=wd.skin_temp,
                )
            count += 1

        return count

    async def _paginated_get(self, endpoint: str, params: dict) -> list[dict]:
        """Handle Oura's cursor-based pagination."""
        all_records: list[dict] = []
        next_token = None
        # Copy to avoid mutating the caller's dict with pagination tokens
        params = {**params}

        for _page in range(100):  # Max 100 pages guard to prevent infinite loops
            if next_token:
                params["next_token"] = next_token
            resp = await self._authed_request("GET", endpoint, params=params)
            data = resp.json()
            records = data.get("data", [])
            all_records.extend(records)
            next_token = data.get("next_token")
            if not next_token or not records:
                break

        return all_records

    async def _authed_request(
        self, method: str, path: str, **kwargs
    ) -> httpx.Response:
        """Make an authenticated request. Auto-refresh on 401.

        Retries up to 3 times with exponential backoff on transient errors
        (ConnectError, TimeoutException, 502/503/504).
        """
        if not self._access_token:
            await self.refresh_access_token()

        url = f"{self._config.oura_api_base}{path}"
        headers = {"Authorization": f"Bearer {self._access_token}"}

        async with httpx.AsyncClient(timeout=30.0) as client:
            last_exc: Exception | None = None
            refreshed = False
            for attempt in range(3):
                try:
                    resp = await client.request(method, url, headers=headers, **kwargs)
                    if resp.status_code == 401 and not refreshed:
                        refreshed = True
                        await self.refresh_access_token()
                        headers["Authorization"] = f"Bearer {self._access_token}"
                        resp = await client.request(method, url, headers=headers, **kwargs)
                    if resp.status_code in (502, 503, 504) and attempt < 2:
                        logger.warning(
                            "Oura %s %s returned %d, retrying (%d/2)",
                            method, path, resp.status_code, attempt + 1,
                        )
                        await asyncio.sleep(1.0 * (2 ** attempt))
                        continue
                    if resp.status_code >= 400:
                        if attempt < 2:
                            await asyncio.sleep(1.0 * (2 ** attempt))
                            continue
                        try:
                            resp.raise_for_status()
                        except httpx.HTTPStatusError as exc:
                            raise OuraAPIError(
                                f"Oura {method} {path} failed (HTTP {resp.status_code})"
                            ) from exc
                    return resp
                except (httpx.ConnectError, httpx.TimeoutException) as exc:
                    last_exc = exc
                    if attempt < 2:
                        logger.warning(
                            "Oura %s %s failed (%s), retrying (%d/2)",
                            method, path, type(exc).__name__, attempt + 1,
                        )
                        await asyncio.sleep(1.0 * (2 ** attempt))
                        continue
                    raise
            raise last_exc  # type: ignore[misc]


def _sec_to_min(seconds: int | float | None) -> int | None:
    """Convert seconds to minutes, handling None."""
    if seconds is None:
        return None
    return int(seconds / 60)
