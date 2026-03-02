"""WHOOP API v2 client with OAuth 2.0.

Uses httpx for async HTTP. OAuth ONLY — never password scraping.
Credentials stored in macOS Keychain.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
from datetime import date
from urllib.parse import urlencode

import httpx

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import WhoopDaily
from healthbot.security.keychain import Keychain
from healthbot.security.vault import Vault

logger = logging.getLogger("healthbot")


class WhoopAuthError(Exception):
    """Raised when WHOOP authentication fails."""


class WhoopClient:
    """WHOOP API v2 async client with OAuth."""

    SCOPES = "offline read:recovery read:sleep read:workout read:cycles"

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
        client_id = self._keychain.retrieve("whoop_client_id")
        if not client_id:
            raise WhoopAuthError(
                "WHOOP client_id not found in Keychain. "
                "Run: healthbot --setup and configure WHOOP credentials."
            )
        state = secrets.token_urlsafe(32)
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": self.SCOPES,
            "state": state,
        }
        return f"{self._config.whoop_auth_url}?{urlencode(params)}", state

    async def exchange_code(
        self, code: str, redirect_uri: str = "http://localhost:8765/callback"
    ) -> None:
        """Exchange authorization code for tokens."""
        client_id = self._keychain.retrieve("whoop_client_id")
        client_secret = self._keychain.retrieve("whoop_client_secret")
        if not client_id or not client_secret:
            raise WhoopAuthError("WHOOP credentials not found in Keychain.")

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                self._config.whoop_token_url,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
            )
            if resp.status_code >= 400:
                raise WhoopAuthError(
                    f"Token exchange failed ({resp.status_code}). Check your WHOOP credentials."
                )
            try:
                data = resp.json()
            except Exception as exc:
                raise WhoopAuthError("WHOOP returned invalid token response.") from exc

        if "access_token" not in data:
            raise WhoopAuthError("WHOOP token response missing access_token.")
        self._access_token = data["access_token"]
        self._refresh_token = data.get("refresh_token")
        # Store refresh token encrypted
        if self._refresh_token:
            self._vault.store_blob(
                self._refresh_token.encode(), blob_id="whoop_refresh_token"
            )

    async def refresh_access_token(self) -> None:
        """Refresh the access token."""
        if not self._refresh_token:
            # Try loading from vault
            try:
                rt_bytes = self._vault.retrieve_blob("whoop_refresh_token")
                self._refresh_token = rt_bytes.decode()
            except Exception as exc:
                raise WhoopAuthError(
                    "No refresh token available. Re-authorize with /whoop_auth.",
                ) from exc

        client_id = self._keychain.retrieve("whoop_client_id")
        client_secret = self._keychain.retrieve("whoop_client_secret")
        if not client_id or not client_secret:
            raise WhoopAuthError("WHOOP credentials not found in Keychain. Run /whoop_auth.")

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                self._config.whoop_token_url,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
            )
            if resp.status_code >= 400:
                raise WhoopAuthError(
                    f"Token refresh failed ({resp.status_code}). Re-authorize with /whoop_auth."
                )
            try:
                data = resp.json()
            except Exception as exc:
                raise WhoopAuthError("WHOOP returned invalid refresh response.") from exc

        if "access_token" not in data:
            raise WhoopAuthError("WHOOP refresh response missing access_token.")
        self._access_token = data["access_token"]
        if "refresh_token" in data:
            self._refresh_token = data["refresh_token"]
        # Always persist the current refresh token (server may not rotate it)
        if self._refresh_token:
            self._vault.store_blob(
                self._refresh_token.encode(), blob_id="whoop_refresh_token"
            )

    async def fetch_recovery(self, start: str, end: str) -> list[dict]:
        """Fetch recovery data for a date range."""
        return await self._paginated_get(
            "/recovery", {"start": start, "end": end}
        )

    async def fetch_sleep(self, start: str, end: str) -> list[dict]:
        """Fetch sleep data for a date range."""
        return await self._paginated_get(
            "/activity/sleep", {"start": start, "end": end}
        )

    async def fetch_workouts(self, start: str, end: str) -> list[dict]:
        """Fetch workout data for a date range."""
        return await self._paginated_get(
            "/activity/workout", {"start": start, "end": end}
        )

    async def sync_daily(
        self,
        db: HealthDB,
        days: int = 30,
        user_id: int = 0,
        clean_db: object | None = None,
    ) -> int:
        """Sync WHOOP recovery, sleep, and workout data. Returns records synced.

        If clean_db is provided, writes directly to Clean DB (skips
        encrypt/decrypt cycle — wearable data is purely numeric, no PII).
        """
        from datetime import timedelta

        end = date.today().isoformat() + "T23:59:59.999Z"
        start = (date.today() - timedelta(days=days)).isoformat() + "T00:00:00.000Z"
        logger.info("WHOOP sync: fetching %d days (%s to %s)", days, start[:10], end[:10])

        # Fetch all data types
        recovery_data = await self.fetch_recovery(start, end)
        sleep_data = await self.fetch_sleep(start, end)
        workout_data = await self.fetch_workouts(start, end)
        logger.info(
            "WHOOP API returned: %d recovery, %d sleep, %d workout records",
            len(recovery_data or []), len(sleep_data or []), len(workout_data or []),
        )

        if not recovery_data and not sleep_data:
            logger.info("WHOOP sync: no data returned, skipping")
            return 0

        # Index sleep and workout data by date for merging
        sleep_by_date: dict[str, dict] = {}
        for s in (sleep_data or []):
            d = s.get("created_at", "")[:10]
            score = s.get("score") or {}
            stages = score.get("stage_summary") or {}
            bed = stages.get("total_in_bed_time_milli")
            rem = stages.get("total_rem_sleep_time_milli")
            deep = stages.get("total_slow_wave_sleep_time_milli")
            light = stages.get("total_light_sleep_time_milli")
            latency = stages.get("latency_time_milli")
            sleep_by_date[d] = {
                "sleep_score": score.get("sleep_performance_percentage"),
                "sleep_duration_min": bed // 60000 if bed else None,
                "rem_min": rem // 60000 if rem else None,
                "deep_min": deep // 60000 if deep else None,
                "light_min": light // 60000 if light else None,
                "resp_rate": score.get("respiratory_rate"),
                "sleep_latency_min": (
                    latency / 60000 if latency else None
                ),
                "wake_episodes": score.get("disturbance_count"),
                "sleep_efficiency_pct": score.get(
                    "sleep_efficiency_percentage",
                ),
            }

        strain_by_date: dict[str, float] = {}
        calories_by_date: dict[str, float] = {}
        workout_meta_by_date: dict[str, dict] = {}
        for w in (workout_data or []):
            d = w.get("created_at", "")[:10]
            score = w.get("score", {})
            if score.get("strain") is not None:
                strain_by_date[d] = score["strain"]
            if score.get("kilojoule") is not None:
                calories_by_date[d] = score["kilojoule"] / 4.184
            workout_meta_by_date[d] = {
                "sport_name": (
                    w.get("sport_name")
                    or str(w.get("sport_id", ""))
                ),
                "avg_hr": score.get("average_heart_rate"),
                "max_hr": score.get("max_heart_rate"),
            }

        count = 0
        for item in (recovery_data or []):
            score = item.get("score", {})
            raw_date = item.get("created_at", "")
            if not raw_date:
                continue
            d = raw_date[:10]
            try:
                parsed_date = date.fromisoformat(d)
            except ValueError:
                continue
            sleep = sleep_by_date.get(d, {})
            workout_meta = workout_meta_by_date.get(d, {})

            wd = WhoopDaily(
                id=hashlib.sha256(f"whoop-{d}".encode()).hexdigest(),
                date=parsed_date,
                recovery_score=score.get("recovery_score"),
                rhr=score.get("resting_heart_rate"),
                hrv=score.get("hrv_rmssd_milli"),
                spo2=score.get("spo2_percentage"),
                skin_temp=score.get("skin_temp_celsius"),
                sleep_score=sleep.get("sleep_score"),
                sleep_duration_min=sleep.get("sleep_duration_min"),
                rem_min=sleep.get("rem_min"),
                deep_min=sleep.get("deep_min"),
                light_min=sleep.get("light_min"),
                resp_rate=sleep.get("resp_rate"),
                strain=strain_by_date.get(d),
                calories=calories_by_date.get(d),
                sleep_latency_min=sleep.get("sleep_latency_min"),
                wake_episodes=sleep.get("wake_episodes"),
                sleep_efficiency_pct=sleep.get("sleep_efficiency_pct"),
                workout_sport_name=workout_meta.get("sport_name"),
                workout_avg_hr=workout_meta.get("avg_hr"),
                workout_max_hr=workout_meta.get("max_hr"),
            )
            # Always write to raw vault (Tier 1 source of truth)
            db.insert_wearable_daily(wd, user_id=user_id)
            # Also write to Clean DB if available (pre-anonymized, faster AI access)
            if clean_db is not None:
                clean_db.upsert_wearable(
                    wearable_id=wd.id,
                    date=wd.date.isoformat(),
                    provider="whoop",
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
                    sleep_latency_min=wd.sleep_latency_min,
                    wake_episodes=wd.wake_episodes,
                    sleep_efficiency_pct=wd.sleep_efficiency_pct,
                    workout_sport_name=wd.workout_sport_name,
                    workout_avg_hr=wd.workout_avg_hr,
                    workout_max_hr=wd.workout_max_hr,
                    skin_temp=wd.skin_temp,
                )
            count += 1
            logger.debug(
                "WHOOP %s: HRV=%s RHR=%s recovery=%s sleep=%smin",
                d, wd.hrv, wd.rhr, wd.recovery_score, wd.sleep_duration_min,
            )

        logger.info(
            "WHOOP sync complete: %d records written (%s)",
            count, "clean_db" if clean_db else "tier1",
        )
        return count

    async def _paginated_get(self, endpoint: str, params: dict) -> list[dict]:
        """Handle WHOOP's cursor-based pagination."""
        all_records: list[dict] = []
        next_token = None
        # Copy to avoid mutating the caller's dict with pagination tokens
        params = {**params}

        for _page in range(100):  # Max 100 pages guard to prevent infinite loops
            if next_token:
                params["nextToken"] = next_token
            resp = await self._authed_request("GET", endpoint, params=params)
            data = resp.json()
            records = data.get("records", [])
            all_records.extend(records)
            next_token = data.get("next_token")
            if not next_token or not records:
                break

        return all_records

    async def _authed_request(
        self, method: str, path: str, **kwargs
    ) -> httpx.Response:
        """Make an authenticated request. Auto-refresh if 401.

        Retries up to 3 times with exponential backoff on transient errors
        (ConnectError, TimeoutException, 502/503/504).
        """
        if not self._access_token:
            await self.refresh_access_token()

        url = f"{self._config.whoop_api_base}{path}"
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
                            "WHOOP %s %s returned %d, retrying (%d/2)",
                            method, path, resp.status_code, attempt + 1,
                        )
                        await asyncio.sleep(1.0 * (2 ** attempt))
                        continue
                    if resp.status_code >= 400:
                        body = resp.text[:200] if resp.text else ""
                        raise WhoopAuthError(
                            f"WHOOP API error: {resp.status_code} on {method} {path} — {body}"
                        )
                    return resp
                except (httpx.ConnectError, httpx.TimeoutException) as exc:
                    last_exc = exc
                    if attempt < 2:
                        logger.warning(
                            "WHOOP %s %s failed (%s), retrying (%d/2)",
                            method, path, type(exc).__name__, attempt + 1,
                        )
                        await asyncio.sleep(1.0 * (2 ** attempt))
                        continue
                    raise
            raise last_exc  # type: ignore[misc]
