"""OpenSky Network REST client.

Two behaviours here are deliberate and worth stating:

1. **429 is not retried.** OpenSky's quota is a daily credit allowance, not a
   short burst limit. Retrying a 429 in-process burns the remaining budget
   without any prospect of success, so the client raises and lets the
   scheduler decide when to try again. Only genuinely transient failures -
   timeouts, connection errors, 5xx - are retried.

2. **Tokens are cached with a refresh margin.** Access tokens live 30 minutes;
   the cache refreshes early so a long-running job never presents a token that
   expires mid-flight.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from ingestion.config import Settings
from ingestion.logging_setup import get_logger

log = get_logger(__name__)

# Refresh a token this many seconds before it actually expires.
TOKEN_REFRESH_MARGIN_SECONDS = 60


class OpenSkyError(Exception):
    """Base class for all OpenSky failures."""


class OpenSkyAuthError(OpenSkyError):
    """Credentials were rejected. Not retryable."""


class OpenSkyRateLimited(OpenSkyError):
    """Daily credit allowance exhausted. Not retryable in-process."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class OpenSkyTransient(OpenSkyError):
    """A failure that is worth retrying: timeout, connection error, or 5xx."""


@dataclass(frozen=True, slots=True)
class StatesResponse:
    api_snapshot_time: datetime
    rows: list[Any]
    http_status: int


class TokenCache:
    """Caches an OAuth2 client-credentials token until shortly before expiry."""

    def __init__(
        self,
        settings: Settings,
        client: httpx.Client,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._settings = settings
        self._client = client
        self._clock = clock
        self._token: str | None = None
        self._expires_at: float = 0.0

    @property
    def is_anonymous(self) -> bool:
        return self._settings.auth_mode == "anonymous"

    def token(self) -> str | None:
        if self.is_anonymous:
            return None
        if self._token is not None and self._clock() < self._expires_at:
            return self._token
        return self._fetch()

    def _fetch(self) -> str:
        try:
            response = self._client.post(
                self._settings.opensky_token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._settings.opensky_client_id,
                    "client_secret": self._settings.opensky_client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self._settings.opensky_timeout_seconds,
            )
        except httpx.RequestError as exc:
            raise OpenSkyTransient(f"token request failed: {exc}") from exc

        if response.status_code in (400, 401, 403):
            raise OpenSkyAuthError(
                f"OpenSky rejected the client credentials (HTTP {response.status_code}). "
                "Basic auth is no longer supported; check OPENSKY_CLIENT_ID/SECRET."
            )
        if response.status_code >= 500:
            raise OpenSkyTransient(f"token endpoint returned HTTP {response.status_code}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise OpenSkyTransient("token endpoint returned a non-JSON body") from exc

        token = payload.get("access_token")
        if not token:
            raise OpenSkyAuthError("token response contained no access_token")

        expires_in = float(payload.get("expires_in", 1800))
        self._token = token
        self._expires_at = self._clock() + max(expires_in - TOKEN_REFRESH_MARGIN_SECONDS, 0)
        log.info("opensky.token_refreshed", expires_in_seconds=expires_in)
        return token

    def invalidate(self) -> None:
        self._token = None
        self._expires_at = 0.0


class OpenSkyClient:
    """Fetches state vectors for the configured bounding box."""

    def __init__(
        self,
        settings: Settings,
        client: httpx.Client | None = None,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._settings = settings
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=settings.opensky_timeout_seconds)
        self._tokens = TokenCache(settings, self._client, clock=clock)
        self._sleep = sleeper

    def __enter__(self) -> OpenSkyClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def fetch_states(self) -> StatesResponse:
        """Fetch one snapshot, retrying only genuinely transient failures."""
        attempts = self._settings.opensky_max_attempts
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                return self._fetch_once()
            except OpenSkyTransient as exc:
                last_error = exc
                if attempt == attempts:
                    break
                # Exponential backoff with full jitter, so that a fleet of
                # retrying jobs does not resynchronise into a thundering herd.
                delay = random.uniform(0, min(2**attempt, 30))
                log.warning(
                    "opensky.retrying",
                    attempt=attempt,
                    max_attempts=attempts,
                    delay_seconds=round(delay, 2),
                    error=str(exc),
                )
                self._sleep(delay)

        raise OpenSkyTransient(
            f"giving up after {attempts} attempts: {last_error}"
        ) from last_error

    def _fetch_once(self) -> StatesResponse:
        headers = {"Accept": "application/json"}
        token = self._tokens.token()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        url = f"{self._settings.opensky_api_base.rstrip('/')}/states/all"

        try:
            response = self._client.get(
                url,
                params=self._settings.bbox_params,
                headers=headers,
                timeout=self._settings.opensky_timeout_seconds,
            )
        except httpx.RequestError as exc:
            raise OpenSkyTransient(f"request failed: {exc}") from exc

        if response.status_code == 429:
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            raise OpenSkyRateLimited(
                "OpenSky credit allowance exhausted (HTTP 429). "
                "Not retrying in-process; the scheduler should back off.",
                retry_after=retry_after,
            )
        if response.status_code in (401, 403):
            # A cached token may have been revoked server-side; drop it so the
            # next run re-authenticates cleanly rather than looping on a stale
            # credential.
            self._tokens.invalidate()
            raise OpenSkyAuthError(f"OpenSky refused the request (HTTP {response.status_code})")
        if response.status_code >= 500:
            raise OpenSkyTransient(f"OpenSky returned HTTP {response.status_code}")
        if response.status_code != 200:
            raise OpenSkyError(f"unexpected HTTP {response.status_code} from OpenSky")

        try:
            payload = response.json()
        except ValueError as exc:
            raise OpenSkyTransient("OpenSky returned a non-JSON body") from exc

        if not isinstance(payload, dict):
            raise OpenSkyTransient("OpenSky returned an unexpected payload shape")

        snapshot_epoch = payload.get("time")
        if not isinstance(snapshot_epoch, (int, float)) or isinstance(snapshot_epoch, bool):
            raise OpenSkyTransient("OpenSky response is missing a usable 'time' field")

        rows = payload.get("states") or []
        if not isinstance(rows, list):
            raise OpenSkyTransient("OpenSky 'states' field is not a list")

        return StatesResponse(
            api_snapshot_time=datetime.fromtimestamp(float(snapshot_epoch), tz=UTC),
            rows=rows,
            http_status=response.status_code,
        )


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None
