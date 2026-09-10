"""OpenSky client: authentication, retry policy and error classification."""

from __future__ import annotations

import httpx
import pytest
import respx

from ingestion.config import Settings
from ingestion.opensky import (
    OpenSkyAuthError,
    OpenSkyClient,
    OpenSkyRateLimited,
    OpenSkyTransient,
)

STATES_PATH = "/api/states/all"
TOKEN_PATH = "/auth/realms/opensky-network/protocol/openid-connect/token"


def _payload(states: list[list] | None = None, snapshot: int = 1_757_500_000) -> dict:
    return {"time": snapshot, "states": states if states is not None else []}


@pytest.fixture
def authed_settings() -> Settings:
    return Settings(
        opensky_client_id="test-client",
        opensky_client_secret="test-secret",
        opensky_max_attempts=4,
    )


class FakeClock:
    """Controllable time source, so token expiry is tested without waiting."""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _client(settings: Settings, clock=None) -> OpenSkyClient:
    # sleeper is stubbed out so retry tests do not actually back off.
    return OpenSkyClient(settings, clock=clock or FakeClock(), sleeper=lambda _: None)


@respx.mock
def test_fetch_returns_snapshot_time_and_rows(settings, sample_states):
    respx.get(path=STATES_PATH).mock(
        return_value=httpx.Response(200, json=_payload(sample_states))
    )

    with _client(settings) as client:
        response = client.fetch_states()

    assert response.http_status == 200
    assert len(response.rows) == 3
    assert response.api_snapshot_time.isoformat().endswith("+00:00")


@respx.mock
def test_bounding_box_is_sent_as_query_parameters(settings):
    route = respx.get(path=STATES_PATH).mock(return_value=httpx.Response(200, json=_payload()))

    with _client(settings) as client:
        client.fetch_states()

    query = route.calls.last.request.url.params
    assert float(query["lamin"]) == settings.opensky_lat_min
    assert float(query["lamax"]) == settings.opensky_lat_max
    assert float(query["lomin"]) == settings.opensky_lon_min
    assert float(query["lomax"]) == settings.opensky_lon_max


@respx.mock
def test_anonymous_mode_sends_no_authorization_header(settings):
    assert settings.auth_mode == "anonymous"
    route = respx.get(path=STATES_PATH).mock(return_value=httpx.Response(200, json=_payload()))

    with _client(settings) as client:
        client.fetch_states()

    assert "authorization" not in route.calls.last.request.headers


@respx.mock
def test_authenticated_mode_presents_a_bearer_token(authed_settings):
    token_route = respx.post(path=TOKEN_PATH).mock(
        return_value=httpx.Response(200, json={"access_token": "tok-abc", "expires_in": 1800})
    )
    states_route = respx.get(path=STATES_PATH).mock(
        return_value=httpx.Response(200, json=_payload())
    )

    with _client(authed_settings) as client:
        client.fetch_states()

    assert token_route.call_count == 1
    assert states_route.calls.last.request.headers["authorization"] == "Bearer tok-abc"


@respx.mock
def test_token_is_reused_until_shortly_before_expiry(authed_settings):
    clock = FakeClock()
    token_route = respx.post(path=TOKEN_PATH).mock(
        return_value=httpx.Response(200, json={"access_token": "tok-abc", "expires_in": 1800})
    )
    respx.get(path=STATES_PATH).mock(return_value=httpx.Response(200, json=_payload()))

    with _client(authed_settings, clock=clock) as client:
        client.fetch_states()
        clock.advance(600)  # well inside the 30-minute lifetime
        client.fetch_states()

    assert token_route.call_count == 1, "token should be cached, not refetched every call"


@respx.mock
def test_token_is_refreshed_once_the_margin_is_reached(authed_settings):
    clock = FakeClock()
    token_route = respx.post(path=TOKEN_PATH).mock(
        return_value=httpx.Response(200, json={"access_token": "tok-abc", "expires_in": 1800})
    )
    respx.get(path=STATES_PATH).mock(return_value=httpx.Response(200, json=_payload()))

    with _client(authed_settings, clock=clock) as client:
        client.fetch_states()
        # 1800s lifetime minus a 60s refresh margin means renewal at 1740s.
        clock.advance(1_741)
        client.fetch_states()

    assert token_route.call_count == 2


@respx.mock
def test_rate_limiting_is_not_retried(settings):
    """A daily credit quota cannot be waited out in-process; retrying burns budget."""
    route = respx.get(path=STATES_PATH).mock(
        return_value=httpx.Response(429, headers={"Retry-After": "3600"})
    )

    with _client(settings) as client, pytest.raises(OpenSkyRateLimited) as exc_info:
        client.fetch_states()

    assert route.call_count == 1, "429 must not be retried"
    assert exc_info.value.retry_after == 3600.0


@respx.mock
def test_server_errors_are_retried_then_surface_as_transient(settings):
    route = respx.get(path=STATES_PATH).mock(return_value=httpx.Response(503))

    with _client(settings) as client, pytest.raises(OpenSkyTransient):
        client.fetch_states()

    assert route.call_count == settings.opensky_max_attempts


@respx.mock
def test_transient_failure_recovers_without_raising(settings):
    route = respx.get(path=STATES_PATH)
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(200, json=_payload()),
    ]

    with _client(settings) as client:
        response = client.fetch_states()

    assert response.http_status == 200
    assert route.call_count == 2


@respx.mock
def test_connection_errors_are_treated_as_transient(settings):
    respx.get(path=STATES_PATH).mock(side_effect=httpx.ConnectError("boom"))

    with _client(settings) as client, pytest.raises(OpenSkyTransient):
        client.fetch_states()


@respx.mock
def test_unauthorised_response_raises_auth_error_and_drops_cached_token(authed_settings):
    respx.post(path=TOKEN_PATH).mock(
        return_value=httpx.Response(200, json={"access_token": "tok-abc", "expires_in": 1800})
    )
    respx.get(path=STATES_PATH).mock(return_value=httpx.Response(401))

    client = _client(authed_settings)
    with pytest.raises(OpenSkyAuthError):
        client.fetch_states()

    assert client._tokens._token is None, "a revoked token must not stay cached"
    client.close()


@respx.mock
def test_rejected_credentials_are_not_retried(authed_settings):
    token_route = respx.post(path=TOKEN_PATH).mock(return_value=httpx.Response(401))

    with _client(authed_settings) as client, pytest.raises(OpenSkyAuthError):
        client.fetch_states()

    assert token_route.call_count == 1


@respx.mock
@pytest.mark.parametrize(
    "body",
    [
        {"states": []},  # no snapshot time
        {"time": "not-a-number", "states": []},
        [1, 2, 3],  # wrong shape entirely
        {"time": 1_757_500_000, "states": "nope"},
    ],
)
def test_malformed_payloads_are_transient_not_silent(settings, body):
    respx.get(path=STATES_PATH).mock(return_value=httpx.Response(200, json=body))

    with _client(settings) as client, pytest.raises(OpenSkyTransient):
        client.fetch_states()


@respx.mock
def test_null_states_field_yields_no_rows(settings):
    """OpenSky returns states: null for an empty box rather than an empty list."""
    respx.get(path=STATES_PATH).mock(
        return_value=httpx.Response(200, json={"time": 1_757_500_000, "states": None})
    )

    with _client(settings) as client:
        response = client.fetch_states()

    assert response.rows == []
