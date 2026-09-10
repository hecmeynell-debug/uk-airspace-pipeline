"""The read API.

The interesting assertions here are the negative ones. It is easy to write a
dashboard that happens not to show identity today; what matters is that this
service cannot serve it, and that its query windows cannot be widened into a
bulk export by anyone who edits a URL.
"""

from __future__ import annotations

import psycopg
import pytest
from fastapi.testclient import TestClient

from api.main import app

pytestmark = pytest.mark.integration

# Anything that would amount to per-airframe data leaving the API.
FORBIDDEN_KEYS = {
    "icao24",
    "callsign",
    "squawk",
    "registration",
    "tail_number",
    "serial_number",
    "origin_country",
    "spi",
    "latitude",
    "longitude",
}

DATA_ENDPOINTS = [
    "/api/freshness",
    "/api/activity/grid?hours=24",
    "/api/activity/hourly?hours=48",
    "/api/activity/altitude?hours=24",
    "/api/activity/daily?days=14",
    "/api/pipeline-health?hours=24",
]


@pytest.fixture(scope="module")
def client(db_settings):
    if not db_settings.airspace_reader_password:
        pytest.skip("AIRSPACE_READER_PASSWORD not configured")
    with TestClient(app) as test_client:
        yield test_client


def _keys(node, found: set[str]) -> set[str]:
    """Every key appearing anywhere in a nested response."""
    if isinstance(node, dict):
        for key, value in node.items():
            found.add(key.lower())
            _keys(value, found)
    elif isinstance(node, list):
        for item in node:
            _keys(item, found)
    return found


def test_service_health_is_independent_of_the_pipeline(client):
    """Liveness must not depend on data existing, or restarts become circular."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_meta_states_the_limitations_and_non_goals(client):
    body = client.get("/api/meta").json()

    assert "OBSERVED" in body["limitations"]
    assert any("no individual aircraft tracking" in g.lower() for g in body["non_goals"])
    assert body["region"]["lat_min"] < body["region"]["lat_max"]


@pytest.mark.parametrize("endpoint", DATA_ENDPOINTS)
def test_endpoints_respond(client, endpoint):
    """200 with data, or 503 saying the marts are not built. Never a 500."""
    response = client.get(endpoint)
    assert response.status_code in (200, 503), response.text


@pytest.mark.parametrize("endpoint", DATA_ENDPOINTS)
def test_no_endpoint_can_return_aircraft_identity(client, endpoint):
    response = client.get(endpoint)
    if response.status_code != 200:
        pytest.skip("marts not built")

    leaked = _keys(response.json(), set()) & FORBIDDEN_KEYS
    assert not leaked, f"{endpoint} exposed identity fields: {leaked}"


@pytest.mark.parametrize(
    ("endpoint", "bad_values"),
    [
        ("/api/activity/grid?hours={}", ["0", "-1", "100000", "abc"]),
        ("/api/activity/daily?days={}", ["0", "-5", "9999", "abc"]),
    ],
)
def test_window_parameters_are_bounded(client, endpoint, bad_values):
    """A window parameter must not be widenable into a bulk export."""
    for value in bad_values:
        response = client.get(endpoint.format(value))
        assert response.status_code == 422, f"{endpoint.format(value)} was accepted"


def test_api_connects_as_the_reader_role(db_settings):
    """The service must hold the least-privileged credential, not a convenient one."""
    with psycopg.connect(db_settings.reader_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT current_user")
        assert cur.fetchone()[0] == "airspace_reader"

        # And that role must still be unable to reach the landing zone.
        with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
            conn.execute("SELECT * FROM raw.state_vectors LIMIT 1")
