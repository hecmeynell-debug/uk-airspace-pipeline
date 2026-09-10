"""Shared fixtures.

Integration tests need a live Postgres. They skip rather than fail when one is
not reachable, so `pytest` is useful on a laptop without Docker running, while
CI (which always has the service container) still runs everything.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import psycopg
import pytest

from ingestion.config import Settings

# A realistic /states/all payload. Field order matters and mirrors the API:
# icao24, callsign, origin_country, time_position, last_contact, longitude,
# latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate,
# sensors, geo_altitude, squawk, spi, position_source, category
SNAPSHOT_EPOCH = 1_757_500_000  # 2025-09-10T10:26:40Z, inside the plausible window

SAMPLE_STATES: list[list] = [
    [
        "4009f5",
        "BAW123  ",  # padded, as the API returns it
        "United Kingdom",
        SNAPSHOT_EPOCH - 5,
        SNAPSHOT_EPOCH - 2,
        -0.4614,
        51.4700,
        11277.6,
        False,
        243.5,
        95.4,
        0.0,
        None,
        11582.4,
        "2000",
        False,
        0,
        1,
    ],
    [
        "40643a",
        "EZY84NX ",
        "United Kingdom",
        SNAPSHOT_EPOCH - 11,
        SNAPSHOT_EPOCH - 1,
        -2.7189,
        53.3542,
        2308.86,
        False,
        178.2,
        271.9,
        -5.2,
        None,
        2407.92,
        "6154",
        False,
        0,
        1,
    ],
    [
        "3c6444",
        None,  # no callsign reported
        "Germany",
        None,  # no position report; observed_at falls back to last_contact
        SNAPSHOT_EPOCH - 30,
        None,
        None,
        None,
        True,
        None,
        None,
        None,
        None,
        None,
        None,
        False,
        0,
        0,
    ],
]


@pytest.fixture
def sample_states() -> list[list]:
    """Fresh copy per test, so mutation in one test cannot leak into another."""
    return [list(row) for row in SAMPLE_STATES]


@pytest.fixture
def snapshot_time() -> datetime:
    return datetime.fromtimestamp(SNAPSHOT_EPOCH, tz=UTC)


@pytest.fixture
def parse_now() -> datetime:
    """A fixed 'now' so fixture timestamps never drift out of the plausible window."""
    return datetime.fromtimestamp(SNAPSHOT_EPOCH + 60, tz=UTC)


@pytest.fixture
def settings() -> Settings:
    return Settings(
        postgres_host="localhost",
        postgres_db="airspace",
        airspace_owner_password="owner_local_dev_pw",
        airspace_ingest_password="ingest_local_dev_pw",
        opensky_client_id="",
        opensky_client_secret="",
    )


def _can_connect(dsn: str) -> bool:
    try:
        with psycopg.connect(dsn, connect_timeout=3):
            return True
    except psycopg.Error:
        return False


@pytest.fixture(scope="session")
def db_settings() -> Settings:
    """Settings pointed at the local Docker Postgres, or skip the whole suite."""
    resolved = Settings()
    if not resolved.airspace_owner_password or not resolved.airspace_ingest_password:
        pytest.skip("database passwords not configured; copy .env.example to .env")
    if not _can_connect(resolved.owner_dsn):
        pytest.skip("no Postgres reachable; run `docker compose up -d`")
    return resolved


@pytest.fixture
def migrated_db(db_settings: Settings):
    """A migrated, empty database. Yields an ingest-privileged connection.

    Cleanup runs on the owner connection because the ingest role deliberately
    has no TRUNCATE or DELETE rights - which is itself part of what the suite
    asserts.
    """
    from ingestion import db as db_module

    truncate = "TRUNCATE raw.state_vectors, raw.ingestion_runs RESTART IDENTITY CASCADE"

    # autocommit, deliberately: TRUNCATE takes an ACCESS EXCLUSIVE lock, and an
    # uncommitted one on this connection would block the ingest connection
    # below for as long as the fixture lives.
    with psycopg.connect(db_settings.owner_dsn, autocommit=True) as owner_conn:
        db_module.apply_migrations(owner_conn)
        owner_conn.execute(truncate)

        with psycopg.connect(db_settings.ingest_dsn) as ingest_conn:
            yield ingest_conn

        owner_conn.execute(truncate)


@pytest.fixture
def run_id() -> uuid.UUID:
    return uuid.uuid4()
