"""End-to-end: ingest a snapshot, then transform it, then check what came out.

The dbt suite already tests the models against whatever happens to be in the
database. This test controls both ends - it puts a known payload in and asserts
the marts describe exactly that payload - so it catches the class of bug where
ingestion and transformation each look correct in isolation but disagree about
what a row means.

It also re-asserts the aggregate-only constraint from the Python side. The dbt
test does the same thing; duplicating it is deliberate, because the constraint
is the one thing in this project that must not be able to regress quietly.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import httpx
import psycopg
import pytest
import respx

from ingestion.config import Settings
from ingestion.run import ingest_once

pytestmark = pytest.mark.integration

STATES_PATH = "/api/states/all"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TRANSFORM_DIR = PROJECT_ROOT / "transform"

# Columns that must never appear in a mart. Kept in step with
# transform/tests/assert_marts_expose_no_aircraft_identity.sql.
FORBIDDEN_MART_COLUMNS = {
    "icao24",
    "callsign",
    "squawk",
    "registration",
    "tail_number",
    "serial_number",
    "origin_country",
    "spi",
    "sensors",
    "latitude",
    "longitude",
}


def _dbt_executable() -> str | None:
    """Prefer the dbt beside the running interpreter over one on PATH."""
    candidate = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    if candidate.exists():
        return str(candidate)
    return shutil.which("dbt")


@pytest.fixture(scope="module")
def dbt() -> str:
    executable = _dbt_executable()
    if executable is None:
        pytest.skip("dbt is not installed; pip install -e '.[transform]'")
    return executable


@pytest.fixture(scope="module", autouse=True)
def _leave_no_stale_marts(db_settings: Settings):
    """Drop the built schemas afterwards.

    These tests build marts from a three-row fixture and then truncate raw, so
    without this the warehouse is left holding aggregates whose source rows no
    longer exist. The dbt reconciliation test would correctly - and confusingly
    - fail on the next build. Cleaning up here keeps a test run from poisoning
    the next real one.
    """
    yield

    if not db_settings.airspace_transform_password:
        return

    with psycopg.connect(db_settings.transform_dsn, autocommit=True) as conn:
        for schema in ("analytics_marts", "analytics_intermediate", "analytics_staging"):
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _run_dbt(dbt: str, settings: Settings, *args: str) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "POSTGRES_HOST": settings.postgres_host,
        "POSTGRES_PORT": str(settings.postgres_port),
        "POSTGRES_DB": settings.postgres_db,
        "AIRSPACE_TRANSFORM_PASSWORD": settings.airspace_transform_password,
        "DBT_PROFILES_DIR": str(TRANSFORM_DIR),
    }
    return subprocess.run(
        [dbt, *args, "--project-dir", str(TRANSFORM_DIR)],
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
        check=False,
    )


@respx.mock
def test_ingested_snapshot_is_faithfully_aggregated(migrated_db, db_settings, dbt, sample_states):
    if not db_settings.airspace_transform_password:
        pytest.skip("AIRSPACE_TRANSFORM_PASSWORD not configured")

    respx.get(path=STATES_PATH).mock(
        return_value=httpx.Response(
            200, json={"time": int(sample_states[0][4]) + 2, "states": sample_states}
        )
    )

    result = ingest_once(db_settings)
    assert result is not None and result.rows_inserted == 3

    # --full-refresh so the assertions describe this payload alone, rather than
    # whatever the incremental mart happened to be carrying already.
    completed = _run_dbt(dbt, db_settings, "build", "--full-refresh")
    assert completed.returncode == 0, (
        f"dbt build failed:\n{completed.stdout[-4000:]}\n{completed.stderr[-2000:]}"
    )

    # Read through the consumer credential, not a privileged one: this asserts
    # the marts are actually reachable by the role a dashboard would use.
    with psycopg.connect(db_settings.reader_dsn) as conn, conn.cursor() as cur:
        # Two of the three fixture rows carry a position; the third reports no
        # lat/lon and must be excluded from the gridded aggregates rather than
        # silently counted at (0, 0).
        cur.execute(
            "SELECT coalesce(sum(observation_count), 0), coalesce(sum(distinct_aircraft), 0) "
            "FROM analytics_marts.fct_airspace_activity_hourly"
        )
        observations, aircraft = cur.fetchone()
        assert observations == 2
        assert aircraft == 2

        cur.execute(
            "SELECT coalesce(sum(observation_count), 0) "
            "FROM analytics_marts.fct_airspace_activity_daily"
        )
        assert cur.fetchone()[0] == 2

    # The positionless row must still be visible upstream - dropped from the
    # gridded models, not lost from the pipeline. Read as the transform role,
    # because the reader deliberately cannot see staging.
    with psycopg.connect(db_settings.transform_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM analytics_staging.stg_opensky__state_vectors "
            "WHERE not has_position"
        )
        assert cur.fetchone()[0] == 1


def _state_vector(icao24: str, epoch: int, lat: float, lon: float, track: float) -> list:
    """A minimal airborne state vector, in the API's positional field order."""
    return [
        icao24,
        "TEST123 ",
        "United Kingdom",
        epoch - 2,
        epoch,
        lon,
        lat,
        10000.0,
        False,
        220.0,
        track,
        0.0,
        None,
        10200.0,
        "1000",
        False,
        0,
        1,
    ]


@respx.mock
def test_headings_are_averaged_circularly_not_arithmetically(migrated_db, db_settings, dbt):
    """350 degrees and 10 degrees average to 0, not to 180.

    This is the single easiest thing to get wrong about heading data, and
    getting it wrong points the flow arrows in precisely the opposite
    direction while looking entirely plausible. Two aircraft in one cell,
    straddling north.
    """
    if not db_settings.airspace_transform_password:
        pytest.skip("AIRSPACE_TRANSFORM_PASSWORD not configured")

    epoch = 1_757_500_000
    states = [
        _state_vector("aaaaaa", epoch, 51.4, -0.6, 350.0),
        _state_vector("bbbbbb", epoch, 51.6, -0.4, 10.0),
    ]
    respx.get(path=STATES_PATH).mock(
        return_value=httpx.Response(200, json={"time": epoch + 2, "states": states})
    )

    assert ingest_once(db_settings).rows_inserted == 2

    completed = _run_dbt(dbt, db_settings, "build", "--full-refresh")
    assert completed.returncode == 0, completed.stdout[-3000:]

    with psycopg.connect(db_settings.reader_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT dominant_track_deg, track_concentration, track_sample_count
              FROM analytics_marts.fct_airspace_activity_hourly
             WHERE grid_lat = 51 AND grid_lon = -1
            """
        )
        bearing, concentration, samples = cur.fetchone()

    assert samples == 2
    # The circular mean of 350 and 10 is 0/360. Allow a degree of slack for
    # floating point, and accept either end of the wrap.
    assert min(bearing, 360 - bearing) < 1.0, (
        f"expected a mean bearing near 0/360, got {bearing} - "
        "an arithmetic mean would give 180, pointing exactly backwards"
    )
    # Two headings 20 degrees apart agree strongly.
    assert concentration > 0.98


def test_reader_role_reaches_marts_but_nothing_upstream(db_settings, dbt):
    """The read-only consumer must be unable to reach per-airframe data at all.

    This is the structural half of the aggregate-only constraint: not "the
    dashboard does not query icao24" but "the dashboard's credential cannot".
    """
    if not db_settings.airspace_reader_password:
        pytest.skip("AIRSPACE_READER_PASSWORD not configured")

    with psycopg.connect(db_settings.reader_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM analytics_marts.fct_airspace_activity_hourly")
            assert cur.fetchone()[0] >= 0
        conn.rollback()

        for forbidden in (
            "SELECT * FROM raw.state_vectors LIMIT 1",
            "SELECT * FROM analytics_staging.stg_opensky__state_vectors LIMIT 1",
            "SELECT * FROM analytics_intermediate.int_state_vectors__gridded LIMIT 1",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
                conn.execute(forbidden)


def test_marts_expose_no_aircraft_identity(db_settings, dbt):
    """The aggregate-only constraint, checked against the built schema."""
    if not db_settings.airspace_reader_password:
        pytest.skip("AIRSPACE_READER_PASSWORD not configured")

    # Inspected as the reader, so this describes what a consumer can actually
    # see - information_schema only lists columns the role has rights on.
    with psycopg.connect(db_settings.reader_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name
              FROM information_schema.columns
             WHERE table_schema = 'analytics_marts'
            """
        )
        columns = cur.fetchall()

    if not columns:
        pytest.skip("marts not built yet; run dbt build first")

    leaked = [
        f"{table}.{column}" for table, column in columns if column.lower() in FORBIDDEN_MART_COLUMNS
    ]
    assert not leaked, f"per-airframe identity reached a mart: {leaked}"
