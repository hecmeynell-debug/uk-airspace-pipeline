"""Idempotent loading of state vectors into the raw schema.

Idempotency is enforced by the database, not by application logic: the primary
key on (icao24, observed_at) plus ON CONFLICT DO NOTHING means re-ingesting the
same window is a no-op no matter how many times it happens, or how many workers
do it concurrently. Application-side "have I seen this?" checks would race;
a unique constraint cannot.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime

import psycopg

from ingestion.config import Settings
from ingestion.logging_setup import get_logger
from ingestion.models import StateVector

log = get_logger(__name__)

STATE_VECTOR_COLUMNS = (
    "icao24",
    "observed_at",
    "time_position",
    "last_contact",
    "callsign",
    "origin_country",
    "longitude",
    "latitude",
    "baro_altitude_m",
    "geo_altitude_m",
    "on_ground",
    "velocity_ms",
    "true_track_deg",
    "vertical_rate_ms",
    "squawk",
    "spi",
    "position_source",
    "category",
    "ingestion_run_id",
    "api_snapshot_time",
)

# Postgres caps a statement at 65535 bind parameters. At 20 columns that is
# 3276 rows, so 1000 leaves generous headroom while keeping round trips low.
# COPY into a staging table would scale further, but it needs TEMP rights on
# the database, which the ingest role deliberately does not have.
DEFAULT_BATCH_SIZE = 1000


@dataclass(frozen=True, slots=True)
class LoadResult:
    run_id: uuid.UUID
    rows_received: int
    rows_rejected: int
    rows_inserted: int
    rows_duplicate: int


def start_run(
    conn: psycopg.Connection,
    settings: Settings,
    *,
    endpoint: str = "/states/all",
    run_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """Record the intent to ingest, before any network call is made.

    Written first so that a run which dies mid-flight still leaves a 'running'
    row behind. A stuck 'running' row is a visible symptom; a missing row is
    not.
    """
    new_id = run_id or uuid.uuid4()
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO raw.ingestion_runs
                (run_id, source_endpoint, request_bbox, auth_mode, credits_estimated)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                new_id,
                endpoint,
                json.dumps(settings.bbox_params),
                settings.auth_mode,
                settings.credit_cost,
            ),
        )
    return new_id


def insert_state_vectors(
    conn: psycopg.Connection,
    run_id: uuid.UUID,
    api_snapshot_time: datetime,
    vectors: list[StateVector],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Insert vectors, skipping any that already exist. Returns rows inserted."""
    if not vectors:
        return 0

    columns = ", ".join(STATE_VECTOR_COLUMNS)
    placeholder_row = "(" + ", ".join(["%s"] * len(STATE_VECTOR_COLUMNS)) + ")"
    inserted = 0

    with conn.transaction(), conn.cursor() as cur:
        for start in range(0, len(vectors), batch_size):
            chunk = vectors[start : start + batch_size]
            params: list[object] = []
            for vector in chunk:
                params.extend(_row_params(vector, run_id, api_snapshot_time))

            statement = (
                f"INSERT INTO raw.state_vectors ({columns}) VALUES "
                + ", ".join([placeholder_row] * len(chunk))
                + " ON CONFLICT (icao24, observed_at) DO NOTHING"
            )
            cur.execute(statement, params)
            inserted += cur.rowcount

    return inserted


def _row_params(
    vector: StateVector, run_id: uuid.UUID, api_snapshot_time: datetime
) -> tuple[object, ...]:
    return (
        vector.icao24,
        vector.observed_at,
        vector.time_position,
        vector.last_contact,
        vector.callsign,
        vector.origin_country,
        vector.longitude,
        vector.latitude,
        vector.baro_altitude_m,
        vector.geo_altitude_m,
        vector.on_ground,
        vector.velocity_ms,
        vector.true_track_deg,
        vector.vertical_rate_ms,
        vector.squawk,
        vector.spi,
        vector.position_source,
        vector.category,
        run_id,
        api_snapshot_time,
    )


def finish_run(
    conn: psycopg.Connection,
    run_id: uuid.UUID,
    *,
    status: str,
    http_status: int | None = None,
    api_snapshot_time: datetime | None = None,
    rows_received: int | None = None,
    rows_rejected: int | None = None,
    rows_inserted: int | None = None,
    rows_duplicate: int | None = None,
    error_message: str | None = None,
) -> None:
    """Finalise a run record with its outcome and counts."""
    if status not in ("succeeded", "failed"):
        raise ValueError(f"invalid terminal status: {status}")

    with conn.transaction():
        conn.execute(
            """
            UPDATE raw.ingestion_runs
               SET status            = %s,
                   finished_at       = now(),
                   http_status       = %s,
                   api_snapshot_time = %s,
                   rows_received     = %s,
                   rows_rejected     = %s,
                   rows_inserted     = %s,
                   rows_duplicate    = %s,
                   error_message     = %s
             WHERE run_id = %s
            """,
            (
                status,
                http_status,
                api_snapshot_time,
                rows_received,
                rows_rejected,
                rows_inserted,
                rows_duplicate,
                # Truncated: an error message is a diagnostic, not a payload
                # store, and an unbounded driver traceback should not be able
                # to bloat the provenance table.
                error_message[:2000] if error_message else None,
                run_id,
            ),
        )
