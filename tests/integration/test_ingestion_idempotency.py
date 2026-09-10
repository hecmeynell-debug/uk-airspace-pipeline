"""Integration tests against a live Postgres.

The central claim of Phase 1 is that re-running the same window does not create
duplicates. That is asserted here three ways: at the loader level, at the
whole-job level, and by confirming provenance is not rewritten on the second
pass.
"""

from __future__ import annotations

import httpx
import psycopg
import pytest
import respx

from ingestion import db as db_module
from ingestion import loader
from ingestion.models import deduplicate, parse_states
from ingestion.run import ingest_once

pytestmark = pytest.mark.integration

STATES_PATH = "/api/states/all"


def _load_once(conn, settings, vectors, snapshot_time):
    run_id = loader.start_run(conn, settings)
    inserted = loader.insert_state_vectors(conn, run_id, snapshot_time, vectors)
    loader.finish_run(
        conn,
        run_id,
        status="succeeded",
        http_status=200,
        api_snapshot_time=snapshot_time,
        rows_received=len(vectors),
        rows_rejected=0,
        rows_inserted=inserted,
        rows_duplicate=len(vectors) - inserted,
    )
    return run_id, inserted


def _count(conn, table: str) -> int:
    conn.rollback()  # ensure a fresh snapshot rather than a stale transaction
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def test_reingesting_the_same_snapshot_inserts_nothing_new(
    migrated_db, db_settings, sample_states, snapshot_time, parse_now
):
    conn = migrated_db
    vectors, rejected = parse_states(sample_states, now=parse_now)
    unique, _ = deduplicate(vectors)
    assert not rejected and len(unique) == 3

    run_a, inserted_a = _load_once(conn, db_settings, unique, snapshot_time)
    run_b, inserted_b = _load_once(conn, db_settings, unique, snapshot_time)

    assert inserted_a == 3, "first load must land every row"
    assert inserted_b == 0, "second load of the same window must insert nothing"
    assert _count(conn, "raw.state_vectors") == 3
    assert _count(conn, "raw.ingestion_runs") == 2, "both attempts stay auditable"

    # Provenance records first observation, so the re-run must not rewrite it.
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ingestion_run_id FROM raw.state_vectors")
        owners = [row[0] for row in cur.fetchall()]
    assert owners == [run_a]
    assert run_b not in owners


def test_a_later_position_for_the_same_aircraft_is_a_new_row(
    migrated_db, db_settings, sample_states, snapshot_time, parse_now
):
    """Deduplication must not collapse genuinely distinct observations."""
    conn = migrated_db
    first, _ = parse_states([sample_states[0]], now=parse_now)
    _load_once(conn, db_settings, first, snapshot_time)

    moved = list(sample_states[0])
    moved[3] = sample_states[0][3] + 5  # a fresher time_position
    later, _ = parse_states([moved], now=parse_now)
    _, inserted = _load_once(conn, db_settings, later, snapshot_time)

    assert inserted == 1
    assert _count(conn, "raw.state_vectors") == 2


def test_every_landed_row_carries_provenance(
    migrated_db, db_settings, sample_states, snapshot_time, parse_now
):
    conn = migrated_db
    vectors, _ = parse_states(sample_states, now=parse_now)
    unique, _ = deduplicate(vectors)
    _load_once(conn, db_settings, unique, snapshot_time)

    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM raw.state_vectors sv
              JOIN raw.ingestion_runs r ON r.run_id = sv.ingestion_run_id
             WHERE sv.ingested_at IS NOT NULL
               AND sv.api_snapshot_time IS NOT NULL
               AND r.request_bbox IS NOT NULL
               AND r.auth_mode IS NOT NULL
            """
        )
        fully_attributed = cur.fetchone()[0]

    assert fully_attributed == 3, "every row must be traceable to a run and its request"


def test_batching_does_not_change_the_result(
    migrated_db, db_settings, sample_states, snapshot_time, parse_now
):
    """A batch size smaller than the payload must land exactly the same rows."""
    conn = migrated_db
    vectors, _ = parse_states(sample_states, now=parse_now)
    unique, _ = deduplicate(vectors)

    run_id = loader.start_run(conn, db_settings)
    inserted = loader.insert_state_vectors(conn, run_id, snapshot_time, unique, batch_size=2)

    assert inserted == 3
    assert _count(conn, "raw.state_vectors") == 3


def test_ingest_role_cannot_mutate_landed_rows(
    migrated_db, db_settings, sample_states, snapshot_time, parse_now
):
    """The landing zone is append-only to the ingest role. Least privilege, enforced."""
    conn = migrated_db
    vectors, _ = parse_states(sample_states, now=parse_now)
    unique, _ = deduplicate(vectors)
    _load_once(conn, db_settings, unique, snapshot_time)
    conn.rollback()

    for statement in (
        "UPDATE raw.state_vectors SET callsign = 'TAMPERED'",
        "DELETE FROM raw.state_vectors",
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
            conn.execute(statement)


def test_ingest_role_cannot_issue_ddl(migrated_db):
    conn = migrated_db
    with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
        conn.execute("CREATE TABLE raw.should_not_exist (id int)")


def test_migrations_are_idempotent(db_settings):
    with psycopg.connect(db_settings.owner_dsn) as conn:
        db_module.apply_migrations(conn)
        second_pass = db_module.apply_migrations(conn)

    assert second_pass == [], "a second migration run must be a no-op"


def test_migrations_leave_no_transaction_open(db_settings):
    """Regression: a lingering implicit transaction demotes every per-migration
    transaction to a savepoint, so a late failure would roll back migrations
    that had already reported success - and holds locks until the connection
    closes."""
    with psycopg.connect(db_settings.owner_dsn) as conn:
        db_module.apply_migrations(conn)
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE


def test_editing_an_applied_migration_is_detected(db_settings, tmp_path):
    """Silent drift between environments must fail loudly instead."""
    migration = tmp_path / "0001_test.sql"
    migration.write_text("SELECT 1;", encoding="utf-8")

    with psycopg.connect(db_settings.owner_dsn) as conn:
        try:
            db_module.apply_migrations(conn, migrations_dir=tmp_path)
            migration.write_text("SELECT 2;", encoding="utf-8")

            with pytest.raises(db_module.MigrationError, match="has changed"):
                db_module.apply_migrations(conn, migrations_dir=tmp_path)
        finally:
            with conn.transaction():
                conn.execute(
                    "DELETE FROM raw.schema_migrations WHERE filename = %s", ("0001_test.sql",)
                )


@respx.mock
def test_end_to_end_job_is_idempotent(migrated_db, db_settings, sample_states):
    """The whole job, twice, against a mocked API and a real database."""
    respx.get(path=STATES_PATH).mock(
        return_value=httpx.Response(
            200, json={"time": int(sample_states[0][4]) + 2, "states": sample_states}
        )
    )

    first = ingest_once(db_settings)
    second = ingest_once(db_settings)

    assert first is not None and second is not None
    assert first.rows_inserted == 3
    assert first.rows_duplicate == 0
    assert second.rows_inserted == 0
    assert second.rows_duplicate == 3, "the second pass sees every row as already present"
    assert _count(migrated_db, "raw.state_vectors") == 3
    assert _count(migrated_db, "raw.ingestion_runs") == 2


@respx.mock
def test_a_failed_run_is_recorded_rather_than_vanishing(migrated_db, db_settings):
    from ingestion.opensky import OpenSkyTransient

    respx.get(path=STATES_PATH).mock(return_value=httpx.Response(503))

    with pytest.raises(OpenSkyTransient):
        ingest_once(db_settings)

    conn = migrated_db
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("SELECT status, error_message FROM raw.ingestion_runs")
        rows = cur.fetchall()

    assert len(rows) == 1
    assert rows[0][0] == "failed"
    assert rows[0][1], "a failed run must record why"
    assert _count(conn, "raw.state_vectors") == 0
