"""Pipeline health checks.

Each test drives a check into its failing state deliberately. A monitoring
check that has only ever been observed passing is not a check - it is a
decoration, and it will keep quietly passing on the day the pipeline dies.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from ingestion import checks, loader
from ingestion.models import deduplicate, parse_states

pytestmark = pytest.mark.integration


def _names(results: list[checks.CheckResult]) -> dict[str, checks.CheckResult]:
    return {result.name: result for result in results}


def test_empty_landing_zone_fails_freshness(migrated_db, db_settings):
    """An empty database must fail loudly, not report healthy by vacuous truth."""
    results = _names(checks.run_health_checks(migrated_db, db_settings))

    assert results["freshness"].ok is False
    assert "empty" in results["freshness"].detail


def test_fresh_ingestion_passes_every_check(
    migrated_db, db_settings, sample_states, snapshot_time, parse_now
):
    conn = migrated_db
    vectors, _ = parse_states(sample_states, now=parse_now)
    unique, _ = deduplicate(vectors)

    run_id = loader.start_run(conn, db_settings)
    inserted = loader.insert_state_vectors(conn, run_id, snapshot_time, unique)
    loader.finish_run(
        conn,
        run_id,
        status="succeeded",
        http_status=200,
        api_snapshot_time=snapshot_time,
        rows_received=len(unique),
        rows_rejected=0,
        rows_inserted=inserted,
        rows_duplicate=0,
    )
    conn.rollback()

    results = checks.run_health_checks(conn, db_settings)

    failed = [r.name for r in results if not r.ok]
    assert failed == [], f"unexpected failures: {[(r.name, r.detail) for r in results if not r.ok]}"


def test_abandoned_run_is_detected(migrated_db, db_settings):
    """A run row is written before the network call, so a crash strands it."""
    conn = migrated_db
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO raw.ingestion_runs
                (run_id, source_endpoint, request_bbox, auth_mode, credits_estimated, started_at)
            VALUES (%s, '/states/all', '{}'::jsonb, 'anonymous', 3,
                    now() - interval '2 hours')
            """,
            (uuid.uuid4(),),
        )
    conn.rollback()

    result = _names(checks.run_health_checks(conn, db_settings))["no_stuck_runs"]

    assert result.ok is False
    assert result.value == 1


def test_failed_runs_breach_the_failure_rate(migrated_db, db_settings):
    conn = migrated_db
    for _ in range(3):
        run_id = loader.start_run(conn, db_settings)
        loader.finish_run(conn, run_id, status="failed", error_message="synthetic failure")
    conn.rollback()

    result = _names(checks.run_health_checks(conn, db_settings))["run_failure_rate"]

    assert result.ok is False
    assert result.value == 100.0


def test_malformed_feed_breaches_the_reject_rate(migrated_db, db_settings):
    """A rising reject rate means the source is serving junk, not that anything flew oddly."""
    conn = migrated_db
    run_id = loader.start_run(conn, db_settings)
    loader.finish_run(
        conn,
        run_id,
        status="succeeded",
        http_status=200,
        api_snapshot_time=datetime.now(tz=UTC),
        rows_received=100,
        rows_rejected=50,
        rows_inserted=50,
        rows_duplicate=0,
    )
    conn.rollback()

    result = _names(checks.run_health_checks(conn, db_settings))["reject_rate"]

    assert result.ok is False
    assert result.value == pytest.approx(50.0)


def test_credit_budget_tracks_the_documented_allowance(migrated_db, db_settings):
    """Anonymous access allows 400 credits/day; the check must use that, not 4000."""
    conn = migrated_db
    assert db_settings.daily_credit_allowance == 400, "fixture assumes anonymous access"

    # 130 runs at 3 credits = 390, past the 90% (360) comfort threshold.
    with conn.transaction():
        for _ in range(130):
            conn.execute(
                """
                INSERT INTO raw.ingestion_runs
                    (run_id, source_endpoint, request_bbox, auth_mode,
                     credits_estimated, started_at, finished_at, status)
                VALUES (%s, '/states/all', '{}'::jsonb, 'anonymous', 3,
                        now() - interval '1 minute', now(), 'succeeded')
                """,
                (uuid.uuid4(),),
            )
    conn.rollback()

    result = _names(checks.run_health_checks(conn, db_settings))["credit_budget"]

    assert result.ok is False
    assert result.value == 390.0
    assert "400 allowance" in result.detail
