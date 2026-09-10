"""Ingestion entry point.

    python -m ingestion.run --migrate     apply schema migrations, then exit
    python -m ingestion.run --once        fetch and load one snapshot
    python -m ingestion.run --dry-run     fetch and validate, write nothing
    python -m ingestion.run --loop        poll continuously (local demo only)

Exit codes are distinct so a scheduler can react appropriately:
    0  success
    1  failure
    2  rate limited - back off until the daily allowance resets
    3  configuration or migration error
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime

from pydantic import ValidationError

from ingestion import db, loader
from ingestion.config import Settings
from ingestion.logging_setup import configure_logging, get_logger
from ingestion.models import deduplicate, parse_states
from ingestion.opensky import (
    OpenSkyAuthError,
    OpenSkyClient,
    OpenSkyError,
    OpenSkyRateLimited,
)

log = get_logger(__name__)

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_RATE_LIMITED = 2
EXIT_CONFIG_ERROR = 3


def ingest_once(settings: Settings, *, dry_run: bool = False) -> loader.LoadResult | None:
    """Fetch one snapshot and land it. Returns None for a dry run."""
    started = time.monotonic()

    with OpenSkyClient(settings) as client:
        if dry_run:
            response = client.fetch_states()
            vectors, rejected = parse_states(response.rows)
            unique, intra_batch_duplicates = deduplicate(vectors)
            log.info(
                "ingest.dry_run",
                rows_received=len(response.rows),
                rows_valid=len(vectors),
                rows_rejected=len(rejected),
                rows_unique=len(unique),
                rows_duplicate_in_batch=intra_batch_duplicates,
                reject_reasons=_reason_counts(rejected),
                api_snapshot_time=response.api_snapshot_time.isoformat(),
                lag_seconds=_lag_seconds(response.api_snapshot_time),
            )
            return None

        with db.connect(settings.ingest_dsn) as conn:
            # The run row is written before the network call, so a crash leaves
            # a visible 'running' record rather than no trace at all.
            run_id = loader.start_run(conn, settings)
            log.info(
                "ingest.started",
                run_id=str(run_id),
                auth_mode=settings.auth_mode,
                bbox=settings.bbox_params,
                credits_estimated=settings.credit_cost,
            )

            try:
                response = client.fetch_states()
                vectors, rejected = parse_states(response.rows)
                unique, intra_batch_duplicates = deduplicate(vectors)

                inserted = loader.insert_state_vectors(
                    conn, run_id, response.api_snapshot_time, unique
                )
                # Anything valid that was not inserted was already present from
                # an earlier run - which is exactly the idempotency guarantee.
                already_present = len(unique) - inserted
                duplicates = already_present + intra_batch_duplicates

                loader.finish_run(
                    conn,
                    run_id,
                    status="succeeded",
                    http_status=response.http_status,
                    api_snapshot_time=response.api_snapshot_time,
                    rows_received=len(response.rows),
                    rows_rejected=len(rejected),
                    rows_inserted=inserted,
                    rows_duplicate=duplicates,
                )
            except Exception as exc:
                loader.finish_run(
                    conn, run_id, status="failed", error_message=f"{type(exc).__name__}: {exc}"
                )
                raise

            result = loader.LoadResult(
                run_id=run_id,
                rows_received=len(response.rows),
                rows_rejected=len(rejected),
                rows_inserted=inserted,
                rows_duplicate=duplicates,
            )

            log.info(
                "ingest.completed",
                run_id=str(run_id),
                rows_received=result.rows_received,
                rows_rejected=result.rows_rejected,
                rows_inserted=result.rows_inserted,
                rows_duplicate=result.rows_duplicate,
                reject_reasons=_reason_counts(rejected),
                api_snapshot_time=response.api_snapshot_time.isoformat(),
                lag_seconds=_lag_seconds(response.api_snapshot_time),
                duration_seconds=round(time.monotonic() - started, 3),
            )
            return result


def _reason_counts(rejected: list) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rejected:
        counts[row.reason] = counts.get(row.reason, 0) + 1
    return counts


def _lag_seconds(snapshot_time: datetime) -> float:
    """How stale the API snapshot was when we received it."""
    return round((datetime.now(tz=UTC) - snapshot_time).total_seconds(), 1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airspace-ingest",
        description="Ingest public ADS-B state vectors into the raw schema.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--migrate", action="store_true", help="apply schema migrations and exit")
    mode.add_argument("--once", action="store_true", help="fetch and load a single snapshot")
    mode.add_argument("--dry-run", action="store_true", help="fetch and validate, write nothing")
    mode.add_argument("--loop", action="store_true", help="poll continuously (local demo only)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        settings = Settings()
    except ValidationError as exc:
        configure_logging("INFO")
        log.error("config.invalid", errors=exc.errors())
        return EXIT_CONFIG_ERROR

    configure_logging(settings.log_level)

    if args.migrate:
        try:
            # Migrations need DDL rights, so this is the only place the owner
            # connection is used.
            with db.connect(settings.owner_dsn) as conn:
                applied = db.apply_migrations(conn)
            log.info("migrate.done", applied=applied, count=len(applied))
            return EXIT_OK
        except db.MigrationError as exc:
            log.error("migrate.failed", error=str(exc))
            return EXIT_CONFIG_ERROR

    if settings.auth_mode == "anonymous":
        log.warning(
            "opensky.anonymous_mode",
            message=(
                "Running without credentials: 400 credits/day and 10s resolution. "
                "Suitable for a smoke test only."
            ),
            daily_credit_budget=settings.daily_credit_budget,
        )
    elif settings.daily_credit_budget > 4000:
        log.warning(
            "opensky.budget_exceeded",
            daily_credit_budget=settings.daily_credit_budget,
            allowance=4000,
            message="Configured cadence would exceed the registered daily allowance.",
        )

    try:
        if args.loop:
            while True:
                cycle_started = time.monotonic()
                ingest_once(settings)
                elapsed = time.monotonic() - cycle_started
                # Subtract work already done so the cadence - and therefore the
                # credit budget - stays fixed regardless of how long a fetch took.
                time.sleep(max(settings.poll_interval_seconds - elapsed, 0))
        else:
            ingest_once(settings, dry_run=args.dry_run)
    except OpenSkyRateLimited as exc:
        log.error("ingest.rate_limited", error=str(exc), retry_after=exc.retry_after)
        return EXIT_RATE_LIMITED
    except OpenSkyAuthError as exc:
        log.error("ingest.auth_failed", error=str(exc))
        return EXIT_CONFIG_ERROR
    except OpenSkyError as exc:
        log.error("ingest.source_failed", error=str(exc), error_type=type(exc).__name__)
        return EXIT_FAILURE
    except KeyboardInterrupt:
        log.info("ingest.interrupted")
        return EXIT_OK
    except Exception as exc:
        log.error("ingest.failed", error=str(exc), error_type=type(exc).__name__)
        return EXIT_FAILURE

    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
