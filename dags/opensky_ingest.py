"""Scheduled ingestion of public ADS-B state vectors.

Polls OpenSky's `/states/all` for the configured bounding box and lands the
result in the `raw` schema. Aggregate, descriptive use only - this DAG does no
tracking, scoring, prediction or per-airframe analysis, and nothing downstream
of it may either. See CONSTRAINTS.md.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=5),
}


@dag(
    dag_id="opensky_ingest",
    description="Land public ADS-B state vectors for the UK FIR into the raw schema",
    schedule="*/2 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Backfill is meaningless here and would be actively harmful: OpenSky
    # serves live state vectors (one hour of history for registered users), so
    # a catch-up run cannot retrieve the window it is nominally filling. It
    # would simply re-fetch *now* under an old logical date, mislabelling the
    # data and burning credits. Historical gaps stay gaps, honestly.
    catchup=False,
    # One run at a time. Overlapping runs would race for the same credit
    # budget and produce nothing extra, since the loader is idempotent.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "opensky", "raw"],
    doc_md=__doc__,
)
def opensky_ingest() -> None:
    @task
    def ingest_snapshot() -> dict[str, int]:
        # Imported inside the task, not at module scope: the dag-processor
        # re-parses this file constantly, and it should not pay to import the
        # ingestion stack every time just to build the graph.
        from ingestion.config import Settings
        from ingestion.logging_setup import configure_logging
        from ingestion.opensky import OpenSkyAuthError, OpenSkyRateLimited
        from ingestion.run import ingest_once

        settings = Settings()
        configure_logging(settings.log_level)

        try:
            result = ingest_once(settings)
        except OpenSkyRateLimited as exc:
            # Fail without retrying. The quota is a daily credit allowance, so
            # Airflow's retries would spend what is left of it for nothing.
            raise AirflowFailException(
                f"OpenSky credit allowance exhausted; not retrying: {exc}"
            ) from exc
        except OpenSkyAuthError as exc:
            # Credentials do not become valid by trying again either.
            raise AirflowFailException(f"OpenSky rejected our credentials: {exc}") from exc

        if result is None:  # pragma: no cover - only reachable in dry-run mode
            return {}

        # Returned so the counts land in XCom and are visible per run in the UI.
        return {
            "rows_received": result.rows_received,
            "rows_rejected": result.rows_rejected,
            "rows_inserted": result.rows_inserted,
            "rows_duplicate": result.rows_duplicate,
        }

    ingest_snapshot()


opensky_ingest()
