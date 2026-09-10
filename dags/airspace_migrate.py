"""Apply raw-schema migrations.

Deliberately a separate, manually triggered DAG rather than a task inside
`opensky_ingest`. Schema changes are a deployment step, not something a
data pipeline should perform on every scheduled cycle - and this is the only
place in the running system that connects with DDL privileges.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="airspace_migrate",
    description="Apply pending raw-schema migrations (manual, runs as airspace_owner)",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["schema", "migration"],
    doc_md=__doc__,
)
def airspace_migrate() -> None:
    @task
    def apply() -> list[str]:
        from ingestion import db
        from ingestion.config import Settings
        from ingestion.logging_setup import configure_logging

        settings = Settings()
        configure_logging(settings.log_level)

        with db.connect(settings.owner_dsn) as conn:
            applied = db.apply_migrations(conn)

        return applied

    apply()


airspace_migrate()
