"""Thin read API over the aggregate marts.

Connects as ``airspace_reader`` and nothing else. That role has no privileges
on raw, staging or intermediate, so this service is structurally incapable of
serving per-airframe data - it is not a matter of the endpoints choosing not
to. See CONSTRAINTS.md.

Read-only by construction: no endpoint writes, no request value reaches SQL
text, and every window parameter is bounded.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg_pool import ConnectionPool

from api import queries
from ingestion.config import Settings
from ingestion.logging_setup import configure_logging, get_logger

log = get_logger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"

LIMITATIONS = (
    "Counts describe traffic OBSERVED by the OpenSky community receiver network, "
    "which is not the same as traffic that flew. Coverage is good over populated "
    "land and sparse over open ocean, so North Atlantic figures systematically "
    "understate reality. State vectors are sampled every two minutes, so events "
    "between polls are invisible. Aggregate and descriptive only."
)

_settings = Settings()
_pool: ConnectionPool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    configure_logging(_settings.log_level)

    # A small pool: this is a dashboard, not a high-traffic service, and an
    # oversized pool on a single Postgres would just hold idle connections.
    _pool = ConnectionPool(
        conninfo=_settings.reader_dsn,
        min_size=1,
        max_size=4,
        open=True,
        timeout=10,
        kwargs={"autocommit": True},
    )
    log.info("api.started", role="airspace_reader", pool_max=4)
    try:
        yield
    finally:
        _pool.close()
        log.info("api.stopped")


app = FastAPI(
    title="UK Airspace Pipeline",
    description=(
        "Aggregate, descriptive read API over public ADS-B data. "
        "No individual aircraft tracking, no intent prediction, no threat scoring."
    ),
    version="0.1.0",
    lifespan=lifespan,
)


def get_conn():
    """Yield a pooled read-only connection."""
    assert _pool is not None, "connection pool not initialised"
    with _pool.connection() as conn:
        yield conn


def _payload(rows: Any, **extra: Any) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "data": rows,
        **extra,
    }


def _not_ready() -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "status": "marts_not_built",
            "detail": "The transformation layer has not run yet. Run `dbt build`.",
        },
    )


@app.get("/health", tags=["service"])
def service_health() -> dict[str, str]:
    """Liveness of this service. Says nothing about the pipeline."""
    return {"status": "ok"}


@app.get("/api/meta", tags=["metadata"])
def meta() -> dict[str, Any]:
    """What this data is, and - just as importantly - what it is not."""
    return {
        "region": {
            "lat_min": _settings.opensky_lat_min,
            "lat_max": _settings.opensky_lat_max,
            "lon_min": _settings.opensky_lon_min,
            "lon_max": _settings.opensky_lon_max,
        },
        "source": "OpenSky Network public ADS-B API",
        "poll_interval_seconds": _settings.poll_interval_seconds,
        "limitations": LIMITATIONS,
        "non_goals": [
            "No individual aircraft tracking",
            "No intent prediction, threat scoring or anomaly-as-threat",
            "No non-public data sources",
            "No operational alerting",
            "Aggregate and descriptive outputs only",
        ],
        "generated_at": datetime.now(tz=UTC).isoformat(),
    }


@app.get("/api/freshness", tags=["pipeline"])
def freshness(conn=Depends(get_conn)):
    try:
        return _payload(queries.freshness(conn))
    except queries.MartsNotBuilt:
        return _not_ready()


@app.get("/api/activity/grid", tags=["activity"])
def activity_grid(
    hours: int = Query(24, ge=1, le=720, description="Look-back window in hours"),
    conn=Depends(get_conn),
):
    """Observation density per grid cell. Cells, not positions."""
    try:
        return _payload(queries.grid_density(conn, hours), window_hours=hours)
    except queries.MartsNotBuilt:
        return _not_ready()


@app.get("/api/activity/hourly", tags=["activity"])
def activity_hourly(
    hours: int = Query(48, ge=1, le=720),
    conn=Depends(get_conn),
):
    try:
        return _payload(queries.hourly_totals(conn, hours), window_hours=hours)
    except queries.MartsNotBuilt:
        return _not_ready()


@app.get("/api/activity/altitude", tags=["activity"])
def activity_altitude(
    hours: int = Query(24, ge=1, le=720),
    conn=Depends(get_conn),
):
    try:
        return _payload(queries.altitude_profile(conn, hours), window_hours=hours)
    except queries.MartsNotBuilt:
        return _not_ready()


@app.get("/api/activity/daily", tags=["activity"])
def activity_daily(
    days: int = Query(14, ge=1, le=365),
    conn=Depends(get_conn),
):
    try:
        return _payload(queries.daily_profile(conn, days), window_days=days)
    except queries.MartsNotBuilt:
        return _not_ready()


@app.get("/api/pipeline-health", tags=["pipeline"])
def api_pipeline_health(
    hours: int = Query(24, ge=1, le=720),
    conn=Depends(get_conn),
):
    """Pipeline health only: runs, error rates, latency, credit spend."""
    try:
        return _payload(queries.pipeline_health(conn, hours), window_hours=hours)
    except queries.MartsNotBuilt:
        return _not_ready()


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
