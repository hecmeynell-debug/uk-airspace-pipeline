"""Read queries for the dashboard.

Every query here reads the marts and nothing else, and every one is a grouped
aggregate. That is not merely a convention: the connection this module runs on
is `airspace_reader`, which has no privileges on raw, staging or intermediate,
so a query for per-airframe data would fail rather than succeed quietly.

All SQL is static with bound parameters. Nothing from the request reaches the
query text.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

# Marts only. Named here so the surface is obvious at a glance.
HOURLY = "analytics_marts.fct_airspace_activity_hourly"
DAILY = "analytics_marts.fct_airspace_activity_daily"
HEALTH = "analytics_marts.fct_ingestion_health"


class MartsNotBuilt(RuntimeError):
    """The marts do not exist yet - dbt has not run against this database."""


def _fetch(conn: psycopg.Connection, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except (psycopg.errors.UndefinedTable, psycopg.errors.InvalidSchemaName) as exc:
        # A fresh database has no marts until dbt builds them. That is an
        # expected state on first run, not an error worth a 500.
        conn.rollback()
        raise MartsNotBuilt("marts have not been built yet; run dbt build") from exc


def grid_density(conn: psycopg.Connection, hours: int) -> list[dict[str, Any]]:
    """Observation density and dominant flow direction per grid cell.

    Cells, not positions. The resolution is coarse by design - it shows where
    traffic was dense and which way it was broadly moving, not where anything
    was or where any particular aircraft went.

    Combining the per-hour circular means back into one bearing needs care.
    Each row stores a mean bearing and a resultant length, which together are
    the polar form of a vector; converting back to components, taking a
    sample-weighted mean of those, and re-deriving the angle is exact. Taking
    a plain average of the bearings would not be - it would put the mean of
    350 and 10 degrees at 180, pointing precisely backwards.
    """
    return _fetch(
        conn,
        f"""
        WITH cells AS (
            SELECT grid_lat,
                   grid_lon,
                   sum(observation_count)   AS observation_count,
                   sum(distinct_aircraft)   AS aircraft_bucket_total,
                   sum(track_sample_count)  AS track_sample_count,
                   sum(track_sample_count * track_concentration
                       * sin(radians(dominant_track_deg)))
                       / nullif(sum(track_sample_count), 0) AS mean_sin,
                   sum(track_sample_count * track_concentration
                       * cos(radians(dominant_track_deg)))
                       / nullif(sum(track_sample_count), 0) AS mean_cos
              FROM {HOURLY}
             WHERE activity_hour >= now() - make_interval(hours => %s)
             GROUP BY grid_lat, grid_lon
        )
        SELECT grid_lat,
               grid_lon,
               observation_count,
               aircraft_bucket_total,
               track_sample_count,
               CASE WHEN mean_sin IS NOT NULL
                    THEN round(mod(degrees(atan2(mean_sin, mean_cos))::numeric + 360, 360), 1)
               END AS dominant_track_deg,
               CASE WHEN mean_sin IS NOT NULL
                    THEN round(sqrt(mean_sin ^ 2 + mean_cos ^ 2)::numeric, 3)
               END AS track_concentration
          FROM cells
         ORDER BY grid_lat, grid_lon
        """,
        (hours,),
    )


def hourly_totals(conn: psycopg.Connection, hours: int) -> list[dict[str, Any]]:
    return _fetch(
        conn,
        f"""
        SELECT activity_hour,
               sum(observation_count) AS observation_count,
               sum(distinct_aircraft) AS aircraft_bucket_total,
               count(*)               AS populated_cells
          FROM {HOURLY}
         WHERE activity_hour >= now() - make_interval(hours => %s)
         GROUP BY activity_hour
         ORDER BY activity_hour
        """,
        (hours,),
    )


def altitude_profile(conn: psycopg.Connection, hours: int) -> list[dict[str, Any]]:
    return _fetch(
        conn,
        f"""
        SELECT altitude_band,
               sum(observation_count)                   AS observation_count,
               round(avg(avg_velocity_kts)::numeric, 1) AS avg_velocity_kts
          FROM {HOURLY}
         WHERE activity_hour >= now() - make_interval(hours => %s)
         GROUP BY altitude_band
         ORDER BY sum(observation_count) DESC
        """,
        (hours,),
    )


def daily_profile(conn: psycopg.Connection, days: int) -> list[dict[str, Any]]:
    return _fetch(
        conn,
        f"""
        SELECT activity_date,
               altitude_band,
               observation_count,
               distinct_aircraft,
               distinct_grid_cells
          FROM {DAILY}
         WHERE activity_date >= (current_date - make_interval(days => %s))
         ORDER BY activity_date, altitude_band
        """,
        (days,),
    )


def pipeline_health(conn: psycopg.Connection, hours: int) -> list[dict[str, Any]]:
    """Pipeline health only. Nothing here describes the airspace."""
    return _fetch(
        conn,
        f"""
        SELECT health_hour,
               run_count,
               succeeded_count,
               failed_count,
               success_rate_pct,
               reject_rate_pct,
               rows_inserted,
               rows_duplicate,
               avg_lag_seconds,
               credits_consumed
          FROM {HEALTH}
         WHERE health_hour >= now() - make_interval(hours => %s)
         ORDER BY health_hour
        """,
        (hours,),
    )


def freshness(conn: psycopg.Connection) -> dict[str, Any]:
    """How current the published aggregates are, from the marts alone."""
    rows = _fetch(
        conn,
        f"""
        SELECT max(last_ingested_at)                                    AS last_ingested_at,
               max(activity_hour)                                       AS latest_activity_hour,
               extract(epoch from (now() - max(last_ingested_at)))       AS age_seconds
          FROM {HOURLY}
        """,
    )
    return rows[0] if rows else {}
