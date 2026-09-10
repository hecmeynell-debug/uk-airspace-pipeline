"""Pipeline health checks.

Every check here is about the *pipeline*: is data still arriving, are runs
succeeding, is the feed well-formed, are we inside the credit allowance.
CONSTRAINTS.md permits alerting on exactly this and nothing else - there is
deliberately no check derived from what the aircraft were doing, and adding one
would breach the project's non-goals rather than merely be out of scope.

Designed to be callable three ways with the same semantics: from the CLI
(`--check`), from an Airflow task, and from CI. Each returns a structured
result rather than printing, so the caller decides what failure means.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg

from ingestion.config import Settings
from ingestion.logging_setup import get_logger

log = get_logger(__name__)

# Multiples of the poll interval before staleness is a problem. Three missed
# polls is a pattern; one is a blip on a public API with no SLA.
STALENESS_POLL_MULTIPLE = 3

# A run stuck in 'running' for longer than this has almost certainly died
# without recording its outcome.
STUCK_RUN_MINUTES = 15

MAX_FAILURE_RATE_PCT = 20.0
MAX_REJECT_RATE_PCT = 1.0

# Fraction of the daily allowance that counts as comfortable.
CREDIT_BUDGET_WARN_FRACTION = 0.9


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    ok: bool
    detail: str
    value: float | None = None
    threshold: float | None = None

    @property
    def status(self) -> str:
        return "ok" if self.ok else "FAIL"


def _scalar(conn: psycopg.Connection, sql: str, params: tuple = ()) -> object:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def check_freshness(conn: psycopg.Connection, settings: Settings) -> CheckResult:
    """Is data still arriving?"""
    threshold_minutes = (settings.poll_interval_seconds * STALENESS_POLL_MULTIPLE) / 60

    age = _scalar(
        conn,
        "SELECT extract(epoch from (now() - max(ingested_at))) / 60 FROM raw.state_vectors",
    )

    if age is None:
        return CheckResult(
            name="freshness",
            ok=False,
            detail="raw.state_vectors is empty; no ingestion has succeeded yet",
            threshold=threshold_minutes,
        )

    age_minutes = float(age)
    return CheckResult(
        name="freshness",
        ok=age_minutes <= threshold_minutes,
        detail=(
            f"most recent row landed {age_minutes:.1f} min ago "
            f"(threshold {threshold_minutes:.1f} min)"
        ),
        value=round(age_minutes, 1),
        threshold=threshold_minutes,
    )


def check_run_failure_rate(conn: psycopg.Connection, _settings: Settings) -> CheckResult:
    """Are runs succeeding?"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)                                    AS total,
                   count(*) FILTER (WHERE status = 'failed')   AS failed
              FROM raw.ingestion_runs
             WHERE started_at >= now() - interval '1 hour'
            """
        )
        total, failed = cur.fetchone()

    if not total:
        return CheckResult(
            name="run_failure_rate",
            ok=True,
            detail="no runs in the last hour; nothing to judge",
        )

    rate = 100.0 * failed / total
    return CheckResult(
        name="run_failure_rate",
        ok=rate <= MAX_FAILURE_RATE_PCT,
        detail=f"{failed}/{total} runs failed in the last hour ({rate:.1f}%)",
        value=round(rate, 1),
        threshold=MAX_FAILURE_RATE_PCT,
    )


def check_no_stuck_runs(conn: psycopg.Connection, _settings: Settings) -> CheckResult:
    """A run row is written before the network call, so a crash leaves it 'running'."""
    stuck = _scalar(
        conn,
        """
        SELECT count(*) FROM raw.ingestion_runs
         WHERE status = 'running'
           AND started_at < now() - make_interval(mins => %s)
        """,
        (STUCK_RUN_MINUTES,),
    )
    count = int(stuck or 0)
    return CheckResult(
        name="no_stuck_runs",
        ok=count == 0,
        detail=(
            f"{count} run(s) still marked running after {STUCK_RUN_MINUTES} min"
            if count
            else "no abandoned runs"
        ),
        value=count,
        threshold=0,
    )


def check_reject_rate(conn: psycopg.Connection, _settings: Settings) -> CheckResult:
    """Is the feed well-formed? A rising reject rate means OpenSky is serving junk."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT coalesce(sum(rows_received), 0),
                   coalesce(sum(rows_rejected), 0)
              FROM raw.ingestion_runs
             WHERE status = 'succeeded'
               AND started_at >= now() - interval '1 hour'
            """
        )
        received, rejected = cur.fetchone()

    if not received:
        return CheckResult(
            name="reject_rate",
            ok=True,
            detail="no rows received in the last hour; nothing to judge",
        )

    rate = 100.0 * rejected / received
    return CheckResult(
        name="reject_rate",
        ok=rate <= MAX_REJECT_RATE_PCT,
        detail=f"{rejected}/{received} rows rejected in the last hour ({rate:.3f}%)",
        value=round(rate, 3),
        threshold=MAX_REJECT_RATE_PCT,
    )


def check_credit_budget(conn: psycopg.Connection, settings: Settings) -> CheckResult:
    """Are we inside the API allowance? Exceeding it stops ingestion entirely."""
    allowance = settings.daily_credit_allowance
    limit = allowance * CREDIT_BUDGET_WARN_FRACTION

    spent = _scalar(
        conn,
        """
        SELECT coalesce(sum(credits_estimated), 0)
          FROM raw.ingestion_runs
         WHERE started_at >= now() - interval '24 hours'
        """,
    )
    spent_credits = float(spent or 0)

    return CheckResult(
        name="credit_budget",
        ok=spent_credits <= limit,
        detail=(
            f"{spent_credits:.0f} credits spent in 24h against a {allowance} allowance "
            f"({settings.auth_mode})"
        ),
        value=spent_credits,
        threshold=limit,
    )


ALL_CHECKS = (
    check_freshness,
    check_run_failure_rate,
    check_no_stuck_runs,
    check_reject_rate,
    check_credit_budget,
)


def run_health_checks(
    conn: psycopg.Connection, settings: Settings, *, now: datetime | None = None
) -> list[CheckResult]:
    """Run every check. Never raises for a failed check - that is the caller's call."""
    del now  # reserved for future time-travelling tests; queries use now() in SQL
    results = [check(conn, settings) for check in ALL_CHECKS]

    for result in results:
        log.info(
            "healthcheck.result",
            check=result.name,
            status=result.status,
            detail=result.detail,
            value=result.value,
            threshold=result.threshold,
        )

    failed = [r.name for r in results if not r.ok]
    log.info(
        "healthcheck.summary",
        checked_at=(datetime.now(tz=UTC)).isoformat(),
        total=len(results),
        failed=len(failed),
        failed_checks=failed,
    )
    return results
