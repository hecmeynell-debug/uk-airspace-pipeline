# Failure modes

What breaks, how you find out, and what to do about it. Written from the
assumption that this pipeline depends on a free, community-run public API with
no SLA — most of these are "when", not "if".

Detection is either a **health check** (`python -m ingestion.run --check`), a
**dbt test** (`dbt build`), or a **log event**. Every log line is JSON, so the
event names below are greppable.

---

## Source failures

### OpenSky daily credit allowance exhausted

- **Symptom:** HTTP 429. Ingestion stops entirely until the allowance resets.
- **Detection:** `ingest.rate_limited` log event; exit code `2`; the
  `credit_budget` health check fails once 90% of the allowance is spent.
- **Deliberately not retried.** The quota is a *daily* allowance, so in-process
  retries spend what remains for no chance of success. The job exits and the
  scheduler backs off. The Airflow task raises `AirflowFailException` so
  Airflow's own retries do not fire either.
- **Response:** reduce cadence (`POLL_INTERVAL_SECONDS`), shrink the bounding
  box below 100 sq° to halve the per-call cost, or register credentials to move
  from 400 to 4,000 credits/day. The arithmetic is in
  [ADR-0001](adrs/0001-opensky-and-overall-architecture.md).

### Credentials rejected

- **Symptom:** HTTP 401/403 from the API or the token endpoint.
- **Detection:** `ingest.auth_failed`; exit code `3`.
- **Not retried** — credentials do not become valid by trying again. A cached
  token is dropped so the next run re-authenticates cleanly rather than looping
  on a stale credential.
- **Response:** check `OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET`. Note that
  username/password basic auth was withdrawn; only OAuth2 client credentials
  work.

### OpenSky unavailable or slow

- **Symptom:** 5xx, connection errors, timeouts.
- **Detection:** `opensky.retrying`, then `ingest.source_failed`; exit code `1`.
- **Handled:** up to `OPENSKY_MAX_ATTEMPTS` with exponential backoff and full
  jitter, so a fleet of retrying jobs does not resynchronise into a thundering
  herd.
- **Response:** usually none. A missed poll is a gap, and gaps are expected.
  Sustained failure shows up as the `freshness` and `run_failure_rate` checks.

### Feed serves malformed records

- **Symptom:** rows with a null `on_ground`, implausible timestamps,
  out-of-range coordinates.
- **Detection:** `rows_rejected` climbs in `ingest.completed`, with a
  `reject_reasons` breakdown; the `reject_rate` check fails above 1%.
- **Handled:** one bad row never fails the batch. Each is rejected with a
  stable reason code and counted.
- **Response:** inspect `reject_reasons`. A new reason code appearing in bulk
  usually means the API changed shape — check the field order in
  `ingestion/models.py` against the current documentation.

### Coverage gaps

- **Symptom:** counts drop, especially over the North Atlantic.
- **Detection:** none, and this is important — **a coverage gap is
  indistinguishable from an absence of traffic.**
- **Response:** none available. This is a permanent property of a
  community-receiver network, not a fault. It is why every aggregate in this
  project describes *observed* traffic and why the README says so plainly.
  Do not build anything that treats a drop in counts as meaningful.

---

## Pipeline failures

### Ingestion process dies mid-run

- **Symptom:** a row in `raw.ingestion_runs` stuck at `status = 'running'`.
- **Detection:** the `no_stuck_runs` check fails after 15 minutes.
- **Why it is visible:** the run row is written *before* the network call, so a
  crash leaves a record rather than no trace at all. A missing row would be
  indistinguishable from a run that never started.
- **Response:** safe to ignore once; investigate if repeated. Re-running the
  window is harmless — the load is idempotent.

### Database unreachable

- **Symptom:** connection errors at startup.
- **Detection:** `ingest.failed`; exit code `1`. Connections use an explicit
  `connect_timeout` so the job fails fast instead of hanging.
- **Response:** `docker compose ps` — Postgres has a healthcheck. Note the DSN
  uses `127.0.0.1`, not `localhost`: on Windows the latter resolves to `::1`
  first while Compose publishes on the IPv4 loopback only, which stalls every
  connection for the full TCP timeout.

### Data lands later than the reprocessing window

- **Symptom:** the hourly mart disagrees with its source for an old hour.
- **Detection:** the `assert_hourly_mart_reconciles_with_source` dbt test fails.
- **Why:** the hourly mart rebuilds only the last `late_arrival_lookback_hours`
  (default 3). Anything older is never revisited.
- **Response:** `dbt build --full-refresh --select fct_airspace_activity_hourly`.
  If it recurs, raise the lookback. The failure is deliberate — a silently
  stale mart would be worse than a loud one.

### An applied migration was edited

- **Symptom:** `MigrationError: ... has changed since it was applied`.
- **Detection:** at startup, before anything runs. Migrations are checksummed.
- **Response:** revert the edit and add a new migration. This exists to turn
  silent divergence between environments into a loud failure.

### Identity leaks into a mart

- **Symptom:** a mart gains an `icao24`, `callsign`, `origin_country` or
  coordinate column.
- **Detection:** `assert_marts_expose_no_aircraft_identity` fails, and CI goes
  red. The `airspace_reader` role separately cannot reach upstream schemas at
  all.
- **Response:** **treat as a release blocker, not a test to adjust.** This is
  the project's central constraint. See [CONSTRAINTS.md](../CONSTRAINTS.md).

### Running the test suite destroys local data

- **Symptom:** `raw.state_vectors` is unexpectedly empty.
- **Detection:** the integration fixtures refuse to run against a populated
  landing zone unless `AIRSPACE_ALLOW_DESTRUCTIVE_TESTS=1`.
- **Response:** intended behaviour in CI, where the database is throwaway. The
  guard exists because it caught exactly this happening by accident.

---

## Things deliberately *not* monitored

Alerting fires on pipeline health only: freshness, row counts, error rates,
latency, credit spend. Nothing in this project alerts on airspace *content* —
no volume anomalies, no "unusual activity", no thresholds on what aircraft were
doing. That is a
[hard non-goal](../CONSTRAINTS.md), not a gap in the implementation, and a
change that adds such a check should be rejected on sight.
