# ADR-0002: Idempotency enforced in the database, and how late data is handled

- **Status:** Accepted
- **Date:** 2026-09-10
- **Deciders:** Project author
- **Relates to:** [ADR-0001](0001-opensky-and-overall-architecture.md)

## Context

Every scheduled pipeline reprocesses data eventually. A task retries after a
timeout, an operator reruns a window, a deploy replays a backfill, two workers
overlap. Duplicates are not an edge case — they are the *default* outcome
unless something actively prevents them.

There is a second, less obvious problem. An observation can arrive long after
the period it belongs to. A poll at 12:01 returns positions timestamped 11:58.
A retry after an outage lands positions much older than that. So "don't
duplicate" and "don't leave an already-built period stale" are two different
requirements, and solving the first does nothing for the second.

## Decision

### Idempotency is a database constraint, not application logic

`raw.state_vectors` has a primary key on `(icao24, observed_at)`, and every
load uses `INSERT ... ON CONFLICT DO NOTHING`.

`observed_at` is `time_position` where the API reported one, and `last_contact`
otherwise. The fallback matters: a row with no position report still needs to
be addressable, and dropping it would silently lose the ability to measure how
much of the feed lacks a position.

The key point is *where* the guarantee lives. An application-side check —
"have I seen this row?" — is a read followed by a write, and two workers can
both read "no" before either writes. A unique constraint cannot be raced. The
database is the only place in this system where the guarantee can be made
absolutely, so that is where it is made.

### Provenance records first observation, not last

`ON CONFLICT DO NOTHING` rather than an upsert, deliberately. A re-ingested row
keeps the `ingestion_run_id` of the run that *first* saw it. Overwriting would
mean the provenance column answered "which run most recently touched this?",
which is a much less useful question than "where did this row come from?".

Re-running a window therefore leaves the landed rows untouched and adds a
second row to `raw.ingestion_runs` recording the attempt. Both facts stay
visible.

### Late-arriving data is handled by rebuilding periods, not appending to them

`fct_airspace_activity_hourly` is incremental with
`incremental_strategy = 'delete+insert'` keyed on `activity_hour`, and its
incremental filter is on **`observed_at`** with a lookback window
(`late_arrival_lookback_hours`, default 3).

Filtering on `ingested_at` instead — the obvious choice — would collect the
late rows but leave the *hour* they belong to already built and now wrong. The
mart would be quietly inconsistent with its own source, and nothing would say
so. Filtering on `observed_at` with a lookback and rebuilding each affected
hour wholesale means a recomputed hour is always correct, at the cost of
recomputing hours that had not changed. That trade is worth taking: rebuilding
an hour is cheap, and reasoning about a partially-merged aggregate is not.

Data arriving *beyond* the lookback is not silently half-merged. The
`assert_hourly_mart_reconciles_with_source` test compares the mart against its
source for every closed hour and fails, naming `--full-refresh` as the remedy.

### The daily mart is not rolled up from the hourly one

`fct_airspace_activity_daily` is built from the intermediate model directly.
Distinct airframe counts cannot be summed across hours — an aircraft seen in
three hours is one aircraft that day, not three — so rolling up would inflate
the figure in a way that looks entirely plausible. It costs another scan of
the same data. At this volume, correctness is worth more than the scan.

## Alternatives considered

| Option | Why rejected |
|---|---|
| **Application-side deduplication** | A read-then-write cannot be made safe against concurrency without a database constraint anyway, so it adds code without adding a guarantee. |
| **`ON CONFLICT DO UPDATE` (upsert)** | Destroys first-observation provenance, and for an append-only landing zone there is nothing to update: the row is a historical fact, not a mutable record. |
| **Deduplicate only in the transformation layer** | The raw table would accumulate duplicates indefinitely, growing without bound and making every downstream `count(*)` a lie until deduplicated. Errors should be prevented at the boundary, not compensated for downstream. |
| **Filter incrementally on `ingested_at`** | Picks up late rows but leaves their period stale. This is the specific bug the current design exists to avoid. |
| **Full refresh of every mart, every run** | Correct and simple, and genuinely viable at today's volume. Rejected because the late-arrival problem is the interesting one to demonstrate, and a design that only works while the data is small is not worth showing. |

## Consequences

**Accepted:**

- The primary key defines what "one observation" means. Two genuinely distinct
  reports from the same airframe at the same instant would be collapsed. For
  ADS-B state vectors that cannot happen; for another source it might, and the
  key would need revisiting.
- Late data beyond the lookback needs manual intervention
  (`dbt build --full-refresh`). This is a deliberate loud failure rather than a
  quiet inconsistency, but it is still manual.
- Rebuilding whole hours does more work than strictly necessary on every run.
- `rows_duplicate` in the run record is a useful signal precisely *because*
  duplicates are expected: a sudden drop to zero would suggest the poll cadence
  or the key had changed, not that quality had improved.

**Verified:** a live run inserted 629 rows; an immediate second run inserted 0
and reported 629 duplicates. The same guarantee is asserted deterministically
in `test_end_to_end_job_is_idempotent`.
