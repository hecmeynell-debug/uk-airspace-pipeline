# ADR-0001: OpenSky Network as sole data source, and the overall pipeline architecture

- **Status:** Accepted
- **Date:** 2026-09-10
- **Deciders:** Project author
- **Supersedes:** —

## Context

The project needs a lawfully accessible, genuinely public source of ADS-B
state vectors covering the UK Flight Information Regions and a margin of the
eastern North Atlantic, plus an architecture that demonstrates production data
engineering rather than analytics.

Two constraints dominate the choice and are recorded in
[CONSTRAINTS.md](../../CONSTRAINTS.md):

1. **Public sources only.** No scraping, no undocumented endpoints, no
   restricted feeds.
2. **Aggregate and descriptive outputs only.** The source must be usable
   without building anything that resembles individual tracking or threat
   analytics.

## Decision

### Data source: OpenSky Network REST API

Use `GET /states/all` with a bounding box, on a scheduled poll, as the single
ingestion source. No other feed is added without a superseding ADR.

Facts verified against the [official API documentation](https://openskynetwork.github.io/opensky-api/rest.html)
on 2026-09-10:

- **Authentication is OAuth2 client-credentials only.** Basic authentication
  with username and password is no longer accepted. Tokens come from
  `https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token`
  and expire after 30 minutes.
- **Quotas are credit-based, per day:**

  | Tier | Credits/day | Resolution | History |
  |---|---|---|---|
  | Anonymous (per IP) | 400 | 10 s | live only, `time` ignored |
  | Registered | 4,000 | 5 s | up to 1 hour |
  | Active feeder (≥30% uptime) | 8,000 | 5 s | up to 1 hour |

- **A `/states/all` call costs credits by bounding-box area** (latitude range ×
  longitude range, in square degrees): ≤ 25 = 1 credit, 25–100 = 2,
  100–400 = 3, > 400 or global = 4.
- The API is provided **for research and non-commercial purposes**; commercial
  use requires contacting OpenSky. Publications must cite Schäfer et al.,
  *Bringing Up OpenSky: A Large-scale ADS-B Sensor Network for Research*,
  IPSN 2014.

#### Credit budget

Default region: latitude 49.0–61.0, longitude −10.0–3.0 = 12 × 13 =
**156 sq°**, which falls in the 100–400 band at **3 credits per call**.

| Cadence | Calls/day | Credits/day | % of 4,000 allowance |
|---|---|---|---|
| 60 s | 1,440 | 4,320 | 108% — over budget |
| **120 s (default)** | **720** | **2,160** | **54%** |
| 300 s | 288 | 864 | 22% |

120 seconds is the default: comfortably inside the registered-user allowance
with roughly 46% headroom for retries, backfills and local development, while
still resolving traffic finely enough for hourly and daily aggregates.
Anonymous operation (400 credits/day) supports 133 calls/day and is therefore
treated as a smoke-test mode only, not a running configuration.

Credit cost is a step function of area, so shrinking the box below 100 sq°
would halve it. That is deliberately not done: the UK FIRs plus an Atlantic
margin is the analytically meaningful region, and 54% of budget is affordable.

### Rejected alternatives

| Option | Why rejected |
|---|---|
| **ADS-B Exchange** | Access is via paid commercial API plans. It also markets itself on carrying unfiltered data that other aggregators suppress, which pulls directly against this project's non-goals — the point here is undifferentiated traffic volume, not visibility of specific airframes. |
| **Flightradar24 / FlightAware commercial APIs** | Commercial licensing, cost, and terms restricting redistribution of derived data. Poor fit for an open portfolio repository. |
| **Self-hosted receiver (RTL-SDR + dump1090)** | Best data quality and no third-party terms, but needs hardware and a fixed location. A reviewer cannot reproduce the project from a `git clone`, which fails the Definition of Done. Noted as a possible future *addition* rather than a replacement. |
| **OpenSky historical (Trino/Impala) archive** | Genuinely useful for backfill and it is the same provider, but access needs separate approval and the interface is heavyweight. Deferred; would be a superseding ADR if backfill becomes necessary. |
| **Eurocontrol / NATS open datasets** | Authoritative and valuable, but published as periodic static releases rather than a live feed, so they do not exercise the scheduled-ingestion and idempotency behaviour this project exists to demonstrate. Candidate for later enrichment. |

### Architecture

```
OpenSky /states/all   (public, OAuth2, bounded bbox, scheduled poll)
        |
        v
Ingestion job         rate-limit aware client, retry with backoff,
        |             structured logs
        v
raw schema            append-only; ingestion timestamp + source metadata
(PostgreSQL 16)       on every row; natural-key uniqueness constraint
        |
        v
dbt Core              sources -> staging -> intermediate -> marts,
        |             tests at each layer
        v
marts                 hourly / daily aggregate counts and distributions
(PostgreSQL 16)
        |
        v
(optional) thin read API or dashboard   read-only role, marts only
```

Orchestration is Apache Airflow. Everything runs under Docker Compose. CI is
GitHub Actions. Tests are pytest plus dbt tests.

#### Why these components

- **PostgreSQL 16** — one engine for landing zone *and* marts keeps the local
  footprint at a single container and makes the idempotency guarantee
  enforceable in the database itself, through unique constraints and
  `INSERT ... ON CONFLICT DO NOTHING`, rather than in application logic. At a
  bounded region and 120 s cadence the volume is small enough that object
  storage plus a warehouse would be ceremony without benefit. If volume grew,
  the landing zone would move to object storage first; the layer boundary is
  drawn so that this stays a contained change.
- **dbt Core** — makes the transformation layer declarative, version
  controlled, tested and documented, and forces the source/staging/mart
  separation the Definition of Done implies. Chosen over hand-written SQL
  scripts because data tests and lineage come with it.
- **Airflow** — already known to the author, and its scheduling model
  (backfill over discrete intervals) is exactly what an idempotency guarantee
  must be demonstrated against. Dagster was the alternative and remains a
  reasonable fit; familiarity decided it.
- **Docker Compose** — the Definition of Done requires a stranger to run the
  project with one command.
- **Named Docker volume rather than a bind mount** — the working copy lives in
  a OneDrive-synced directory, and file-sync agents corrupt Postgres data
  files. The volume lives inside the Docker VM instead.

#### Least privilege from the start

Four roles are created at database bootstrap, before any table exists:

| Role | Privileges |
|---|---|
| `airspace_owner` | Owns `raw`; applies DDL and migrations |
| `airspace_ingest` | `USAGE` on `raw`, `SELECT`/`INSERT` on its tables. No DDL |
| `airspace_transform` | `SELECT` on `raw`, `CREATE` on the database for dbt targets |
| `airspace_reader` | Marts only, read-only. No grants until marts exist |

`PUBLIC` is stripped of its default `CONNECT` and `public`-schema privileges.
Doing this at bootstrap rather than retrofitting it means no component is ever
accidentally written against a superuser connection.

## Consequences

**Accepted:**

- Coverage is limited to what OpenSky's community receiver network sees.
  Coverage over open ocean is sparse, so aggregates describe *observed*
  traffic, not actual traffic. The README must say so plainly.
- The 4,000 credits/day ceiling caps cadence. Sub-minute resolution is not
  available to this project.
- Non-commercial terms mean the repository stays a portfolio artefact. Stated
  in the README.
- Tokens expire every 30 minutes, so the client must handle refresh. Phase 1
  work.
- Single-provider dependency: if OpenSky changes terms or quotas, ingestion
  stops. Acceptable for a portfolio piece, and the ingestion interface is kept
  narrow so a second source could be added behind it.

**Explicitly out of scope as a consequence of this ADR:** per-airframe
trajectory reconstruction, identity enrichment, and any per-aircraft output.
The raw schema retains `icao24` because it is the natural key needed for
deduplication and provenance, but it does not leave the `raw` schema in any
published mart.
