# UK Airspace Pipeline

A data engineering pipeline that ingests **public** ADS-B data for the UK
Flight Information Regions and the eastern North Atlantic, lands it with full
provenance, transforms it with dbt, and publishes **aggregate, descriptive**
datasets for situational awareness and resilience analysis.

> **All data used here is publicly broadcast ADS-B, retrieved through the
> OpenSky Network's documented public API. This repository is a platform
> engineering demonstration and a portfolio artefact. It is not an operational
> system, not a decision-support tool, and must not be deployed or presented
> as either.**

**It does not do individual aircraft tracking, intent prediction, threat
scoring, or any military-specific analytics.** Those are hard non-goals, not
unimplemented features — see [CONSTRAINTS.md](CONSTRAINTS.md).

## Why it exists

To demonstrate production engineering habits on a real, messy, rate-limited
public data source: idempotent loads, provenance on every record,
least-privilege database roles, tested transformations, structured logging,
and CI — rather than to produce analytics.

## Architecture

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

Orchestrated by Apache Airflow, containerised with Docker Compose, tested with
pytest and dbt tests, built on GitHub Actions.

The reasoning behind every one of those choices — including the ones rejected
— is in [ADR-0001](docs/adrs/0001-opensky-and-overall-architecture.md).

## Status

Built in gated phases. Each phase is reviewed and signed off before the next
one starts.

| Phase | Scope | Status |
|---|---|---|
| 0 | Repository foundations, Postgres 16, least-privilege roles, ADR-0001 | ✅ Complete |
| 1 | OpenSky client, raw schema, idempotent ingestion, scheduled job | Not started |
| 2 | dbt Core: staging → intermediate → marts, late-arriving data handling | Not started |
| 3 | GitHub Actions CI, structured logging, row-count and lag checks | Not started |
| 4 | Cloud deployment, IaC, secrets management | Not started |
| 5 | Documentation, ADRs, portfolio packaging | Not started |

## Quickstart

Requires Docker and Docker Compose.

```bash
git clone <this-repo>
cd uk-airspace-pipeline

cp .env.example .env      # edit if you like; defaults work for local dev
docker compose up -d

docker compose ps         # postgres should report (healthy)
```

Connect and confirm the bootstrap:

```bash
docker compose exec postgres psql -U postgres -d airspace \
  -c "\dn" -c "\du airspace_*"
```

You should see the `raw` schema owned by `airspace_owner`, and four
least-privilege roles. Tear down with `docker compose down`, or
`docker compose down -v` to also drop the data volume.

### OpenSky credentials

Ingestion (Phase 1) runs anonymously by default, which is enough for a smoke
test. For sustained use, register at
[opensky-network.org](https://opensky-network.org), create an API client, and
set `OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET` in `.env`. The API uses
OAuth2 client credentials; username/password authentication is no longer
accepted.

## Limitations

Read these before drawing any conclusion from the output.

- **Observed traffic, not actual traffic.** OpenSky is a community receiver
  network. Coverage is good over populated land and sparse over open ocean, so
  North Atlantic counts systematically understate reality. Every aggregate
  describes what was *observed*, and coverage varies by time and place.
- **ADS-B is not universal.** Aircraft without ADS-B Out, or with transponders
  off or transmitting incomplete data, are absent. Some fields (callsign,
  altitude, velocity) are frequently null.
- **Sampled, not continuous.** State vectors are polled every 120 seconds, so
  short events between polls are invisible. This is a rate-limit consequence,
  documented in ADR-0001.
- **No identity resolution.** `icao24` is used only as a deduplication key
  inside the `raw` schema. Nothing per-airframe is published.
- **Not real time.** Batch pipeline with a scheduled cadence. It is not, and
  will not become, an alerting or monitoring system for airspace content.

## Data source, licence and attribution

Data from the [OpenSky Network](https://opensky-network.org), used under its
terms for **research and non-commercial purposes**. This repository is
non-commercial.

If you use this work in a publication, cite the OpenSky Network paper:

> Matthias Schäfer, Martin Strohmeier, Vincent Lenders, Ivan Martinovic,
> Matthias Wilhelm. *Bringing Up OpenSky: A Large-scale ADS-B Sensor Network
> for Research*. ACM/IEEE IPSN, 2014, pp. 83–94.

## Repository layout

```
ingestion/      OpenSky client and load jobs
transform/      dbt Core project
dags/           Airflow DAGs
tests/          pytest unit and integration tests
db/init/        database bootstrap: roles, schemas, grants
docs/adrs/      architecture decision records
CONSTRAINTS.md  hard non-goals and definition of done
```

## Constraints

The hard non-goals, how each is enforced in practice, the legal position and
the Definition of Done are all in [CONSTRAINTS.md](CONSTRAINTS.md). They are
not negotiable within this project.
