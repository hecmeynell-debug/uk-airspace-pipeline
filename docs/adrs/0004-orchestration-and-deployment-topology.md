# ADR-0004: Airflow locally, a scheduled runner in production

- **Status:** Accepted
- **Date:** 2026-09-10
- **Deciders:** Project author
- **Relates to:** [ADR-0001](0001-opensky-and-overall-architecture.md)

## Context

[ADR-0001](0001-opensky-and-overall-architecture.md) chose Apache Airflow for
orchestration. That decision was made about the *development* stack, and it
holds there: the DAG model — discrete intervals, backfill, retries with
policies attached to task instances — is exactly the vocabulary an idempotency
guarantee needs to be demonstrated against.

Deployment surfaces a cost the local decision did not. The workload is one
task, taking under a second, every two minutes. Airflow to run it is an
API server, a scheduler, a DAG processor and its own metadata database: four
always-on containers wanting a couple of gigabytes of memory continuously.
Everything else in this project is either stateless or idle, and therefore
nearly free to host. Airflow is the entire hosting bill.

There is a second cost that is easier to overlook. An internet-facing Airflow
is a remote code execution surface by design — running arbitrary Python is what
it is *for* — so deploying it means taking on real authentication and network
hardening work, for one task that takes under a second.

## Decision

**Two topologies, both first-class and both documented.**

### Local and demonstration: Airflow

The `docker-compose.airflow.yml` overlay stays exactly as it is. It is how the
project is developed and how the orchestration story is shown. CI builds the
image and parses the DAGs on every push, asserting the two invariants that
would quietly cost money or mislabel data if changed — `catchup` off and
`max_active_runs` at 1 — so the DAGs cannot rot while unused.

### Production: managed Postgres, a scheduled runner, a stateless dashboard

```
scheduled runner (cron / CI schedule)  ->  python -m ingestion.run --once
                                           |
managed Postgres  <------------------------+
        ^
        +-----------------------------------  dbt build (same schedule or after)
        |
        +-----------------------------------  dashboard container (reader role)
```

The ingestion job is already a process that starts, does one thing, exits with
a meaningful code, and is safe to run twice. That is precisely a cron job. Its
exit codes (`0` ok, `1` failed, `2` rate limited, `3` config error) exist so a
scheduler can respond sensibly, and a plain scheduler can use them as well as
Airflow can.

**This is right-sizing, not a downgrade.** One task every two minutes does not
justify five containers, and the load is idempotent, so it does not need
Airflow's execution guarantees. Choosing the heavier tool because it is more
impressive would be the wrong instinct, and defending the lighter one is the
more honest answer.

### Deployment is demonstrated, then torn down

Providers bill by the hour. The project deploys for real, captures evidence —
logs, a working URL, the least-privilege grants confirmed against a remote
database — and is then destroyed. The Definition of Done asks for a *documented
cloud deployment path*, and a documented-and-demonstrated path satisfies it
better than an indefinitely-running box that will be broken in three months.

## Alternatives considered

| Option | Why rejected |
|---|---|
| **Airflow in production on a small VPS** | Entirely feasible at roughly £4/month, and the compose files already do it. Rejected because the cost is recurring, the security work is real, and the resulting architecture would be one nobody would choose on merit for this workload. |
| **Cloud Run** | Ruled out by the author. Independently a poor fit: a scheduler must run continuously, which is the one thing a scale-to-zero runtime is designed not to do. |
| **ECS Fargate** | More AWS surface to talk about, at several times the cost and complexity. The complexity would be the point of the exercise rather than a consequence of the problem. |
| **Managed Airflow (MWAA, Composer, Astronomer)** | Removes the operational burden and adds an order of magnitude to the bill. Correct at organisational scale, absurd for one two-minute task. |
| **Keeping Airflow as the only path and skipping deployment** | Leaves a Definition of Done item unmet and dodges the question rather than answering it. |

## Consequences

**Accepted:**

- Two orchestration paths exist and both must keep working. CI tests the DAGs
  on every push specifically so the unused one does not decay.
- Production loses Airflow's backfill UI and per-task retry history. The
  mitigation is that the loader is idempotent, so recovery is "run it again",
  and `raw.ingestion_runs` already records every attempt with its outcome —
  the audit trail does not depend on the orchestrator.
- A cron-based scheduler typically offers coarser granularity and can drift
  under load. Acceptable here: gaps are already an accepted property of the
  data, and the credit budget caps cadence well above that granularity anyway.
- Anyone reading the repository could reasonably ask why Airflow is present at
  all if production does not use it. The answer belongs in the README rather
  than being left implicit.

**Hard prerequisites before anything faces the internet:**

- `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS` must not survive. It makes
  the local UI passwordless, which is a convenience locally and a remote code
  execution surface publicly.
- The local development passwords must be replaced with generated secrets held
  outside the repository.
- Postgres currently binds to loopback only. That property must be preserved
  remotely rather than reintroduced as a public listener.
