# Architecture Decision Records

Short, dated records of decisions that were expensive to make and would be
expensive to reverse. Each ADR states the context, the decision, the options
that were rejected and why, and the consequences accepted.

Format: lightweight [MADR](https://adr.github.io/madr/). One file per decision,
numbered sequentially, never edited after acceptance — superseded instead.

| ADR | Title | Status | Date |
|---|---|---|---|
| [0001](0001-opensky-and-overall-architecture.md) | OpenSky Network as sole data source, and the overall pipeline architecture | Accepted | 2026-09-10 |
| [0002](0002-idempotency-and-late-arriving-data.md) | Idempotency enforced in the database, and how late data is handled | Accepted | 2026-09-10 |
| [0003](0003-enforcing-the-aggregate-only-constraint.md) | Enforcing the aggregate-only constraint in code, not in documentation | Accepted | 2026-09-10 |
| [0004](0004-orchestration-and-deployment-topology.md) | Airflow locally, a scheduled runner in production | Accepted | 2026-09-10 |
