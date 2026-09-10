# ADR-0003: Enforcing the aggregate-only constraint in code, not in documentation

- **Status:** Accepted
- **Date:** 2026-09-10
- **Deciders:** Project author
- **Relates to:** [CONSTRAINTS.md](../../CONSTRAINTS.md)

## Context

[CONSTRAINTS.md](../../CONSTRAINTS.md) states that this project does no
individual aircraft tracking and that every published output is aggregate and
descriptive. That is the single most important property of the system: it is
what makes a pipeline over aircraft movements, built for a defence-adjacent
portfolio, a reasonable thing to have written.

A document cannot enforce it. Six months from now, adding `icao24` to a
`group by` is a two-character change that makes a dashboard more useful and
breaks the project's central premise. It would pass code review from anyone who
had not read CONSTRAINTS.md, because it would look like an obvious improvement.

The constraint therefore needs mechanisms, not prose. It also needs mechanisms
that fail *loudly* and *by default*, because the failure mode is not a crash —
it is a system that works better than before while no longer being the thing it
claimed to be.

## Decision

Three overlapping mechanisms, deliberately redundant.

### 1. A test that inspects the built schema

`transform/tests/assert_marts_expose_no_aircraft_identity.sql` queries
`information_schema.columns` for the marts schema and fails if any column is
named `icao24`, `callsign`, `squawk`, `origin_country`, `latitude`,
`longitude`, or similar.

It inspects the *built artefact* rather than the model source, so it catches a
violation whatever route it arrives by: a new model, an edited one, a renamed
column, a macro that generates SQL. Anything that reaches the warehouse is
visible to it.

`origin_country` is on the list deliberately. Breaking traffic down by country
of registration would be ordinary aviation statistics in another project. Here
it invites exactly the reading this project exists to avoid, and excluding it
costs nothing analytically.

### 2. A credential that cannot reach the data

`airspace_reader` — the role the dashboard and any future consumer uses — is
granted `USAGE` and `SELECT` on the marts schema and nothing else. It cannot
read `raw`, `staging` or `intermediate`.

This is the mechanism that matters most, because it does not depend on anyone
choosing correctly. A consumer built on that credential is *structurally*
incapable of serving per-airframe data. Someone who wanted to add aircraft
tracking to the dashboard would have to notice that it cannot read the tables
containing it, and go and grant themselves access — which is a deliberate act
that leaves a trace, not an oversight.

The grant is applied by a dbt `on-run-end` hook, because dbt owns the schemas
it builds and is therefore the only thing that can grant on them.

### 3. The same assertion again, from Python

`tests/integration/test_api.py` walks every API response recursively and fails
if any key anywhere matches the forbidden set, and separately asserts that the
reader role is *denied* on `raw`, `staging` and `intermediate`.

Duplicating the dbt test is intentional. This is the one property that must not
be able to regress quietly, and two independent checks in two languages are
harder to disable by accident than one.

### Resolution is a constraint, not a preference

Aggregation is at 1° cells, which is coarse enough that a cell describes a
region rather than a location. Recorded here because it is easy to treat as a
tunable: going finer would need a **minimum-count suppression threshold**,
because a 0.25° cell containing a single aircraft over open water localises
that aircraft to roughly 28 km. That is individual tracking arriving through
the back door of a fine enough grid, and it would pass review precisely because
nobody would frame it as tracking.

## Alternatives considered

| Option | Why rejected |
|---|---|
| **Code review and documentation alone** | The failure mode is a change that looks like an improvement. Relying on every future reviewer having internalised the constraint is relying on memory, which is the thing mechanisms exist to replace. |
| **Linting the model SQL for forbidden identifiers** | Checks source rather than output, so it misses generated SQL, renames and anything added outside the linted paths. Inspecting the built schema is strictly stronger. |
| **Column-level grants on individual mart tables** | Fiddly to maintain, and does nothing about a *new* mart, which is the likeliest way a violation arrives. Schema-level grants plus a schema-level test cover the new-model case. |
| **Dropping `icao24` from the raw schema entirely** | Would make the constraint unbreakable, but deduplication and provenance both need a natural key. The compromise is that identity exists in `raw` and `intermediate`, is used only for `count(distinct …)`, and does not cross into a mart. |

## Consequences

**Accepted:**

- A legitimate column that happens to be named `latitude` in a mart would fail
  the test. Given what this project is, having to rename a column or justify an
  exception is the right amount of friction.
- The forbidden list exists in two places (the dbt test and the Python test)
  and can drift. A comment in each points at the other.
- The reader role must actually be the credential consumers are given. Handing
  a dashboard the `transform` password would bypass the strongest of the three
  mechanisms, which is why the API container is given the reader password and
  no other.
- None of this constrains the `raw` schema, which necessarily holds
  per-airframe rows. The guarantee is about what is *published*, not about what
  is stored — and the least-privilege roles are what keep the two separate.

**Verified:** deliberately leaking `icao24` and `callsign` into a mart on a
branch turned CI red on
`assert_marts_expose_no_aircraft_identity` (`Got 1 result, configured to fail
if != 0`), and the pull request could not merge. The check was then confirmed
to pass again once the violation was removed — a test that has only ever been
observed passing has not been shown to work.
