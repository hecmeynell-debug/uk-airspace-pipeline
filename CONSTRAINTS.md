# Constraints

This file is the project's contract with itself. Every phase begins by
re-reading it, and every change is reviewed against it. If a proposed feature
conflicts with anything below, the feature is dropped - not the constraint.

## What this project is

A descriptive, aggregate situational-awareness and resilience-analysis pipeline
over **public** ADS-B data for a bounded UK / eastern North Atlantic region.
It exists to demonstrate production data engineering practice: reliable
ingestion, idempotent loads, tested transformations, provenance, CI and
observability.

## Hard non-goals (never violate)

- **No individual aircraft tracking for operational or targeting purposes.**
- **No prediction of intent, threat scoring, anomaly-as-threat, or
  military-specific analytics.**
- **No use of non-public, scraped, or restricted data sources.**
- **No real-time alerting that could be misconstrued as operational.**
- **No scope creep into pure data science or ML modelling.**
- **Outputs must remain aggregate and descriptive only.**

### How these are enforced in practice

| Constraint | Enforcement |
|---|---|
| No individual tracking | Marts expose counts, rates and distributions over time/space buckets. No per-airframe trajectory model, no per-airframe identity in any published mart. |
| No intent/threat analytics | No scoring, classification, or "unusual behaviour" model of any kind. Data quality checks flag *pipeline* problems, never aircraft. |
| Public sources only | OpenSky Network documented REST API, used within its published rate limits. No scraping, no undocumented endpoints, no credential sharing, no multi-account evasion. |
| No operational alerting | Scheduled batch only. Alerting, where it exists, is on pipeline health (freshness, row counts, error rates) - not on airspace content. |
| No ML scope creep | No model training, no feature stores, no inference services. |
| Aggregate only | Every mart is a grouped aggregate. Raw state vectors stay in the `raw` schema behind least-privilege roles. |

A note on the data itself: OpenSky's feed is community-sourced and includes
whatever transponders broadcast. This project deliberately does **not** filter
for, enrich, highlight, or separately analyse state or military aircraft.
Everything is treated as undifferentiated traffic volume.

## Legal and licensing position

- All data is publicly broadcast ADS-B, retrieved through OpenSky Network's
  documented public API.
- OpenSky's API is provided for **research and non-commercial purposes**.
  This repository is a non-commercial portfolio and learning artefact.
- Attribution and citation requirements are recorded in the README.
- The system is a **platform engineering demonstration only**. It is not an
  operational system, not a decision-support tool, and must not be presented
  or deployed as either.

## Definition of Done

- [ ] Fully reproducible with `docker compose up` locally
- [ ] Documented cloud deployment path
- [ ] CI green on `main`
- [ ] Idempotent: re-running the same time window does not create duplicates
- [ ] Clear provenance on every record
- [ ] README allows a stranger to understand purpose, architecture and
      limitations in under 5 minutes
- [ ] Every major design choice is defensible in an interview
- [ ] Explicit statement that all data is public and the system is for
      platform demonstration only

## Delivery discipline

- One phase at a time. A phase is not started until the previous one has been
  explicitly signed off by a human.
- Every phase ends with concrete evidence: passing tests, a file list, and a
  checklist against the Definition of Done.
- Small, reviewable changes. No large dumps.
- Every diff is read by a human before it is committed.
