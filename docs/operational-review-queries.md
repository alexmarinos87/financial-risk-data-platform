# Operational Review Query Contract

## Outcome

This SQL layer turns retained operational service-level, rolling-objective and
readiness evidence into dashboard-ready grains without deploying or claiming a
dashboard:

```text
append-only SLI reports
  + append-only rolling SLO reports
  + append-only readiness decisions
  -> one current health row per exact operating contract and market session
  -> one current exception row per metric, objective or blocking reason
  -> readiness decision recency
  -> rolling objective trends
  -> evidence drill-through identities
```

## Current Health Card Grain

`risk_platform.current_operational_health_summary` contains one row per:

```text
operational policy and fingerprint
+ schedule and fingerprint
+ calendar
+ portfolio and risk-limit policy
+ mandate fingerprint
+ latest expected market session
```

It exposes the current SLI report, all current objective-report identities, the
current readiness decision, evidence timestamps and one deterministic health
status. Status precedence is:

```text
blocked
critical
missed
readiness_missing
service_level_missing
objective_missing
warning
insufficient
ok
```

Missing evidence remains explicit. It is not removed through an inner join.
`current_exception_count` combines current SLI exceptions, current SLO exceptions
and current readiness blocking reasons.

## Current Exception Table Grain

`risk_platform.current_operational_exception_summary` contains one row per:

```text
exception type + evidence identity + metric/objective/reason name
```

The three exception types are:

- `service_level_metric`;
- `service_level_objective`; and
- `readiness_reason`.

The common contract carries exact policy, schedule, portfolio, mandate and market
session identities plus observed values and thresholds where those concepts
exist. Readiness reasons retain the source operational report ID as their parent
evidence when one exists.

## Recent Decisions

`risk_platform.recent_operational_readiness_decisions` retains all decision
history and adds `decision_recency_rank` within the exact current serving grain.
Consumers can filter `decision_recency_rank <= 20` without changing the durable
view contract. Rank one matches `latest_operational_readiness_decisions`.

## Rolling Objective Trend Grain

`risk_platform.rolling_operational_objective_attainment` contains one row per:

```text
objective report + objective name
```

It retains observation counts, missing sessions, success/failure counts,
attainment and target ratios, `insufficient`/`met`/`missed` status, an attainment
gap and a numeric status rank. `through_session` is the business-session axis;
`calculated_at` is the evidence-generation timestamp.

## Drill-through Grain

`risk_platform.operational_evidence_drillthrough` contains one row per retained:

- operational service-level report;
- operational objective report; or
- readiness decision.

Every row carries an evidence type, model, stable evidence ID, document SHA-256,
business session, evidence timestamp and parent report IDs. Objective reports
retain all input report IDs; readiness decisions retain their exact source report
ID or an empty parent array for missing-report blocks.

## Suggested Dashboard Mapping

A reporting tool can map the views without additional semantic joins:

| Visual | View | Grain |
| --- | --- | --- |
| Current health card | `current_operational_health_summary` | operating contract/session |
| Current exception table | `current_operational_exception_summary` | exception/evidence |
| Readiness decision timeline | `recent_operational_readiness_decisions` | decision |
| Objective attainment trend | `rolling_operational_objective_attainment` | report/objective |
| Drill-through page | `operational_evidence_drillthrough` | retained evidence |

Filters should preserve the exact policy and fingerprint columns. A consumer
must not combine different schedule, mandate, policy or objective fingerprints
under one unlabelled series.

## Reconciliation

`sql/operational_review_consistency_checks.sql` verifies current-health grain and
status, exception counts and identities, readiness ranks, objective trend
arithmetic, drill-through row counts, parent references and evidence uniqueness.
The live PostgreSQL contract executes this query pack against real retained
reports, objective corrections and readiness corrections.

## Boundary

These are read-only PostgreSQL views. No dashboard is deployed. No alert is sent,
no schedule is executed, no checkpoint is changed, and no cloud resource is
created. The contract prepares the review surface required before readiness is
allowed to affect schedule planning.
