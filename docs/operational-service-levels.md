# Operational Service-Level Evidence

## Outcome

This path turns the existing local schedule, market-freshness records and
notification delivery history into deterministic operational evidence with an
append-only PostgreSQL serving contract:

```text
local schedule checkpoint
  + exchange-calendar expected session
  + current constituent freshness
  + pending notification delivery attempts
  -> four bounded service-level indicators
  -> one reproducible overall status
  -> credential-free JSON evidence
  -> append-only PostgreSQL history
  -> latest metric status and current exception views
```

It is reporting evidence only. It does not execute the schedule, request market
data, retry notifications, deliver messages, activate a cloud schedule or page an
operator.

## Policy

`config/operational_service_levels.yaml` defines one versioned policy. Its
fingerprint binds the policy ID, schedule ID, model version and all warning and
critical thresholds. Threshold changes therefore produce a new report identity.

The supported indicators are:

| Indicator | Meaning |
| --- | --- |
| `schedule_lag_sessions` | Expected market sessions after the last successful local schedule checkpoint |
| `market_freshness_exception_count` | Configured constituents whose current freshness evidence is missing, stale, gapped or for an older session |
| `notification_retry_exhausted_count` | Undelivered notification events that reached the configured webhook attempt limit |
| `notification_oldest_dead_letter_age_seconds` | Age of the oldest retry-exhausted event, measured from its latest attempt or event time |

A value below the warning threshold is `ok`. A value at or above warning but
below critical is `warning`. A value at or above critical is `critical`. A
missing schedule checkpoint is explicitly `critical` with reason
`checkpoint_missing` rather than being converted into an invented lag value.
The overall status uses `critical > warning > ok` precedence.

## Report command

Use an explicit UTC instant so repeated reviews are deterministic:

```bash
.venv/bin/python -m src.orchestration.run_operational_service_levels \
  --policy-id us-tech-local \
  --as-of 2026-04-01T12:00:00Z \
  --summary-json .demo/operational-service-levels.json
```

The runner reads:

- `config/operational_service_levels.yaml`;
- `config/local_portfolio_schedules.yaml`;
- `config/market_calendars.yaml`;
- the effective portfolio mandate;
- `config/notification_delivery.yaml`;
- the bounded local schedule checkpoint; and
- PostgreSQL current freshness and notification-delivery status views.

It writes only the optional local JSON summary.

## Schedule completion

The schedule indicator uses the same schedule fingerprint and market calendar as
the existing local scheduler. A checkpoint belonging to another schedule,
configuration version or model fails closed. A checkpoint after the latest
expected market session also fails.

The lag is the number of configured market sessions after the checkpoint through
the latest expected session on the report date. Weekends and holidays therefore
do not create false calendar-day lag.

## Market freshness

The active portfolio mandate determines the exact source/symbol keys expected for
the latest session. A constituent is exceptional when:

- no current freshness row exists;
- the row belongs to another session;
- `freshness_status` is `gap_detected` or `stale`; or
- trailing missing sessions are non-zero.

Duplicate evidence for one constituent fails closed. Unrelated freshness rows are
ignored after the PostgreSQL read is bounded.

## Notification delivery

The webhook configuration supplies the reviewed maximum attempts per event. An
undelivered event becomes retry-exhausted once `attempt_count` reaches that
limit. Dead-letter age is measured from `last_attempted_at`, falling back to the
notification event time when no attempt timestamp exists.

Future timestamps, duplicate event IDs, invalid attempt counts and oversized
result sets fail closed. The report never retries or delivers an event.

## Bounds and evidence

The runner caps input at:

- 1,000 current freshness rows;
- 5,000 pending notification rows; and
- the existing 64 KiB local schedule-state limit.

The deterministic calculation ID binds policy, schedule, calendar, checkpoint,
expected constituents, exceptions, retry-exhausted events, thresholds and the
explicit report instant.

## Append-only PostgreSQL history

Record one generated report with:

```bash
.venv/bin/python -m src.warehouse.operational_service_level_recorder \
  --report .demo/operational-service-levels.json
```

The recorder accepts one regular local JSON file no larger than 1 MB. It validates
the complete report shape, canonical metric order, thresholds, statuses,
side-effect flags, dates and identifiers before opening PostgreSQL.

The base table is:

```text
risk_platform.operational_service_level_reports
```

It stores both extracted query columns and the complete canonical report as
JSONB. `document_sha256` binds the exact canonical document. The deterministic
`calculation_id` is the primary key:

- an exact retry converges on the existing row;
- the same calculation ID with different content fails closed; and
- UPDATE and DELETE are blocked by append-only triggers.

## Serving views

The complete metric series is expanded through:

```text
risk_platform.operational_service_level_metric_history
```

The latest report remains independently queryable for each exact policy,
schedule, portfolio and mandate fingerprint through:

```text
risk_platform.latest_operational_service_level_reports
```

The latest four indicator rows are exposed through:

```text
risk_platform.current_operational_service_level_metric_status
```

Current warning and critical indicators are filtered through:

```text
risk_platform.current_operational_service_level_exceptions
```

The current grain retains policy, schedule and mandate fingerprints. Threshold,
schedule or mandate changes therefore remain separate evidence contracts rather
than silently replacing one another.

## Reconciliation

`sql/operational_service_levels_consistency_checks.sql` verifies:

- exact metric names, count and canonical order;
- value, threshold and status semantics;
- overall-status severity precedence;
- unique calculation identities and latest grains;
- newest-report selection;
- four metric-history rows per report; and
- exact agreement between current non-OK metrics and the exception view.

The PostgreSQL CI job also records two reports, proves retry convergence, rejects
conflicting calculation reuse, exercises current views and verifies that UPDATE
and DELETE fail.

## Current boundary

This path retains deterministic report history and current exception views. It
does not yet enforce an execution gate, define rolling availability objectives,
render a dashboard or deliver alerts. Those are separate increments so evidence
semantics and persistence remain independently reviewable.
