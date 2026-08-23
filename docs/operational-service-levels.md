# Operational Service-Level Evidence

## Outcome

This increment turns the existing local schedule, market-freshness records and
notification delivery history into one deterministic operational report:

```text
local schedule checkpoint
  + exchange-calendar expected session
  + current constituent freshness
  + pending notification delivery attempts
  -> four bounded service-level indicators
  -> one reproducible overall status
  -> credential-free JSON evidence
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

## Operator command

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

## Current boundary

This slice produces a deterministic JSON report. It does not yet retain report
history in PostgreSQL, enforce an execution gate, define rolling availability
objectives, render a dashboard or deliver alerts. Those are separate increments
so reporting semantics can be reviewed before persistence and enforcement.
