# Bounded Dead-Letter Retry Planning

## Outcome

The platform can now build a deterministic, delivery-free retry plan from retained
notification outbox and delivery-attempt evidence:

```text
current pending notification events
  + append-only webhook attempts
  + current breach acknowledgement evidence
  + reviewed delivery and retry policy
  -> bounded as-of candidate read
  -> deterministic classification
  -> exact retryable event list
  -> credential-free JSON plan
```

Primary arc42 block: `orchestration`, with a read-only interface to the
`warehouse`. This increment creates planning evidence only. It does not execute a
retry or change retained notification evidence.

## Reviewed Policy

The committed policy remains in:

```text
config/notification_delivery.yaml
```

The existing webhook contract still supplies:

- maximum delivery attempts per event;
- initial backoff seconds;
- maximum batch size; and
- a deterministic delivery configuration fingerprint.

The new `retry_planning` section supplies:

- the maximum candidate rows that one PostgreSQL read may inspect;
- the maximum retryable events permitted in one plan;
- the maximum event age;
- the maximum cumulative retry backoff;
- sorted retryable HTTP statuses; and
- sorted retryable bounded error codes.

`max_plan_events` may not exceed the webhook `max_batch_events`. The retry policy
fingerprint and webhook configuration fingerprint are both bound into the plan ID.
A reviewed policy change therefore creates a different plan identity.

## As-Of Evidence

`--planned-at` is required. The PostgreSQL reader includes only evidence visible at
or before that timestamp:

- pending outbox events whose ingest timestamp is not later than the plan time;
- delivery attempts whose attempt timestamp is not later than the plan time;
- successful deliveries are excluded;
- the latest acknowledgement at or before the plan time is selected; and
- the latest failed attempt is selected by attempt number, timestamp and attempt ID.

Optional policy and portfolio filters are applied inside the bounded PostgreSQL
query. The query requests at most `max_candidate_rows + 1`; observing the extra
row fails the plan instead of silently truncating evidence.

## Classification Precedence

Each exact event identity is classified once using this precedence:

1. `invalid` for incompatible event, payload, attempt or acknowledgement evidence;
2. `acknowledged` when the source risk-limit evaluation has current acknowledgement
   evidence;
3. `expired` when the event age exceeds the reviewed retry policy;
4. `attempts_exhausted` when the delivery attempt limit has been reached;
5. `invalid` when there is no failed attempt or the latest failure is not retryable;
6. `not_yet_eligible` while cumulative exponential backoff remains active; or
7. `retryable` when all exact policy, failure and timing conditions are satisfied.

A retryable HTTP failure must have the matching bounded `http_<status>` error code.
A retryable transport failure must use one reviewed bounded error code, such as
`network_error`. Arbitrary exception messages and response bodies are not used.

## Deterministic Identity

For every candidate the plan retains exact event identities and evidence:

- notification event ID;
- source risk-limit evaluation ID;
- policy and portfolio identity;
- event time;
- SHA-256 of the canonical event envelope and payload;
- exact latest attempt ID, number, outcome, status/error and timestamp;
- exact acknowledgement ID, disposition and timestamp when present;
- event age and next eligible timestamp;
- classification and bounded reason.

The plan ID binds the ordered event evidence, `planned_at`, filters, channel,
delivery configuration fingerprint and retry policy fingerprint. Input row order
does not change the event order or plan identity.

If the number of retryable events exceeds `max_plan_events`, the planner fails
closed. It does not select an arbitrary subset.

## Operator Command

Build a plan without network activity:

```bash
.venv/bin/python -m src.orchestration.plan_portfolio_risk_notification_retries \
  --planned-at 2026-04-01T12:00:00Z \
  --policy-id us-tech-standard \
  --portfolio-id us-tech-equal \
  --summary-json .demo/notification-retry-plan.json
```

The PostgreSQL DSN comes from `WAREHOUSE_POSTGRES_DSN` or `--dsn`. It is never
included in the summary.

The summary exposes classification counts, all bounded candidate evidence and the
ordered `retryable_event_ids` list. It also declares:

```text
delivery_performed = false
delivery_attempt_written = false
dead_letter_mutated = false
external_request_performed = false
```

## Boundary

This command has no `--execute` option. It performs no webhook request, no delivery attempt and no dead-letter mutation. It also performs no retry sleep or attempt insert.
It does not acknowledge or resolve a breach, expose endpoints or credentials,
schedule itself, deploy infrastructure, activate cloud scheduling or run
`terraform apply`.

The next dependency-safe roadmap slice is **P4c — explicit manual retry
execution**. That later increment must consume one exact reviewed plan under an
explicit execution gate and retain its own attempt evidence; it must not weaken the
report-only boundary established here.
