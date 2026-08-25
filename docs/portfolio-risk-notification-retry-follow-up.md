# Notification Retry Operational Follow-up

## Outcome

This increment turns retained notification, attempt, acknowledgement and retry
execution evidence into current read-only operational queues:

```text
current pending notification outbox
  + append-only delivery attempts
  + append-only breach acknowledgements
  + append-only retry execution history
  -> historical request-to-event expansion
  -> deterministic current execution per event
  -> one current follow-up row per pending event
  -> delivery-failure and ambiguity queues
```

Primary arc42 block: `warehouse`. No delivery authority or mutation is added.

## Historical event grain

The view:

```text
risk_platform.notification_retry_execution_event_history
```

has one row per terminal retry record and requested event ordinal. It exposes:

- terminal record, request and plan identities;
- request ordering;
- whether the event was recorded in the persisted prefix;
- the corresponding persisted attempt ID;
- whether that attempt exists in append-only delivery evidence; and
- bounded attempt outcome fields.

`failed_before_request` records contain no requested events and remain available in
the request-level failure view rather than fabricating an event identity.

## Current execution per event

The view:

```text
risk_platform.latest_notification_retry_execution_by_event
```

selects one current terminal record for each event using:

```text
finished_at DESC
record_id DESC
event_ordinal DESC
```

Older records remain in history. A newer execution therefore supersedes an older
`persistence_uncertain` record for the same event without deleting it.

## Request-level review

The view:

```text
risk_platform.current_notification_retry_request_failures
```

classifies every non-completed terminal request as:

```text
pre_request_review
post_request_review
ambiguous_remote_outcome_review
```

This queue retains failures that cannot be assigned to an event because no request
was made.

## Current persistence uncertainty

The view:

```text
risk_platform.current_notification_retry_persistence_uncertainty
```

contains only current event versions where:

- the latest terminal status is `persistence_uncertain`;
- no persisted attempt was bound to the record; and
- no append-only attempt for the event appeared at or after that execution started.

The broader consumer view is:

```text
risk_platform.current_notification_ambiguous_outcomes
```

It joins the uncertainty to current pending notification metadata. Historical or
superseded ambiguity remains queryable through event history but is not left in the
current queue.

## One follow-up row per pending event

The principal serving view is:

```text
risk_platform.current_notification_retry_follow_up
```

Its grain is one row per current notification whose outbox disposition is
`pending`. It combines:

- policy, portfolio, metric and subject identity;
- delivery attempt counts and latest outcome;
- the latest breach acknowledgement;
- the current retry execution record; and
- the current uncertainty record.

The fail-safe precedence is:

```text
delivered
  > acknowledged
  > persistence_review_required
  > initial_delivery_required
  > execution_review_required
  > retry_plan_required
  > review_required
```

`delivered` and `acknowledged` require no follow-up. `initial_delivery_required` is
work that has never been attempted and is deliberately not labelled a delivery
failure.

A failed initial or completed retry attempt becomes `retry_plan_required`. A current
`failed_after_request` execution becomes `execution_review_required`. An unresolved
request-without-persistence boundary becomes `persistence_review_required`, which
takes precedence over ordinary retry planning.

## Failure queues

The view:

```text
risk_platform.current_notification_delivery_failures
```

contains current:

```text
persistence_review_required
execution_review_required
retry_plan_required
review_required
```

It excludes `initial_delivery_required`, `delivered` and `acknowledged` rows.

The ambiguity view is a strict subset containing only
`persistence_review_required` rows.

## PostgreSQL validation

The PostgreSQL 16 contract inserts transaction-scoped fixtures for:

- a never-attempted event;
- a failed event requiring a retry plan;
- unresolved persistence uncertainty;
- a failed-after-request execution requiring review;
- a delivered event;
- an acknowledged event; and
- an older uncertain record superseded by a newer terminal execution.

It proves exact classifications, one current row per event, stale uncertainty
exclusion, delivery-failure partitioning and all reconciliation checks. The fixture
transaction is rolled back after validation.

## Reconciliation

`sql/portfolio_risk_notification_retry_follow_up_consistency_checks.sql` verifies:

- requested and persisted event expansion counts;
- historical and current grain uniqueness;
- persisted-event references to append-only attempts;
- deterministic current-version selection;
- valid current uncertainty;
- one follow-up row per current pending event;
- delivered and acknowledged closure;
- valid persistence, initial-delivery, retry-plan and execution-review states; and
- exact delivery-failure and ambiguity partitions.

## Boundary

These are read-only PostgreSQL views. This increment does not:

- send a webhook;
- create a retry plan;
- execute a retry;
- acknowledge a breach;
- mutate outbox, attempt or retry history;
- schedule delivery;
- provision a destination;
- deploy infrastructure; or
- run `terraform apply`.

Committed webhook delivery, retry execution and destination activation remain
disabled. Ordinary CI performs no external request.
