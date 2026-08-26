# Destination-Aware Notification Retry Follow-Up

Primary arc42 block: `warehouse`.

## Goal

Expose the reviewed destination that governed each retained retry execution
without changing notification delivery behaviour:

```text
append-only retry execution history
  + append-only destination authority bindings
  + current retry follow-up evidence
  -> destination-aware execution history
  -> one destination-aware current row per event
  -> delivery-failure, ambiguity and missing-binding review queues
```

## Query grains

`notification_retry_destination_execution_history` has one row per terminal
retry record and requested event ordinal. It preserves unbound legacy history
and exposes `destination_bound = false` rather than dropping it.

`latest_notification_retry_destination_by_event` reuses the existing current
retry selection. The current record is still selected by:

```text
finished_at DESC
record_id DESC
event_ordinal DESC
```

A destination binding attached to an older superseded retry record cannot leak
into the current event row.

`current_notification_retry_destination_follow_up` preserves one row per
current pending notification and adds:

- destination authority, destination ID and destination fingerprint;
- endpoint environment-variable name, never its value;
- destination evaluation and binding timestamps;
- `destination_binding_status`; and
- `destination_review_required`.

The binding status is:

```text
no current retry execution                 -> not_applicable
current execution with destination binding -> bound
current execution without binding          -> destination_binding_missing
```

The missing-binding state is expected for retained legacy executions created
before destination enforcement. It is surfaced for explicit operator review;
it is not silently treated as a reviewed destination.

## Operational queues

The destination-aware failure and ambiguity views preserve the exact row counts
and classifications of the existing current queues while adding destination
drill-through evidence.

`current_notification_retry_destination_binding_reviews` contains only current
retry executions whose destination binding is absent. A bound ambiguity can
therefore be reviewed with its exact destination identity, while an unbound
legacy failure is clearly separated for governance review.

## Reconciliation

PostgreSQL checks prove that:

- history and current grains are not multiplied or lost;
- the destination-aware latest record matches the existing latest record;
- bound rows contain complete destination evidence;
- unbound rows contain no stray destination fields;
- follow-up, delivery-failure and ambiguity partitions retain their counts;
- binding-review rows are exactly the current executions without bindings; and
- destination identity in current follow-up matches the selected execution.

The transaction-scoped PostgreSQL fixture includes a bound current ambiguity,
an unbound failed execution, and a superseded older binding. It proves that the
current views select the right destination evidence and rolls all synthetic
records back.

## Safety boundary

These are read-only serving views. Ordinary CI performs no external webhook
request, delivery-attempt write outside the transaction fixture, outbox
mutation, acknowledgement, provider request, cloud schedule activation,
infrastructure deployment or `terraform apply`. No endpoint URL, credential,
payload body, response body, DSN or environment value is exposed by the views.
