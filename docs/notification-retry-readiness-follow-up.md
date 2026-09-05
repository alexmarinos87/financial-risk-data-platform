# Notification Retry Readiness Follow-Up

Primary arc42 block: `warehouse`.

## Goal

Expose the exact readiness authority that governed each retained notification
retry terminal result without changing delivery or retry behaviour:

```text
destination-aware retry history
  + append-only retry readiness bindings
  + current retry follow-up evidence
  -> readiness-aware execution history
  -> one readiness-aware current row per event
  -> failure, ambiguity and missing-binding review queues
```

## Query grains

`notification_retry_readiness_execution_history` has one row per terminal retry
record and requested event ordinal. It preserves legacy records and exposes
`readiness_bound = false` when no readiness binding exists.

`latest_notification_retry_readiness_by_event` reuses the existing current retry
selection:

```text
finished_at DESC
record_id DESC
event_ordinal DESC
```

The readiness binding is joined by the exact selected terminal `record_id`.
Authority from an older superseded execution cannot leak into a newer current
execution.

`current_notification_retry_readiness_follow_up` preserves one row per current
pending notification and exposes:

- readiness binding, readiness record and readiness request IDs;
- retained and refreshed decision IDs;
- enforcement ID and enforcement timestamp;
- readiness destination identity;
- retained and refreshed decision timestamps;
- shared delivery-lock model, scope and key fingerprint;
- terminal, enforcement and binding document SHA-256 values; and
- an explicit readiness review status.

## Review states

The current status is:

```text
no current retry execution                 -> not_applicable
current execution without readiness        -> readiness_binding_missing
bound readiness and destination disagree   -> readiness_destination_mismatch
exact current readiness binding            -> bound
```

A missing binding is expected for legacy terminal history created before
readiness enforcement. It remains visible for review rather than being
retroactively inferred or backfilled.

A destination mismatch is fail-closed even though the append-only recorders
normally prevent it. The view therefore remains useful if historical evidence
is imported or independently audited.

## Operational queues

The readiness-aware failure and ambiguity views preserve the exact membership
of the existing destination-aware queues while adding readiness drill-through
evidence.

`current_notification_retry_readiness_binding_reviews` contains current retry
executions that are missing readiness evidence or whose destination identity
differs. `current_notification_retry_readiness_bound` contains only exact bound
current executions.

## PostgreSQL proof

The transaction-scoped fixture uses the retained P4d5d history contract to prove
both sides of current-version selection:

1. a pending event initially resolves to an older terminal result with an exact
   readiness binding;
2. a newer terminal result for the same event is inserted without a binding;
3. the current row changes to `readiness_binding_missing`;
4. the older binding remains in immutable history but is excluded from the
   current event; and
5. failure and ambiguity queue membership remains unchanged.

The reconciliation suite verifies history and current grains, complete bound
rows, clean unbound rows, exact current selection, status flags, review
partitions and exclusion of superseded bindings.

## Safety boundary

These are read-only serving views. Ordinary CI performs no network request,
webhook delivery, provider request, delivery-attempt write outside its
transaction fixture, outbox mutation, acknowledgement, cloud schedule
activation, infrastructure deployment or `terraform apply`. The views expose no
endpoint value, complete URL, credential, payload body, response body,
environment value or PostgreSQL DSN.
