# Append-Only Notification Retry Execution History

## Outcome

This increment retains one terminal PostgreSQL record for each explicitly governed
notification retry invocation:

```text
exact P4b retry plan
  + explicit P4c execution gate
  + current-evidence revalidation
  + shared PostgreSQL delivery lock
  + observed request and attempt persistence
  -> completed | failed_before_request | failed_after_request
     | persistence_uncertain
  -> canonical terminal record
  -> append-only PostgreSQL history
```

Primary arc42 blocks: `orchestration` and `warehouse`.

The operator wrapper is:

```text
src/orchestration/run_recorded_portfolio_risk_notification_retries.py
```

The canonical contract and recorder are:

```text
src/warehouse/notification_retry_execution_contract.py
src/warehouse/notification_retry_execution_recorder.py
```

The wrapper does not replace the P4c executor. It delegates exact plan validation,
current-evidence rebuilding, lock acquisition, one-attempt delivery and append-only
attempt persistence to the existing implementation, while observing enough bounded
evidence to write one terminal record.

## Terminal states

The `portfolio-risk-notification-retry-execution-record-v1` contract supports four
terminal states.

### `completed`

The executor returned a complete summary, every requested event has one persisted
attempt, the shared lock was held through revalidation and persistence, and lock
release was confirmed.

A completed record retains the credential-free P4c execution summary and exact
execution ID.

### `failed_before_request`

Execution failed before the first webhook request. Examples include stale or changed
plan evidence, disabled configuration, invalid endpoint configuration, lock
contention, or bounded PostgreSQL evidence failure before delivery.

No requested event or attempt identity is fabricated.

### `failed_after_request`

At least one request was made and every observed request has corresponding persisted
attempt evidence, but the invocation still failed afterwards. A lock-release failure
after successful attempt persistence is one example.

This state does not claim that every remote receiver accepted the notification. It
only states that local attempt evidence exists for every observed request.

### `persistence_uncertain`

A request was observed but the corresponding append-only attempt could not be
confirmed locally. The record keeps two separate ordered identities:

```text
requested_event_ids
persisted_event_ids
```

The difference is the bounded ambiguous remote outcome requiring operator review.
The platform does not claim success or silently retry it. The stable notification
`Idempotency-Key` remains the remote deduplication control.

## Exact retry convergence

The operator request ID is the idempotency identity for the terminal record.

- The first invocation records one canonical terminal document.
- An exact retry with the same request ID and plan returns the retained document and
  performs no additional webhook request.
- Reusing the request ID for a different plan fails closed.
- Reusing the same request or record identity with different evidence fails closed.

This exact retry convergence is separate from the remote notification
`Idempotency-Key`, which remains the event ID.

## Canonical evidence

Every record binds:

```text
record model version
operator request ID
retained plan ID
completed execution ID when available
terminal status and bounded failure code
started and finished timestamps
current delivery fingerprint
current retry-policy fingerprint
current retry-execution-policy fingerprint
delivery-lock model and key fingerprint
ordered requested event IDs
ordered successfully persisted event IDs
ordered persisted attempt IDs
completed credential-free execution summary when available
```

The record ID and document SHA-256 are recalculated from canonical JSON. Unknown
fields, duplicate IDs, non-prefix persisted evidence, invalid timestamps, arbitrary
failure text and inconsistent terminal states are rejected.

## Append-only PostgreSQL contract

The history table is:

```text
risk_platform.portfolio_risk_notification_retry_executions
```

It stores the canonical JSON document alongside queryable identities and counts.
Direct `UPDATE` and `DELETE` operations are rejected by PostgreSQL triggers.
Completed execution IDs are unique, and request IDs are unique across all terminal
states.

The initial serving view is:

```text
risk_platform.recent_notification_retry_executions
```

Current delivery-failure, ambiguous-outcome and event-level retry-follow-up views are
kept for the next bounded PR so this slice remains focused on durable history.

## Operator command

After generating and reviewing an exact retry plan, run the recorded wrapper rather
than the unrecorded executor:

```bash
.venv/bin/python \
  -m src.orchestration.run_recorded_portfolio_risk_notification_retries \
  --plan .demo/notification-retry-plan.json \
  --confirm-plan-id '<exact-plan-id>' \
  --request-id 'RETRY-2026-001' \
  --execute \
  --summary-json .demo/recorded-notification-retry.json
```

Committed webhook delivery and retry execution remain disabled. The endpoint still
comes only from the local environment and is not retained in full.

## Failure and secret boundary

Failure records keep bounded codes such as:

```text
validation_failed
delivery_overlap
storage_failed
attempt_persistence_uncertain
unexpected_local_failure
```

Arbitrary exception messages, response bodies, payload bodies, endpoint URLs,
credentials and PostgreSQL DSNs are excluded.

The record explicitly preserves the distinction between a requested event and a
successfully persisted attempt. This prevents an ambiguous remote outcome from being
reported as either a confirmed success or a safe automatic retry.

## Validation boundary

Unit tests use fake readers, transports, clocks, locks and attempt writers. The live
PostgreSQL 16 contract proves completed terminal history creation, exact retry
convergence, conflicting request reuse rejection, `persistence_uncertain` history
creation, and append-only `UPDATE` and `DELETE` rejection.

There is no webhook request in ordinary CI. This increment does not activate a real
destination, schedule delivery, deploy infrastructure, mutate portfolio positions or
run `terraform apply`.
