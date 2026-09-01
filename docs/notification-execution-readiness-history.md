# Append-Only Notification Execution Readiness History

Primary arc42 blocks: `orchestration` and `warehouse`.

## Goal

P4d5b retains the read-only P4d5a decision as durable, reviewable evidence:

```text
canonical allow or block decision
  + operator request identity
  + recording time
  -> independent decision revalidation
  -> append-only PostgreSQL history
  -> deterministic current state per destination and execution kind
```

The retained wrapper model is
`portfolio-risk-notification-execution-readiness-record-v1`.

## Record contract

The history contract independently revalidates the complete
`portfolio-risk-notification-execution-readiness-gate-v1` document before
constructing a record. It then binds:

```text
request_id
recorded_at
full canonical readiness decision
```

`recorded_at` may not precede `evaluated_at`. The resulting `record_id` changes
when the request identity, recording time, or any decision evidence changes.

## Replay and conflict behaviour

The operator request ID is the append-only retry boundary:

```text
same request ID + same canonical record
  -> return the retained row

same request ID + changed time or decision
  -> reject
```

The decision ID is also unique. Reusing a decision under another request or
changed canonical wrapper fails closed. Direct `UPDATE` and `DELETE` operations
are rejected by PostgreSQL triggers.

## Retained evidence

The table
`risk_platform.notification_execution_readiness_decisions` retains the complete
canonical decision and queryable fields for:

- destination and execution kind;
- evaluation and recording time;
- `allow` or `block` and ordered blocking reasons;
- delivery, retry-planning, and retry-execution policy fingerprints;
- endpoint environment-variable identity and destination fingerprint;
- activation authority, checklist, review status, and ready state;
- transition record, rehearsal, review status, and ready state; and
- ambiguity counts and bounded event/record identities.

It retains no endpoint value, credential, payload body, response body, or DSN.
The canonical JSON must declare read-only execution and all delivery side effects
as false.

## Current-version selection

`latest_notification_execution_readiness_decisions` selects one decision per:

```text
destination_id
execution_kind
```

using:

```text
evaluated_at DESC
record_id DESC
```

The current review cross-joins every destination transition review with both
`initial` and `retry`, then emits one of:

```text
decision_missing
decision_superseded
decision_stale
blocked
allowed
```

A decision is superseded when its destination, activation, or transition
identity no longer matches current reviewed evidence. Supersession takes
precedence over age because an evidence mismatch is more material than an old
but otherwise matching decision.

A matching decision becomes stale five minutes after evaluation. This short
lifetime limits the period in which a later execution increment may rely on
retained evidence without refreshing current state.

Only `allowed` produces `execution_ready = true`. Separate serving views expose:

```text
risk_platform.current_notification_execution_readiness_allowed
risk_platform.current_notification_execution_readiness_blocked
risk_platform.current_notification_execution_readiness_stale
risk_platform.current_notification_execution_readiness_superseded
risk_platform.current_notification_execution_readiness_missing
```

## PostgreSQL proof

The PostgreSQL 16 contract:

1. Creates current activation and rotate/disable/rollback evidence.
2. Retains an old matching initial decision and proves `decision_stale`.
3. Retains a newer initial `allow` and proves `allowed`.
4. Retains a retry decision with retry execution disabled and proves `blocked`.
5. Replays the exact initial request and proves convergence.
6. Rejects changed evidence under the same request ID.
7. Adds a newer independently reviewed receiver configuration.
8. Proves both retained decisions become `decision_superseded`.
9. Rejects direct update and delete operations.
10. Runs the complete history and serving-view reconciliation suite.

## Safety boundary

This increment records decisions but does not enforce or execute them. It
performs no DNS lookup, socket operation, webhook request, delivery-attempt
write, outbox mutation, acknowledgement, cloud scheduling, deployment, or
`terraform apply`.

P4d5c will require and refresh an exact current `allow` decision while holding
the shared notification delivery lock. Committed notification delivery and retry
execution remain disabled by default.
