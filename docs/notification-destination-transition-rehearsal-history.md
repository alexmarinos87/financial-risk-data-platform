# Append-Only Notification Destination Transition History

Primary arc42 blocks: `orchestration` and `warehouse`.

## Goal

Retain completed rotation, disablement, and rollback rehearsal evidence and make
its relationship to the current activation review queryable:

```text
canonical no-network transition rehearsal
  + operator request identity
  -> strict independent revalidation
  -> append-only PostgreSQL history
  -> deterministic latest transition per destination
  -> current ready, missing, superseded, or activation-not-ready state
```

The retained record model is
`portfolio-risk-notification-destination-transition-record-v1`.

## Independent record validation

The recorder does not trust a rehearsal JSON document merely because it was
produced by the orchestration module. It independently checks:

- exact top-level and stage fields;
- the `rotate`, `disable`, `rollback` stage order;
- all plan, authority, checklist, destination, and endpoint-environment
  identities;
- the authority-free and request-free disablement stage;
- ordered request and receipt evidence;
- canonical receiver rehearsal IDs;
- same-key, same-payload duplicate arithmetic;
- start, receipt, finish, and record timestamps;
- the deterministic transition rehearsal ID; and
- every no-network and no-mutation declaration.

Only a completed three-stage rehearsal can be retained by this contract. Other
failure semantics remain in their existing bounded source contracts rather than
being reclassified during persistence.

## Replay and conflict behaviour

The operator `request_id` is the append-only idempotency boundary:

```text
same request ID + same canonical record
  -> return the retained row

same request ID + changed rehearsal or record time
  -> reject
```

The rehearsal ID is also unique. Reusing it under a different operator request
or changed canonical evidence fails closed. PostgreSQL triggers reject direct
`UPDATE` and `DELETE` operations.

## Current review contract

`latest_notification_destination_transition_rehearsals` selects one retained
transition per destination using:

```text
finished_at DESC
record_id DESC
```

`current_notification_destination_transition_review` joins that evidence to the
current activation-checklist and controlled-receiver review. It emits exactly
one of:

```text
activation_not_ready
transition_rehearsal_missing
transition_rehearsal_superseded
ready
```

A transition is ready only when the current activation review is itself ready
and the transition rollback checklist, destination fingerprint, and authority
ID exactly match the current activation evidence.

A newer receiver checklist and successful controlled rehearsal can therefore
supersede an older transition rehearsal without deleting either history. The
operator must produce a new transition rehearsal for the newly reviewed
configuration.

The serving views are:

```text
risk_platform.latest_notification_destination_transition_rehearsals
risk_platform.current_notification_destination_transition_review
risk_platform.current_notification_destination_transition_review_failures
risk_platform.current_notification_destination_transition_ready
```

## PostgreSQL evidence

The PostgreSQL 16 contract:

1. Builds an exact rotation, disablement, and rollback chain.
2. Runs the existing in-memory controlled receiver for rotation and rollback.
3. Persists a current rollback checklist and controlled-receiver rehearsal.
4. Records the transition rehearsal and proves the current state is `ready`.
5. Replays the exact operator request and proves convergence.
6. Rejects changed evidence under the same request ID.
7. Persists a newer independently reviewed receiver checklist and rehearsal.
8. Proves the old transition becomes `transition_rehearsal_superseded`.
9. Rejects direct update and delete attempts.
10. Runs the complete transition reconciliation suite.

## Safety boundary

The history retains endpoint environment-variable names, approved host names,
event identities, and payload SHA-256 values. It retains no endpoint value or
full URL, endpoint path, payload body, response body, credential, environment
value, or PostgreSQL DSN.

Ordinary validation performs no DNS lookup, socket operation, external request,
delivery-attempt write, outbox mutation, acknowledgement, cloud scheduling,
infrastructure deployment, or `terraform apply`. Committed destination
activation and notification delivery remain disabled.
