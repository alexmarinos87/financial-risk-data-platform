# Notification Destination Transition Rehearsal

Primary arc42 block: `orchestration`.

## Goal

Exercise a complete destination rotation, disablement, and rollback chain using
the existing controlled in-memory receiver:

```text
three exact transition plans
  + current and target destination authorities
  + activation-ready receiver checklists
  + bounded canonical request packages
  -> rotate receiver rehearsal
  -> zero-request disablement stage
  -> rollback receiver rehearsal
  -> deterministic no-network evidence
```

The model is
`portfolio-risk-notification-destination-transition-rehearsal-v1`.

## Chain validation

The rehearsal requires:

- operations ordered as `rotate`, `disable`, `rollback`;
- one destination ID across all plans;
- non-decreasing plan timestamps;
- the rotation target to equal the disablement current state;
- the disablement target to equal the rollback current state;
- the rollback plan to reference the exact disablement plan; and
- rollback to restore the original endpoint environment-variable identity.

The baseline authority is validated against the rotation current state. The
rotated authority is validated against the disablement current state. Both are
then deliberately tested against later targets and must fail. A stale authority
therefore cannot authorise either the rotated destination or the freshly
reviewed rollback destination.

## Active stages

Rotation and rollback each require:

- exact target destination authority;
- an activation-ready checklist matching destination, fingerprint, and
  authority ID;
- between one and fifty canonical requests;
- an explicit controlled HTTPS host allow-list; and
- receiver-side stable `Idempotency-Key` behaviour.

The receiver accepts a same-key, same-payload duplicate and rejects changed
content under the same key. Request bodies are not retained; receipts contain
only host, event identity, event type, payload SHA-256, time, status, and
same-content duplicate classification.

## Disablement stage

The disablement target is explicitly authority-free and request-free:

```text
target_authority_required = false
authority_id = null
request_count = 0
receiver_summary = null
```

This prevents a disablement rehearsal from being presented as a delivery
attempt.

## Deterministic evidence

The rehearsal ID binds:

```text
all three plan IDs
baseline, rotated, and rollback authority evidence
rotation and rollback checklist IDs
ordered credential-free receiver receipts
start and finish timestamps
stale-authority rejection results
```

Exact evidence produces the same rehearsal ID. Changed plan continuity,
authority, checklist, request ordering, payload hash, host, or timestamp changes
the result or fails closed.

## Safety boundary

The rehearsal uses `ControlledNotificationReceiver`, which opens no socket and
performs no DNS lookup or external request. The summary omits endpoint URLs and
paths, endpoint values, payload bodies, response bodies, credentials,
environment values, and PostgreSQL DSNs.

It performs no delivery-attempt write, outbox mutation, acknowledgement, cloud
scheduling, infrastructure deployment, or `terraform apply`. Committed
destination activation and notification delivery remain disabled.
