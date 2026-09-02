# Notification Execution Readiness Enforcement

Primary arc42 blocks: `orchestration` and `warehouse`.

## Goal

P4d5c applies the retained readiness decision to the initial webhook execution
boundary:

```text
current allowed serving row
  + independently validated append-only readiness record
  + fresh gate evaluation
  + acquired shared delivery lock
  -> fail-closed execution authority
  -> first network request may begin
```

The enforcement model is
`portfolio-risk-notification-execution-readiness-enforcement-v1`.

## Exact retained authority

Execution reads exactly one row for the destination and `initial` execution kind
from:

```text
risk_platform.current_notification_execution_readiness_review
```

The row must be `allowed` with `execution_ready = true`. Its record, request,
decision, destination, execution kind, evaluation time and recording time must
all match the canonical append-only document retained in:

```text
risk_platform.notification_execution_readiness_decisions
```

Missing, blocked, stale, superseded, duplicate or internally inconsistent
evidence fails closed before transport or delivery-attempt persistence.

## Fresh under-lock evaluation

The initial sender first acquires the same shared delivery lock used by governed
retry execution. While that lock remains held it:

1. selects the zero-attempt candidates;
2. resolves the active destination and event allow-list;
3. reads and validates the current retained readiness record;
4. reruns the P4d5a gate at the sender's exact execution timestamp; and
5. compares the retained and refreshed substantive governed evidence.

The refresh receives a new timestamp-derived decision ID. Enforcement therefore
retains both IDs but compares the configuration, destination, activation,
transition, ambiguity, decision and side-effect evidence after excluding only
the decision ID and evaluation timestamp.

A retained decision may be at most five minutes old. A refreshed block or any
substantive mismatch is treated as supersession during preflight.

## Delivery evidence

A successful preflight adds credential-free evidence to the delivery summary:

- enforcement model and deterministic enforcement ID;
- destination and execution kind;
- readiness record and request IDs;
- retained and refreshed decision IDs and evaluation times;
- shared lock model, scope and key fingerprint;
- `allowed` review status; and
- confirmation that substantive evidence matched.

It does not retain the endpoint value, endpoint path, headers, credentials,
response body, payload body or PostgreSQL DSN.

## Ordering guarantee

The call order is intentionally:

```text
acquire lock
  -> candidate read
  -> destination authority
  -> readiness enforcement
  -> transport
  -> append-only attempt write
  -> release lock
```

A readiness failure therefore occurs before the first network request. The lock
remains held through every request and attempt insert.

## Operational boundary

Plan-only delivery remains read-only and does not require execution authority or
acquire the lock. Explicit execution now requires current retained and refreshed
readiness authority in addition to the existing destination and endpoint checks.

Committed webhook delivery, destination activation and retry execution remain
disabled by default. This increment does not schedule delivery, enable a
destination, resolve an endpoint value during CI, perform DNS in tests, mutate
the outbox, acknowledge a breach, deploy infrastructure or run `terraform
apply`.

Retry execution adopts the same shared enforcement contract in the next bounded
increment, with a separate append-only binding to terminal retry evidence.
