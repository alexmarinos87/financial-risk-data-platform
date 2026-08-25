# PostgreSQL Notification Delivery Concurrency Control

## Outcome

This increment prevents two local operators or separate repository checkouts from
executing notification delivery at the same time:

```text
explicit initial delivery or exact retry execution
  -> non-blocking PostgreSQL advisory-lock acquisition
  -> candidate selection or current-evidence revalidation
  -> bounded HTTPS request
  -> append-only delivery-attempt insert
  -> advisory-lock release
```

Primary arc42 block: `orchestration`, with PostgreSQL used as the shared
coordination boundary.

The implementation is:

```text
src/orchestration/portfolio_risk_notification_delivery_lock.py
```

The lock model is:

```text
portfolio-risk-notification-delivery-lock-v1
```

## Why the lock is global

The local delivery path is deliberately low-volume and operator-driven. One global
lock is simpler and safer than event-by-event locking because it also protects the
bounded candidate-selection and retry-revalidation windows.

While the lock is held, no other initial-delivery or governed-retry process can
enter the delivery lane, even when it is running from another checkout on the same
machine or another machine using the same PostgreSQL database.

This conservative scope trades parallelism for an unambiguous operational control.
A future high-throughput delivery service may replace it with leased event-level
work claiming, but that is outside this local manual boundary.

## Acquisition semantics

The control uses a deterministic signed 64-bit key derived from the lock model and
scope. Summaries expose only a SHA-256-derived key fingerprint, not the raw key or
PostgreSQL DSN.

Acquisition uses:

```sql
SELECT pg_try_advisory_lock(...)
```

It is non-blocking. When another process already owns the lock, execution fails
closed immediately and performs no candidate read, current-plan revalidation,
network request or attempt insert.

The same PostgreSQL session remains open for the complete protected operation.
Session close releases the lock after process failure. Normal completion also calls
`pg_advisory_unlock` explicitly and treats an unexplained release failure as a
storage error.

## Initial delivery ordering

`deliver_portfolio_risk_notifications` remains plan-only without a lock when
`--execute` is absent. Explicit delivery follows this order:

```text
validate reviewed enablement and HTTPS endpoint
  -> acquire global delivery lock
  -> read current zero-attempt candidates
  -> validate first-attempt-only evidence
  -> send one request per candidate
  -> persist each append-only attempt
  -> release lock
```

Candidate selection therefore occurs inside the shared lock rather than before it.

## Governed retry ordering

`execute_portfolio_risk_notification_retries` follows this order:

```text
validate retained plan, confirmation and reviewed configuration
  -> acquire global delivery lock
  -> read current event/attempt/acknowledgement evidence
  -> rebuild and compare the exact current retry plan
  -> send one request per retained retryable event
  -> persist each append-only attempt
  -> release lock
```

The lock is held through current-evidence revalidation and attempt persistence, so a
second operator cannot pass the same stale revalidation window concurrently.

The lock model version and key fingerprint are bound into the deterministic retry
execution identity.

## Evidence

Successful execution summaries include:

```text
concurrency_control.performed = true
concurrency_control.acquired = true
concurrency_control.released = true
concurrency_control.held_through_attempt_persistence = true
```

Retry summaries additionally declare:

```text
concurrency_control.held_through_revalidation = true
```

Plan-only initial-delivery summaries declare that concurrency control was not
performed because they make no external request or database mutation.

## Failure boundary

The lock prevents concurrent local senders from entering the delivery lane. It does
not remove the distributed ambiguity in which a remote receiver accepts a request
but local attempt persistence fails. The stable notification `Idempotency-Key`
remains the control for that failure window, and receivers must deduplicate it.

The lock is coordination, not authentication. An operator able to change and run
repository code remains inside the local trust boundary.

## Validation boundary

Unit tests use fake PostgreSQL sessions and prove:

- deterministic signed advisory-lock identity;
- acquisition and explicit release on one session;
- immediate rejection when the lock is held;
- connection close after body failure;
- release-failure reporting after a successful body;
- initial delivery acquires before candidate selection; and
- governed retry acquires before current-evidence revalidation.

Ordinary CI uses fake transports and performs no webhook request. The committed
webhook and retry-execution settings remain disabled. This increment does not
activate a destination, schedule delivery, deploy infrastructure or run
`terraform apply`.

The next dependency-safe P4d slice is append-only retry-execution history and
current delivery-failure, ambiguous-outcome and retry-follow-up operational views.
