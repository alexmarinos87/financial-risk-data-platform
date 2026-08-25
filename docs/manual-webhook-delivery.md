# Manual Webhook Notification Delivery

## Outcome

This adapter makes the **first** delivery attempt for current pending portfolio risk
notifications while retaining append-only evidence:

```text
current pending notification outbox
  -> acquire shared PostgreSQL delivery lock
  -> exclude delivered events and every event with an existing attempt
  -> bounded manual batch
  -> one HTTPS POST with stable Idempotency-Key
  -> append-only PostgreSQL delivery attempt
  -> release shared delivery lock
  -> pending or succeeded operational views
```

The implementation is
`src/orchestration/deliver_portfolio_risk_notifications.py`. The committed
configuration is `config/notification_delivery.yaml`.

## Disabled-by-default activation

The committed webhook configuration has `enabled: false` and contains no endpoint
or recipient. Normal invocation is plan-only. External delivery requires both:

1. a reviewed configuration change setting `enabled: true`; and
2. an explicit `--execute` invocation with `RISK_NOTIFICATION_WEBHOOK_URL` set
   locally.

The endpoint must be HTTPS and may not contain embedded credentials or a URL
fragment. Only the endpoint host is recorded in evidence. The full URL and
environment value are not written to summaries or the database.

## Shared concurrency control

Plan-only invocation performs no external request or mutation and does not acquire a
lock. Explicit delivery acquires the non-blocking PostgreSQL advisory lock defined
by:

```text
portfolio-risk-notification-delivery-lock-v1
```

The lock is acquired before candidate selection and held through every network
request and append-only attempt insert. A second local operator or separate checkout
using the same PostgreSQL database is rejected immediately while the lock is held.
It performs no candidate read or webhook request.

The summary exposes the lock model, scope and a credential-free key fingerprint.
The raw advisory key and PostgreSQL DSN are not retained.

## Initial-attempt boundary

The default PostgreSQL reader selects only events with zero retained webhook
attempts. A custom reader that supplies an event with any prior attempt is rejected
before the first network request.

The adapter therefore performs exactly one attempt numbered `1` per selected event.
It has no internal retry loop and performs no retry sleep. A failed initial attempt
must continue through the governed workflow:

```text
P4b deterministic retry plan
  -> exact retained plan review
  -> P4c explicit manual retry execution
```

This prevents the older delivery command from bypassing current event,
acknowledgement, attempt, expiry and backoff revalidation.

The reviewed `max_attempts_per_event` and `initial_backoff_seconds` settings remain
part of the delivery fingerprint and are consumed by retry planning and exact retry
execution. They no longer authorize hidden retries inside this first-attempt
adapter.

## Idempotency

Every request uses the notification `event_id` as the `Idempotency-Key`. The first
attempt and every later governed retry therefore carry the same remote
deduplication identity.

Attempt identity binds:

```text
notification event ID
webhook channel
attempt number
portfolio-risk-webhook-delivery-v1
```

The shared lock prevents simultaneous local senders. There remains an unavoidable
failure window in which the remote receiver accepts a request but local attempt
persistence fails. The stable `Idempotency-Key` is the control for that ambiguity;
receivers must deduplicate it.

## Evidence boundary

Each attempt stores:

- attempt and notification IDs;
- attempt number and timestamp;
- outcome;
- HTTP status or bounded error code;
- endpoint host;
- payload SHA-256; and
- idempotency key.

Response bodies, endpoint paths, credentials, headers containing secrets and
arbitrary exception messages are never stored. Invalid transport status values fail
before an attempt row is written.

The execution summary also declares whether concurrency control was acquired,
released and held through attempt persistence.

## Operator commands

Plan the current **initial-attempt** batch without network activity:

```bash
.venv/bin/python -m src.orchestration.deliver_portfolio_risk_notifications \
  --summary-json .demo/webhook-delivery-plan.json
```

After reviewing and enabling the configuration:

```bash
export RISK_NOTIFICATION_WEBHOOK_URL='https://alerts.example.com/risk'

.venv/bin/python -m src.orchestration.deliver_portfolio_risk_notifications \
  --execute \
  --summary-json .demo/webhook-delivery-run.json
```

The PostgreSQL DSN comes from `WAREHOUSE_POSTGRES_DSN` or `--dsn`. It is not
included in summaries.

For an event that already has a failed attempt, generate and execute an exact retry
plan instead of invoking this adapter again:

```bash
.venv/bin/python \
  -m src.orchestration.plan_portfolio_risk_notification_retries \
  --planned-at 2026-04-01T12:00:00Z \
  --summary-json .demo/notification-retry-plan.json
```

The separate P4c command consumes the exact resulting plan and uses the same shared
delivery lock before current-evidence revalidation.

## PostgreSQL serving

The append-only table is:

```text
risk_platform.portfolio_risk_notification_delivery_attempts
```

Operational views are:

```text
risk_platform.portfolio_risk_notification_delivery_status
risk_platform.portfolio_risk_notification_delivery_pending
risk_platform.portfolio_risk_notification_delivery_succeeded
```

The source outbox remains immutable. Delivery status is derived from attempts rather
than written back to notification records.

## Boundary

This adapter does not schedule itself, retry an existing attempt, discover
recipients, create webhook endpoints, rotate secrets, acknowledge or resolve
breaches, mutate analytical evidence, block trading, deploy infrastructure, or run
`terraform apply`. CI uses fake transports and performs no network request.
