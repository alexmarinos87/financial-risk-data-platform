# Manual Webhook Notification Delivery

## Outcome

This adapter makes the **first** delivery attempt for current pending portfolio risk
notifications while retaining append-only evidence:

```text
current pending notification outbox
  -> acquire shared PostgreSQL delivery lock
  -> select zero-attempt events
  -> resolve one active reviewed destination
  -> verify endpoint-environment identity and event allow-list
  -> one bounded HTTPS request per event
  -> append-only delivery attempt
  -> release shared delivery lock
```

The implementation is
`src/orchestration/deliver_portfolio_risk_notifications.py`. Delivery configuration
is in `config/notification_delivery.yaml`; destination ownership and review evidence
is in `config/notification_destinations.yaml`.

## Disabled-by-default activation

Committed webhook delivery and destination activation both remain disabled.
External delivery requires all of the following:

1. reviewed webhook enablement;
2. reviewed destination activation with unexpired approval evidence;
3. an explicit `--execute` invocation;
4. an exact destination ID;
5. matching endpoint-environment identity; and
6. the endpoint value supplied locally through the reviewed environment variable.

The endpoint must be HTTPS without embedded credentials or a fragment. Only its host
is recorded. The full endpoint URL and environment value are never retained.

## Active destination authority

Before the first request, the sender resolves
`portfolio-risk-notification-destination-authority-v1` at a clock-derived execution
time. The authority binds:

- destination ID and fingerprint;
- review status and expiry;
- endpoint environment-variable identity; and
- the canonical event types selected for the batch.

The destination must be `active`. Disabled, future-reviewed and expired
destinations fail closed. The destination endpoint environment variable must match
the delivery configuration, and every selected event type must be present in the
reviewed allow-list.

The validation runs after candidate selection while the shared delivery lock is
held, but before transport or attempt persistence. A failing destination check
therefore performs no webhook request.

Plan-only invocation still evaluates the destination contract, but it may report an
inactive state without granting delivery authority.

## Shared concurrency control

Plan-only invocation performs no external request or mutation and does not acquire a
lock. Explicit delivery acquires the non-blocking PostgreSQL advisory lock defined
by `portfolio-risk-notification-delivery-lock-v1`.

The lock is acquired before candidate selection and held through destination
validation, every network request and append-only attempt insert. A contending
operator is rejected before candidate read or transport. Summary evidence exposes
only the lock model, scope and credential-free key fingerprint.

## Initial-attempt boundary

The PostgreSQL reader selects only events with zero retained webhook attempts. A
custom reader that supplies any prior attempt is rejected before the first request.

The adapter performs exactly one attempt numbered `1` per selected event. It has no
hidden retry loop and no retry sleep. A failed initial attempt continues through:

```text
P4b deterministic retry plan
  -> exact retained plan review
  -> P4c governed manual retry execution
```

## Idempotency and evidence

Every request uses the notification `event_id` as its stable `Idempotency-Key`.
Receivers must deduplicate this value because remote success can occur before local
attempt persistence.

Each append-only attempt stores:

- attempt and event IDs;
- attempt number and timestamp;
- success or bounded failure evidence;
- HTTP status or bounded error code;
- endpoint host;
- payload SHA-256; and
- idempotency key.

The initial-delivery summary additionally retains the exact destination authority,
including destination ID, fingerprint, activation status, evaluation time and
evaluated event types. No endpoint value is included.

Response bodies, endpoint paths, credentials, secret-bearing headers, database DSNs
and arbitrary exception messages are never stored. Invalid transport status values
fail before an attempt row is written.

## Operator commands

Plan without network activity:

```bash
.venv/bin/python -m src.orchestration.deliver_portfolio_risk_notifications \
  --destination-id risk-operations-webhook \
  --summary-json .demo/webhook-delivery-plan.json
```

After reviewed activation:

```bash
export RISK_NOTIFICATION_WEBHOOK_URL='https://alerts.example.com/risk'

.venv/bin/python -m src.orchestration.deliver_portfolio_risk_notifications \
  --destination-id risk-operations-webhook \
  --execute \
  --summary-json .demo/webhook-delivery-run.json
```

The PostgreSQL DSN comes from `WAREHOUSE_POSTGRES_DSN` or `--dsn` and is not placed
in the summary.

## PostgreSQL serving

Append-only attempts remain in:

```text
risk_platform.portfolio_risk_notification_delivery_attempts
```

Operational views remain:

```text
risk_platform.portfolio_risk_notification_delivery_status
risk_platform.portfolio_risk_notification_delivery_pending
risk_platform.portfolio_risk_notification_delivery_succeeded
```

The source outbox stays immutable. Delivery status is derived from attempt history.

## Boundary

This adapter does not schedule itself, retry existing attempts, discover
recipients, create endpoints, rotate secrets, acknowledge or resolve breaches,
mutate analytical evidence, block trading, deploy infrastructure, or run
`terraform apply`. CI uses fake transports and performs no network request.
