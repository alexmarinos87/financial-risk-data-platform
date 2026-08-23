# Manual Webhook Notification Delivery

## Outcome

This adapter delivers current pending portfolio risk notifications to one explicitly configured HTTPS webhook while retaining append-only attempt evidence:

```text
current pending notification outbox
  -> exclude an already succeeded event
  -> bounded manual batch
  -> HTTPS POST with stable Idempotency-Key
  -> bounded retry and exponential backoff
  -> append-only PostgreSQL delivery attempt
  -> pending or succeeded operational views
```

The implementation is `src/orchestration/deliver_portfolio_risk_notifications.py`. The committed configuration is `config/notification_delivery.yaml`.

## Disabled-by-default activation

The committed webhook configuration has `enabled: false` and contains no endpoint or recipient. Normal invocation is plan-only. External delivery requires both:

1. a reviewed configuration change setting `enabled: true`; and
2. an explicit `--execute` invocation with `RISK_NOTIFICATION_WEBHOOK_URL` set locally.

The endpoint must be HTTPS and may not contain embedded credentials or a URL fragment. Only the endpoint host is recorded in evidence. The full URL and environment value are not written to summaries or the database.

## Idempotency and retries

Every request uses the notification `event_id` as the `Idempotency-Key`. Retries of the same event therefore carry the same remote deduplication identity.

Attempt identity binds:

```text
notification event ID
webhook channel
attempt number
portfolio-risk-webhook-delivery-v1
```

Attempts are append-only and numbered from one. A successful attempt prevents the event from being selected again. A failed attempt may be retried until `max_attempts_per_event` is reached. Backoff begins at `initial_backoff_seconds`, doubles after each failure, and is capped at 60 seconds.

There remains an unavoidable failure window in which the remote receiver accepts a request but local attempt persistence fails. The stable `Idempotency-Key` is the control for that ambiguity; receivers must deduplicate it.

## Evidence boundary

Each attempt stores:

- attempt and notification IDs;
- attempt number and timestamp;
- outcome;
- HTTP status or bounded error code;
- endpoint host;
- payload SHA-256; and
- idempotency key.

Response bodies, endpoint paths, credentials, headers containing secrets, and arbitrary exception messages are never stored.

## Operator commands

Plan the current batch without network activity:

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

The PostgreSQL DSN comes from `WAREHOUSE_POSTGRES_DSN` or `--dsn`. It is not included in summaries.

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

The source outbox remains immutable. Delivery status is derived from attempts rather than written back to notification records.

## Boundary

This adapter does not schedule itself, discover recipients, create webhook endpoints, rotate secrets, acknowledge or resolve breaches, mutate analytical evidence, block trading, deploy infrastructure, or run `terraform apply`. CI uses fake transports and performs no network request.
