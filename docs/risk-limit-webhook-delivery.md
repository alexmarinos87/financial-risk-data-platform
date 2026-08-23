# Explicit Risk-Limit Webhook Delivery

## Outcome

Current pending risk-limit notification intent can be delivered through one explicitly configured webhook adapter:

```text
PostgreSQL pending notification view
  -> disabled adapter configuration
  -> explicit runtime enable flag
  -> endpoint and authorization from environment
  -> stable Idempotency-Key
  -> bounded retry and backoff
  -> sanitized append-only local attempt records
```

The transport is `src/delivery/risk_limit_webhook.py`. The operator runner is `src/orchestration/run_risk_limit_webhook_delivery.py`.

## Two enable gates

The bundled configuration contains:

```yaml
enabled: false
```

The runner also requires:

```text
--enable-external-delivery
```

Both gates must be enabled. A runtime flag cannot override a disabled configuration. Normal CI, readiness checks, and local development therefore make no external request.

## Secret-safe configuration

The configuration stores environment-variable names, never endpoint or authorization values:

```text
RISK_LIMIT_WEBHOOK_URL
RISK_LIMIT_WEBHOOK_AUTHORIZATION
```

The run summary records only those variable names. Endpoint and authorization values are not written to summaries or attempt files. There is no default endpoint or recipient.

## Explicit invocation

After reviewing and enabling `config/notification_delivery.yaml`:

```bash
export RISK_LIMIT_WEBHOOK_URL='https://receiver.example/risk-limit'
export RISK_LIMIT_WEBHOOK_AUTHORIZATION='Bearer resolved-at-runtime'

.venv/bin/python -m src.orchestration.run_risk_limit_webhook_delivery \
  --adapter-id risk-limit-webhook \
  --enable-external-delivery \
  --summary-json .demo/risk-limit-webhook-delivery.json
```

The runner reads only:

```text
risk_platform.pending_portfolio_risk_limit_notifications
```

Historical or corrected stale breaches excluded by the current pending view are not delivered.

## Idempotency and retries

Every request uses the notification deduplication identity as the HTTP `Idempotency-Key`. The receiver should honour that key so a network timeout after server-side acceptance cannot create a duplicate external message.

Attempt identity binds:

```text
adapter ID
notification ID
attempt number
```

Attempt files contain only:

```text
attempt identity and number
notification and deduplication identity
attempted-at timestamp
HTTP status or sanitized error class
result status
```

They do not contain the endpoint, authorization value, request payload, or response body.

Retryable responses are HTTP 408, 425, 429, and 5xx plus transport timeouts/errors. Other 4xx responses are permanent failures. Backoff is exponential and bounded by configuration. A delivered or permanently failed notification is not sent again on an identical rerun.

## Bounds

Configuration caps attempts at ten and one invocation at 1,000 notifications, with a lower default of 100. Attempt history is capped at 100,000 files. Delivery is one-shot; no background worker or resident retry process is started.

## Boundary

This adapter does not discover recipients, manage credentials, acknowledge or resolve a breach, mutate notification rows, guarantee receiver behaviour, schedule itself, deploy infrastructure, or run `terraform apply`. Production use requires review of the receiver's authentication, idempotency, retention, availability, and incident procedures.
