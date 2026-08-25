# Explicit Manual Notification Retry Execution

## Outcome

This increment consumes one exact retained notification retry plan under a separate,
disabled-by-default execution gate:

```text
retained portfolio-risk-dead-letter-retry-plan-v1
  + exact plan confirmation
  + current reviewed delivery and retry policy
  + current event, attempt and acknowledgement evidence
  -> fail-closed revalidation before the first request
  -> one bounded HTTPS attempt per planned event
  -> stable notification Idempotency-Key
  -> append-only delivery-attempt evidence
  -> deterministic credential-free execution summary
```

Primary arc42 block: `orchestration`, using the existing read-only warehouse evidence
interface and append-only delivery-attempt writer.

The implementation is:

```text
src/orchestration/execute_portfolio_risk_notification_retries.py
```

The strict retained-plan validator is:

```text
src/orchestration/portfolio_risk_notification_retry_plan_contract.py
```

## Separate activation gates

Committed configuration remains disabled:

```yaml
webhook:
  enabled: false
retry_execution:
  enabled: false
```

External retry delivery requires all of the following:

1. A reviewed configuration change enabling `delivery.webhook.enabled`.
2. A reviewed configuration change enabling `delivery.retry_execution.enabled`.
3. A fresh retry plan generated after those settings were enabled.
4. A locally supplied HTTPS endpoint through `RISK_NOTIFICATION_WEBHOOK_URL`.
5. An explicit `--execute` flag.
6. The exact retained `plan_id` repeated through `--confirm-plan-id`.
7. A bounded operator request ID and explicit execution timestamp.

A plan generated while webhook delivery was disabled cannot later be executed merely
by changing the configuration. A new plan must bind the enabled delivery
fingerprint.

## Retained plan contract

The executor accepts one regular JSON file no larger than 1 MB. Symbolic links,
malformed JSON, unknown fields and incomplete evidence fail closed.

The validator independently checks:

- exact top-level, event, attempt and acknowledgement fields;
- the `portfolio-risk-dead-letter-retry-plan-v1` model and webhook channel;
- deterministic plan identity;
- canonical event ordering and unique event IDs;
- event-age arithmetic at the original planning time;
- classification and retryable-event reconciliation;
- attempt-number and acknowledgement consistency;
- remaining attempt capacity;
- backoff and expiry semantics; and
- all original plan side-effect declarations remaining false.

The retained plan is evidence, not a general instruction file. The executor does not
accept an arbitrary list of event IDs.

## Revalidation immediately before delivery

After validating the file and current configuration, the executor performs one
bounded PostgreSQL read at the declared `executed_at` timestamp. It rebuilds the
retry plan using the current evidence and compares it with the retained plan before
the first network request.

Execution is rejected when any of the following changes:

- delivery configuration fingerprint;
- retry-planning policy fingerprint;
- policy or portfolio filter;
- exact pending event set;
- event identity or payload SHA-256;
- source risk-limit evaluation identity;
- latest delivery-attempt identity, number, timestamp, status or error code;
- acknowledgement identity or disposition;
- retry classification or next-eligible timestamp; or
- ordered retryable event IDs.

Only the naturally increasing `event_age_seconds` value is excluded from the exact
equality comparison. Expiry, backoff and classification are recalculated and must
still agree.

A newly delivered, acknowledged, exhausted, expired or otherwise changed event is
therefore rejected rather than retried from stale evidence.

## Bounded execution policy

The reviewed configuration declares:

```yaml
retry_execution:
  enabled: false
  max_plan_age_seconds: 3600
  max_events: 25
```

The execution event limit may not exceed either:

- the retry plan's `max_plan_events`; or
- the webhook delivery `max_batch_events`.

The maximum plan age may not exceed the retry policy's event-age limit. Bounds are
validated before PostgreSQL access and before any external request.

## One attempt per event

This command performs exactly one new attempt for each event listed in the retained
`retryable_event_ids` array.

It deliberately has no internal retry loop and performs no retry sleep. A failed
manual attempt produces new append-only evidence. A later operator action must
create a new current plan after the configured backoff has elapsed.

Attempt identity continues to bind:

```text
notification event ID
webhook channel
attempt number
portfolio-risk-webhook-delivery-v1
```

The next attempt number is derived from the exact retained attempt count and must
remain within the configured maximum.

## Idempotency and ambiguous remote success

Every webhook request uses the notification `event_id` as its stable
`Idempotency-Key`. The full endpoint is never retained; only its host is included in
evidence.

There remains an unavoidable ambiguity if the receiver accepts a request but local
attempt persistence fails. The executor reports failure rather than claiming
success, and the receiver must deduplicate the stable idempotency key. Concurrent
operators are inside the local trust boundary and must rely on the same remote
deduplication contract until a later distributed execution-lock increment is added.

## Append-only outcome evidence

Each external request is followed by an insert into the existing table:

```text
risk_platform.portfolio_risk_notification_delivery_attempts
```

The attempt retains:

- deterministic attempt ID;
- event ID and stable idempotency key;
- attempt number and timestamp;
- succeeded or failed outcome;
- HTTP status or bounded transport error code;
- endpoint host; and
- canonical payload SHA-256.

No response body, endpoint path, credential, DSN or arbitrary exception text is
retained. An invalid transport response is rejected before an attempt row is
written.

## Deterministic execution summary

The execution ID binds:

```text
retained plan ID
operator request ID
executed-at timestamp
endpoint host
current delivery fingerprint
current retry-policy fingerprint
current execution-policy fingerprint
ordered expected attempt IDs and numbers
```

With identical inputs and deterministic test clocks, the execution summary and
execution ID are identical.

The summary exposes:

- plan and request identity;
- current configuration fingerprints;
- revalidation result;
- bounded event and attempt counts;
- attempt IDs, timestamps and outcomes;
- endpoint host only; and
- explicit no-mutation declarations.

It declares:

```text
response_bodies_recorded = false
plan_mutated = false
acknowledgement_mutated = false
dead_letter_mutated = false
```

## Operator workflow

First apply a reviewed local configuration that enables both webhook delivery and
manual retry execution, then create a fresh plan:

```bash
export RISK_NOTIFICATION_WEBHOOK_URL='https://alerts.example.com/risk'

.venv/bin/python \
  -m src.orchestration.plan_portfolio_risk_notification_retries \
  --planned-at 2026-04-01T12:00:00Z \
  --policy-id us-tech-standard \
  --portfolio-id us-tech-equal \
  --summary-json .demo/notification-retry-plan.json
```

Inspect the complete plan, record its exact `plan_id`, and execute it explicitly:

```bash
.venv/bin/python \
  -m src.orchestration.execute_portfolio_risk_notification_retries \
  --plan .demo/notification-retry-plan.json \
  --confirm-plan-id '<exact-plan-id>' \
  --request-id 'RETRY-2026-001' \
  --executed-at 2026-04-01T12:05:00Z \
  --execute \
  --summary-json .demo/notification-retry-execution.json
```

The PostgreSQL DSN comes from `WAREHOUSE_POSTGRES_DSN` or `--dsn` and is not
included in the result.

## Validation boundary

Unit tests use fake readers, transports, clocks and attempt writers. Ordinary CI
performs no external delivery and the committed configuration remains disabled.

This increment does not:

- schedule retry execution;
- enable the committed webhook configuration;
- discover recipients or create endpoints;
- mutate acknowledgements, risk evidence, outbox events or dead-letter state;
- deliver provider data or mutate portfolio positions;
- deploy infrastructure; or
- run `terraform apply`.

The next dependency-safe roadmap item is **P4d — reviewed external alert delivery
activation and operational hardening**, kept separate from this local manual
authority contract.