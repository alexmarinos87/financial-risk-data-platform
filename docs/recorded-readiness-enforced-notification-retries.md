# Recorded Readiness-Enforced Notification Retries

Primary arc42 blocks: `orchestration` and `warehouse`.

## Goal

This increment completes the operator-facing half of P4d5e:

```text
exact retained retry plan
  + current retained retry allow decision
  + fresh under-lock readiness evaluation
  + exact destination authority
  -> bounded retry execution
  -> terminal retry record
  -> readiness binding
  -> atomic terminal-plus-readiness commit
```

The entry point is
`src.orchestration.run_recorded_readiness_enforced_portfolio_risk_notification_retries`.
It composes the P4d5c single-lock executor with the P4d5d binding contract and
the P4d5e atomic storage primitive rather than introducing another readiness
model.

## Execution ordering

The command first validates `--execute`, the operator request ID and the exact
retained plan confirmation. Before a new request it checks for an existing
terminal record under that request ID.

For a new execution it then delegates to the established readiness-enforced
retry path. That path:

1. acquires the shared notification-delivery advisory lock;
2. validates one retained current `retry` allow record;
3. reruns the readiness gate beneath the same lock;
4. rejects changed, stale, blocked or superseded evidence;
5. revalidates the exact retry plan and current event evidence;
6. performs only the bounded planned requests; and
7. persists the existing append-only delivery-attempt evidence.

The recorded wrapper observes and validates the exact readiness enforcement and
destination authority returned by that path. It never creates readiness
authority after transport.

## Terminal outcomes

Once readiness has been granted, every terminal outcome receives a readiness
binding:

```text
all requests and attempts retained -> completed or failed_after_request
request without attempt evidence   -> persistence_uncertain
failure before the first request   -> failed_before_request
```

The terminal record and readiness binding are passed to the atomic governance
bundle recorder, so both commit or both roll back. The destination-authority
binding remains the established separate append-only evidence stream and is
recorded after the atomic terminal/readiness bundle.

A failure before readiness authority exists is rejected without a terminal
record. Such a rejection did not receive permission to execute and must not be
represented as an authorised terminal attempt.

## Exact replay

The operator request ID remains the idempotency boundary. Before any network or
attempt side effect, replay reads the retained terminal record and requires its
exact readiness binding.

```text
matching completed terminal + matching readiness binding
  -> reconstruct governed summary
  -> no external request is replayed

matching failed terminal + matching readiness binding
  -> return the retained bounded failure
  -> no external request is replayed

terminal present + readiness binding absent
  -> reject as legacy or incomplete evidence

changed plan under the same request ID
  -> reject
```

Replay validates that the binding was built from the exact retained terminal.
It does not infer or backfill readiness authority for older history.

## Operator command

```bash
.venv/bin/python \
  -m src.orchestration.run_recorded_readiness_enforced_portfolio_risk_notification_retries \
  --plan .demo/notification-retry-plan.json \
  --confirm-plan-id '<exact-plan-id>' \
  --request-id 'RETRY-2026-001' \
  --destination-id risk-operations-webhook \
  --execute \
  --summary-json .demo/recorded-readiness-retry.json
```

Committed webhook delivery, destination activation and retry execution remain
disabled. The operator must still supply reviewed configuration and the endpoint
environment value outside source control.

## Safety boundary

Ordinary CI uses injected readiness, transport, attempt, history and persistence
functions. It performs no network request, DNS lookup, socket operation,
webhook delivery, provider request, real delivery-attempt write,
acknowledgement, outbox mutation, schedule activation, deployment or
`terraform apply`. Retained governance evidence contains no endpoint value,
complete URL, credential, payload body, response body, environment value or
PostgreSQL DSN.
