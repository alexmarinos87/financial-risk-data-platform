# Notification Retry Readiness Enforcement

Primary arc42 blocks: `orchestration` and `warehouse`.

## Goal

P4d5c applies the retained notification execution-readiness decision to the
manual retry execution boundary:

```text
exact retained retry plan
  + retained retry allow decision
  + fresh P4d5a evaluation
  + one physical advisory-lock acquisition
  -> fail-closed retry execution authority
  -> existing exact-plan revalidation
  -> first request may begin
```

The wrapper is
`src.orchestration.run_readiness_enforced_portfolio_risk_notification_retries`
and its model is
`portfolio-risk-notification-retry-readiness-enforced-v1`.

## Single-lock ordering

The wrapper acquires the shared notification-delivery advisory lock once. While
that physical lock remains held it:

1. reads and validates the current retained `retry` readiness record;
2. reruns the P4d5a gate at the execution clock;
3. requires retained and refreshed decisions to allow the same destination;
4. delegates to the existing retry executor;
5. lets that executor perform exact retry-plan and current-event revalidation;
6. retains each append-only delivery attempt; and
7. releases the physical lock only after the executor returns.

The lower-level executor receives a held-lock context. It therefore observes
the same lock identity without a nested PostgreSQL advisory-lock acquisition.
Failure to enter that held-lock context exactly once is rejected.

## Fail-closed evidence

Execution is rejected before transport when:

- `--execute` is absent;
- the retained plan confirmation differs;
- readiness is missing, blocked, stale, superseded, or internally inconsistent;
- the readiness record is for `initial` rather than `retry`;
- the destination identity differs;
- the refreshed decision blocks or differs substantively;
- the executor changes the plan, request, destination, or lock identity; or
- the executor does not run beneath the shared lock.

The output retains the readiness enforcement ID, retained and refreshed
decision IDs, destination, plan, and credential-free lock identity. It records
that there was one physical acquisition and no nested reacquisition.

## Operator path

After generating and reviewing an exact retry plan, the governed execution
entry point is:

```bash
.venv/bin/python \
  -m src.orchestration.run_readiness_enforced_portfolio_risk_notification_retries \
  --plan .demo/notification-retry-plan.json \
  --confirm-plan-id '<exact-plan-id>' \
  --request-id 'RETRY-2026-001' \
  --destination-id risk-operations-webhook \
  --execute \
  --summary-json .demo/readiness-enforced-retry.json
```

Committed configuration remains disabled for webhook delivery, destination
activation, and manual retry execution.

## Safety boundary

The new wrapper contains no transport implementation and opens no socket. It
delegates only after readiness succeeds and relies on the existing bounded
transport and append-only attempt contract. It retains no endpoint value,
complete URL, payload body, response body, credential, environment value, or
PostgreSQL DSN. Ordinary CI uses injected evidence and fake executors, performs
no external request or provider call, deploys nothing, and does not run
`terraform apply`.
