# Notification Retry Readiness Binding History

Primary arc42 blocks: `warehouse` and `orchestration`.

## Goal

P4d5d makes the deterministic retry-readiness binding from PR #138 durable:

```text
append-only retry terminal record
  + append-only retained retry readiness decision
  + exact under-lock readiness enforcement
  -> independently reconciled canonical binding
  -> append-only PostgreSQL history
  -> bound or binding_missing operational state
```

The canonical document remains
`portfolio-risk-notification-retry-readiness-binding-v1`. Persistence does not
introduce a competing identity model.

## Source reconciliation

Before insertion, the recorder independently reloads and validates both source
documents.

For terminal history it verifies the record, request, plan, execution, status,
timestamps, request and attempt counts, and canonical document SHA-256 against
`portfolio_risk_notification_retry_executions`.

For readiness history it verifies the readiness record and request IDs, retained
decision ID, destination, `retry` execution kind, decision evaluation time and
canonical source digest against
`notification_execution_readiness_decisions`. The retained decision must be an
`allow` decision with no blocking reasons.

A binding cannot be inserted for a missing, changed, blocked or initial-delivery
readiness source.

## Append-only identity and replay

Each terminal record can have at most one readiness binding. Each enforcement
identity can also be retained at most once.

```text
same terminal record + identical canonical binding
  -> exact replay converges with created = false

same terminal record + changed binding time, authority or decision evidence
  -> reject

same enforcement identity under another terminal record
  -> reject
```

Direct `UPDATE` and `DELETE` operations are rejected by PostgreSQL triggers.

## Legacy visibility

The status view preserves every terminal retry record. History created before
this contract is not rewritten or assigned inferred authority:

```text
retained binding present -> bound
retained binding absent  -> binding_missing
```

The `binding_missing` view is therefore an explicit governance-review queue,
not evidence that the historical retry was authorised by the new contract.

## PostgreSQL evidence

The transaction-independent contract fixture proves:

- canonical source decision and terminal retention;
- first insert and exact replay convergence;
- canonical reader round-trip;
- conflicting terminal binding rejection;
- missing readiness source rejection;
- visible legacy `binding_missing` classification;
- direct update and delete rejection; and
- twelve reconciliation checks across source identity, view grain and
  append-only controls.

## Safety boundary

This increment persists credential-free evidence only. It performs no network
request, DNS lookup, socket operation, webhook delivery, provider request,
delivery-attempt write, acknowledgement, outbox mutation, schedule activation,
deployment or `terraform apply`. It retains no endpoint value, complete URL,
credential, payload body, response body, environment value or PostgreSQL DSN.
Committed notification delivery, destination activation and retry execution
remain disabled.
