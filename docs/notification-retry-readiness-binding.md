# Notification Retry Readiness Binding

Primary arc42 blocks: `orchestration` and `warehouse`.

## Goal

This increment defines the deterministic evidence contract that links one exact
terminal record for a retry to the exact readiness enforcement that permitted
its execution. The exact terminal record and exact readiness enforcement are
independently validated before the binding is constructed:

```text
canonical retry terminal record
  + canonical retry readiness enforcement
  + binding recording time
  -> terminal document SHA-256
  -> readiness document SHA-256
  -> deterministic readiness binding
```

The model is:

```text
portfolio-risk-notification-retry-readiness-binding-v1
```

## Cross-contract validation

The builder independently validates both source contracts before constructing a
binding. It requires:

- a canonical `portfolio-risk-notification-retry-execution-record-v1` terminal
  record;
- a canonical
  `portfolio-risk-notification-execution-readiness-enforcement-v1` authority;
- execution kind `retry`;
- enforcement time within the terminal execution window;
- binding time no earlier than terminal record persistence;
- matching delivery-lock model and key identities when the terminal record
  contains them; and
- matching lock scope for completed execution summaries.

A terminal record that reports one or more external requests must retain its
lock model and key fingerprint. It cannot be bound to an authority with another
lock identity.

## Deterministic identity

The binding ID covers:

```text
terminal record ID, request ID, plan ID, execution ID and status
terminal execution timestamps, counts and canonical document SHA-256
readiness enforcement ID and canonical document SHA-256
destination and execution kind
retained and refreshed readiness decision identities
readiness record and request identities
shared delivery-lock identity
binding recording time
```

Changing any of those values produces a different binding ID. Reordering JSON
keys does not.

The retained terminal-record digest is calculated from the full validated
terminal document. The retained readiness digest is calculated from the full
validated enforcement document. A later persistence increment can therefore
reconcile the binding to both append-only sources without embedding another
copy of the terminal record.

## Local builder

The credential-free builder consumes the terminal record and the P4d5c execution
summary produced by the readiness-enforced retry wrapper:

```bash
.venv/bin/python \
  -m src.orchestration.build_notification_retry_readiness_binding \
  --terminal-record .demo/retry-terminal-record.json \
  --execution-summary .demo/readiness-enforced-retry.json \
  --recorded-at 2026-09-02T19:00:00Z \
  --summary-json .demo/retry-readiness-binding.json
```

Both inputs must be regular non-symbolic-link JSON files no larger than 1 MB.
The execution summary must contain exact `execution_readiness` evidence.

## Deliberate boundary

This increment builds and validates canonical binding evidence but does not
persist it. A following PR can add append-only PostgreSQL history, exact replay
convergence, foreign-key reconciliation, and integration with the recorded
retry operator path without changing this identity contract.

There is no network request in this binding increment. The binding retains no
endpoint value, complete URL, environment value, credential, payload body,
response body, arbitrary exception text, or database DSN. The builder performs
no database operation, DNS lookup, socket operation, delivery attempt,
acknowledgement, outbox mutation, provider request, deployment, or
`terraform apply`.
