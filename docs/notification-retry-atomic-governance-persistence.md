# Atomic Notification Retry Governance Persistence

Primary arc42 blocks: `warehouse` and `orchestration`.

## Goal

P4d5e begins by making the retry terminal record and its exact readiness binding
one PostgreSQL transaction:

```text
canonical terminal retry record
  + canonical readiness binding built from that record
  -> validate both documents and their cross-contract identity
  -> append terminal record
  -> append readiness binding
  -> commit both or roll back both
```

The bundle model is
`portfolio-risk-notification-retry-governance-bundle-v1`.

## Shared transaction helpers

The established terminal recorder now exposes a cursor-scoped function in
addition to its existing standalone API. The standalone behavior remains the
same, while the bundle can compose terminal and readiness persistence beneath
one caller-owned transaction.

The readiness-binding recorder already exposes the equivalent cursor-scoped
function. It reloads the just-inserted terminal row through the same transaction
and independently reconciles the retained readiness source before accepting the
binding.

## Cross-contract validation

Before opening PostgreSQL, the bundle:

- validates the canonical terminal record;
- validates the canonical readiness binding;
- rebuilds the binding from the supplied terminal and retained enforcement; and
- rejects any terminal, request, plan, execution, timing, count, lock or digest
  mismatch.

A binding built for another terminal cannot enter the transaction.

## Atomic outcomes

For new evidence:

```text
terminal insert succeeds
  + readiness source and binding validation succeeds
  -> both rows commit

terminal insert succeeds
  + readiness source or binding validation fails
  -> transaction rolls back
  -> neither row remains
```

Exact replay requires both rows to exist and match canonically. A historical
terminal row with no readiness binding is classified as legacy evidence and is
not silently backfilled through the atomic execution path.

```text
matching terminal + matching binding -> exact replay
matching terminal + missing binding  -> reject legacy backfill
changed terminal or binding           -> reject conflict
```

## PostgreSQL evidence

The PostgreSQL 16 contract fixture proves:

- a fresh terminal and readiness binding commit together;
- exact bundle replay converges without another row;
- an existing legacy terminal cannot be silently backfilled;
- failure of the second write because its readiness source is absent rolls the
  newly inserted terminal row back; and
- the complete retry-readiness history reconciliation remains green.

## Deliberate boundary

This increment delivers the atomic persistence primitive. It does not yet
replace the operator-facing recorded retry command; that wiring is the next
bounded PR so execution orchestration and storage transaction behavior remain
independently reviewable.

The bundle performs no network request, webhook delivery, provider request,
delivery-attempt write, acknowledgement, outbox mutation, schedule activation,
deployment or `terraform apply`. It retains no endpoint value, full URL,
credential, payload body, response body, environment value or PostgreSQL DSN.
Committed delivery, destination activation and retry execution remain disabled.
