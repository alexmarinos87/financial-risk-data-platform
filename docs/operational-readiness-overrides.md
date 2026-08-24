# Operational Readiness Override Evidence

## Outcome

A blocked operational readiness decision can receive narrowly scoped,
time-bounded and revocable human-authority evidence without changing the original
block:

```text
retained block decision
  -> explicit override request
  -> exact decision and operating-contract binding
  -> mandatory expiry of no more than 24 hours
  -> append-only approval or revocation event
  -> deterministic current and active override views
```

An override is evidence of reviewed authority. It is not a new readiness decision
and does not rewrite `block` to `allow`.

## Approval Command

```bash
.venv/bin/python -m src.warehouse.operational_readiness_override_registry approve \
  --decision-id operational-readiness-gate-v1-decision-... \
  --request-id READINESS-OVERRIDE-2026-001 \
  --expires-at 2026-04-01T18:00:00Z \
  --approved-by operator@example.test \
  --reason "Reviewed for one bounded local operating window."
```

`approved_at` defaults to the current UTC time and may be supplied explicitly for
replayable historical evidence. It cannot predate the blocked decision. Expiry is
mandatory, must be after approval and cannot exceed 24 hours.

An override binds the exact:

- blocked readiness decision and document SHA-256;
- gate and gate fingerprint;
- operational policy and fingerprint;
- schedule and fingerprint;
- calendar, portfolio and risk-limit policy;
- mandate fingerprint; and
- latest expected market session.

An `allow` decision cannot be overridden. PostgreSQL independently validates the
target status, metadata and timestamp ordering.

## Revocation Command

```bash
.venv/bin/python -m src.warehouse.operational_readiness_override_registry revoke \
  --override-id operational-readiness-override-v1-... \
  --request-id READINESS-OVERRIDE-REVOKE-2026-001 \
  --revoked-by operator@example.test \
  --reason "Override withdrawn after operational review."
```

Revocation is a separate append-only event. It cannot predate approval. A later
override uses a new request and produces a new retained row.

## Retry And Conflict Semantics

Approval and revocation request IDs are idempotency keys within their event type:

- an identical retry returns the retained event;
- conflicting reuse of one request ID fails closed; and
- no UPDATE path is exposed.

PostgreSQL triggers reject direct UPDATE and DELETE against both history tables.

## PostgreSQL Contract

History tables:

```text
risk_platform.operational_readiness_overrides
risk_platform.operational_readiness_override_revocations
```

Serving views:

```text
risk_platform.operational_readiness_override_history
risk_platform.current_operational_readiness_override_status
risk_platform.active_operational_readiness_overrides
```

The current grain is one latest override per blocked `decision_id`, ranked by:

```text
approved_at DESC
override_id DESC
```

The current status is:

- `pending` before approval time;
- `revoked` when the latest revocation is effective;
- `expired` at or after expiry; or
- `active` otherwise.

The runtime lookup accepts an explicit evaluation timestamp and does not rely on
wall-clock view status when deciding whether an override is active. This allows
repeatable enforcement tests and correct boundary behaviour at approval,
revocation and expiry instants.

## Reconciliation

`sql/operational_readiness_overrides_consistency_checks.sql` verifies:

- every target exists and remains a block decision;
- all copied contract fields and document SHA-256 match the decision;
- approvals do not predate decisions;
- expiry windows are positive and no longer than 24 hours;
- revocations reference overrides and do not predate approval;
- event and request identities are unique;
- current selection and status are correct;
- active rows match current status; and
- all four append-only mutation triggers exist.

The live PostgreSQL contract proves exact approval/revocation retries, conflict
rejection, rejection of an allow target, revocation, replacement override,
explicit-time active/expired lookup and mutation rejection.

## Boundary

This layer records authority but does not consume it. It does not execute the
local schedule, update a checkpoint, make a provider request, deliver a
notification or activate a cloud schedule. Execution enforcement is the next
separate dependency layer.
