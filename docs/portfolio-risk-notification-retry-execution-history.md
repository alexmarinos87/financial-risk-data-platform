# Append-only Notification Retry Execution History

## Outcome

This increment retains one canonical terminal record for every governed manual retry
execution invoked through the recorded operator path:

```text
exact P4b retry plan
  + clock-bound P4c execution
  + shared PostgreSQL delivery lock
  + request and append-only attempt evidence
  -> completed or bounded failure classification
  -> canonical terminal execution record
  -> append-only PostgreSQL history
  -> exact retry convergence
```

Primary arc42 block: `warehouse`, with the existing orchestration executor providing
the source execution evidence.

The recorded operator entry point is:

```text
src/orchestration/run_recorded_portfolio_risk_notification_retries.py
```

The record contract and recorder are:

```text
src/warehouse/notification_retry_execution_contract.py
src/warehouse/notification_retry_execution_recorder.py
```

## Terminal statuses

Every retained request has exactly one terminal status:

```text
completed
failed_before_request
failed_after_request
persistence_uncertain
```

`completed` means every recorded external request has matching append-only attempt
evidence and the exact P4c summary confirms that the delivery lock was held through
revalidation and persistence, then released.

`failed_before_request` means no external request was observed and no attempt row was
persisted. Examples include stale plan evidence, disabled configuration, lock
contention and endpoint validation failure.

`failed_after_request` means one or more requests occurred and every request has a
matching append-only attempt row, but a later local step failed. Retained attempt
outcomes remain the source of truth; the terminal execution is not reported as
completed.

`persistence_uncertain` means an external request was observed without matching
local attempt persistence. The exact requested event IDs are retained separately
from persisted event IDs so an operator can identify the ambiguous remote outcome.
The system does not claim success. The receiver must continue deduplicating by the
stable notification `Idempotency-Key`.

## Exact evidence

The `portfolio-risk-notification-retry-execution-record-v1` document binds:

- deterministic record ID;
- exact operator request ID and retained retry-plan ID;
- P4c execution ID when completed;
- terminal status and bounded failure code;
- start, finish and recording timestamps;
- external request count;
- persisted attempt count and succeeded/failed counts;
- ordered requested event IDs;
- ordered persisted event IDs;
- deterministic attempt IDs;
- endpoint host only;
- delivery, retry and retry-execution policy fingerprints;
- delivery-lock model and credential-free key fingerprint;
- lock acquisition/release evidence when known; and
- the exact P4c execution summary for completed runs.

Full endpoint URLs, payload bodies, response bodies, PostgreSQL DSNs, environment
values, credentials and arbitrary exception text are not stored.

For failed executions, exception types are reduced to one bounded code:

```text
overlap_error
storage_error
unexpected_error
validation_error
```

## Recorded execution ordering

The wrapper starts a local clock-bound execution record, invokes the existing exact
P4c executor with tracking transport and attempt-writer interfaces, and records one
terminal document after the executor returns or raises.

```text
validate retained plan and request identity
  -> invoke exact P4c executor
  -> observe each external request identity
  -> observe each successful append-only attempt persistence
  -> classify terminal boundary
  -> validate canonical record
  -> append terminal history
```

The wrapper does not replace P4c plan validation, current-evidence revalidation or
delivery locking. It delegates all delivery authority to the existing executor.

## Append-only PostgreSQL history

The history table is:

```text
risk_platform.portfolio_risk_notification_retry_executions
```

`request_id` is the idempotency identity for terminal recording:

- an exact retry converges on the retained row;
- conflicting reuse of the request ID fails closed;
- a completed execution ID is unique; and
- direct UPDATE and DELETE operations are rejected by a trigger.

The canonical JSON document and SHA-256 are stored alongside queryable scalar and
array fields. PostgreSQL checks enforce status-specific request/persistence counts,
completed lock evidence and the distinct requested-versus-persisted event sets used
for ambiguous outcomes.

## Operator command

Use the recorded wrapper rather than invoking the lower-level retry executor
directly when durable terminal history is required:

```bash
.venv/bin/python \
  -m src.orchestration.run_recorded_portfolio_risk_notification_retries \
  --plan .demo/notification-retry-plan.json \
  --confirm-plan-id '<exact-plan-id>' \
  --request-id 'RETRY-2026-001' \
  --execute \
  --summary-json .demo/recorded-notification-retry.json
```

The wrapper retains terminal failure history before returning a non-zero exit code.
If PostgreSQL itself is unavailable, durable history cannot be claimed and the
command fails explicitly.

## Validation boundary

Unit tests use fake transports, attempt writers, clocks, executors and history
recorders. The PostgreSQL 16 contract proves:

- one terminal row is created;
- an exact retry converges;
- conflicting request reuse fails;
- direct mutation is rejected; and
- retained evidence remains credential-free.

Ordinary CI performs no webhook request. Committed webhook and retry-execution
activation remain disabled.

## Boundary

This increment does not schedule retries, activate a destination, provision a
webhook, change notification recipients, mutate outbox or acknowledgement evidence,
change portfolio positions, deploy infrastructure or run `terraform apply`.

Current delivery-failure, ambiguous-outcome and retry-follow-up serving views are a
separate follow-up PR so persistence and query semantics remain independently
reviewable.
