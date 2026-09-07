# Observe worker attempt attribution without inventing durability

Primary arc42 block: `orchestration`. Goal #215; depends on context #213/#211.

`WorkerAttemptObserver` is an explicitly constructed decorator for the existing
attempt-writer callback. It binds a producer-shaped attempt to a caller-supplied,
validated worker execution context and keeps detached receipts in memory. It is
not installed in any default execution path and adds no scheduler or transport.

## Order and failure semantics

The initial and retry producers call their attempt writer after transport. The
observer therefore captures the input and forwards a detached copy to the base
writer before checking attribution semantics. A bad kind, time, duplicate event
or exceeded observation limit cannot erase a normally returned base write. Such
an error is reported as `attribution_rejected_after_writer`, with the returned
write counted as unattributed, and subsequent callbacks are refused.

A capture/copy failure is distinct: no delegate call occurred and the observer
records `capture_failed_before_writer`. Producer input must be an ordinary stable
mapping; this adapter is not a hostile-object or process-memory sandbox. No claim
is made that every possible failing callback preserves a database record.

The delegate must follow the existing callback contract and return None. A normal
return increments `writer_returns`; it does not establish database durability.
A writer exception marks its outcome uncertain, even when it may have committed
before raising. The adapter never retries. Unsupported acknowledgements also
mark uncertainty. Ordinary failures have fixed diagnostics with no source values;
interruptions propagate after the corresponding failure state is latched.
Callers must stop after any error and reconcile the original persistence path.

## Receipt and scope

Receipts reuse the established attempt model and ID function. Initial attempts
must be number 1 and retries 2 through 10. Attempt time must fall in the execution
context's inclusive start / exclusive expiry interval. HTTP status, outcome and
error code must agree with the existing producer shape. Each receipt binds the
canonical source-attempt digest, context and scope IDs, worker, destination, kind,
event, attempt number, timestamp and observed transport outcome.

The source digest includes the normalized complete input attempt; the receipt
and report omit endpoint hosts, endpoint values, payload bodies and raw error
messages. Event IDs are bounded text, not a secret scanner. A base writer can
mutate its private copy; the receipt still describes the input observed at the
callback, not independent proof of the row ultimately stored by that writer.

Attribution is explicitly `caller_supplied_execution_context`. The observer
cannot infer worker/destination identity from the legacy attempt shape, verify
that the actual delivery configuration matches the context, or authenticate the
caller. Preserving that correspondence is the future runtime adapter's job.
Nothing in these receipts establishes current authority or readiness permission.

## Bounded in-memory reporting

A context's reviewed max_events limits retained receipts. One event may occur
only once per invocation, consistent with the current producer loops. A duplicate
or first overflow write is forwarded before attribution fails; later callbacks
are refused. The report exposes observed attempt totals, not consecutive-failure
counts or complete history. The object is owner-thread-only and non-reentrant;
it is not the shared delivery lock or a durable slot claim.

`snapshot()` returns detached canonical report data. `seal()` stops further
callbacks and is idempotent. An empty sealed report is not a verified healthy
zero. `attribution_durable`, `database_commit_verified`, `failure_history_complete`
and `runtime_permission_granted` remain false in every report. Receipts and
failure state can be lost on a process crash. No atomic source/receipt transaction,
crash cleanup or cross-process concurrency protection is claimed.

## Executable evidence and next boundary

```bash
python -m pytest -q \
  tests/unit/test_worker_execution_context.py \
  tests/unit/test_worker_attempt_observer.py \
  tests/unit/test_worker_attempt_producer_wiring.py
```

The focused observer cases cover both kinds, strict source identity, transport
failures versus write failures, source-window bounds, writer mutation, duplicate
and overflow callbacks, interruption, sealing and exclusive use. The separate
wiring cases execute the actual initial-attempt producer loop using only injected
no-network transport and in-memory writer callbacks. They prove transport-before-
write order, exact source digests, no second event after a failure, and persistence
of a returned callback even when attribution is expired. They do not exercise
live execution gates or the complete retry operator. The commit-then-raise unit
case is an in-memory simulation, not a real PostgreSQL transaction proof.

After acceptance, the next bounded work is a durable context/attempt attribution
ledger with replay/conflict semantics, complete coverage markers, atomic source
and receipt persistence, and real PostgreSQL rollback/crash reconciliation.
Only then can retained worker failure histories be independently derived and
connected to suspension. Do not backfill missing legacy attribution by inference.

No existing producer, schema, workflow, dependency, activation default or live
execution path is modified. The supplied writer may perform its explicit I/O;
this adapter itself creates no connection, sends no request, activates nothing
and deploys nothing. Existing runtime gates must not be bypassed to use it.
Independent review and explicit final-diff acceptance remain separate from CI.
