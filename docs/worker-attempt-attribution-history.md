# Atomic source-attempt and worker-attribution persistence

Primary arc42 block: `warehouse`. Goal #230; depends on #229 / #226 after #213 and #218.

`record_worker_attempt_attribution` explicitly retains a validated attribution
record. A new context, original delivery-attempt row and complete attribution are
written together. It is not installed in the existing producer, observer or any
scheduler. Installing its SQL schema is a separate reviewed operational action;
there is no new Compose mount, automatic migration or deployment command.

## Transaction contract

The cursor variant requires a non-autocommit, exclusively used READ COMMITTED
connection. A first SELECT establishes the caller's transaction if needed; the
operation then runs inside a nested transaction context (a savepoint). It takes
the existing per-worker authority lock and uses bounded statement/lock timeouts.
Any operation failure rolls its writes back without independently committing the
caller's other work. The caller still owns the outer commit or rollback.

New writes reopen the exact retained authority and register the exact context.
Context ID, invocation request ID and authority/slot/kind scope cannot identify
different contexts. One event can be attributed only once within a context and
its reviewed event limit is enforced. Source rows use the original twelve attempt
columns; returned data is compared against the normalized source document.

An existing source attempt without matching attribution is not backfilled. The
operation rejects it, including after a racing source insertion, and rolls back
any new context. Existing complete attribution can replay only when the entire
record, registered context, retained authority and source attempt still agree.
No old authority is promoted. A source mismatch is a reconciliation failure, not
permission to overwrite evidence or send again.

The public API acknowledges success only after the connection context completes
its commit. A commit error is unconfirmed and has a fixed redacted diagnostic;
exact replay is the recovery operation, never another transport call. The cursor
result has commit_acknowledged=false because its caller has not committed yet.
Acknowledgement is not a promise against every database/storage failure mode.

References: Psycopg 3, Transactions management and Connection.transaction;
PostgreSQL 16, INSERT / ON CONFLICT / RETURNING. The former distinguishes an outer
transaction from nested savepoints; the latter returns only actually inserted
rows for the source conflict check.

## SQL and trust boundaries

Two new append-only tables retain canonical context and complete attribution
bytes, SHA-256, exact projected identities and foreign-key links. Insert checks
reconcile the retained context, authority and source attempt. Capacity and unique
constraints prevent a second attribution of the same attempt/event scope.
UPDATE, DELETE and TRUNCATE on the two new tables are rejected.

The original attempt table is not redesigned. Its source fields are checked at
insertion and on replay; this is not a new global source-immutability guarantee.
Full semantic reconstruction and the no-backfill policy belong to the Python API,
not arbitrary direct SQL callers. Privileged owners can bypass triggers. Database
permissions, operator authentication and immutable schema ownership remain
separate controls.

Recording is historical audit work, not a live execution gate. It can retain a
past attempt after the referenced authority expired or was stopped; future-dated
attempts are rejected against the database clock. Caller-supplied attribution does
not independently prove which runtime worker or configuration sent a request.
Neither complete failure-history coverage nor runtime permission is asserted.
Scope uniqueness in an audit table is not a pre-transport slot claim.

Do not substitute this API for the existing post-transport callback without a
separately reviewed runtime design: malformed attribution is rejected before
writing. A previous external request cannot be undone by this transaction, and
this API neither captures nor reconciles every possible crash between transport
and recording. The preceding observer's write-order rules remain unchanged.

## Executable evidence

The existing authority PostgreSQL fixture invokes the new proof after its own
mutation probes. The proof applies the new SQL and creates all synthetic outbox,
context, attempt and attribution rows inside a forced-rollback savepoint. It
explicitly seeds an evaluation/outbox event using the established fixture helper
and retained portfolio-attribution data, then copies that exact seed and checks
each event exists before recording attempts. It does not assume an earlier test
left an outbox row behind. Schema presence, all synthetic outbox rows and the
seed evaluation are checked after rollback.
The existing surrounding authority fixture and disposable-service teardown are
preserved. The supplied database must be the established disposable CI service,
not production. No persistent schema installation is performed by the proof.

Ten required real-database proof groups cover initial/retry atomic creation,
forced final-write rollback, exact replay, no legacy backfill, scope conflict,
changed-source conflict, capacity, append-only operations, unchanged historical
head and fixture rollback. Unit tests additionally exercise wrong session modes,
source/digest corruption, exceptions/interruption and public commit failures.

Run the focused `tests/unit/test_worker_attempt_attribution_history.py` suite and
normal repository quality/security/readiness checks. `make postgres-contract-check`
reaches the new proof through its unchanged authority-fixture invocation. The
new database proof is transaction/savepoint evidence, not a multi-session commit,
crash-restart or complete retry-operator demonstration. No new concurrent-request
stress proof is claimed. Further durable coverage markers and gated producer
integration remain separate work before worker-health histories are complete.

No producer/default switch, dependency, workflow, scheduler, external transport,
notification, deployment or Terraform apply is changed. Independent review and
explicit acceptance of the final diff remain pending until performed.
