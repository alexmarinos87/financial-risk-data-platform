# Append-only notification worker authority history

Primary arc42 block: `warehouse`.

## Goal

P4e2b retains the exact P4e2a authority transition without introducing another
approval model or activating a scheduler:

```text
canonical transition + exact current predecessor
  -> per-worker transaction lock
  -> independently validated chain
  -> append-only history
  -> current active / suspended / expired / disabled evidence
```

Use `src/warehouse/notification_worker_authority_history.py` and
`sql/notification_worker_authority_schema.sql`. An unknown worker is `inactive`
in the reader; the SQL view contains only workers with retained history.

## Recording and concurrency

The recorder validates the entire transition before opening PostgreSQL. Within
one READ COMMITTED transaction it sets bounded lock and statement timeouts,
acquires a per-worker transaction advisory lock, checks exact request replay,
reads the latest sequence, and validates the predecessor and state transition.
A new insert and its server-assigned sequence commit together or roll back.

The insert trigger independently checks the current predecessor, worker and
destination continuity, source state, chronology, stop-plan equality and resume
cooldown. Unique root, predecessor and worker-sequence constraints reject forks
as a second line of defence. The complete JSON, canonical text and SHA-256 are
retained together; SQL checks reconcile their digest and projected identities.
Full plan semantics are enforced by the Python contract, not reimplemented in
SQL. Direct SQL callers must not bypass that application validation boundary.

```text
same request + identical transition -> return original sequence, no insert
same request + changed evidence    -> reject
new request + stale predecessor    -> reject
second root for the same worker    -> reject
```

Historical exact replay remains possible after later transitions. It never
promotes an old grant to current authority. The trigger rejects UPDATE, DELETE
and TRUNCATE. Database owners capable of disabling triggers or changing the
schema remain privileged; these controls are not protection from the owner.

## Time and current state

The database records observation time itself and rejects future-effective
heads. This increment retains effective transitions, not a queue of future
approval changes. Active grants still cover exactly one bounded schedule slot.
Expiry is exclusive; expired grants cannot silently revive.

`risk_platform.current_notification_worker_authority` uses the highest sequence
per worker and evaluates expiry using the database statement clock, not a
long-running transaction's start time. The Python reader uses the same clock.
Suspended and disabled states persist until a valid subsequent transition.

The view and reader deliberately return `runtime_permission_granted = false`.
An active authority is evidence, not a claim that current readiness or receiver
health permits a request. P4e2c must check those conditions independently.

## Operator usage

Validate a locally constructed P4e2a document without opening a database:

```bash
.venv/bin/python -m src.warehouse.notification_worker_authority_history \
  --transition .demo/worker-authority.json
```

Explicitly retain it using an operator-provided DSN outside source control:

```bash
.venv/bin/python -m src.warehouse.notification_worker_authority_history \
  --transition .demo/worker-authority.json --record
```

The recording command reads `WAREHOUSE_POSTGRES_DSN` unless `--dsn` is supplied.
The input is bounded to 1 MiB and rejects duplicate JSON keys, malformed
contracts, symlinks and non-regular files. On POSIX, the opened final component
is checked without following symlinks; the parent directory must be trusted.
Driver failures are sanitized, including uncertain commit acknowledgements;
exact request replay is the recovery mechanism, not blind reinsertion.

## Database proof

The existing `make postgres-contract-check` target invokes the new live fixture.
The byte-pinned Actions workflow is unchanged. The fixture uses real planner
output and real PostgreSQL transactions to exercise lifecycle and sequence,
current active/expired/suspended/disabled states, inactive unknown workers,
historical replay without promotion, changed request rejection, stale heads,
direct SQL fork rejection, mutation rejection, future-head rejection, history
counts, two-session lock exclusion/release, and final fixture rollback.
The lock test is not a production concurrency stress benchmark.

## Boundaries

Docker initialization loads schema 28 on a fresh disposable database. Existing
databases need an explicitly reviewed schema application; initialization mounts
do not automatically migrate an existing data volume. No database deployment
or production migration is performed by this PR workflow.

Reviewers and content hashes are still evidence, not authenticated identities
or signatures. Restricted database writers and authenticated operational
approval remain future runtime responsibilities. Committed worker, destination,
webhook and retry configuration stays disabled. No webhook, provider request,
scheduler mutation, deployment or `terraform apply` is performed. The only
external I/O of the recorder is the explicitly selected PostgreSQL connection.
