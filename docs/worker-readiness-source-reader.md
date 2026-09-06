# Read-only PostgreSQL worker readiness sources

Primary arc42 block: `warehouse`. Third readiness-source adapter layer.

`read_current_worker_readiness(dsn=..., worker_id=...)` opens a dedicated
connection in autocommit mode, sets read-only READ COMMITTED defaults and a
10-second statement timeout, then selects all data in one parameterized statement.
The DSN must be explicitly supplied; no credential value is retained or printed.

The statement selects the actual highest-sequence worker authority, expands only
its selected execution kinds, and joins the current readiness view to each exact
retained readiness record. Record JSON is size-checked server-side before transfer.
A third row is retained as an ambiguity sentinel and rejected instead of silently
truncating an unexpected inventory. The aggregate Python boundary remains 1 MiB.
The reader independently verifies authority identity/digest and reopens each
source through semantic and current-review reconciliation.

## Clock and transaction contract

The original timestamp-equality guard rejected a fresh parameterized query in CI
run 463 before the unknown-worker scenario could complete. Transaction freshness
is now checked from connection state **before** the data statement: the connection
must be open, explicitly autocommit, IDLE, outside pipeline mode, and exclusively
owned for the operation. Existing, failed, active and unknown transactions are
rejected without sending SQL. Neither a clock tolerance nor equal timestamps can
bypass this check. The cursor variant never changes caller transaction settings.

The SELECT verifies read-only READ COMMITTED mode and returns both timestamps.
`observed_at` uses the new transaction's timestamp, matching CURRENT_TIMESTAMP in
the existing views. Statement time is parsed but is not compared for equality;
protocol preparation may produce distinct timestamps in a fresh operation. The
owning function closes its dedicated connection on both success and failure.
Concurrent use of a supplied cursor/connection is outside this API's contract.

References: Psycopg, "Transactions management" and `ConnectionInfo.transaction_status`;
PostgreSQL, "Message Flow", extended query protocol and timestamp behaviour.

## Meaning and limitations

An unknown worker returns `authority_missing`, not health. Missing records block;
newer stops replace old active heads. A `ready_sources` result still does not verify
current reviewed configuration, due-slot/replay ownership, health-history
completeness, operator authentication or live under-lock readiness. Failure-history
verification and runtime permission always remain false. The inner pure snapshot
retains its own unverified-provenance flags; the outer result separately records
that actual single-statement database source selection was performed.

The adapter trusts the configured database and its schema owners. A compromised
owner or fabricated cursor remains inside that trust boundary. It reads current
view projections, not cryptographic approvals. No scheduler consumes this output
and no unaccepted suspension or preflight-diagnostics code is imported.

## Executable evidence

`make postgres-contract-check` invokes the source-reader proof after its established
readiness and authority fixtures. It reopens actual retained records, detects
superseded activation reviews, reconciles exact selected IDs, checks unknown-worker
blocking and repeated read-side immutability, observes a newer stop, and rejects
an existing explicit transaction. Unit tests also cover matching ready sources,
malformed rows, bounds, wrong modes, tampered digests and connection cleanup.

The session regression tests cover IDLE/autocommit, all four non-IDLE states,
pipeline mode, missing state evidence and strict booleans. Reader tests check that
unequal clocks in an idle session are accepted and that equal clocks cannot hide
an existing transaction. All original source-integrity assertions remain enabled.

The proof commits two synthetic authority records so dedicated read-only sessions
can see them. They remain only in the disposable contract database until CI
teardown. This is not a rollback-only fixture and must not run against production.
No schema, application configuration, workflow or dependency is changed.

The application reader performs PostgreSQL reads only. No notification, provider
request, delivery lock, scheduler activation, deployment or Terraform apply occurs.
Passing CI does not authorize execution or replace exact-diff human acceptance.
