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
source through the preceding semantic and current-review reconciliation layers.

## Clock and transaction contract

A single statement supplies both data and observation time. Existing review views
use CURRENT_TIMESTAMP, which is transaction-start time. The reader therefore
requires transaction and statement clocks to agree and verifies read-only READ
COMMITTED mode. Its dedicated autocommit SELECT has a fresh transaction clock;
a cursor borrowed from an older explicit transaction is rejected, not silently
reported as current. The cursor variant never changes caller transaction settings.

PostgreSQL documents transaction_timestamp and statement_timestamp under Date/Time
Functions, section Current Date/Time; they agree for the first command of a
transaction. Psycopg autocommit avoids implicitly keeping a transaction open across
unrelated commands. This adapter uses only session-local settings and SELECT.

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

The existing `make postgres-contract-check` runs the new source-reader proof after
its established readiness and authority fixtures. It uses actual retained records
and a replaced activation review to prove supersession, exact selected IDs,
unknown-worker blocking, no read-side authority writes, replacement by a newer stop,
and rejection of an older transaction clock. Unit tests additionally exercise
matching ready sources, malformed rows, bounds, wrong modes, digest tampering,
parameterization, sanitized failure and connection cleanup.

The proof explicitly commits two synthetic authority records so dedicated read-only
sessions can see them. They remain only in the disposable contract database until
normal CI teardown. This is not a rollback-only fixture and must not run against
production. The test target already creates and tears down that disposable database.
No schema, application configuration, workflow or dependency change is required.

The reader performs PostgreSQL I/O only. No notification, provider request, delivery
lock, scheduler activation, deployment or `terraform apply` occurs. Source validation
and passing CI do not authorize runtime execution or replace exact-diff acceptance.
