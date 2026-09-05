# Atomic worker suspension history

Primary arc42 block: `warehouse`. P4e2c, third dependency-ordered layer.

`record_worker_suspension` explicitly persists the exact bundle from the preceding
orchestration layer. It commits an existing-model authority stop and its full
suspension decision/observation in one PostgreSQL transaction. Failure of either
write rolls both back. There is no scheduler or transport entrypoint.

## Transaction and replay contract

The cursor variant requires one caller-owned READ COMMITTED transaction. It
validates before database access, acquires the existing per-worker transaction
lock, checks retained decision replay, and reopens the current authority. New
writes require the exact active predecessor, an evaluation no later than the
database clock, and age within the plan's readiness limit. The established
recorder assigns the authority sequence and revalidates chain identity.

Both documents must be newly created together. A legacy stop without its decision
is rejected instead of silently backfilled. Exact historical replay validates the
retained bundle and both authority documents, returns the original sequence, and
does not change current authority or refresh evidence age. Changed decision reuse
is rejected. Any cursor-variant exception requires rolling back the caller's
transaction; the connection-owning API enforces this automatically. Commit failure
is reported as unconfirmed, not success; exact replay is the recovery mechanism.

## SQL boundary

`sql/notification_worker_suspension_schema.sql` adds append-only evidence with
canonical bytes, SHA-256, projected identities and composite foreign keys. An
insert trigger reconciles both retained authority documents and requires the
new stop to be current. UPDATE, DELETE and TRUNCATE are rejected. The serving view
joins evidence through the exact current transition, never an older suspension.
A manual suspension may legitimately have no automated decision; the missing
classification is an evidence distinction, not retroactive invalidation.

Python remains responsible for full semantic reconstruction and no-backfill
policy. Direct SQL callers must not bypass that boundary. Privileged owners can
disable triggers; these controls are not authentication or protection from the
owner. Health observations are retained adapter evidence, not independent proof
that this recorder re-read the underlying readiness and failure-source documents.
Authenticated callers and a reviewed live observation adapter remain required
before any operational scheduling integration.

## Validation and operation

Disposable PostgreSQL proof extends the existing worker-authority fixture, reached
through the unchanged `make postgres-contract-check` target. It exercises real
second-write rollback, no-backfill, atomic creation, exact historical replay,
conflicting decision reuse, stale-head rejection, current serving state and
append-only mutation rejection. The fixture rolls all data back. The existing
TRUNCATE rejection probe names both related tables so the new foreign key does
not hide the append-only trigger behind an earlier PostgreSQL dependency error.

Fresh Docker initialization mounts schema 29 after authority schema 28. Existing
databases require separately reviewed schema application; initialization is not
a migration of existing volumes. No production migration is performed here.

Only an explicit call to the recording API connects to PostgreSQL. No CLI or
scheduler is added. Committed worker/destination/delivery switches stay disabled.
No provider request, webhook delivery, live scheduler activation, deployment or
`terraform apply` occurs. `runtime_permission_granted` remains false in all results.
