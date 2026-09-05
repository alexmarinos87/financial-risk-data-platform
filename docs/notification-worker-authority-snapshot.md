# Clocked current worker authority snapshots

Primary arc42 block: `warehouse`. Goal #164 under roadmap #76.

## Decision

`src/warehouse/notification_worker_authority_snapshot.py` adds a read-only
adapter over the existing worker authority ledger. It does not replace the
transition model, sequence recorder or existing reader APIs.

The previous reader evaluated expiry on the database clock but did not return
that observation time. The new reader selects the highest worker sequence and
`statement_timestamp()` together. A left lateral join returns one row even when
no authority exists, so an empty-head observation is clocked rather than inferred.
The worker identifier is validated before I/O and passed as a SQL parameter.

`read_worker_authority_snapshot` opens a new `READ COMMITTED, READ ONLY`
transaction, sets bounded lock/statement timeouts and returns only after the
connection context exits successfully. Its cursor-level counterpart performs
only timeout configuration and one SELECT; the caller owns the transaction.
Neither path acquires an advisory lock or writes ledger records.

## Evidence and checks

The snapshot retains the complete canonical transition, its digest, sequence,
recording time, database observation time and derived authority state. Unknown
workers retain null row metadata and an inactive state. Scope, digest, strict
positive sequence, effective/recorded/observed chronology and exclusive expiry
are checked using the existing authority contract.

`validate_worker_authority_snapshot` rebuilds the entire bounded document and
returns a detached JSON value. Recomputing a snapshot ID cannot hide a changed
state, contradictory digest, invalid metadata or runtime permission. A valid
hash is not authentication: fabricated self-consistent source records are still
inside the caller's trust boundary.

## Limits

A single statement is a snapshot of committed state at its database observation,
not a lock preventing a later stop. A retained file is not proof the head remains
current. Database and configuration files do not share an atomic snapshot.
Health evidence, independent reviewer authentication, exact slot claims and
under-lock revalidation remain separate gates. Runtime permission remains false.

PostgreSQL READ COMMITTED takes a snapshot for each statement. READ ONLY prevents
ordinary table modifications, not every possible disk write. See PostgreSQL's
SET TRANSACTION reference: <https://www.postgresql.org/docs/16/sql-set-transaction.html>.
No claim of a serializable multi-source snapshot is made.

## Validation and integration

```bash
.venv/bin/python -m pytest -q tests/unit/test_notification_worker_authority_snapshot.py
make quality-check
make security-check
make readiness-check
```

Unit tests use fake cursors and connections with the real authority validators.
They prove SQL/parameter/transaction intent, not PostgreSQL execution of the new
SELECT. The unchanged PostgreSQL contract job remains required; a dedicated real
snapshot-query proof is a separate acceptance item before operational use.

No schema, workflow, dependency or committed configuration change. No application
database access during development, notification, scheduler activation,
deployment or Terraform apply. Exact-head CI and self-review are evidence, not
explicit engineer acceptance. The following increments bind reviewed
configuration and expose validation-first operator diagnostics.
