# Committed-state PostgreSQL operator proof

Primary arc42 block: `warehouse`. Goal #172 follows #170/#171.

## Why this proof is separate

The earlier transaction-scoped fixture validates snapshots on its writing cursor
and proves that a separate reader cannot see uncommitted rows. This proof adds the
missing committed-state path: the actual public read-only adapter and complete
operator command observe committed active and stopped authority on new connections.
It also proves an old captured report can still be replayed offline after a stop,
without presenting that replay as current authority or permission to execute.

## Isolation and explicit invocation

```bash
# Only against the disposable local PostgreSQL service, never a production tunnel.
# WAREHOUSE_POSTGRES_DSN is supplied by the operator environment, not an argument.
python -m src.warehouse.notification_worker_preflight_postgres_contract_check \
  --allow-disposable-database
```

The existing `make postgres-contract-check` target invokes this proof with the
acknowledgement and its LOCAL_POSTGRES_DSN through the environment. The Actions
workflow is unchanged. This target already destroys/recreates disposable local
volumes; it is not an application-database validation command.

The proof requires an explicit loopback host and rejects service indirection,
remote host addresses and host lists. That is a mistake-prevention boundary, not
proof that a loopback port is not a production tunnel. The operator must choose
the disposable service. The role needs CREATEDB or equivalent privileges.

The administration connection creates a uniquely generated database from
template0 in autocommit mode. Only that database receives the existing authority
schema and synthetic committed records. The fixture never modifies application
tables in the administration database. Child connections close before DROP; no
FORCE or session-termination command is used. A failed CREATE is never followed
by a DROP. Body failures still attempt exact-name cleanup, and successful cleanup
is verified against pg_database before reporting success. Process termination or
connection loss can leave a fixture database; absence of a success report must
not be interpreted as confirmed cleanup.

## Real checks

The fixture temporarily copies reviewed configurations and uses the existing
planner/authority builders. Its one-minute, zero-jitter plan is already due and
has a bounded five-minute authority window; repository configuration bytes are
unchanged.

The proof verifies the public reader sees the committed active grant, actually
uses READ COMMITTED/READ ONLY, and PostgreSQL rejects a table write with SQLSTATE
25006. It runs the complete live-read command, commits a newer disable transition,
then proves the same old selected grant is blocked. Exact historical replay does
not promote it. Offline replay retains the prior captured result and performs no
database read. Missing-worker observations remain clocked and exactly two intended
authority records exist before the disposable database is dropped.

The test-time cursor audit calls the real reader, inspects real transaction
settings and rolls back the intentionally rejected write using a savepoint. It
does not replace database results with mocks. Separate unit tests use injected
drivers only to prove acknowledgement, name generation and cleanup control flow.

```bash
python -m pytest -q tests/unit/test_worker_preflight_postgres_proof_safety.py
make quality-check
make security-check
make postgres-contract-check
```

References: PostgreSQL 16 CREATE DATABASE and DROP DATABASE require execution
outside a transaction block; DROP cannot run while connected to its target.
<https://www.postgresql.org/docs/16/sql-createdatabase.html>
<https://www.postgresql.org/docs/16/sql-dropdatabase.html>

No application database is contacted during development. CI uses only its
explicit disposable PostgreSQL service. No new schema design, committed worker
switch, dependency, workflow, notification, live provider request, scheduler
activation, cloud deployment or Terraform apply. Explicit engineer acceptance is
still pending, and this proof does not integrate the independent health stack or
make the preflight diagnostic an execution gate.
