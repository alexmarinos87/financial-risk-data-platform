# Explicit routing for the disposable worker fixture

Primary arc42 block: `warehouse`. Goal #188 follows the operator proof in #181.

## Decision

The fixture checked the DSN's host before connection, but an unspecified libpq
`hostaddr` can come from `PGHOSTADDR`. When both host and hostaddr are present,
hostaddr chooses the network target. Checking `host=localhost` alone was therefore
not sufficient to enforce the intended local fixture route.

`_fixture_connection_overrides` now requires an explicit loopback host, one port
in 1–65535, administration database and user. Database/user names use a deliberately
narrow ASCII identifier subset, not nested connection strings. `localhost` pins
IPv4 loopback unless a permitted explicit loopback hostaddr selects IPv6. Numeric
host and hostaddr must agree. No DNS lookup is needed to choose the network target.

`_pinned_fixture_dsn` merges these values with Psycopg's conninfo utility. The same
explicit target is used by the administration connection and every generated
child DSN. Startup connect, statement and lock timeouts are set before the first
SQL. Credential and TLS options are preserved, never echoed or copied to reports.
Neither helper mutates the environment. Service indirection in DSN parameters or
nonempty PGSERVICE, PGSERVICEFILE or PGSYSCONFDIR variables is rejected rather than
silently reading additional connection configuration.

The existing strict acknowledgement, generated database name, CREATE-success
tracking and bounded DROP of only that owned name are unchanged. No application
reader, CLI selection, notification or authority semantics change.

## Validation

```bash
.venv/bin/python -m pytest -q tests/unit/test_worker_fixture_connection_routing.py
.venv/bin/python -m pytest -q tests/unit/test_worker_preflight_postgres_proof_safety.py
make quality-check
make security-check
make postgres-contract-check
```

The last command is the existing destructive **disposable local database** test
target, not an application-database check. Do not run it against valuable data.
Unit tests use synthetic credentials and injected drivers. Real conninfo parsing
and the existing PostgreSQL operator proof run in the complete CI environment.
The successor adds real failure-after-commit cleanup and routing-default proofs.

## Limits and acceptance

Numeric loopback is mistake prevention, not authentication of a disposable
service: it cannot detect a production tunnel. The explicit acknowledgement and
operator's selection of a disposable service remain required. This standalone
fixture assumes trusted code and a stable process environment, not an adversary
modifying process state concurrently. Authentication may still use supplied TLS
or password-file configuration. This is not a sandbox against malicious local
configuration or a least-privilege database role implementation.

No workflow, schema source, dependency, committed activation default or Make
invocation change. The candidate remains pending exact-diff engineer acceptance;
passing tests are evidence, not permission to merge or activate anything.

## Primary references

- PostgreSQL 16 connection parameters (`host`, `hostaddr`, service and defaults):
  <https://www.postgresql.org/docs/16/libpq-connect.html>
- PostgreSQL 16 environment defaults:
  <https://www.postgresql.org/docs/16/libpq-envars.html>
- Psycopg conninfo parsing and keyword overrides:
  <https://www.psycopg.org/psycopg3/docs/api/conninfo.html>
