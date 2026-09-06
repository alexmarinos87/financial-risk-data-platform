# Complete worker-fixture evidence and failure cleanup

Primary arc42 block: `warehouse`. Goal #189 follows routing goal #188 / PR #191.

## Decision

A fixture report must not succeed merely because its exercise function returned
a dictionary. `run_contract_check` now requires exactly eight operator proof
groups, each literally true. Missing groups, extra groups, false values and
truthy non-booleans are rejected. The dictionary is detached before the protected
fixture lifetime ends: incomplete evidence still runs owned-database cleanup.

Only after that database context exits successfully is its cleanup flag added.
Two additional failure exercises and a routing-default proof are required before
the final exact twelve-group result can be emitted. The fixture result version
is `worker-preflight-operator-postgres-contract-v2`; the operator report and
all authority/readiness contracts are unchanged.

## Real PostgreSQL failure exercises

The existing disposable Make target invokes the stronger check automatically.
After the original positive-path database is cleaned up, two more internally
generated fixture databases are created sequentially. One exercise raises the
private sentinel immediately after entering its context. The other creates a
marker table, commits a row, reopens a separate connection and verifies that
committed row belongs to the generated database, then raises the sentinel.

Only that specific injected exception counts as the expected failure. A CREATE,
marker, connection, DROP or catalog-verification error fails the check instead.
Each generated name is verified absent through the administration database's
catalog after its context exits. No marker table is created in the administration
database; only the generated child is used for marker writes.

During these standalone single-threaded probes, synthetic invalid PGHOSTADDR and
PGPORT defaults plus conflicting PGHOST/PGDATABASE/PGUSER values are temporarily
injected. The explicit target from #191 must still succeed. Invalid address text
cannot redirect a regressed connection to a remote host. The scoped test patch
restores the environment even on exceptions; the routing implementation itself
does not mutate it. This is test instrumentation, not an application pattern for
changing environment variables in a concurrent service.

The three additional required proof groups are:

- `early_failure_fixture_dropped`;
- `committed_failure_fixture_dropped`;
- `explicit_target_overrides_environment`.

## Validation and limits

```bash
.venv/bin/python -m pytest -q tests/unit/test_worker_fixture_failure_evidence.py
make quality-check
make security-check
make postgres-contract-check
```

The final command is the existing **disposable-only**, database-recreating test
target. New unit challenges use injected drivers to verify exception and cleanup
control flow; only the real PostgreSQL CI log establishes actual commit/drop
execution. A simulated catalog is not presented as database evidence.

Successful failure injection does not prove cleanup after process termination,
network loss or server crash. Such failures may leave a generated database and
must not produce a success report. There is no forced DROP or session termination,
no new role/grant/migration, and no application database access. Existing schema,
Make invocation, pinned workflow, dependency and activation defaults are unchanged.
The original eight operator assertions and their owned-cleanup proof are retained.

A complete proof manifest is not authenticated evidence or independent approval.
The candidate remains draft pending exact-diff engineer acceptance and ordered
integration of its predecessors. No notification, schedule or cloud deployment
is activated by this check.
