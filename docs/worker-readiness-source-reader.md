# Clocked, read-only worker readiness sources

Primary arc42 block: `warehouse`. Goal #174 follows #173 under roadmap #76.

## Decision

`notification_worker_readiness_sources_reader.py` selects the configured initial
and retry readiness sources in one PostgreSQL statement. It retains the serving
row, complete canonical source document, stored digest and a shared database
statement clock, then delegates verification to the source contract from #173.

The exact worker plan is validated before database access. Destination and
selected kinds are SQL parameters, never interpolated identifiers or filters.
A left join from the selected kinds preserves missing views and missing records.
The query returns at most three rows: a maximum of two kinds plus one duplicate
sentinel. Wrong cardinality or duplicate kind fails instead of being discarded.

A serving row referencing a missing retained document is corruption, not a
healthy source or an ordinary absent decision. The SELECT withholds a JSONB
source larger than 262,144 PostgreSQL text bytes and returns its size instead;
the reader rejects that row. This bound is conservative relative to compact
canonical JSON, whose independent bound is enforced by the source verifier.
The complete returned snapshot is bounded to 1,048,576 canonical bytes.

## Transaction and clock

The public reader opens a new `READ COMMITTED, READ ONLY` transaction with a
five-second lock timeout and ten-second statement timeout. The cursor helper
performs one source SELECT; its caller owns transaction configuration and
lifecycle. Neither path acquires an advisory lock, refreshes a decision, writes
a record, reads worker authority or invokes transport.

Both kinds come from the same statement snapshot and observation clock. The
verifier recomputes freshness against that clock and the worker's maximum age,
never upgrading a more restrictive serving status. The public function returns
only after the connection context exits successfully. Query, decoding,
connection and context-exit failures produce a fixed diagnostic without DSNs,
source contents or driver diagnostics.

PostgreSQL READ COMMITTED gives each statement its own snapshot; READ ONLY
rejects ordinary table modifications, not every possible disk write. Reference:
https://www.postgresql.org/docs/16/sql-set-transaction.html

## Deliberate boundaries

This is a readiness-source capture, not an atomic capture of worker authority,
configuration files and all health history. Callers must not combine independently
captured snapshots and label them a runtime lock or execution permission. A
newer stop or source change can occur immediately after the SELECT.

`worker_authority_verified`, `failure_history_verified` and
`runtime_permission_granted` remain false. No healthy failure count is inferred
from a missing or unbound history. Source provenance and authenticated operator
approval remain separate. There is no CLI, scheduling or execution option.

## Validation

```bash
.venv/bin/python -m pytest -q tests/unit/test_worker_readiness_sources_reader.py
make quality-check
make security-check
make readiness-check
```

Tests use actual accepted plan/readiness contracts with fake cursors to verify
SQL parameters, bounded fetch, shared clocks, missing/duplicate/broken joins,
oversized and malformed records, pre-I/O input rejection and transaction exits.
The following independently reviewable PR adds real PostgreSQL query proof.
No schema, dependency, workflow, configuration default or existing API changes.
Exact-head CI and self-review are not independent review or final acceptance.
