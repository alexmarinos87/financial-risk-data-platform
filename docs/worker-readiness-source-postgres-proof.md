# PostgreSQL proof for verified worker readiness sources

Primary arc42 block: `warehouse`. Goal #175 follows #173 and #174.

## Real query integration

The accepted `notification_execution_readiness_postgres_contract_check.py`
fixture now invokes `notification_worker_readiness_sources_postgres_contract.py`
before and after its existing destination-supersession step. The existing
`make postgres-contract-check` target already executes this fixture. No pinned
Actions workflow, Make target, database schema or application setting changes.

The extension constructs real worker plans and readiness records using the
accepted builders, current fixture destination evidence and actual retry-policy
fingerprints. It enables only in-memory dataclass copies. The ordinary unit
suite checks these inputs, projection logic and the live-fixture wiring; the
PostgreSQL job is still required to establish actual database behavior.

## Proof groups

The extension checks both selected kinds and exact canonical source records;
initial-only isolation; stricter worker freshness; clocked missing sources;
separate-public-reader isolation from uncommitted source records; blocked retry
classification; corrupt digest rejection; SQL-level withholding and rejection of
oversized JSON; rollback with original heads preserved; PostgreSQL READ ONLY
write rejection; and supersession of both sources by newer destination evidence.

The source capture and public reader execute their real SELECT and canonical
validators. Source records for this extension are inserted only in one fixture
transaction. Invalid digest/oversize probes are intentional direct-SQL fixtures
inside savepoints, not an example of approved application recording. They never
commit. A `finally` block rolls back the extension's records, and a subsequent
query checks that their unique request prefix no longer exists. The original
serving record IDs must still be unchanged.

The surrounding pre-existing fixture commits its own disposable records and
checks append-only controls. This extension does not claim to roll back that
older fixture. CI tears down its disposable database service after the contract
job. No application database is accessed during this development workflow.

## Why failure history is still unverified

The legacy `write_delivery_attempt` path stores event, channel, attempt number,
outcome and response metadata. Its attempt rows do not carry destination ID,
worker authority ID or execution kind. Neither an event ID nor an attempt
number safely establishes which managed worker and initial/retry path owns an
attempt. Missing persistence after an external request is also not evidence of
zero failures.

Therefore the readiness-source snapshot keeps `failure_history_verified` false.
A future adapter needs an explicit, reviewed attribution contract and complete
chronological failure/ambiguity evidence before computing per-kind counts. It
must not guess those counts from unbound rows or silently treat missing history
as healthy. That is separate from the already proposed suspension policy.

## Usage and limits

Use only a disposable local/CI database prepared by the normal repository
validation workflow:

```bash
make postgres-contract-check
```

Inspect the readiness fixture's `worker_readiness_source_proofs` JSON object;
each proof must be true. No proof result is claimed merely because SQL parsed or
unit tests used a fake cursor. The proof helper has no standalone CLI and
introduces no operational recording, scheduler, webhook or cloud activation.

Source authenticity, current worker authority, immutable configuration
provenance, complete health and under-lock execution remain separate concerns.
No runtime permission is granted. Successful CI and self-review remain evidence;
independent review and explicit final-diff acceptance are still required.
