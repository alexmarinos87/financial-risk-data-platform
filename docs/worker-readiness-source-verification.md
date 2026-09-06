# Verified readiness sources for managed workers

Primary arc42 block: `warehouse`. Goal #173 under roadmap #76.

## Decision

`notification_worker_readiness_sources.py` verifies the retained records behind
current readiness serving rows. It reuses the accepted worker-plan and readiness
record validators. A source ID, digest string or `allowed` flag alone is not
accepted as the source document.

The builder accepts the exact plan, at most two sorted unique source rows and an
explicit aware observation time. Each row binds destination, selected execution
kind, serving record ID, stored SHA-256, full canonical record and serving flags.
Missing selected kinds produce `missing`, not a fabricated healthy record.

Records are bounded to 262,144 canonical bytes each. The whole retained snapshot
is bounded to 1,048,576 bytes, with detached plan and source documents. The output
validator reconstructs every field rather than checking only the snapshot hash.

## Reconciliation and freshness

The stored digest must match the exact canonical source record. The retained
record ID, destination and execution kind must match the serving row. Status,
execution-ready and current-evidence-match flags must agree. Contradictory or
future-dated records raise `ValidationError`, never a successful report.

Delivery, retry-planning and retry-execution fingerprints, enablement flags and
endpoint environment-variable names are reconciled to the supplied plan. A
valid but differently configured source is `superseded`. Freshness is recomputed
at the observation time against the worker's age limit, including limits below
the serving view's five minutes. The exact maximum age is accepted; one
microsecond beyond it is stale. Stale, blocked or superseded evidence is never
upgraded to allowed simply because its record was internally valid.

Output readiness classifications are `allowed`, `blocked`, `stale`, `superseded`
and `missing`. They classify sources only: this module does not implement the
separate suspension state machine or append authority transitions.

## Boundaries

`all_sources_allowed` means only that all selected readiness sources agree with
the supplied plan at the captured instant. `worker_authority_verified`,
`failure_history_verified` and `runtime_permission_granted` always remain false.
The producer is responsible for selecting actual current rows and proving source
provenance. An internally coherent forged set is not authenticated by its hash.

The next read-only adapter collects serving rows and retained documents in one
statement. That still will not establish atomic authority-plus-health execution,
complete failure history, authenticated approval or a claimed schedule slot.
No source credentials, endpoint values or notification bodies are added here.

Existing authority/preflight and suspension draft stacks remain separate. No
schema, dependency, workflow, committed switch or existing execution API changes.
This module performs no filesystem, database or network operation.

## Validation

```bash
.venv/bin/python -m pytest -q tests/unit/test_worker_readiness_sources.py
make quality-check
make security-check
make readiness-check
```

Tests use real accepted plan/readiness builders, with only in-memory enabled
configuration copies. They cover both kinds, missing/duplicate/swapped sources,
source digest and scope, strict flags, configuration drift, exact freshness,
future recording, bounds, detached replay and rehashed verdict tampering.
CI and self-review do not constitute independent review or engineer acceptance.
