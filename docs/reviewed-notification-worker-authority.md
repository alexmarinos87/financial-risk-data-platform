# Configuration-bound worker authority evidence

Primary arc42 block: `orchestration`.

## Goal

The existing [single-slot authority contract](notification-worker-authority.md)
provides the pure activate/suspend/resume/disable state machine. This increment
adds read-only configuration binding around that contract rather than a second
state machine, a new transition format or an execution service.

The public entry points in
`src/orchestration/reviewed_notification_worker_authority.py` are:

- `build_reviewed_worker_authority_transition`: rebuild the exact plan from
  supplied reviewed configuration snapshots, then construct lifecycle evidence.
- `validate_reviewed_worker_authority_transition`: validate retained evidence,
  its supplied predecessor and the same reviewed configuration binding.

Both return the existing
`portfolio-risk-notification-worker-authority-transition-v1` document. Existing
transition IDs, schema consumers and pure state-machine APIs are unchanged.

## The three distinct checks

A digest binds bytes. Strict retained-plan validation checks internal contract
semantics. Neither proves that the complete source configurations actually
produce that plan. This layer performs the third check: load the selected
worker, webhook/retry policies and destination through their existing bounded
regular-file loaders, rebuild at the retained planning instant, and require
exact canonical agreement.

Each configuration document is loaded once per construction. The parsed
destination used for rebuilding is also used for the review-lifetime check;
there is no second destination-file read that could silently change the review
being assessed within that construction.

This detects internally coherent but unreviewed alternatives, including changed
batch limits with unchanged fingerprints and removed endpoint-mismatch evidence
followed by a recomputed plan ID. It never resolves the endpoint environment
variable or retains its value.

## Review lifetime and stop actions

The underlying state machine still enforces independent reviewer identifiers,
legal transitions, exact predecessor, strict chronology, suspension cooldown,
one schedule slot and its execution-timeout bound.

For activation and resume, this layer additionally requires that the supplied
destination review is active at the transition's effective time. Authority
expiry may equal the destination review's exclusive expiry, but cannot exceed
it. A plan built while a review was active cannot grant a later slot beyond
that approval window.

Suspend and disable do not need a new active destination review. They bind to
the exact previously governed plan using its historical configuration snapshot.
In particular, an expired approval does not prevent explicit disablement. The
existing pure stop-action API also remains available; this wrapper is not a
runtime kill switch.

## Operational trust boundary

The configuration paths must identify immutable, reviewed snapshots in trusted
operator-owned directories. The caller is responsible for snapshot provenance
and consistent version selection across the three files. File checks and
fingerprints do not authenticate a reviewer or establish filesystem isolation
against an adversary able to replace parent directories.

The predecessor is supplied evidence, not a query of the current database head.
Persistence must still reject stale or conflicting predecessors. A historical
configuration match is not proof of present runtime configuration, readiness or
permission to send. Future execution must use authenticated callers, retained
current authority and fresh readiness under the shared delivery lock.

Consumers needing configuration-bound evidence should use these entry points,
not treat the lower-level pure serializer as configuration authentication. No
existing scheduler or persistence consumer is switched on by this increment.

## Validation

```bash
.venv/bin/python -m pytest -q tests/unit/test_reviewed_notification_worker_authority.py
make quality-check
make security-check
make readiness-check
```

Tests cover the complete lifecycle, exact retained predecessor, changed worker,
webhook, retry and destination configurations, rehashed internal alternatives,
removed mismatch evidence, effective-time review expiry, exact expiry equality,
disablement after expiry, symlink rejection and unchanged disabled defaults.
Only temporary test snapshots enable configurations. No committed switch,
workflow, database schema or infrastructure definition changes.

P4e2b persistence and P4e2c runtime-readiness integration remain separate work.
No database connection, lock acquisition, network request, webhook delivery,
schedule activation, deployment or `terraform apply` is performed here.
