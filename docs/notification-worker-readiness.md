# Managed-worker readiness assessment

Primary arc42 block: `orchestration`.

## Goal

P4e2c1 adds `assess_worker_readiness` and
`validate_worker_readiness_assessment` in
`src/orchestration/notification_worker_readiness.py`. They combine supplied
observations into deterministic evidence about one selected worker slot. They
perform no I/O and do not create another authority state machine.

The output is one of:

| Assessment | Meaning |
| --- | --- |
| `may_run` | Supplied observations satisfy the assessment contract at this instant. |
| `wait` | The authority is active and otherwise healthy, but its slot is not due. |
| `must_suspend` | Do not run; remain stopped or request appropriate operator action. |

Every output retains `runtime_permission_granted = false`. `must_suspend` is
advice, not a command to append a suspend transition from an illegal source
state. Neither a passing assessment nor a hash authenticates an operator,
claims a slot, sends a notification or modifies a scheduler.

## Required evidence

The input has exact fields: `worker_id`, `evaluated_at`, `observed_at`,
`scheduled_for`, `selected_authority`, `current_authority`, `configuration_plan`,
`destination_review_expires_at`, `readiness` and `health`.

Authority entries use the existing transition model, independently validated
alongside the stricter retained-plan boundary. Either may be null, but missing
authority cannot produce `may_run`. The selected transition must equal the
supplied current head. A newer stop therefore invalidates a supplied older
active grant. State and exclusive expiry use the existing authority evaluator;
elapsed suspension cooldown never creates an implicit resume.

`configuration_plan` must be reconstructed by the observation producer from its
current reviewed configuration snapshot at the selected plan's original
planning instant. Exact comparison detects changed configuration or a different
plan/slot. The destination review expiry must come from that same snapshot.
The assessment checks current review expiry and prevents active authority from
outliving it. Disabled or blocked configuration cannot pass.

`readiness` has zero to two sorted, unique rows, one required for each configured
execution kind. Rows retain a record and decision identity, evaluation time,
review status, allow/block decision, current-evidence match flag and exact
configuration fingerprints. These correspond to the existing notification
readiness review view. Missing rows, stale/future evidence, blocked/superseded
status and configuration mismatches all prevent `may_run`. An allowed status
contradicting its decision or match flag is malformed, not healthy.

`health` likewise requires explicit observations for each configured kind.
Each binds worker/destination, observation time, evidence identity, completeness,
consecutive failures and persistence ambiguity. Missing or incomplete evidence
is not inferred as zero failures. Counts are bounded integers excluding
booleans. The configured failure threshold is inclusive, and any unresolved
persistence ambiguity blocks. Counts are worker/destination/kind observations,
not a newly invented derivation from individual event-attempt rows.

All time text is canonical UTC. The worker's configured readiness age, at most
300 seconds, bounds the overall observation and both per-kind evidence types.
Evidence cannot be newer than its observation time or evaluation time. The
aggregate input is bounded to 1 MB and snapshotted into detached JSON.

## Determinism and validation

Reasons have fixed evaluation order: authority, scope, configuration, observation
age, destination review, slot, then each configured kind's readiness and health
reasons. An otherwise valid early invocation returns only `slot_not_due`, so it
waits rather than needlessly suspending. Early invocation plus a safety failure
still returns `must_suspend`.

The assessment retains the evidence digest, selected/current transition IDs,
plan and slot identities and side-effect flags. Its validator rebuilds the
entire result from supplied evidence and compares canonical bytes. Rehashing a
changed verdict is insufficient to pass.

## What this does not establish

The observations are explicit inputs, not authenticated database reads. This
increment does not prove current-head selection, consistent transaction
snapshot, configuration provenance, health-counter derivation or lock ownership.
A caller capable of fabricating all observations remains inside the trust
boundary. These APIs must not be used directly as a delivery authorization
service.

A later read-only adapter must load the actual current authority, current
readiness and complete health evidence consistently, reconstruct configuration
from reviewed snapshots and preserve source identities. Runtime integration
must then authenticate callers, refresh evidence under the shared delivery lock
and enforce slot claiming/replay and concurrent-stop semantics before transport.
No such database adapter or runtime execution is claimed here.

## Validation

```bash
.venv/bin/python -m pytest -q tests/unit/test_notification_worker_readiness.py
make quality-check
make security-check
make readiness-check
```

Tests use actual planner and reviewed-authority builders with temporary enabled
configuration snapshots. Committed defaults, database schema, workflow,
dependencies and infrastructure are unchanged. A separate no-network rehearsal
provides an operator walkthrough with explicitly synthetic observations.
