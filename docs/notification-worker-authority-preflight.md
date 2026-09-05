# Current-authority and exact-slot preflight

Primary arc42 block: `orchestration`.

## Decision

`src/orchestration/notification_worker_authority_preflight.py` provides
`evaluate_worker_authority_preflight` and `validate_worker_authority_preflight`.
This is a prerequisite to health evaluation, not a second health or suspension
engine. Concurrent PRs #158 and #159 own those separate contracts; this module
neither imports their unaccepted code nor changes their state machine.

| Outcome | Meaning |
| --- | --- |
| `eligible_for_health_review` | Supplied current authority, configuration and due slot agree. |
| `wait` | All preflight checks pass except the slot is not yet due. |
| `blocked` | Authority, configuration, observation or slot checks fail. |

Every outcome keeps `readiness_evaluated` and `runtime_permission_granted` false.
A passing preflight is not evidence that delivery health is acceptable, a slot
has been claimed, an operator is authenticated, or a shared lock is held.

## Evidence contract

The exact input fields are:

```text
worker_id
selected_transition_id
evaluated_at
observed_at
scheduled_for
current_authority
configuration_plan
destination_review_expires_at
```

The observation producer must select the actual latest retained authority using
the existing ledger reader. `current_authority` contains its complete canonical
transition, not a caller's preferred historical grant. The selected transition
ID must match it. If the current head is a newer stop, selecting an old active
ID cannot pass. The existing authority evaluator determines active, inactive,
expired, suspended or disabled state, with exclusive expiry. Cooldown expiry
never creates a resume.

The producer must rebuild `configuration_plan` from current reviewed immutable
configuration snapshots at the retained plan's original planning instant. Exact
canonical agreement is required. Missing configuration is unverified; disabled
or blocked plans cannot pass. The destination review expiry must come from the
same snapshot and must cover the authority's complete exclusive grant interval.

The selected schedule slot must equal the governed slot. Only an otherwise valid
active authority awaiting its slot returns `wait`. Observations cannot be in the
future, cannot precede the current authority's effective time, and must be no
older than the worker's configured readiness age (at most 300 seconds). An exact
age boundary is accepted. All timestamp text uses canonical UTC representation.

Aggregate input is bounded to 1 MB, snapshotted as detached JSON and checked with
the strict retained-plan validator. Malformed authority, types or additional
fields raise `ValidationError`; absence is represented by a null current
authority or configuration plan and produces a blocked result.

Reasons follow fixed authority, configuration, observation, review and slot
order. Output binds the evidence digest, selected/current identities and slot.
Validation rebuilds the whole result from the supplied evidence: recomputing a
hash after removing blockers or granting permission is insufficient.

## Trust boundary and next composition

This pure function performs no I/O. It verifies supplied evidence agreement,
not actual database provenance, a consistent transaction snapshot, configuration
approval or caller identity. A caller able to fabricate every input remains
inside the trust boundary. Hashes and identifier strings are not signatures.

A later adapter must acquire actual current authority and health observations
consistently, reconstruct configuration from reviewed snapshots and bind both
the preflight and the separately accepted health decision to the same authority
and evaluation instant. Runtime integration must authenticate callers, refresh
under the shared delivery lock and enforce slot-claim/replay and concurrent-stop
semantics. This PR does not implement or claim that adapter or runtime.

The broad prototype #160 was closed unmerged after the concurrent health stack
was discovered. Its duplicate per-kind evaluator is intentionally not included
here. Existing schemas, workflows, configuration defaults, dependencies and
execution paths are unchanged.

## Validation

```bash
.venv/bin/python -m pytest -q tests/unit/test_notification_worker_authority_preflight.py
make quality-check
make security-check
make readiness-check
```

Tests use actual planner and reviewed-authority builders with temporary enabled
snapshots. They cover due/early slots, newer stops, missing sources, selected-ID
and configuration mismatches, review/authority expiry, freshness boundaries,
rehashed verdicts, malformed evidence and detached results. A separate rehearsal
will make these checks inspectable without network or database access.
