# Retained Managed-Worker Plan Validation

Primary arc42 block: `orchestration`.

## Decision

The existing `validate_notification_worker_plan` entry point delegates to
`notification_worker_plan_validation.py`. A document digest detects content
changes; it does not prove that the content obeys the worker contract. The
validator therefore checks nested semantics before accepting the digest.

All nested objects have exact field sets. Worker and delivery enablement,
review status, execution kinds, canonical blockers, overall status and the
schedule activation action must agree. Execution entries use only the reviewed
initial/retry module identities. Event kinds and environment-variable names are
validated without resolving environment values.

The shared-lock identity must be exact, concurrency must be the integer one,
and the plan must not claim to have acquired the lock. Readiness remains
`allowed`, at most 300 seconds old, with refresh beneath that same lock.
Mandatory suspension conditions cannot be omitted or reordered. Every declared
side effect must be the boolean false, not an integer that compares equal.

## Deterministic Scheduling

The builder and validator share one integer UTC calculation for the first
strictly future interval boundary and deterministic jitter. Integer arithmetic
also handles a fractional instant immediately before the Unix epoch correctly;
truncating a floating-point timestamp toward zero would skip its next boundary.
A next slot beyond Python's datetime range is rejected as a validation error.

Existing modern-date plans retain their model version and calculation. Retained
time strings use the builder's canonical UTC ISO representation. Equivalent
noncanonical spellings must be regenerated rather than silently rewritten.

## Remaining Authority Boundary

A self-consistent digest is not authentication or approval. The compact plan
does not embed complete worker, delivery, retry and destination configurations.
For example, it includes the destination environment-variable name but only the
delivery fingerprint, so a retained endpoint-mismatch reason cannot be fully
rederived from that document alone. Unused per-kind limits and all fingerprint
preimages likewise require the source configurations.

Any later lifecycle-authority constructor must independently rebuild the whole
plan from the supplied reviewed configurations at the original planning instant
and require exact canonical agreement. It must then evaluate review lifetimes
at the proposed authority time. This PR validates retained evidence; it does
not grant execution authority, read current PostgreSQL readiness or activate a
scheduler.

The returned document is a detached JSON snapshot, bounded to 1 MB, so later
mutation of the caller's input containers cannot change the validated result.
This is not a cryptographic signature, an immutable Python object or a durable
append-only record.

## Regression Evidence

```bash
.venv/bin/python -m pytest -q tests/unit/test_notification_worker_plan_validation.py
make quality-check
make security-check
make readiness-check
```

Adversarial tests recompute the plan ID after changing nested fields. They cover
entrypoints, lock flags, enablement, canonical blockers, events, fingerprints,
readiness, bounds, timestamps and side effects. Positive cases preserve
initial-only work when retry execution is disabled. Boundary cases cover
pre-epoch fractions, exact interval edges and datetime overflow.

The planner CLI and optional safe summary writer remain unchanged. Committed
worker, delivery, retry and destination switches remain disabled.
