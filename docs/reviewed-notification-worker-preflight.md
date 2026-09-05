# Reviewed-configuration worker preflight

Primary arc42 block: `orchestration`. Goal #165 follows #164 under roadmap #76.

## Decision

`src/orchestration/reviewed_notification_worker_preflight.py` composes the clocked
ledger snapshot with the existing reviewed-plan binder and authority preflight.
It does not implement a second health evaluator, lifecycle model or scheduler.

`build_reviewed_worker_preflight` requires a validated snapshot and an explicit
selected transition ID and schedule slot. It obtains the retained plan from the
snapshot, reconstructs it at the original planning instant from the supplied
reviewed worker/delivery/destination files, and requires exact agreement through
`_bind_reviewed_plan`. Review expiry is obtained from that same destination
object, not supplied independently by the caller.

A changed worker switch, delivery policy, destination review or fingerprint
raises ValidationError. A missing authority produces a blocked preflight and
skips configuration I/O entirely. Otherwise the existing preflight determines
whether the exact captured head/slot is eligible for separate health review,
waiting for the slot, or blocked. A newer stop cannot be bypassed by selecting
an old active transition ID. Expiry remains exclusive; cooldown never resumes.

## Retained evidence

The 1 MB bounded envelope contains the exact authority snapshot, derived
preflight input and result. All values are detached JSON. Validation reopens the
supplied reviewed files and rebuilds the complete envelope. A recomputed bundle
ID cannot hide changed outcomes, evidence times, review expiry or permission.
No configuration paths, DSN, environment values or notification bodies are
included. The existing secret-free plan retains identifiers/fingerprints only.

Evaluation is explicitly scoped to `captured_database_instant`: evaluated and
observed time are the snapshot's database time. Reopening a retained bundle does
not refresh the database or establish current freshness. Passing this preflight
still means `readiness_evaluated = false` and `runtime_permission_granted = false`.

## Trust and trade-offs

Reviewed files must be immutable snapshots in trusted directories. Their
contents are checked, not their provenance, authenticated reviewer identity or
ongoing approval. Filesystem and PostgreSQL state are not an atomic cross-store
snapshot. A concurrent stop or configuration replacement requires a new read.
A forged internally consistent snapshot cannot be authenticated by its hash.
Runtime integration must later reacquire current evidence under the shared
lock, check health, authenticate the caller and enforce slot claim/replay rules.

This change deliberately does not import the unaccepted suspension draft stack.
It is independently reviewable over the first snapshot candidate, not a shortcut
to accepting or activating any other work.

## Validation

```bash
.venv/bin/python -m pytest -q tests/unit/test_reviewed_notification_worker_preflight.py
make quality-check
make security-check
make readiness-check
```

Tests use real existing plan/authority builders with temporary reviewed files
and injected database rows. They cover disabled or changed sources, missing
heads/files, symlinks, newer stops, slot/expiry boundaries, detached replay and
rehashed tampering. No application database or network is accessed.

No schema, workflow, dependency, configuration-default or infrastructure change.
No authority write, notification, shared lock, scheduler mutation, deployment or
Terraform apply. Exact-head CI is evidence; explicit final-diff acceptance is
still required. After the predecessor is accepted and squash-merged, reconstruct
this delta on accepted main and rerun checks before accepting this candidate.
