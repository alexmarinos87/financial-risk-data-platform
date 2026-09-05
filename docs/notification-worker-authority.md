# Single-slot notification worker authority

Primary arc42 block: `orchestration`.

## Goal and boundary

P4e2a adds a pure transition contract over the existing P4e1 worker plan:

```text
validated plan + explicit operator request + exact predecessor
  -> activate | suspend | resume | disable
  -> deterministic, credential-free transition evidence
  -> no scheduler mutation and no delivery
```

The public API is in
`src/orchestration/notification_worker_authority_contract.py`.
Its model is `portfolio-risk-notification-worker-authority-transition-v1`.
This is an evidence contract, not an authenticated approval service. Reviewer
names and SHA-256 identifiers do not authenticate a person or sign a document.
A future runtime must independently authenticate the operator, load current
retained authority, and refresh notification readiness under the delivery lock.
The existing planner remains a planning interface; authority consumers use the
stricter `validate_authority_plan` boundary introduced here.

## Why validation precedes activation

A content hash is not a policy check. Recomputing a hash after changing
`max_concurrency`, an entrypoint, a readiness limit, suspension conditions, or
schedule fields must not make an unsafe plan acceptable. The authority boundary
checks exact nested fields, bounded integers excluding booleans, known
entrypoints, the shared lock identity, required suspension conditions, dependency
blockers, and the deterministic UTC slot and jitter independently of the hash.

The plan does not embed complete delivery configuration or endpoint values.
Its configuration fingerprints remain references to evidence, not proof that
that evidence is still current. Endpoint mismatch evidence is preserved; live
configuration and destination review must be revalidated before execution.

## State machine

| Action | Permitted source states | Result |
| --- | --- | --- |
| activate | inactive, disabled, expired | active |
| suspend | active | suspended |
| resume | suspended | active |
| disable | active, suspended, expired | disabled |

Only initial activation starts a chain. Every subsequent transition references
the exact previous transition ID, advances effective time strictly, and retains
the worker and destination identities. A storage consumer must reconcile that
predecessor against the current retained head, not merely validate its hash.

Activation and resume require a `would_schedule` plan, sorted unique reviewer
identities independent of the requesting operator, and an explicit expiry.
Names differing only in case cannot bypass reviewer separation. Each grant
covers one exact schedule slot: effective time is no later than the slot, and
expiry is after the slot but no later than its bounded execution-timeout end.
Expiry is exclusive. This does not grant permission for an indefinite schedule.

Suspension and disablement reference the exact previously governed plan and
need no new review or grant expiry. Canonical reasons explain the stop; disable
requires `operator_request`. Resume requires the previous suspension cooldown
to elapse and a new independently reviewed plan. It never automatically revives
an expired grant.

## Validation evidence

The tests exercise a complete lifecycle, precise expiry boundaries, cooldown,
wrong predecessor and scope, disabled/blocked plans, reviewer separation,
recomputed-hash attacks, unknown fields, arbitrary entrypoints, deterministic
identity, deep-copy isolation, and compatibility with the real P4e1 planner.

Run:

```bash
.venv/bin/python -m pytest -q tests/unit/test_notification_worker_authority_contract.py
```

## Safety and remaining work

No committed delivery, destination, retry, or worker setting is enabled. This
module imports no network client and performs no database access, lock
acquisition, scheduler activation, webhook delivery, deployment, or
`terraform apply`. It retains neither payload bodies nor endpoint values.

P4e2b must add append-only persistence, conflict-safe head selection and exact
replay. P4e2c must combine that retained authority with current readiness and
failure evidence. Neither transition construction nor a passing test is human
approval to activate infrastructure.
