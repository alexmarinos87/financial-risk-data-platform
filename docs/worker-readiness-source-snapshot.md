# Reconciling worker readiness sources

Primary arc42 block: `warehouse`. Second readiness-source adapter layer.

`build_worker_readiness_snapshot` combines one canonical worker authority with
at most two source entries. Each selected execution kind requires the complete
persisted readiness record, its stored SHA-256, and a narrow current-review
projection. It reuses the preceding verifier and the existing authority contract.
No alternate readiness or lifecycle policy is introduced.

The current projection names the selected record and current destination,
activation authority/checklist, activation status, transition record/rehearsal,
transition status and endpoint environment-variable identity. The adapter compares
these to the reopened source, not to an unverified `allowed` label. It also checks
delivery, retry-planning and retry-execution fingerprints and enablement against
the governed worker plan. Changed evidence becomes `superseded`, stale evidence
remains stale, retained blocks remain blocked, and every missing kind blocks.

The bounded aggregate input is detached JSON. Inventory order does not affect
the result; duplicate or unselected kinds and inconsistent record/view joins are
rejected. Output binds authority and complete source digests, exposes the exact
per-kind record references, and can be reconstructed by its validator. Unknown
projection fields, including a caller-supplied status label, are rejected.

`ready_sources` describes only the supplied readiness sources. It is not a claim
that the producer actually selected current database state, that reviewed
configuration is still current, that a schedule slot is due or claimed, or that
failure history is healthy. Accordingly `current_authority_verified`,
`failure_history_verified` and `runtime_permission_granted` stay false. The next
read-only database adapter establishes source selection; the separately reviewed
preflight still owns current configuration and exact-slot checks.

Initial-delivery attempts do not currently retain worker and execution-kind
identities. This layer does not guess those identities, infer zero failures from
missing rows, or fabricate the failure summaries required by the suspension
contract. A provenance-aware history adapter remains separate work.

Tests use real readiness records and the existing worker authority builder. They
cover independent kinds, missing rows, review replacement, policy drift, freshness,
malformed joins, deterministic ordering, immutable inputs and rehashed verdicts.
Run `python -m pytest -q tests/unit/test_worker_readiness_snapshot.py` and the full
repository quality/security/readiness checks.

No I/O, schema change, configuration activation, scheduler mutation, delivery,
deployment or `terraform apply`. Source IDs and hashes are not authentication.
