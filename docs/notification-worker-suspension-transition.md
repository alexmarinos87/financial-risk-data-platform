# Binding suspension decisions to lifecycle transitions

Primary arc42 block: `orchestration`. P4e2c, second dependency-ordered layer.

`build_worker_suspension_bundle` accepts one exact authority, its canonical
suspension decision and an operator identifier. It reconstructs both source
contracts, permits only the `suspend` outcome and calls the existing
`build_worker_authority_transition`. The result is an atomic-persistence input
containing `authority`, `decision` and `transition`, not a competing state machine.

The stop retains the exact governed plan and predecessor, copies the decision's
canonical reasons and evaluation time, and has no reviewers or grant expiry.
Its request ID is deterministically derived from the complete decision. Repeating
the same input converges; changing the operator for that same decision retains
the request ID but changes the transition, so persistence must reject reuse.
Operator strings and hashes are evidence, not authentication.

Healthy and inactive decisions cannot produce a transition. Expired authority
already remains inactive and is not converted into a new suspension. Expired
destination review does not prevent stopping an otherwise active authority.
Evaluation at the predecessor's exact effective time cannot fabricate a later
clock value: the existing strict chronology check rejects the stop.

`validate_worker_suspension_bundle` reconstructs the decision and transition and
compares canonical bytes. Rehashed changes to reasons, timestamps, action,
predecessor, request identity or side-effect flags are rejected. Inputs are not
mutated and the entire bundle is limited to 1 MiB.

This layer performs no database read/write, lock acquisition, endpoint resolution,
network request, delivery, scheduler mutation, deployment or `terraform apply`.
No committed activation default is changed. Current retained-head selection,
authentication and health-source provenance remain trusted-caller obligations.
The next warehouse layer must reload the exact predecessor under its per-worker
transaction lock and atomically retain both the stop and decision evidence.

Run `python -m pytest -q tests/unit/test_notification_worker_suspension_transition.py`
plus the full repository quality/security checks. Review the exact decision-to-stop
binding before accepting; passing tests are not human approval.
