# Worker execution attribution context

Primary arc42 block: `orchestration`. Goal #211, under roadmap #76.

The delivery-attempt schema retains event and attempt identities, but not the
worker, worker authority or execution kind. Those missing identities must not be
inferred later from a matching endpoint host or an empty query result. This
context is the first explicit attribution boundary for a future worker adapter.
It is not a replacement authority model or permission to call a transport.

## Contract

`build_worker_execution_context` takes one existing canonical authority, an
invocation request ID, a selected execution kind and an explicit start instant.
The existing authority validator checks the full plan and lifecycle evidence.
The context derives the worker and destination identities, all policy
fingerprints, entrypoint, batch limit and exact slot from that validated input.
Starts are permitted from the slot inclusively until authority expiry exclusively,
not until an independently selected timeout. Stopped authority is rejected.

`scope_id` identifies worker / authority / slot / kind. It remains unchanged when
a caller changes invocation ID or start time within that slot. `context_id`
identifies the complete context. A future durable claim can use the scope to spot
competing invocations, but this module neither claims it nor enforces uniqueness.
Distinct authority transitions have distinct scopes, including for the same slot.

`validate_worker_execution_context` rebuilds every projected field against the
exact authority. Unknown fields, stale identities and rehashed contradictions
fail. Timestamps are normalized to UTC; results do not alias input dictionaries.
The serialized authority is bounded to 1 MiB and context to 16 KiB. These checks
are after canonical serialization, not pre-allocation or process-memory limits;
they reuse the accepted authority encoder rather than importing pending helpers.

## Trust and integration

The four flags `current_authority_verified`, `readiness_verified`, `slot_claimed`
and `runtime_permission_granted` always remain false. A valid historical snapshot
may already have been superseded. The caller must still authenticate the operator,
verify reviewed configuration and current authority, enforce the existing shared
delivery lock/readiness gates and durably claim the slot before real execution.
Creating a context does not verify that any invocation actually started.

The next separate layer observes the established attempt-writer callback. Durable
context/attempt storage, crash reconciliation, complete failure-history coverage
and runtime integration remain separate work. No legacy row is backfilled and no
zero-failure or consecutive-failure count is inferred by this contract.

## Validation

```bash
python -m pytest -q tests/unit/test_worker_execution_context.py
```

Forty cases use the existing plan and authority builders. They cover both kinds,
exact first/last instants, shortened grant expiry, normalization, invocation scope,
initial-only plans, stopped authority, malformed inputs, detached evidence and
rehashed changes to all derived authority/permission fields. Run the repository
quality/security/readiness checks as well. Automated evidence is not independent
review or explicit final-diff acceptance.

No I/O, schema, workflow, dependency, configuration enablement, scheduler,
notification, deployment or Terraform apply is added. This layer starts at accepted
main and does not import any unaccepted readiness, preflight or suspension branch.
