# Operational Readiness Decision History

## Outcome

The read-only `operational-readiness-gate-v1` output can be retained as immutable
PostgreSQL evidence without changing the gate decision or executing a schedule:

```text
current operational service-level report
  + current gate, schedule, calendar and mandate contract
  -> deterministic allow or block decision
  -> strict canonical validation
  -> append-only PostgreSQL history
  -> current allowed and blocked views
```

The gate remains responsible for evaluating evidence. The recorder only validates
and persists the exact decision document; it does not recalculate operational
metrics or reinterpret a block as an allow.

## Operator Flow

Generate a decision:

```bash
.venv/bin/python -m src.warehouse.operational_readiness_gate \
  --gate-id us-tech-local \
  --evaluated-at 2026-04-01T12:00:00Z \
  --summary-json .demo/operational-readiness-gate.json
```

Record the decision:

```bash
.venv/bin/python -m src.warehouse.operational_readiness_decision_recorder \
  --decision .demo/operational-readiness-gate.json
```

The recorder accepts one regular JSON file no larger than 1 MB. Symbolic links,
unknown fields, non-canonical timestamps, incompatible identities, inconsistent
age arithmetic, incorrect reason ordering and any side-effect flag set to true
fail before PostgreSQL mutation.

## Decision Contract

The deterministic decision identity binds:

- gate fingerprint;
- operational policy fingerprint;
- schedule and schedule fingerprint;
- calendar, portfolio, risk-limit policy and mandate fingerprint;
- latest expected market session;
- evaluation timestamp;
- retained report identity and SHA-256 when present;
- ordered blocking reasons; and
- final `allow` or `block` decision.

The recorder reconstructs the same identity and requires the supplied
`decision_id` to match. Report age and future-offset values are recomputed from
`evaluated_at` and `report_as_of`. Reasons are reconstructed in canonical order:

```text
report_missing
report_timestamp_future
report_age_exceeds_limit
report_session_mismatch
report_status_critical
report_status_warning
```

An allow decision has no reasons. A block decision has at least one reason.
Missing-report evidence uses exactly `report_missing` and null report fields.

## PostgreSQL Contract

Every version is retained in:

```text
risk_platform.operational_readiness_decisions
```

`decision_id` is the append-only identity. An exact retry converges on the
existing row when the canonical document SHA-256 matches. Reusing one decision ID
with different content fails closed. PostgreSQL triggers reject UPDATE and DELETE.

History and reason-level evidence are exposed through:

```text
risk_platform.operational_readiness_decision_history
risk_platform.operational_readiness_reason_history
```

The current grain preserves the complete current gate, operational-policy,
schedule, calendar, portfolio, risk-limit-policy, mandate and expected-session
contract. Corrections rank by:

```text
evaluated_at DESC
decision_id DESC
```

Current results are exposed through:

```text
risk_platform.latest_operational_readiness_decisions
risk_platform.current_allowed_operational_readiness_decisions
risk_platform.current_blocked_operational_readiness_decisions
```

The original operational report remains referenced by calculation ID and document
digest. Missing-report blocks retain no fabricated report reference.

## Reconciliation

`sql/operational_readiness_decisions_consistency_checks.sql` verifies:

- report references and digests;
- report, schedule, calendar, portfolio and mandate metadata;
- report age and future-offset arithmetic;
- blocking-reason semantics;
- decision and latest-grain uniqueness;
- newest-decision selection;
- allowed/blocked partitioning;
- reason-row expansion; and
- both append-only triggers.

## Boundary

No schedule command is executed. No provider request, notification delivery,
checkpoint update, cloud schedule activation, deployment or Terraform apply is
performed by the gate or recorder. Persisted readiness evidence becomes an audit
foundation for later plan-only schedule integration and reviewed override
semantics; it is not execution authority by itself.
