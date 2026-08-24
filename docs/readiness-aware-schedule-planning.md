# Readiness-Aware Local Schedule Planning

## Outcome

This plan-only layer combines the existing bounded local schedule plan with the
exact retained operational readiness decision:

```text
current schedule, calendar and effective mandate
  + exact current readiness decision
  + bounded local session plan
  -> would_run, would_block or no_work
  -> deterministic JSON evidence
  -> no command execution or checkpoint mutation
```

The existing `run_local_portfolio_schedule` remains the source of session and
command planning. It is invoked only with execution disabled. The readiness-aware
wrapper rejects any planner result that reports command execution, provider
access, delivery or cloud-schedule activity.

## Operator Command

```bash
.venv/bin/python -m src.orchestration.plan_readiness_aware_local_schedule \
  --schedule-id us-tech-local \
  --gate-id us-tech-local \
  --as-of-date 2026-04-01 \
  --summary-json .demo/readiness-aware-schedule-plan.json
```

The command exposes no `--execute` option.

Exit codes are:

- `0` — `would_run` or `no_work`;
- `2` — `would_block` using valid current or missing-decision evidence; and
- `1` — invalid configuration, retained evidence, local state or PostgreSQL
  access.

## Exact Current Contract

Before reading readiness evidence, the wrapper resolves the current:

- readiness gate and fingerprint;
- operational service-level policy and fingerprint;
- local schedule and fingerprint;
- market calendar and latest expected session; and
- effective portfolio mandate and fingerprint.

The operational policy must belong to the selected schedule. The readiness query
uses the exact gate, policy, schedule, calendar, portfolio, risk-limit policy,
mandate and expected-session identities. The selected row is joined back to the
append-only decision history so its complete `decision_json` can be validated and
its canonical SHA-256 can be rechecked.

A missing exact decision is represented explicitly as:

```text
status = missing
decision = block
reasons = [decision_missing]
```

It is not treated as an error because it is valid plan evidence, but it produces
`would_block` whenever work is selected.

## Schedule Effect

The result is derived as follows:

```text
no sessions selected                       -> no_work
sessions selected + current allow decision -> would_run
sessions selected + block/missing decision -> would_block
```

The wrapper still includes the underlying command plans for human inspection.
No command is run.

## Deterministic Identity

`readiness-aware-schedule-plan-v1` binds:

- as-of date and latest expected session;
- schedule, calendar, gate, operational-policy and mandate fingerprints;
- selected sessions and exact command plans;
- checkpoint-before evidence;
- readiness decision ID, decision and reasons; and
- resulting schedule effect.

The random `run_id` produced by the underlying local schedule planner is removed
before identity calculation. Repeating the same state produces the same plan ID.

## Side-Effect Boundary

Every result records:

```text
execution.requested = false
execution.performed = false
checkpoint_updated = false
provider_request_performed = false
notification_delivery_performed = false
cloud_schedule_activated = false
```

This increment does not grant execution authority. Overrides and enforcement are
separate dependency layers so the local schedule cannot accidentally begin using
a newly introduced gate before human-authority semantics have been reviewed.
