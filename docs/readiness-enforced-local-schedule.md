# Readiness-Enforced Local Schedule Execution

## Outcome

The local schedule can now execute only through deterministic authority derived
from current retained operational evidence:

```text
readiness-aware schedule plan
  -> refresh exact current readiness decision
  -> reject changed, future or stale decision evidence
  -> current allow OR exact active override
  -> deterministic execution-authority document
  -> recalculate and validate the schedule plan
  -> execute commands under the established local lock
  -> advance the checkpoint only after a complete authorised session
```

The original blocked readiness decision is never rewritten. An override changes
the authority type, not the decision itself.

## Operator Flow

Plan only:

```bash
.venv/bin/python -m src.orchestration.run_readiness_enforced_local_schedule \
  --schedule-id us-tech-local \
  --gate-id us-tech-local \
  --as-of-date 2026-04-01 \
  --evaluated-at 2026-04-01T12:00:00Z \
  --summary-json .demo/readiness-enforced-schedule.json
```

Explicit local execution:

```bash
.venv/bin/python -m src.orchestration.run_readiness_enforced_local_schedule \
  --schedule-id us-tech-local \
  --gate-id us-tech-local \
  --as-of-date 2026-04-01 \
  --evaluated-at 2026-04-01T12:00:00Z \
  --execute \
  --summary-json .demo/readiness-enforced-schedule.json
```

The bundled local schedule remains disabled. It must be enabled through reviewed
configuration before an authorised execution can proceed.

Exit codes are:

- `0` — plan/no-work result or successful authorised execution;
- `2` — valid evidence produced a blocked execution decision; and
- `1` — invalid configuration, evidence, authority, local state or execution.

## Current-Evidence Refresh

The wrapper first builds the plan-only contract introduced by PR #101. For an
execution request it then reads the exact current readiness decision again using
the current gate, policy, schedule, calendar, portfolio, risk-limit policy,
mandate and expected-session contract.

If the decision ID or canonical document SHA-256 changed after planning, execution
fails and a new plan is required. A missing decision cannot be overridden.

The explicit `evaluated_at` instant must not predate the readiness decision. The
decision age must remain within the gate's configured
`max_report_age_seconds`; future and stale decisions block before override lookup.

## Authority Types

### Gate allow

A current `allow` decision creates:

```text
authority_type = gate_allow
override_id = null
```

The decision must have no blocking reasons.

### Active override

A current `block` decision can create:

```text
authority_type = active_override
override_id = operational-readiness-override-v1-...
```

only when the explicit-time override lookup finds a current, unexpired and
unrevoked override targeting the exact decision and document digest. Gate,
policy, schedule, calendar, portfolio, mandate and expected-session identities
must all match. The authority is invalid at or after override expiry.

A blocked decision without active override returns `decision = block` and does
not call the schedule executor.

## Deterministic Authority Contract

`operational-readiness-execution-authority-v1` binds:

- readiness-aware plan ID;
- schedule, calendar, portfolio, risk-limit policy and mandate identities;
- as-of date, latest expected session and exact selected sessions;
- gate and operational-policy fingerprints;
- readiness decision ID, canonical document SHA-256, decision and reasons;
- explicit authorisation timestamp; and
- override ID and expiry when authority is override-based.

The base scheduler independently validates the authority ID and all current plan
fields before checking whether the schedule is enabled, acquiring the execution
lock, running a command or writing state.

Direct `run_local_portfolio_schedule --execute` has no mechanism for supplying an
authority document and therefore fails before commands. Programmatic execution
must supply an exact validated authority.

The base scheduler currently requires one effective mandate fingerprint across
all selected catch-up sessions. A catch-up window that crosses a mandate boundary
must be split and replanned rather than authorised under one ambiguous mandate.

## Checkpoint And Failure Semantics

The established local schedule behavior remains:

- one repository-local schedule lock;
- commands run in session order;
- the checkpoint is written only after every command for that session succeeds;
- a failed command does not advance the incomplete session; and
- lock cleanup runs in `finally`.

The enforcement wrapper also rejects an executor result when completed sessions,
authority evidence or side-effect flags do not match the authorised plan.

## Summary Evidence

The credential-free result includes:

- readiness-aware plan ID;
- current decision ID, decision and reasons;
- deterministic authority ID and authority type;
- override ID and expiry where applicable;
- authorised and completed session dates;
- checkpoint before and after;
- `executed`, `block`, `no_work` or plan-only decision; and
- explicit provider, notification-delivery and cloud-schedule side-effect flags.

## Boundary

This is a local execution control, not authentication or a cryptographic access
control system. An operator who can modify and run repository code remains inside
the trust boundary. The contract supplies deterministic governance evidence and
prevents accidental or ordinary direct execution paths from bypassing current
readiness and override semantics.

No live provider request is added to CI, no external notification is delivered,
no cloud schedule is activated, no infrastructure is deployed and no
`terraform apply` is run by this increment.
