# Append-Only Local Schedule Run History

## Outcome

The readiness-enforced local scheduler now retains one immutable terminal document
for every authorised execution attempt:

```text
readiness-aware plan
  + exact current readiness decision
  + gate_allow or active_override authority
  + selected local sessions
  -> bounded stage execution
  -> completed or failed terminal run document
  -> append-only PostgreSQL evidence
  -> current failure, incomplete-session and recent-run views
```

Primary arc42 block: `warehouse`, with a bounded instrumentation interface from
`orchestration`. The base scheduler remains responsible for locking, command order
and checkpoint mutation. The warehouse layer validates and retains the resulting
terminal evidence.

## Terminal Run Contract

`local-schedule-run-v1` binds:

- a deterministic run ID and request ID;
- the readiness-aware plan ID;
- execution-authority ID and `gate_allow` or `active_override` type;
- exact readiness-decision ID and canonical document SHA-256;
- exact override ID when override authority was used;
- schedule, calendar, portfolio, risk-limit policy and mandate identities;
- as-of date and latest expected market session;
- checkpoint before and after the attempt;
- selected, started and completed session counts;
- ordered terminal session outcomes;
- ordered bounded stage outcomes for each started session; and
- one stable failure code and failed session/stage identity where applicable.

The execution-authority ID is the idempotency request key. Re-recording the exact
same canonical document converges. Reusing that request identity with different
evidence fails closed.

## Session And Stage Semantics

Sessions use one terminal prefix:

```text
completed* [failed]? selected*
```

A completed session contains only completed stages and ends with the successful
`checkpoint` stage. Its `checkpoint_after` is the session date.

A failed session has exactly one failed final attempted stage. Later selected
sessions remain explicit and unstarted. A failure before the first session retains
all sessions as selected and records a run-level failure code without inventing a
failed session.

Stage names identify the bounded operation only, for example:

```text
run_daily_risk:AAPL
run_market_freshness:AAPL
run_governed_portfolio_cycle
make:portfolio-risk-limits-warehouse-load
checkpoint
```

**No command arguments**, environment variables, PostgreSQL DSNs, API keys or
other credentials are retained in the history document.

## Scheduler Integration

`run_local_portfolio_schedule` validates the current execution authority before
creating a run identity. Plan-only and no-work calls remain unrecorded because no
execution attempt starts.

For an authorised attempt the scheduler:

1. creates a deterministic run ID from the plan and authority identity;
2. records bounded stage start/finish evidence while retaining the existing lock;
3. advances the checkpoint only after every command for a session succeeds;
4. records a completed terminal document after all selected sessions finish; or
5. records a failed terminal document before re-raising the original execution
   failure.

A history-recording failure also fails the operation. Successful execution is not
reported without durable terminal evidence.

## PostgreSQL Contract

The schema adds:

```text
risk_platform.local_schedule_runs
risk_platform.local_schedule_run_session_history
risk_platform.local_schedule_run_stage_history
risk_platform.recent_local_schedule_runs
risk_platform.current_local_schedule_run_status
risk_platform.current_local_schedule_run_failures
risk_platform.incomplete_local_schedule_sessions
```

The base table stores the complete canonical JSONB document plus extracted query
columns and its SHA-256. PostgreSQL independently validates the referenced
readiness decision and, for override authority, the exact approved, unexpired and
unrevoked override at run start.

`recent_local_schedule_runs` ranks attempts by schedule and start time.
`current_local_schedule_run_status` exposes one latest attempt per schedule.
`current_local_schedule_run_failures` exposes the latest failed schedules.
`incomplete_local_schedule_sessions` exposes failed and never-started selected
sessions for operational follow-up.

UPDATE and DELETE are rejected by append-only triggers.

## Operator Recording Path

Scheduler instrumentation records terminal evidence automatically. A reviewed
canonical document can also be replayed explicitly:

```bash
.venv/bin/python -m src.warehouse.local_schedule_run_recorder \
  --run .demo/local-schedule-run.json
```

The recorder accepts one regular JSON file no larger than 1 MB and rejects
symbolic links, unknown fields, malformed identities, non-canonical session/stage
ordering and inconsistent checkpoint arithmetic before PostgreSQL mutation.

## Validation

The PostgreSQL 16 contract exercises:

- one completed `gate_allow` run and exact retry convergence;
- conflicting request reuse rejection;
- one later failed `active_override` run;
- completed, failed and selected session expansion;
- completed and failed stage expansion;
- current failure and incomplete-session views;
- readiness and override reference reconciliation; and
- UPDATE and DELETE rejection.

## Boundary

This increment persists local execution evidence. It does not add provider access,
external alerting, authentication, cryptographic authorization, position mutation,
managed scheduling or infrastructure deployment. CI performs no live provider
request, no external notification delivery, no cloud schedule activation and no
`terraform apply`.

The next dependency-safe roadmap slice is **P4b — bounded dead-letter retry
planning**, which remains report-only and delivery-free.
