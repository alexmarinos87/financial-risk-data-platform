# Disabled Local Portfolio Scheduling

## Outcome

This control provides a bounded, explicitly activated local schedule for already-landed daily market data:

```text
reviewed disabled schedule configuration
  -> exchange-calendar session plan
  -> bounded checkpoint catch-up
  -> one schedule-scoped lock
  -> daily risk and calendar freshness per constituent
  -> governed portfolio risk, attribution, and limit evaluation
  -> local PostgreSQL load
  -> optional notification-outbox generation
  -> notification-outbox load
  -> atomic checkpoint after complete session success
```

The implementation is `src/orchestration/run_local_portfolio_schedule.py`. The default configuration is `config/local_portfolio_schedules.yaml`.

## Disabled-by-default contract

The committed schedule has `enabled: false`. Running the command without `--execute` always produces a plan only. Running with `--execute` while the reviewed configuration remains disabled fails before acquiring a lock or invoking a stage.

Activating the local schedule therefore requires both:

1. a reviewed configuration change setting `enabled: true`; and
2. an explicit `--execute` invocation.

There is no Kubernetes CronJob change, GitHub Actions schedule, system timer, daemon, background process, or cloud scheduler in this increment.

## Planning and catch-up

The planner uses the configured exchange calendar. On a first run with no checkpoint, it selects only the latest expected session on or before `as_of_date`. It does not backfill from calendar inception.

After a successful checkpoint, it selects every expected session after the checkpoint through the latest expected session. The schedule fails rather than truncating when the selection exceeds `maximum_catch_up_sessions`. Larger backfills must be run explicitly and reviewed.

A weekend or holiday as-of date selects the preceding expected trading session. The calendar coverage period is enforced.

## Stage sequence

For each selected session, commands execute in this fixed order:

1. `run_daily_risk` for each mandate constituent;
2. `run_market_freshness` for each mandate constituent;
3. `run_governed_portfolio_cycle` for portfolio risk, rolling attribution, and risk-limit evaluation;
4. local PostgreSQL warehouse loading;
5. market-freshness loading;
6. optional notification-outbox generation; and
7. notification-outbox loading.

The schedule supports Alpha Vantage constituents because that is the implemented daily raw source. An unsupported source fails during planning.

The optional outbox wrapper treats a valid day with no actionable transition as a successful no-op. Other validation failures are not suppressed.

## Checkpoint and recovery

One lock named `local-schedule/<schedule_id>` spans all selected sessions. The state file is a bounded JSON document under `.scheduler/` and records:

```text
schedule ID
schedule fingerprint
last successful session
state model version
update timestamp
```

The schedule fingerprint binds all operational parameters. A changed configuration cannot silently continue from an old checkpoint; the operator must archive or reset state deliberately.

Commands for one session must all succeed before the checkpoint advances. If any command fails, execution stops, the lock is released, and the last completed checkpoint remains unchanged. Deterministic underlying writes make rerunning safe.

## Commands

Plan without mutation:

```bash
.venv/bin/python -m src.orchestration.run_local_portfolio_schedule \
  --schedule-id us-tech-local \
  --as-of-date 2026-03-31 \
  --summary-json .demo/local-schedule-plan.json
```

After a reviewed configuration change enables the schedule, execute explicitly:

```bash
.venv/bin/python -m src.orchestration.run_local_portfolio_schedule \
  --schedule-id us-tech-local \
  --as-of-date 2026-03-31 \
  --execute \
  --summary-json .demo/local-schedule-run.json
```

The PostgreSQL DSN is supplied through `WAREHOUSE_POSTGRES_DSN` or `--dsn`. It is passed to child processes through environment variables and is never included in plan or run summaries.

## Safety boundary

The schedule consumes local raw data. It performs no Alpha Vantage request, sends no external notification, changes no position, acknowledges no breach, creates no managed service, deploys no Kubernetes workload, and runs no `terraform apply`.
