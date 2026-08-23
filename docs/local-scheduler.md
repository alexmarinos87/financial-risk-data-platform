# Disabled Local Scheduler

## Outcome

The local scheduler provides one explicit planning or execution invocation over the existing analytical chain:

```text
bounded due dates
  -> daily risk for configured symbols
  -> governed portfolio cycle
  -> deterministic risk-limit notification outbox
  -> atomic local checkpoint
```

It is not a resident process and does not install cron, a Kubernetes CronJob, EventBridge rule, or other unattended trigger.

## Disabled default

`config/local_schedule.yaml` declares:

```yaml
enabled: false
```

A normal invocation against a disabled schedule returns planning evidence and performs no stages. Execution requires the explicit `--allow-disabled-run` flag or a reviewed configuration change enabling the schedule.

## Plan only

```bash
.venv/bin/python -m src.orchestration.run_local_schedule \
  --schedule-id us-tech-daily \
  --as-of-date 2026-03-31 \
  --dry-run \
  --summary-json .demo/local-schedule-plan.json
```

Dry run never acquires a lock, writes a checkpoint, reads provider data, or executes an analytical stage.

## Explicit one-shot execution

```bash
.venv/bin/python -m src.orchestration.run_local_schedule \
  --schedule-id us-tech-daily \
  --as-of-date 2026-03-31 \
  --allow-disabled-run \
  --summary-json .demo/local-schedule-run.json
```

The command uses already-landed local data. It does not call Alpha Vantage.

## Bounded catch-up

A checkpoint stores only:

```text
schedule_id
last_success_date
```

When no checkpoint exists, the configured `initial_lookback_days` controls the first plan. Later invocations plan the dates after `last_success_date`, capped by `max_catchup_days` and a hard maximum of 31 dates.

The scheduler processes the earliest due dates first. It writes the checkpoint atomically after each fully successful date, allowing a later invocation to resume without repeating completed dates. Deterministic downstream calculations also make replay safe if a failure occurs after a stage wrote some evidence but before the checkpoint advanced.

## Concurrency

One lock is held for the complete invocation:

```text
local-schedule/<schedule_id>
```

A second invocation fails instead of overlapping. The lock is released in a `finally` block after success or failure.

## Stage order

For each date:

1. Run daily risk for each configured symbol.
2. Run the governed effective-dated portfolio cycle.
3. Generate deterministic risk-limit notification intent.
4. Advance the checkpoint.

A stage failure prevents downstream stages for that date and all later due dates.

## Boundary

The scheduler does not ingest from an external provider, deliver notifications, mutate acknowledgement state, retry network calls, run continuously, deploy infrastructure, or run `terraform apply`. Enabling a real unattended schedule remains a separate operational decision.
