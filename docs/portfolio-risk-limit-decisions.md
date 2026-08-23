# Portfolio Risk-Limit Decision Serving

## Outcome

Append-only operator decisions are loaded into PostgreSQL without changing the analytical evaluation or notification that prompted them:

```text
portfolio_risk_limit_decisions Parquet
  -> bounded calculation-ID load
  -> immutable decision history
  -> latest decision per notification
  -> open / acknowledged / resolved / waived lifecycle views
  -> lifecycle reconciliation
```

The loader is `src/warehouse/portfolio_risk_limit_decisions_loader.py`. The schema is `sql/portfolio_risk_limit_decisions_schema.sql`.

## Apply and load

Apply the schema after the notification serving schema:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_limit_decisions_schema.sql
```

Inspect the bounded batch without opening a database connection:

```bash
.venv/bin/python -m src.warehouse.portfolio_risk_limit_decisions_loader \
  --dry-run
```

Load the decisions:

```bash
.venv/bin/python -m src.warehouse.portfolio_risk_limit_decisions_loader \
  --dsn postgresql://risk_user:risk_password@localhost:5433/risk_platform
```

Run reconciliation:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_limit_decisions_consistency_checks.sql
```

## Append-only history

The base table is:

```text
risk_platform.portfolio_risk_limit_decisions
```

`decision_id` is the primary and loader conflict key. An identical retry converges on one row. A later acknowledgement, resolution, or waiver remains a separate decision record with its own identity.

The complete source record is retained as JSONB alongside the minimal queryable decision fields. The loader performs no threshold calculation and does not rewrite notification evidence.

## Current decision

```text
risk_platform.latest_portfolio_risk_limit_decisions
```

The current decision for one notification is chosen using:

```text
decided_at DESC
decision_id DESC
```

This is a serving interpretation over immutable records, not a mutable status column.

## Lifecycle views

The lifecycle view joins current notification intent to the latest decision:

```text
risk_platform.portfolio_risk_limit_breach_lifecycle
```

The resulting states are:

| State | Meaning | Operationally closed |
| --- | --- | --- |
| `open` | No decision has been recorded | No |
| `acknowledged` | Seen and under investigation | No |
| `resolved` | Operator records that the condition was addressed | Yes |
| `waived` | Operator explicitly accepts the breach with a reason | Yes |

Convenience views are:

```text
risk_platform.open_portfolio_risk_limit_breaches
risk_platform.acknowledged_portfolio_risk_limit_breaches
risk_platform.resolved_portfolio_risk_limit_breaches
risk_platform.waived_portfolio_risk_limit_breaches
risk_platform.portfolio_risk_limit_decision_summary
```

Only notifications that remain current after stale-breach suppression enter the lifecycle. A corrected risk-limit evaluation that becomes `ok` therefore removes the old notification from current operational views while preserving all historical notification and decision records.

## Safety bounds

The loader:

- reads only the configured `portfolio_risk_limit_decisions` dataset;
- rejects symbolic-link paths and unsafe file types;
- caps input at 4,096 files, 1 GB, and 250,000 rows;
- requires timezone-aware decision and ingest timestamps;
- permits only `acknowledged`, `resolved`, and `waived`; and
- converts only the canonical source record to JSONB.

## Boundary

This serving contract does not authenticate the actor, require a second approver, send messages, mutate notification state, block trading, schedule work, deploy infrastructure, or run `terraform apply`. Those remain separate governance and delivery decisions.
