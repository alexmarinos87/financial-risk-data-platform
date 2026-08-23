# Exchange-Calendar-Aware Market Freshness

## Outcome

This path distinguishes an expected trading session from a weekend or configured
exchange holiday before deciding whether daily market data is stale:

```text
local Alpha Vantage daily events
  -> strict exchange calendar and coverage period
  -> expected sessions through an as-of date
  -> missing-session and trailing-staleness evidence
  -> versioned daily_market_freshness Parquet
  -> PostgreSQL current and exception views
```

The pure calendar and freshness contract is
`src/analytics/market_calendar.py`. The local runner is
`src/orchestration/run_market_freshness.py`.

## Calendar configuration

`config/market_calendars.yaml` contains the bounded XNYS demonstration calendar
for calendar year 2026. It records:

- `America/New_York`;
- Monday-to-Friday session weekdays;
- explicit full-day holidays;
- a regular 16:00 local close; and
- explicit 13:00 local early closes.

Calendar coverage is half-open: `valid_from` is inclusive and `valid_to` is
exclusive. Requests and observations outside the configured period fail closed.
A new reviewed calendar version must be added before operating outside that
period.

## Freshness semantics

The `as_of_date` may itself be a session, weekend, or holiday. The expected
latest session is the most recent configured session on or before that date.

For example, a Saturday as-of date does not make Friday's completed close stale.
A Monday session does make Friday stale when Monday's completed observation is
missing.

The status contract is:

| Status | Meaning |
| --- | --- |
| `current` | Every expected session from the first observation through the as-of date is present |
| `gap_detected` | The latest expected session is present, but one or more earlier expected sessions are missing |
| `stale` | One or more expected sessions after the latest observation are missing |

Observations on configured weekends or holidays are rejected instead of being
silently treated as valid daily bars.

## Deterministic evidence

Each record retains:

```text
calendar ID and fingerprint
calendar coverage and timezone
as-of date and day type
first and latest observation dates
expected latest session
regular or early close time
observation and expected-session counts
missing and trailing-missing counts
ordered missing-session dates
freshness status
input fingerprint and calculation ID
```

The calculation ID binds the model version, calendar fingerprint, source, symbol,
as-of date, and complete selected input fingerprint. Replaying the same source
and calendar state writes no duplicate record. A corrected source observation or
calendar configuration produces distinguishable evidence.

## Operator flow

Generate local freshness evidence without another provider request:

```bash
.venv/bin/python -m src.orchestration.run_market_freshness \
  --symbol AAPL \
  --calendar-id XNYS \
  --as-of-date 2026-01-10 \
  --summary-json .demo/market-freshness-summary.json
```

Load and inspect it locally:

```bash
.venv/bin/python -m src.warehouse.market_freshness_loader --dry-run

make local-db-up
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/market_freshness_schema.sql

.venv/bin/python -m src.warehouse.market_freshness_loader \
  --dsn postgresql://risk_user:risk_password@localhost:5433/risk_platform

docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/market_freshness_consistency_checks.sql
```

PostgreSQL exposes:

```text
risk_platform.daily_market_freshness
risk_platform.latest_daily_market_freshness
risk_platform.current_daily_market_freshness
risk_platform.daily_market_freshness_exceptions
```

## Safety bounds

The existing daily raw reader remains bounded to 2,048 files, 1 GB and 100,000
rows. Calendar evaluation is capped at 3,700 calendar days and 2,500 missing
sessions. The warehouse loader is capped at 4,096 files, 1 GB and 250,000 rows.
Symbolic links and unsafe file types fail closed.

## Boundary

This increment does not fetch an exchange calendar over the network, call Alpha
Vantage, schedule execution, send notifications, change a portfolio, deploy
infrastructure, create cloud storage, or run `terraform apply`.
