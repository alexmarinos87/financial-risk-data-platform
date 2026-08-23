# Exchange-Calendar Freshness

## Outcome

Daily freshness is evaluated against an explicit market calendar rather than calendar-day gaps:

```text
local daily-return observation dates
  -> configured instrument-to-calendar mapping
  -> exchange timezone, close time, grace period, weekends and holidays
  -> expected latest completed trading session
  -> current or stale evidence
  -> missing-session and non-session observation evidence
```

The calendar model is `src/analytics/market_calendar_freshness.py`. The bounded local runner is `src/orchestration/run_market_freshness.py`.

## Operator command

```bash
.venv/bin/python -m src.orchestration.run_market_freshness \
  --source alpha_vantage \
  --symbol IBM \
  --as-of 2026-01-08T22:00:00Z \
  --summary-json .demo/market-freshness-IBM.json
```

The command reads existing `daily_returns` Parquet and makes no provider request.

## Calendar contract

`config/market_calendars.yaml` maps a source-and-symbol key to a calendar ID. A calendar defines:

```text
timezone
local close time
post-close grace period
weekend weekday numbers
explicit holiday dates
```

The bundled XNAS entry is a small demonstration configuration, not a claim to be a complete authoritative exchange holiday feed. An operational deployment must manage and review the full exchange calendar as versioned reference data.

## Expected session

The expected latest observation is the most recent completed trading session.

- On a weekend or configured holiday, the expected session is the previous trading session.
- During a trading day but before local close plus the grace period, the expected session remains the previous session.
- After close plus grace, the current session becomes expected.

This prevents weekends, holidays, and an incomplete current trading day from being reported as missing data.

## Evidence

The summary records:

```text
calendar ID
UTC and exchange-local as-of timestamps
expected latest session
latest observed date
current or stale status
stale trading-session count
missing trading-session dates
observed non-session dates
observed session count
```

A weekend-dated or holiday-dated source observation is not silently discarded. It remains explicit `observed_non_session_dates` evidence for review.

## Bounds

The reader accepts only local `daily_returns` Parquet, rejects symbolic links and unsafe file types, and caps input at 4,096 files, 1 GB, and 250,000 rows. Calendar evaluation caps one span at twenty years and missing-session evidence at 2,500 dates.

## Boundary

This increment does not automatically download exchange calendars, call Alpha Vantage, schedule ingestion, send alerts, change portfolio calculations, deploy infrastructure, or run `terraform apply`. Calendar maintenance and any external reference-data adapter remain explicit future changes.
