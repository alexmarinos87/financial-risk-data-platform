# Alpha Vantage Daily Risk Pipeline

## Outcome

This path turns the implemented Alpha Vantage daily adapter into a complete,
local source-to-curated feature:

```text
TIME_SERIES_DAILY response
  -> canonical MarketEvent rows
  -> immutable raw Parquet
  -> one-day returns
  -> annualised rolling volatility
  -> historical VaR loss and maximum drawdown
  -> versioned curated Parquet
```

It is intentionally separate from `src.orchestration.run_pipeline`. Daily close
observations do not have the one-minute return, five-minute volatility or
lateness semantics of that demo.

## Operator Command

Keep the API key in the environment and run:

```bash
export ALPHA_VANTAGE_API_KEY='set-locally-do-not-commit'
make daily-risk-demo SYMBOL=IBM
```

The default end date is yesterday in UTC, preventing an in-progress daily bar
from becoming the first immutable version. Bounds and model parameters are
explicit:

```bash
make daily-risk-demo \
  SYMBOL=IBM \
  START_DATE=2026-01-01 \
  END_DATE=2026-03-31 \
  MAX_RECORDS=100 \
  VOL_WINDOW=20 \
  VAR_WINDOW=60 \
  VAR_CONFIDENCE=0.95
```

The command first invokes `src.orchestration.ingest_alpha_vantage_daily`, then
runs `src.orchestration.run_daily_risk`. The source command prints no
credential, and the analytical command reads only local raw Parquet.

## Dataset Grains

| Dataset | One row means | Logical identity |
| --- | --- | --- |
| `daily_returns` | One close-to-close return for a source, symbol and event date. | Model version, calculation ID and current source event ID. |
| `daily_volatility` | One annualised sample-volatility observation for a source, symbol, event date and configured return window. | Model version, calculation ID, source event ID and window. |
| `daily_risk_summary` | One daily close risk snapshot containing return, optional volatility, historical VaR loss and maximum drawdown. | Model version, calculation ID and current source event ID. |

Each calculation ID hashes the model version, parameters and ordered immutable
source event IDs. The same history and parameters therefore produce the same
record bytes and content-addressed Parquet filename.

## Formula Semantics

- `return_1d` is close-to-close percentage change between consecutive available
  observations. It is not guaranteed to represent a calendar day across
  weekends or market holidays.
- `volatility_annualized` is the sample standard deviation of the configured
  number of daily returns multiplied by `sqrt(252)`.
- `historical_var_loss` is the positive loss magnitude implied by the lower-tail
  empirical return quantile at the configured confidence. It remains null until
  at least two returns exist in the VaR window.
- `maximum_drawdown` is the most negative peak-to-current close return observed
  through that event date.
- `history_status` is `partial` until both configured windows have enough return
  observations; partial rows remain visible rather than being silently dropped.

These are transparent portfolio calculations, not a claim of a validated bank
capital model.

## Replay And Late History

Curated publication is record-addressed through the existing local Parquet
writer:

1. Re-running the same source history and parameters writes zero duplicate
   records.
2. Adding a previously missing historical date changes downstream calculation
   fingerprints and publishes new analytical versions.
3. Existing analytical files are not overwritten or automatically retired.
4. A larger implementation would use a transactional table format and an
   explicit current-version view.

A partial multi-record publication can leave a valid prefix if the process
stops. Rerunning is safe: already-published record files are skipped and the
remaining records converge.

## Safety Bounds

The raw reader:

- accepts only the configured local raw dataset;
- rejects symbolic-link final paths and non-regular Parquet entries;
- caps inventory at 2,048 files, 1 GB and 100,000 matching rows;
- filters to `alpha_vantage`, one canonical symbol and a completed end date;
- verifies UTC-midnight event time and the provider's stable daily event ID;
- converts DuckDB timestamps through exact epoch microseconds.

CI and readiness tests never make a live provider request.

## Current Boundary

This slice writes local curated Parquet only. It does not add daily PostgreSQL
tables, a scheduler, cloud persistence, dashboards or alert delivery. Those are
separate product and schema decisions after the local source-to-curated contract
is accepted.
