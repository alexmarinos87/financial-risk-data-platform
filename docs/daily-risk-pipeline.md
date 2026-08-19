# Alpha Vantage Daily Risk Pipeline

## Outcome

This path turns the implemented Alpha Vantage daily adapter into a complete,
local source-to-serving feature:

```text
TIME_SERIES_DAILY response
  -> canonical MarketEvent rows
  -> immutable raw Parquet
  -> one-day returns
  -> annualised rolling volatility
  -> historical VaR loss and maximum drawdown
  -> versioned curated Parquet
  -> PostgreSQL history and current-serving views
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
| `daily_returns` | One close-to-close return for a source, symbol and event date. | `calculation_id`, derived from model version and the two immutable source event IDs. |
| `daily_volatility` | One annualised sample-volatility observation for a source, symbol, event date and configured return window. | `calculation_id`, derived from model version, window parameters and immutable source event IDs. |
| `daily_risk_summary` | One daily close risk snapshot containing return, optional volatility, historical VaR loss and maximum drawdown. | `calculation_id`, derived from model version, full parameter set and ordered immutable source history. |

Each calculation ID hashes the model version, parameters and ordered immutable
source event IDs. The same history and parameters therefore produce the same
record bytes, Parquet filename and PostgreSQL conflict key.

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
- `volatility_window`, `annualization_days`, `var_window` and `var_confidence`
  are explicit columns, so consumers never have to infer model settings from an
  opaque calculation hash.

The explicit summary-parameter schema is versioned as `daily-risk-v2`. The
warehouse can retain earlier v1 rows with unknown new fields as null, while v2
rows must populate them.

These are transparent portfolio calculations, not a claim of a validated bank
capital model.

## Replay, Late History And Current Versions

Curated publication is record-addressed through the existing local Parquet
writer:

1. Re-running the same source history and parameters writes zero duplicate
   records.
2. Adding a previously missing historical date changes downstream calculation
   fingerprints and publishes new analytical versions.
3. Existing analytical files and warehouse rows are not overwritten by a new
   calculation ID.
4. Loading the same calculation ID again is idempotent through the warehouse
   conflict key.

PostgreSQL retains every version in the three daily tables. The view
`risk_platform.latest_daily_risk_summary` uses the following parameterised grain:

```text
source
symbol
ts_event
model_version
volatility_window
var_window
var_confidence
annualization_days
```

Within that grain it selects the greatest `ts_ingest`, then the greatest
`calculation_id` as a deterministic tie-break. Parameter changes remain visible
as separate serving rows rather than competing with one another.

The source-aware view `risk_platform.daily_risk_semantic_model` enriches the
current daily version through `current_symbol_dimension` on both `symbol` and
`source`. That avoids the ambiguous symbol-only join retained in the original
minute-oriented demonstration.

## Warehouse Workflow

Inspect planned row counts without connecting to PostgreSQL:

```bash
make daily-risk-warehouse-dry-run
```

Load the local Parquet outputs into the Docker PostgreSQL warehouse:

```bash
make local-db-up
make daily-risk-warehouse-load
```

The load target reapplies the idempotent schema before loading, so an already
running local database receives the new tables and views. Then run focused
reconciliation:

```bash
make check-daily-risk-consistency
```

`sql/daily_risk_consistency_checks.sql` verifies:

- daily rows reference their immutable raw source events;
- volatility rows have a matching daily return date;
- calculation IDs are unique within each dataset;
- the current-version view has one row per parameterised grain;
- the view does not select a stale version;
- `ready` history has the declared window evidence; and
- the source-aware semantic view does not multiply or lose current rows.

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

This slice remains local: Parquet and Docker PostgreSQL. It does not add a
scheduler, cloud persistence, dashboards, alert delivery or a deployment. Those
remain separate product and operating decisions after the source-to-serving
contract is accepted.
