# Portfolio Daily Risk

## Outcome

This increment aggregates current daily-return versions into a deterministic,
long-only portfolio risk series:

```text
current daily-return versions
  -> complete-date alignment
  -> configured weighted contributions
  -> portfolio daily return
  -> annualised rolling volatility
  -> historical VaR loss
  -> maximum drawdown
  -> versioned curated Parquet
```

The pure calculation is owned by `src/analytics/portfolio_risk.py`. The local
runner is `src/orchestration/run_portfolio_risk.py`.

## Configuration

Portfolio definitions live in `config/portfolios.yaml`:

```yaml
portfolios:
  us-tech-equal:
    base_currency: USD
    constituents:
      - source: alpha_vantage
        symbol: AAPL
        weight: 0.5
      - source: alpha_vantage
        symbol: MSFT
        weight: 0.5
```

The first contract is deliberately narrow:

- two to fifty unique constituents;
- positive, long-only weights;
- weights must sum to one;
- one three-letter reporting currency;
- source and symbol form the constituent identity.

The base currency is descriptive in this slice. The pipeline does not perform FX
conversion.

## Operator Flow

First create daily-return history for every configured constituent using a common
date range:

```bash
export ALPHA_VANTAGE_API_KEY='set-locally-do-not-commit'

make daily-risk-demo SYMBOL=AAPL END_DATE=2026-03-31
make daily-risk-demo SYMBOL=MSFT END_DATE=2026-03-31
```

Then calculate portfolio risk without another provider request:

```bash
make portfolio-risk-demo \
  PORTFOLIO_ID=us-tech-equal \
  END_DATE=2026-03-31 \
  VOL_WINDOW=20 \
  VAR_WINDOW=60 \
  VAR_CONFIDENCE=0.95
```

The summary is written to `.demo/portfolio-risk-summary.json`.

## Current-Version Input Selection

`daily_returns` may contain more than one calculation for the same
`(source, symbol, ts_event)` after late historical data arrives. The portfolio
runner selects the current component row by:

```text
ts_ingest DESC
calculation_id DESC
```

This mirrors the daily warehouse current-version tie-break while keeping the
portfolio calculation independent of PostgreSQL.

An identical duplicated row is ignored. Reusing the same `calculation_id` for
conflicting fields fails closed because the portfolio calculation must not make a
file-order-dependent choice.

Dates are processed only when every configured constituent has a return.
Incomplete dates are excluded and counted in the run diagnostics. The pipeline
fails when fewer than two fully aligned dates remain.

## Formula And Weighting Semantics

For constituent weights `w_i` and daily returns `r_i,t`:

```text
portfolio_return_t = sum(w_i * r_i,t)
```

The persisted weighting method is:

```text
constant_weight_daily_rebalanced
```

The same target weights are therefore applied independently to every aligned
daily return. This is equivalent to rebalancing back to the configured weights
at each daily boundary. Transaction costs, drift between rebalances and custom
rebalancing schedules are not modelled.

The calculation records each component return, contribution, weight and
component calculation ID as stable JSON evidence.

Risk metrics are calculated from the aligned portfolio return series:

- annualised sample volatility uses the configured window and `sqrt(252)`;
- historical VaR is the positive lower-tail loss magnitude;
- maximum drawdown is calculated from compounded portfolio wealth, including an
  initial wealth level of one;
- `history_status` remains `partial` until both configured windows are ready.

The calculation is a transparent portfolio demonstration, not a validated
regulatory capital model.

## Dataset Grains

| Dataset | One row means | Identity |
| --- | --- | --- |
| `portfolio_daily_returns` | One weighted, daily-rebalanced portfolio return for a portfolio and fully aligned event date. | Deterministic calculation ID from definition, weighting method and current component calculations. |
| `portfolio_daily_risk_summary` | One portfolio return and rolling risk snapshot for a date and parameter set. | Deterministic calculation ID from definition, return history, weighting method and risk parameters. |

Identical definitions, current component versions and parameters reproduce the
same calculation IDs and content-addressed Parquet files. A component correction
or weight change creates a distinguishable version.

## Safety Bounds

The local reader:

- accepts only the configured `daily_returns` dataset;
- rejects symbolic-link paths and non-regular Parquet files;
- caps the scan at 4,096 files, 1 GB and 250,000 rows;
- reads only rows no later than the requested end date;
- requires exact daily-return contract fields;
- performs no live provider request.

## Current Boundary

This slice writes curated portfolio Parquet only. PostgreSQL portfolio tables,
current-version views, marginal risk attribution, covariance matrices, FX
conversion, non-daily rebalancing schedules, transaction costs, short positions,
leverage, scheduling, dashboards and alert delivery remain separate decisions.
