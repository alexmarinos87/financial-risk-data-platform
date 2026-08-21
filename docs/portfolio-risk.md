# Portfolio Daily Risk

## Outcome

This path aggregates current daily-return versions into a deterministic,
long-only portfolio risk series and serves every retained calculation through
PostgreSQL:

```text
current daily-return versions
  -> complete-date alignment
  -> configured weighted contributions
  -> portfolio daily return
  -> annualised rolling volatility
  -> historical VaR loss
  -> maximum drawdown
  -> versioned curated Parquet
  -> version-preserving PostgreSQL tables
  -> current-version, semantic and contribution views
```

The pure calculation is owned by `src/analytics/portfolio_risk.py`. The local
runner is `src/orchestration/run_portfolio_risk.py`. The warehouse contract is in
`sql/portfolio_schema.sql` and is loaded by
`src/warehouse/postgres_loader.py`.

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

Create daily-return history for every configured constituent using a common date
range:

```bash
export ALPHA_VANTAGE_API_KEY='set-locally-do-not-commit'

make daily-risk-demo SYMBOL=AAPL END_DATE=2026-03-31
make daily-risk-demo SYMBOL=MSFT END_DATE=2026-03-31
```

Calculate portfolio risk without another provider request:

```bash
make portfolio-risk-demo \
  PORTFOLIO_ID=us-tech-equal \
  END_DATE=2026-03-31 \
  VOL_WINDOW=20 \
  VAR_WINDOW=60 \
  VAR_CONFIDENCE=0.95
```

The run summary is written to `.demo/portfolio-risk-summary.json`.

Inspect the complete warehouse batch without connecting to PostgreSQL:

```bash
make portfolio-risk-warehouse-dry-run
```

Load all available source, daily and portfolio outputs, then run the focused
portfolio reconciliation checks:

```bash
make local-db-up
make portfolio-risk-warehouse-load
make check-portfolio-risk-consistency
```

`portfolio-risk-warehouse-load` reapplies both idempotent schema files before
loading. This lets an already-running local database receive newly introduced
portfolio objects without recreating its Docker volume.

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

The same target weights are applied independently to every aligned daily return.
This is equivalent to rebalancing back to the configured weights at each daily
boundary. Transaction costs, drift between rebalances and custom schedules are
not modelled.

The calculation records each component return, contribution, weight and
component calculation ID as stable JSON evidence.

Risk metrics are calculated from the aligned portfolio return series:

- annualised sample volatility uses the configured window and `sqrt(252)`;
- historical VaR is the positive lower-tail loss magnitude;
- maximum drawdown uses compounded portfolio wealth, including an initial value
  of one;
- `history_status` remains `partial` until both configured windows are ready.

The calculation is a transparent portfolio demonstration, not a validated
regulatory capital model.

## Curated Dataset Grains

| Dataset | One row means | Identity |
| --- | --- | --- |
| `portfolio_daily_returns` | One weighted, daily-rebalanced portfolio return for a portfolio definition and fully aligned event date. | Deterministic calculation ID from the definition, weighting method and current component calculations. |
| `portfolio_daily_risk_summary` | One portfolio return and rolling risk snapshot for a date and parameter set. | Deterministic calculation ID from definition, return history, weighting method and risk parameters. |

Identical definitions, current component versions and parameters reproduce the
same calculation IDs and content-addressed Parquet files. A component correction
or weight change creates a distinguishable version.

## PostgreSQL Serving Contract

The warehouse retains every calculation in:

```text
risk_platform.portfolio_daily_returns
risk_platform.portfolio_daily_risk_summary
```

Both tables use `calculation_id` as the primary and loader conflict key. Replaying
an identical output converges on one row; a different component version,
definition fingerprint or parameter set remains a separate row.

`definition_fingerprint` stays in every current-version grain. Two weight
configurations can therefore use the same human-readable `portfolio_id` without
silently replacing one another.

The current views are:

```text
risk_platform.latest_portfolio_daily_returns
risk_platform.latest_portfolio_daily_risk_summary
```

The return view ranks versions within:

```text
(portfolio_id, definition_fingerprint, ts_event,
 model_version, weighting_method)
```

The summary view additionally includes:

```text
(volatility_window, var_window, var_confidence, annualization_days)
```

Within either grain, the greatest `ts_ingest` wins and `calculation_id` is the
deterministic tie-break.

Reporting consumers can use:

```text
risk_platform.portfolio_risk_semantic_model
risk_platform.portfolio_daily_contribution_model
```

The semantic model exposes the current summary. The contribution model expands
one current portfolio return into one row per constituent with its configured
weight, component return, component calculation ID and contribution.

## Reconciliation Evidence

`sql/portfolio_risk_consistency_checks.sql` verifies:

- component calculation IDs reference retained `daily_returns` rows;
- the four JSON evidence objects have the same constituent keys;
- weights sum to one;
- contributions sum to the persisted portfolio return;
- each contribution equals weight multiplied by component return;
- summaries reference the matching portfolio return calculation;
- calculation IDs are unique;
- current views select the newest row in each declared grain;
- `ready` rows contain the declared volatility and VaR history;
- semantic and contribution views neither lose nor multiply current facts.

## Safety Bounds

The local reader:

- accepts only the configured `daily_returns` dataset;
- rejects symbolic-link paths and non-regular Parquet files;
- caps the scan at 4,096 files, 1 GB and 250,000 rows;
- reads only rows no later than the requested end date;
- requires the daily-return contract fields;
- performs no live provider request.

The warehouse path remains local and explicit. It does not create a managed
database or run a cloud deployment.

## Current Boundary

Marginal risk attribution, covariance matrices, FX conversion, non-daily
rebalancing schedules, transaction costs, short positions, leverage, production
scheduling, dashboards and alert delivery remain separate decisions.
