# Portfolio Covariance And Volatility Attribution

## Outcome

This path turns retained portfolio daily returns into deterministic covariance,
correlation and Euler volatility-attribution snapshots and serves every retained
version through PostgreSQL:

```text
current portfolio_daily_returns versions
  -> exact definition-fingerprint selection
  -> complete rolling covariance windows
  -> annualised sample covariance matrix
  -> Pearson correlation matrix
  -> Euler component contributions to portfolio volatility
  -> versioned portfolio_risk_attribution Parquet
  -> version-preserving PostgreSQL history
  -> current covariance, correlation and contribution views
```

The latest-snapshot calculation is owned by
`src/analytics/portfolio_attribution.py`. Rolling historical attribution is
owned by `src/analytics/portfolio_attribution_history.py`. The runners are:

```text
src/orchestration/run_portfolio_attribution.py
src/orchestration/run_portfolio_attribution_history.py
```

The warehouse schema is `sql/portfolio_attribution_schema.sql`, and the dedicated
loader is `src/warehouse/portfolio_attribution_loader.py`.

## Operator Flow

Generate constituent and portfolio history first:

```bash
export ALPHA_VANTAGE_API_KEY='set-locally-do-not-commit'

make daily-risk-demo SYMBOL=AAPL END_DATE=2026-03-31
make daily-risk-demo SYMBOL=MSFT END_DATE=2026-03-31
make portfolio-risk-demo \
  PORTFOLIO_ID=us-tech-equal \
  END_DATE=2026-03-31
```

### Latest snapshot

Calculate only the latest complete window:

```bash
make portfolio-attribution-demo \
  PORTFOLIO_ID=us-tech-equal \
  END_DATE=2026-03-31 \
  COVARIANCE_WINDOW=20
```

The summary is written to `.demo/portfolio-attribution-summary.json`.

### Rolling history

Calculate one snapshot for every eligible window end date:

```bash
make portfolio-attribution-history-demo \
  PORTFOLIO_ID=us-tech-equal \
  START_DATE=2026-01-01 \
  END_DATE=2026-03-31 \
  COVARIANCE_WINDOW=20
```

`START_DATE` filters emitted snapshot dates. Earlier current portfolio returns are
still retained as calculation context so the first selected date can use its full
covariance window. Omitting `START_DATE` emits every complete window no later than
`END_DATE`.

The rolling command writes a credential-free summary to
`.demo/portfolio-attribution-history-summary.json`. Both commands publish to:

```text
data/curated/portfolio_risk_attribution
```

The history command is bounded by `MAX_HISTORY_SNAPSHOTS = 2_500`. A larger
request fails before publication and should be split into date ranges. Each
snapshot is published independently, so a partial local failure is replay-safe.

## Input Version Selection

`portfolio_daily_returns` can retain several calculations for one portfolio
definition and event date after an upstream correction. Both attribution paths
filter to the fingerprint produced by the selected entry in
`config/portfolios.yaml`, then choose the current row for each event date by:

```text
ts_ingest DESC
calculation_id DESC
```

An identical duplicate row is ignored. Reusing one portfolio-return
`calculation_id` for conflicting content fails closed.

Every selected record is checked against the configured definition:

- portfolio ID, definition fingerprint, base currency and weighting method;
- constituent count and exact constituent-key set;
- configured weights;
- component calculation IDs and finite component returns; and
- persisted portfolio return equal to the weighted component returns.

Historical snapshots are recalculated from the **current** version of each
portfolio-return date. A late correction therefore creates new deterministic
snapshot versions for every affected rolling window while preserving the earlier
warehouse rows.

## Covariance And Correlation

For a complete window of aligned constituent-return vectors `r_t`, the
calculation uses:

```text
Sigma_daily = sample_covariance(r_t)
Sigma_annual = 252 * Sigma_daily
```

The persisted covariance matrix is annualised. Correlation is the ordinary
Pearson correlation matrix calculated over the same observations.

A zero-variance constituent makes its correlations undefined. Those cells are
persisted as JSON `null`, not non-standard `NaN`, and the snapshot records:

```text
correlation_status = undefined_zero_variance
undefined_correlation_cells = <count>
```

The covariance matrix remains valid because zero variance is represented by a
zero diagonal and zero covariance values.

## Component Volatility Contribution

For configured weight vector `w` and annualised covariance matrix `Sigma`:

```text
portfolio_variance = w' * Sigma * w
portfolio_volatility = sqrt(portfolio_variance)

marginal_contribution_i = (Sigma * w)_i / portfolio_volatility
component_contribution_i = w_i * marginal_contribution_i
contribution_share_i = component_contribution_i / portfolio_volatility
```

The component contributions use Euler decomposition and reconcile to portfolio
volatility within a strict numerical tolerance:

```text
sum(component_contribution_i) = portfolio_volatility
```

Long-only weights do not imply every component contribution must be positive. A
constituent can reduce total volatility through negative covariance and therefore
have a negative component contribution.

When portfolio volatility is numerically zero, all marginal, component and share
values are explicitly set to zero and `volatility_status` is `zero`.

## Curated Dataset Grain

`portfolio_risk_attribution` contains one row for:

```text
portfolio definition fingerprint
+ snapshot event date
+ covariance window
+ attribution model version
+ weighting, covariance and correlation methods
+ ordered current portfolio-return calculation IDs
```

Each row includes:

- `portfolio-attribution-v1` and a deterministic calculation ID;
- selected window boundaries and ordered input calculation IDs;
- weights and annualised constituent volatilities;
- annualised covariance and Pearson correlation matrices;
- marginal and component volatility contributions;
- contribution shares;
- annualised portfolio variance and volatility; and
- correlation, zero-volatility and Euler-reconciliation evidence.

The deterministic calculation ID binds the definition fingerprint, methods,
covariance window, annualisation basis and ordered current input IDs. Replaying
the same state writes no duplicate Parquet record. A correction, definition
change or window change produces a distinguishable version.

## PostgreSQL Serving Contract

The warehouse retains every calculation in:

```text
risk_platform.portfolio_risk_attribution
```

`calculation_id` is the primary and loader conflict key. Replaying an identical
snapshot converges on one stored row. Rolling history requires no schema change:
`ts_event` already belongs to the current-version grain.

The current grain is:

```text
portfolio_id
definition_fingerprint
ts_event
model_version
weighting_method
covariance_method
correlation_method
covariance_window
annualization_days
```

Within that grain, `ts_ingest DESC, calculation_id DESC` selects the current
version through:

```text
risk_platform.latest_portfolio_risk_attribution
```

The complete current snapshot is exposed through:

```text
risk_platform.portfolio_attribution_semantic_model
```

Matrices and contribution vectors are expanded without recalculation:

```text
risk_platform.portfolio_covariance_model
risk_platform.portfolio_correlation_model
risk_platform.portfolio_volatility_contribution_model
```

The matrix views expose one row per ordered constituent pair. Undefined
correlation cells remain SQL `NULL`. The contribution view exposes one row per
constituent with weight, annualised standalone volatility, marginal contribution,
Euler component contribution and contribution share.

## Warehouse Operation And Reconciliation

Inspect the attribution batch without connecting to PostgreSQL:

```bash
make portfolio-attribution-warehouse-dry-run
```

Load prerequisites, attribution history and reconciliation evidence:

```bash
make local-db-up
make portfolio-attribution-warehouse-load
make check-portfolio-attribution-consistency
```

`portfolio-attribution-warehouse-load` reapplies the core, portfolio and
attribution schemas, loads prerequisite facts, then loads all retained attribution
snapshots.

`sql/portfolio_attribution_consistency_checks.sql` verifies:

- every retained input ID references a portfolio-return row;
- current attribution uses current portfolio-return versions;
- input IDs, window boundaries and definition metadata align;
- JSON vector and matrix keys match the declared constituents;
- weights sum to one;
- covariance is symmetric with non-negative diagonals;
- correlation is symmetric, bounded and has the declared null count;
- squared portfolio volatility matches portfolio variance;
- Euler component contributions and shares reconcile;
- calculation IDs and current grains remain unique;
- the latest view selects the newest candidate; and
- semantic, matrix and contribution views have expected row counts.

## Safety Bounds

The analytics readers:

- accept only `portfolio_daily_returns` from the configured curated base path;
- filter to one portfolio ID, definition fingerprint and completed end date;
- reject symbolic-link paths and non-regular Parquet files;
- cap the scan at 4,096 files, 1 GB and 250,000 matching rows;
- require exact portfolio-return fields;
- cap one history request at 2,500 snapshots; and
- perform no provider request.

The warehouse loader applies equivalent file, byte and row limits to
`portfolio_risk_attribution`, converts only declared evidence fields to JSONB,
and performs no analytical recalculation.

## Current Boundary

This path implements current-version rolling historical attribution with sample
covariance and Euler volatility allocation. It does not implement shrinkage or
exponentially weighted covariance estimators, factor models, marginal VaR, FX
conversion, leverage, short positions, transaction costs, scheduled rebalancing,
production scheduling, dashboards or alert delivery.
