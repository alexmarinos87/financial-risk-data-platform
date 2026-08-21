# Portfolio Covariance And Volatility Attribution

## Outcome

This path turns retained portfolio daily returns into one deterministic
risk-attribution snapshot for a configured portfolio definition and serves every
retained snapshot through PostgreSQL:

```text
current portfolio_daily_returns versions
  -> exact definition-fingerprint selection
  -> bounded covariance window
  -> annualised sample covariance matrix
  -> Pearson correlation matrix
  -> Euler component contributions to portfolio volatility
  -> versioned portfolio_risk_attribution Parquet
  -> version-preserving PostgreSQL history
  -> current covariance, correlation and contribution views
```

The pure calculation is owned by
`src/analytics/portfolio_attribution.py`. The local runner is
`src/orchestration/run_portfolio_attribution.py`. The warehouse schema is
`sql/portfolio_attribution_schema.sql`, and the dedicated loader is
`src/warehouse/portfolio_attribution_loader.py`.

## Operator Flow

Generate the constituent and portfolio history first:

```bash
export ALPHA_VANTAGE_API_KEY='set-locally-do-not-commit'

make daily-risk-demo SYMBOL=AAPL END_DATE=2026-03-31
make daily-risk-demo SYMBOL=MSFT END_DATE=2026-03-31
make portfolio-risk-demo \
  PORTFOLIO_ID=us-tech-equal \
  END_DATE=2026-03-31
```

Calculate the latest attribution snapshot without another provider request:

```bash
make portfolio-attribution-demo \
  PORTFOLIO_ID=us-tech-equal \
  END_DATE=2026-03-31 \
  COVARIANCE_WINDOW=20
```

The credential-free run summary is written to
`.demo/portfolio-attribution-summary.json`. The curated snapshot is written under
`data/curated/portfolio_risk_attribution`.

Inspect the attribution warehouse batch without connecting to PostgreSQL:

```bash
make portfolio-attribution-warehouse-dry-run
```

Load the prerequisite daily and portfolio calculations, load attribution, and
run the focused reconciliation checks:

```bash
make local-db-up
make portfolio-attribution-warehouse-load
make check-portfolio-attribution-consistency
```

`portfolio-attribution-warehouse-load` reapplies the core, portfolio and
attribution schemas, loads the prerequisite warehouse facts, and then loads the
attribution snapshot. This allows an already-running local database to receive
the new objects without recreating its Docker volume.

## Input Version Selection

`portfolio_daily_returns` can retain several calculations for one portfolio
definition and event date after an upstream component correction. The attribution
runner filters to the fingerprint produced by the selected entry in
`config/portfolios.yaml`, then chooses the current row for each event date by:

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

## Covariance And Correlation

For a window of aligned constituent-return vectors `r_t`, the calculation uses
the sample covariance matrix:

```text
Sigma_daily = sample_covariance(r_t)
Sigma_annual = 252 * Sigma_daily
```

The persisted covariance matrix is annualised. Correlation is the ordinary
Pearson correlation matrix calculated over the same observations.

A zero-variance constituent makes its correlations undefined. Those cells are
persisted as JSON `null` rather than non-standard `NaN`, and the snapshot records:

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
+ requested end date/current final event date
+ covariance window
+ attribution model version
+ weighting method
```

The row includes:

- `portfolio-attribution-v1` and a deterministic calculation ID;
- the selected window boundaries and ordered portfolio-return calculation IDs;
- weights and annualised constituent volatilities;
- annualised covariance and correlation matrices;
- marginal and component volatility contributions;
- contribution shares;
- annualised portfolio variance and volatility; and
- correlation, zero-volatility and Euler-reconciliation evidence.

The deterministic calculation ID binds the definition fingerprint, weighting
method, covariance window, annualisation basis and ordered current input
calculation IDs. Replaying the same state writes no duplicate Parquet record. An
upstream correction, weight change or window change produces a distinct version.

## PostgreSQL Serving Contract

The warehouse retains every calculation in:

```text
risk_platform.portfolio_risk_attribution
```

`calculation_id` is the primary and loader conflict key. Replaying an identical
snapshot converges on one stored row. A corrected input, changed definition or
changed covariance window remains a distinct retained version.

The current-version grain is:

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

The scalar and JSON snapshot contract is exposed through:

```text
risk_platform.portfolio_attribution_semantic_model
```

The matrices and contribution vectors are expanded without recalculating the
analytics:

```text
risk_platform.portfolio_covariance_model
risk_platform.portfolio_correlation_model
risk_platform.portfolio_volatility_contribution_model
```

`portfolio_covariance_model` and `portfolio_correlation_model` expose one row per
ordered constituent pair. Undefined correlation cells remain SQL `NULL`.
`portfolio_volatility_contribution_model` exposes one row per constituent with
its weight, annualised standalone volatility, marginal contribution, component
contribution and contribution share.

## Reconciliation Evidence

`sql/portfolio_attribution_consistency_checks.sql` verifies:

- every retained input calculation ID references a portfolio-return row;
- the current attribution snapshot uses current portfolio-return versions;
- input IDs, window boundaries and definition metadata align;
- JSON vector and matrix keys match the declared constituents;
- portfolio weights sum to one;
- covariance is symmetric with non-negative diagonals;
- correlation is symmetric, bounded and has the declared null count;
- squared portfolio volatility matches portfolio variance;
- Euler component contributions and shares reconcile;
- calculation IDs and current grains remain unique;
- the latest view selects the newest candidate; and
- semantic, matrix and contribution views have the expected row counts.

## Safety Bounds

The analytics reader:

- accepts only `portfolio_daily_returns` from the configured curated base path;
- filters to one portfolio ID, definition fingerprint and completed end date;
- rejects symbolic-link paths and non-regular Parquet files;
- caps the scan at 4,096 files, 1 GB and 250,000 matching rows;
- requires exact portfolio-return fields; and
- performs no provider request.

The warehouse loader applies the same file, byte and row limits to
`portfolio_risk_attribution`, rejects unsafe local paths, converts only the
declared JSON evidence fields to JSONB, and performs no analytical
recalculation.

## Current Boundary

This path stores one latest-window snapshot per explicit calculation identity. It
does not yet calculate historical attribution snapshots for every event date,
shrinkage or exponentially weighted covariance estimators, factor models,
marginal VaR, FX conversion, leverage, short positions, transaction costs,
scheduled rebalancing, production scheduling, dashboards or alert delivery.
