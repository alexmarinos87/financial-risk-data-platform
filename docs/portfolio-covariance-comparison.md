# Sample And EWMA Portfolio Covariance Comparison

## Outcome

This increment adds a second, versioned covariance estimator without changing the
existing sample-covariance path:

```text
one aligned current portfolio-return window
  -> existing annualised sample covariance
  -> fixed-decay zero-mean EWMA covariance
  -> method-specific implied correlation
  -> method-specific Euler volatility attribution
  -> two independently versioned portfolio_risk_attribution rows
  -> paired comparison summary
```

The existing sample model remains:

```text
model_version      = portfolio-attribution-v1
covariance_method  = sample_annualized
correlation_method = pearson
```

The additional estimator is:

```text
model_version      = portfolio-attribution-ewma-v1
covariance_method  = ewma_zero_mean_lambda_0_94_annualized
correlation_method = implied_from_ewma_covariance
EWMA decay         = 0.94
```

The pure EWMA calculation is implemented in:

```text
src/analytics/portfolio_attribution_ewma.py
```

The paired latest-window runner is:

```text
src/orchestration/run_portfolio_covariance_comparison.py
```

## Why A Separate Model Version

The sample model is already retained, served and reconciled. Its calculation IDs
must remain stable for replay and warehouse history.

EWMA therefore uses a separate model version and method identity rather than
changing the meaning of `portfolio-attribution-v1`. The existing PostgreSQL
grain already includes:

```text
model_version
covariance_method
correlation_method
```

Both estimates can coexist for the same portfolio definition, event date and
window without a table migration or overwrite.

## Input Alignment

The comparison command reads portfolio returns once and applies the same
canonical validation and current-version selection to both estimators.

For each event date, a corrected portfolio return is selected by:

```text
ts_ingest DESC
calculation_id DESC
```

The runner rejects publication unless both models have identical:

- portfolio and base currency;
- definition fingerprint and weighting method;
- covariance window and observation count;
- window start and end;
- event date;
- annualisation basis; and
- ordered input calculation IDs.

Only the covariance estimator, correlation method, model version, calculation ID
and resulting risk values differ.

## Sample Covariance

The existing sample model uses the ordinary demeaned sample covariance over the
complete window:

```text
Sigma_sample_daily  = sample_covariance(r_t)
Sigma_sample_annual = 252 * Sigma_sample_daily
```

Pearson correlation is calculated directly from the same return window.

The implementation and calculation identity of this path are unchanged by this
increment.

## EWMA Covariance

The EWMA model applies exponentially decreasing weights to older observations.
For a window of `n` observations ordered oldest to newest:

```text
raw_weight_i = 0.94 ** (n - 1 - i)
weight_i     = raw_weight_i / sum(raw_weight)
```

The newest observation therefore receives the largest weight. The finite-window
normalisation makes each persisted snapshot self-contained.

The covariance uses the explicit zero-daily-mean assumption:

```text
Sigma_ewma_daily  = sum(weight_i * r_i * r_i')
Sigma_ewma_annual = 252 * Sigma_ewma_daily
```

This is intentionally distinct from an exponentially weighted, demeaned sample
covariance. The method name records the zero-mean and fixed-decay assumptions so
a future estimator cannot silently reuse the same identity.

Correlation is implied from the EWMA covariance matrix:

```text
correlation_ij = covariance_ij / sqrt(variance_i * variance_j)
```

Where either variance is numerically zero, the correlation cell is persisted as
JSON `null`, with the existing explicit undefined-correlation status.

## Euler Attribution

Both covariance matrices feed the same transparent Euler allocation:

```text
portfolio_variance   = w' * Sigma * w
portfolio_volatility = sqrt(portfolio_variance)

marginal_i  = (Sigma * w)_i / portfolio_volatility
component_i = w_i * marginal_i
share_i     = component_i / portfolio_volatility
```

For each model, component contributions must reconcile to that model's portfolio
volatility within the existing numerical tolerance. A component contribution may
be negative when covariance makes the constituent risk-reducing.

## Operator Flow

Generate current portfolio returns first, then run the paired comparison:

```bash
.venv/bin/python -m src.orchestration.run_portfolio_covariance_comparison \
  --portfolio-id us-tech-equal \
  --end-date 2026-03-31 \
  --covariance-window 20 \
  --summary-json .demo/portfolio-covariance-comparison.json
```

The command performs no provider request. It reads bounded local
`portfolio_daily_returns` Parquet and writes both snapshots independently to:

```text
data/curated/portfolio_risk_attribution
```

Independent publication is important for replay. If the sample row already
exists from a previous run, it reports `already_present` while the EWMA row can
still be added without writing a duplicate sample record.

## Summary Evidence

The credential-free summary includes:

- exact window and ordered input calculation IDs;
- sample and EWMA model/method identities;
- both deterministic calculation IDs;
- both annualised variances and volatilities;
- both largest absolute component contributions;
- EWMA decay, oldest/newest weights and effective observation count;
- EWMA minus sample volatility;
- EWMA-to-sample volatility ratio when sample volatility is positive; and
- which estimator produces the higher volatility for the selected window.

The comparison is evidence, not a statement that one estimator is universally
better. The estimators answer different questions: the sample model weights the
window evenly after demeaning, while the fixed-decay model reacts more strongly
to recent squared returns under a zero-mean assumption.

## Warehouse Compatibility

No schema or loader change is required.

`risk_platform.portfolio_risk_attribution` already retains every calculation ID.
`latest_portfolio_risk_attribution` separates current rows by model and methods.
The covariance, correlation and contribution views therefore expose both models
once the normal attribution warehouse load runs.

The existing risk-limit policy remains bound to the sample attribution contract
in this increment. Extending risk-limit policy configuration to choose a
covariance method is a separate governance change; EWMA rows are not silently
substituted into sample-based limits.

## Safety And Bounds

The paired runner reuses the existing attribution reader and therefore:

- accepts only the configured `portfolio_daily_returns` dataset;
- filters to one portfolio and exact definition fingerprint;
- rejects symbolic-link paths and unsafe local files;
- caps input at 4,096 files, 1 GB and 250,000 rows;
- requires a complete covariance window;
- publishes no partial calculation; and
- performs no provider, network, deployment or infrastructure action.

## Current Boundary

This increment implements one fixed-decay latest-window EWMA model and paired
sample comparison. It does not yet add rolling EWMA history, a SQL trend
comparison view, configurable decay, shrinkage covariance, factor models,
marginal VaR, FX conversion, leverage, short positions, transaction costs or
scheduled rebalancing.
