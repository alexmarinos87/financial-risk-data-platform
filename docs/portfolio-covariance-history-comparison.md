# Rolling Sample And EWMA Covariance Comparison

## Outcome

This increment extends the two latest-window covariance estimators into one
bounded, aligned historical comparison:

```text
current portfolio-return versions
  -> complete rolling windows
  -> sample covariance and Euler attribution per date
  -> fixed-decay EWMA covariance and Euler attribution per date
  -> two versioned attribution rows per eligible date
  -> PostgreSQL sample-versus-EWMA trend view
```

The sample history remains implemented by
`src/analytics/portfolio_attribution_history.py`. The additional EWMA history is
implemented by `src/analytics/portfolio_attribution_ewma_history.py`.

The paired runner is:

```text
src/orchestration/run_portfolio_covariance_history_comparison.py
```

## Operator Flow

After current `portfolio_daily_returns` exist, calculate both histories:

```bash
.venv/bin/python -m src.orchestration.run_portfolio_covariance_history_comparison \
  --portfolio-id us-tech-equal \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --covariance-window 20 \
  --max-snapshots 2500 \
  --summary-json .demo/portfolio-covariance-history-comparison.json
```

The command performs no provider request. It reads the existing bounded local
portfolio-return dataset and publishes both model rows to:

```text
data/curated/portfolio_risk_attribution
```

## Historical Semantics

The history is current-version history, not point-in-time-as-known history.
For each portfolio-return date, the canonical selection remains:

```text
ts_ingest DESC
calculation_id DESC
```

A corrected return therefore creates new sample and EWMA calculation IDs for
every affected rolling window. Earlier attribution versions remain retained in
Parquet and PostgreSQL.

For a five-observation history and a three-observation covariance window:

```text
observations 1-3 -> sample row + EWMA row ending date 3
observations 2-4 -> sample row + EWMA row ending date 4
observations 3-5 -> sample row + EWMA row ending date 5
```

## Start-Date Context

`--start-date` filters emitted snapshot dates. It does not discard earlier return
observations needed to build the first complete window.

A snapshot emitted on March 1 with a twenty-observation window may therefore use
February observations. This preserves complete model inputs while allowing a
bounded output range.

## Pair Alignment

Before publication, each sample/EWMA pair must have identical:

- portfolio and base currency;
- definition fingerprint;
- weighting method;
- covariance window and observation count;
- annualisation basis;
- window start and end;
- metric date; and
- ordered input calculation IDs.

The model version, covariance method, correlation method, calculation ID and risk
values must remain distinct.

A mismatch fails before the pair is published. This prevents a comparison from
joining different portfolio definitions, dates, corrections or source windows.

## Bounds And Replay

The existing reader retains its limits of 4,096 files, 1 GB and 250,000 matching
rows. Each model is additionally capped at 2,500 emitted snapshots per request,
so one paired request can publish at most 5,000 attribution rows.

Rows are published independently through the content-addressed writer. A partial
run is replay-safe:

- existing sample or EWMA rows report already present;
- missing rows are written;
- no retained row is overwritten; and
- deterministic IDs allow the run to converge.

The summary reports selected, written and already-present counts for both models.
It does not include every historical matrix, keeping evidence bounded even for a
large date range.

## Comparison Summary

The summary includes:

- first and last paired snapshot dates;
- number of paired dates;
- dates where EWMA volatility is higher;
- dates where sample volatility is higher;
- equal-volatility dates;
- latest sample and EWMA volatility, difference, ratio and calculation IDs; and
- the date with the largest absolute volatility difference.

This is model evidence rather than a recommendation to prefer one estimator.
Sample covariance weights the demeaned window evenly. The EWMA model applies a
fixed 0.94 decay to zero-mean return outer products and therefore responds more
strongly to recent squared returns.

## PostgreSQL Contract

Fresh local PostgreSQL initialization applies:

```text
sql/portfolio_covariance_method_schema.sql
```

after the established attribution schema.

For an already-running local database, apply it explicitly:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_covariance_method_schema.sql
```

The schema adds an idempotent check for the fixed EWMA v1 model and creates:

```text
risk_platform.portfolio_covariance_method_comparison
```

The view joins only current sample and EWMA rows with identical portfolio,
definition, event date, weighting method, covariance window, annualisation basis,
window boundaries, observation count and ordered input IDs.

It exposes:

- sample and EWMA calculation IDs and calculation timestamps;
- both annualised variances and volatilities;
- EWMA minus sample volatility;
- EWMA-to-sample volatility ratio when sample volatility is positive; and
- the higher-volatility model label.

If only one model exists for a date, no comparison row is produced. Different
portfolio definitions and covariance windows remain separate.

## Reconciliation

Run the focused method comparison checks after loading attribution history:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_covariance_method_consistency_checks.sql
```

The checks verify:

- the view contains every exactly aligned current sample/EWMA pair;
- volatility differences reconcile;
- positive-sample volatility ratios reconcile;
- higher-model labels agree with the underlying values; and
- the comparison grain is unique.

## Current Boundary

This increment uses the fixed EWMA model introduced previously. It does not make
decay configurable, change the sample-bound risk-limit policies, create a model
approval workflow, add shrinkage or factor covariance, calculate marginal VaR,
perform FX conversion, support leverage or shorting, schedule execution, deliver
alerts, deploy infrastructure or run Terraform apply.
