# Method-Aware Portfolio Risk Limits

## Outcome

This increment makes risk-limit evaluation explicitly bind the attribution model
and covariance/correlation methods:

```text
sample and EWMA attribution history
  -> explicit method-policy binding
  -> select one supported model/method contract
  -> ignore the other method without failing
  -> versioned portfolio-risk-limits-v2 evaluations
  -> current sample-versus-EWMA limit comparison
```

It fixes an important compatibility boundary introduced when rolling sample and
EWMA attribution began sharing `portfolio_risk_attribution`. The earlier
sample-only evaluator treated a same-portfolio EWMA row as an invalid record.
The method-aware path filters by an explicit contract before current-version
selection, so both methods can coexist safely.

## Configuration

Method bindings live in:

```text
config/portfolio_risk_limit_methods.yaml
```

The bundled bindings are:

```text
us-tech-sample
  -> base policy us-tech-standard
  -> portfolio-attribution-v1
  -> sample_annualized
  -> pearson

us-tech-ewma
  -> base policy us-tech-standard
  -> portfolio-attribution-ewma-v1
  -> ewma_zero_mean_lambda_0_94_annualized
  -> implied_from_ewma_covariance
```

A method binding references one existing effective-dated threshold policy. It
does not copy thresholds. The combined method-policy fingerprint binds:

- the effective-dated base-policy fingerprint;
- method-policy ID;
- attribution model version;
- weighting method;
- covariance method;
- correlation method; and
- risk-limit evaluation model version.

Threshold changes, policy-version changes, or method changes therefore produce a
new identity.

Only two complete method tuples are accepted. Mixing the EWMA model with sample
correlation, or the sample model with the EWMA covariance method, fails closed.

## Evaluation

The evaluator produces `portfolio-risk-limits-v2` rows in the existing curated
dataset:

```text
portfolio_risk_limit_evaluations
```

Every selected attribution snapshot still produces:

- `portfolio_volatility_annualized`; and
- `largest_absolute_component_contribution_share`.

The existing status semantics remain unchanged:

```text
observed < warning              -> ok
warning <= observed < critical  -> warning
observed >= critical            -> critical
```

Current attribution versions are selected within the chosen method by:

```text
ts_ingest DESC
calculation_id DESC
```

A sample binding ignores EWMA rows, and an EWMA binding ignores sample rows.
Malformed matching rows and conflicting calculation-ID reuse still fail closed.

## Operator Commands

Evaluate sample attribution:

```bash
.venv/bin/python -m src.orchestration.run_method_aware_portfolio_risk_limits \
  --method-policy-id us-tech-sample \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --summary-json .demo/sample-risk-limits.json
```

Evaluate EWMA attribution:

```bash
.venv/bin/python -m src.orchestration.run_method_aware_portfolio_risk_limits \
  --method-policy-id us-tech-ewma \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --summary-json .demo/ewma-risk-limits.json
```

Both commands use the existing bounded attribution collector:

- 4,096 Parquet files;
- 1 GB physical input;
- 250,000 rows;
- regular non-symbolic-link files only.

Publication remains one deterministic content-addressed record at a time.
Partial local completion is replay-safe.

## PostgreSQL

Apply the additive method layer after the covariance-method schema:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_limits_method_schema.sql
```

The layer constrains `portfolio-risk-limits-v2` rows to one of the supported
sample or fixed-decay EWMA tuples.

The comparison view is:

```text
risk_platform.portfolio_risk_limit_method_comparison
```

It pairs current sample and EWMA evaluations only when they share:

- base policy ID;
- portfolio and definition;
- event date;
- metric and unit;
- covariance window and annualisation basis; and
- warning and critical thresholds.

The view exposes observed values, statuses, breach flags, absolute and signed
differences, status disagreement, the higher observed method, and the more
severe method.

Run focused reconciliation with:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_limits_method_consistency_checks.sql
```

## Safety Boundary

This increment does not claim that the sample and EWMA models should share the
same thresholds. It requires an explicit method binding when they do. Formal
model approval and method-specific threshold calibration remain separate work.

It does not make provider requests, deliver alerts, acknowledge breaches, mutate
positions, schedule execution, deploy infrastructure, or run `terraform apply`.
