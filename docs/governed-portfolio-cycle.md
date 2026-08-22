# Governed Portfolio Cycle

## Outcome

This command runs the portfolio analytics chain under one selected portfolio mandate and one selected effective-dated risk-limit policy version:

```text
portfolio mandate selected for END_DATE
  -> requested range validated inside the mandate
  -> risk-limit policy version selected for END_DATE
  -> requested range validated inside the policy version
  -> portfolio daily risk
  -> rolling covariance and Euler attribution history
  -> risk-limit evaluation
```

The implementation is `src/orchestration/run_governed_portfolio_cycle.py`.

## Operator command

```bash
.venv/bin/python -m src.orchestration.run_governed_portfolio_cycle \
  --portfolio-id us-tech-equal \
  --policy-id us-tech-standard \
  --start-date 2026-01-02 \
  --end-date 2026-01-31 \
  --vol-window 20 \
  --var-window 60 \
  --var-confidence 0.95 \
  --covariance-window 20 \
  --summary-json .demo/governed-portfolio-cycle-summary.json
```

Use `--dry-run` to validate identifiers, dates, parameters, mandate-policy compatibility, and the planned stage sequence without acquiring a lock or reading and writing analytical data.

## Temporal contract

Both the mandate and policy use inclusive `effective_from` and exclusive nullable `effective_to` bounds. The command selects each contract using `END_DATE`. The complete requested range must fit inside both selected versions.

A request crossing either boundary fails before stage execution:

```text
start date before selected mandate or policy
  -> reject

end date outside selected mandate or policy
  -> reject
```

The operator must split a historical request at the relevant boundary. The runner does not silently mix definitions or threshold versions in one governed cycle.

The summary records:

```text
mandate_id
mandate_fingerprint
mandate effective period
policy_version_id
policy_fingerprint
policy effective period
requested and effective date range
stage summaries
```

## Compatibility checks

Before acquiring the cycle lock, the runner requires:

- policy portfolio ID equals mandate portfolio ID;
- requested covariance window equals the selected policy window;
- policy annualisation basis equals the attribution model's 252 trading days;
- start date is on or before end date; and
- all bounded output controls are within supported limits.

## Shared context

The same `PortfolioMandate` object is injected into the portfolio and attribution stages. The same effective policy version is injected into the risk-limit stage.

Input readers are wrapped so rows outside the mandate period are excluded before they reach a stage. The runner does not independently reimplement return, covariance, attribution, or threshold calculations.

## Concurrency and recovery

One portfolio-scoped lock is held across all three stages:

```text
governed-portfolio/<portfolio_id>
```

Stage order is fixed:

1. `portfolio_risk`
2. `portfolio_attribution_history`
3. `portfolio_risk_limits`

A stage failure prevents downstream execution. The lock is released in a `finally` block. Previously written deterministic Parquet evidence remains replay-safe, so rerunning converges without duplicate calculations.

## Boundary

This is a local analytical control, not a scheduler or transaction across every dataset. It does not send notifications, acknowledge breaches, approve a mandate or policy, deliver an approved change, block trading, deploy infrastructure, or run `terraform apply`.
