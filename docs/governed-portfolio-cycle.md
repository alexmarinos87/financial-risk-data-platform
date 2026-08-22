# Governed Effective-Dated Portfolio Cycle

## Outcome

This operator path runs the portfolio analytics chain under one explicitly
selected effective-dated mandate:

```text
requested portfolio and end date
  -> select the unique effective portfolio mandate
  -> validate the complete date range
  -> validate the risk-limit policy against that mandate
  -> filter pre- and post-mandate inputs
  -> portfolio daily risk
  -> rolling covariance and volatility attribution
  -> portfolio risk-limit evaluation
```

The implementation is:

```text
src/orchestration/run_governed_portfolio_cycle.py
```

It is the recommended local entry point when one run must keep portfolio
aggregation, attribution and risk-limit evidence on the same portfolio mandate.
The lower-level commands remain useful for focused tests and development, but
they do not by themselves coordinate one mandate across all three stages.

## Why This Is Needed

A portfolio ID can have several effective mandates over time. Reusing one
constituent definition across a boundary can mix weights from different mandates
inside a return window, covariance matrix or limit evaluation.

The governed cycle selects the mandate containing `end_date` and then requires:

```text
mandate.effective_from <= effective_start_date <= end_date
end_date < mandate.effective_to
```

The upper check is omitted for an open-ended mandate.

A range crossing a mandate boundary fails before any stage writes data. It must
be split into one run per mandate.

When `start_date` is omitted, the cycle uses the selected mandate's
`effective_from`. This prevents earlier observations from becoming hidden
calculation context.

## Policy Compatibility

The selected risk-limit policy must satisfy all of these checks before execution:

```text
policy.portfolio_id == mandate.portfolio_id
policy.covariance_window == requested covariance window
policy.annualization_days == 252
```

The last constraint matches the implemented portfolio-attribution model.
Unsupported combinations fail before acquiring the execution lock or publishing
curated evidence.

The summary records both:

```text
policy_fingerprint
mandate_fingerprint
```

Threshold or mandate changes therefore remain distinguishable in later evidence.

## Input Filtering

The cycle injects the same `PortfolioMandate` into every existing stage.

It wraps each source reader with `filter_records_to_mandate`:

- daily-return inputs for portfolio aggregation;
- portfolio-return inputs for rolling attribution; and
- attribution inputs for risk-limit evaluation.

The filter rejects malformed or timezone-naive timestamps and excludes records
outside the selected mandate. Every downstream definition fingerprint is the
mandate fingerprint, not only the constituent-definition fingerprint.

## Operator Commands

Validate the mandate, policy and requested parameters without acquiring a lock
or running a stage:

```bash
.venv/bin/python -m src.orchestration.run_governed_portfolio_cycle \
  --portfolio-id us-tech-equal \
  --policy-id us-tech-standard \
  --end-date 2026-03-31 \
  --covariance-window 20 \
  --dry-run \
  --summary-json .demo/governed-portfolio-cycle-plan.json
```

Run the complete local analytics cycle:

```bash
.venv/bin/python -m src.orchestration.run_governed_portfolio_cycle \
  --portfolio-id us-tech-equal \
  --policy-id us-tech-standard \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --vol-window 20 \
  --var-window 60 \
  --var-confidence 0.95 \
  --covariance-window 20 \
  --max-snapshots 2500 \
  --max-evaluations 10000 \
  --summary-json .demo/governed-portfolio-cycle-summary.json
```

The cycle reads already-landed daily-return data. It does not call Alpha Vantage
or another provider.

## Execution Order

The write-producing stages run in this fixed order:

```text
1. portfolio_risk
2. portfolio_attribution_history
3. portfolio_risk_limits
```

A stage must return a structured run summary. An invalid summary fails closed.

If a stage fails, later stages are not called. Earlier writes may already exist,
but their deterministic calculation IDs and content-addressed Parquet make the
whole cycle safe to rerun.

## Overlap Protection

One local lock is acquired for:

```text
governed-portfolio/<portfolio_id>
```

The lock covers all three stages and is always released in a `finally` block.
A concurrent run for the same portfolio is rejected. The default stale-lock
threshold is six hours and can be changed with `--stale-lock-seconds`.

The lock is local filesystem coordination, not a distributed lease.

## Summary Contract

The credential-free summary includes:

- run ID;
- portfolio and policy IDs;
- policy fingerprint;
- mandate ID and fingerprint;
- constituent-definition fingerprint;
- mandate validity dates;
- requested and effective start dates;
- all model parameters;
- whether execution occurred;
- completed stage names; and
- the child summary for every completed stage.

It also states:

```json
{
  "delivery": {
    "performed": false,
    "reason": "analytics_and_evidence_only"
  }
}
```

No database DSN, provider credential, recipient, webhook URL or access token is
included.

## Bounds

The governed cycle preserves the bounded controls of the underlying stages:

- complete mandate range;
- portfolio windows of at least two observations;
- covariance window no larger than the attribution maximum;
- at most 2,500 attribution snapshots;
- at most 10,000 risk-limit evaluations;
- bounded Parquet scans in the existing readers; and
- one local portfolio lock.

## Boundary

This increment does not:

- split one request across several mandates;
- activate a mandate automatically;
- approve or sign off a mandate;
- ingest provider data;
- load PostgreSQL;
- create notification candidates;
- deliver notifications;
- acknowledge breaches;
- schedule execution;
- deploy infrastructure; or
- run `terraform apply`.

PostgreSQL loading, reconciliation, lifecycle views and notification candidates
remain explicit downstream operator actions.
