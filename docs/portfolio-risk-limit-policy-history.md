# Effective-Dated Portfolio Risk-Limit Policies

## Outcome

Risk-limit thresholds now have an explicit temporal-governance contract before
they are wired into historical evaluation:

```text
risk-limit policy history
  -> validate version IDs and effective ranges
  -> reject overlap and ambiguity
  -> select exactly one version for an as-of date
  -> preserve threshold identity separately from temporal policy identity
  -> require bounded runs to remain inside one policy version
```

The contract is implemented in
`src/analytics/portfolio_risk_limit_policies.py`. Existing risk-limit analytics
continue to use the current single configured policy until the separate runtime
wiring increment is reviewed.

## Configuration Shape

A single direct version remains valid:

```yaml
policies:
  us-tech-standard:
    policy_version_id: us-tech-standard-v1
    effective_from: 2026-01-01
    effective_to:
    portfolio_id: us-tech-equal
    covariance_window: 20
    annualization_days: 252
    limits:
      portfolio_volatility_annualized:
        warning: 0.30
        critical: 0.45
      largest_absolute_component_contribution_share:
        warning: 0.65
        critical: 0.80
```

A history uses a `versions` list:

```yaml
policies:
  us-tech-standard:
    versions:
      - policy_version_id: us-tech-standard-v1
        effective_from: 2026-01-01
        effective_to: 2026-07-01
        portfolio_id: us-tech-equal
        covariance_window: 20
        annualization_days: 252
        limits: ...
      - policy_version_id: us-tech-standard-v2
        effective_from: 2026-07-01
        effective_to:
        portfolio_id: us-tech-equal
        covariance_window: 20
        annualization_days: 252
        limits: ...
```

Direct fields and a `versions` list cannot be mixed.

## Date Semantics

`effective_from` is inclusive. `effective_to` is exclusive and may be null only
for the final open-ended version:

```text
v1: [2026-01-01, 2026-07-01)
v2: [2026-07-01, infinity)
```

The parser rejects:

- duplicate version IDs;
- malformed dates or datetime values where a calendar date is required;
- zero-length or reversed ranges;
- overlapping versions;
- a non-final open-ended version;
- histories targeting different portfolio IDs; and
- ambiguous or uncovered as-of dates during selection.

Gaps are allowed because a policy may be inactive. Evaluating a date inside a
gap fails explicitly rather than borrowing a nearby version.

## Identity

Each selected version exposes two fingerprints.

`limit_definition_fingerprint` binds:

- policy and portfolio IDs;
- covariance window;
- annualisation basis; and
- warning and critical thresholds.

`policy_fingerprint` additionally binds:

- policy-version ID;
- inclusive effective-from date; and
- exclusive effective-to date.

A formal policy renewal with unchanged thresholds therefore keeps the same limit
definition identity but receives a distinct temporal policy identity. Threshold
changes alter both identities.

## Selection And Range Validation

Select one version with:

```python
policy = load_effective_portfolio_risk_limit_policy(
    Path("config/portfolio_risk_limits.yaml"),
    "us-tech-standard",
    as_of_date,
)
```

A bounded operation can then call `validate_policy_range`. The current contract
requires the explicit start and end dates to remain within one selected policy
version. A later runtime increment will decide whether the historical evaluator
splits a broad request into per-version segments or requires the operator to do
so explicitly.

## Boundary

This increment defines and tests policy time semantics only. It does not change
existing risk-limit calculations, stored evaluation schemas, breach lifecycle,
notification candidates, acknowledgement state, PostgreSQL data, scheduling,
external delivery, trading controls, credentials, deployment, or Terraform.
