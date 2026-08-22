# Effective-Dated Portfolio Risk-Limit Policies

## Outcome

Risk-limit thresholds are selected by the attribution snapshot's UTC event date:

```text
current portfolio attribution snapshot
  -> policy ID
  -> exactly one effective policy period
  -> deterministic period-aware policy fingerprint
  -> volatility and concentration evaluations
  -> replay-safe Parquet
  -> PostgreSQL policy-period serving and reconciliation
```

The change prevents a new threshold from being applied retroactively to older
risk observations merely because the configuration file changed.

## Configuration

A policy retains stable portfolio, covariance-window and annualisation identity.
Thresholds are supplied as contiguous versions:

```yaml
policies:
  us-tech-standard:
    portfolio_id: us-tech-equal
    covariance_window: 20
    annualization_days: 252
    versions:
      - effective_from: 2026-01-01
        effective_to: 2026-07-01
        limits:
          portfolio_volatility_annualized:
            warning: 0.30
            critical: 0.45
          largest_absolute_component_contribution_share:
            warning: 0.65
            critical: 0.80
      - effective_from: 2026-07-01
        limits:
          portfolio_volatility_annualized:
            warning: 0.28
            critical: 0.42
          largest_absolute_component_contribution_share:
            warning: 0.60
            critical: 0.75
```

`effective_from` is inclusive. `effective_to` is exclusive. Versions must be
contiguous and non-overlapping; only the final version may omit `effective_to`.
A schedule contains at most 100 versions.

All versions under one policy ID must keep the same:

- portfolio ID;
- covariance window; and
- annualisation basis.

Changing those fields is a new policy identity rather than a threshold revision.

## Identity And Replay

The period-aware fingerprint binds:

```text
portfolio-risk-limit-policy-v1
+ existing threshold-policy fingerprint
+ effective_from
+ effective_to
+ period source
```

The evaluation calculation ID then binds that fingerprint, the attribution
calculation ID, metric name and `portfolio-risk-limits-v1` model version.
Therefore a threshold revision creates new evidence only for event dates covered
by the revised period. An identical replay converges on the same files and rows.

Each evaluation persists:

```text
policy_effective_from
policy_effective_to
policy_period_source
```

The supported period sources are:

- `configured`: an explicit schedule version;
- `legacy_unbounded`: a flat pre-schedule policy evaluated through the backwards-
  compatible API; and
- `inferred_event_date`: old Parquet or PostgreSQL evidence migrated as a one-day
  period because no wider historical mandate can be proven.

The migration deliberately does not invent a broad effective period for old
rows.

## Historical Semantics

The input attribution series remains **current-version history**. Corrections are
selected per event date by:

```text
ts_ingest DESC
calculation_id DESC
```

After current input selection, each event date must be covered by exactly one
policy period. A gap or overlap fails before publication. A request spanning a
policy boundary may emit different policy fingerprints and statuses while
retaining one chronological result set.

## PostgreSQL

A fresh Docker database applies:

```text
sql/portfolio_risk_limit_policy_schedule_schema.sql
```

For an existing local database, apply it after the base risk-limit schema:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_limit_policy_schedule_schema.sql
```

The overlay adds period columns and constraints to
`risk_platform.portfolio_risk_limit_evaluations`. It also exposes:

```text
risk_platform.latest_portfolio_risk_limit_policy_evaluations
risk_platform.portfolio_risk_limit_policy_breaches
risk_platform.portfolio_risk_limit_policy_snapshot_status
risk_platform.portfolio_risk_limit_policy_versions_observed
```

`portfolio_risk_limit_policy_versions_observed` provides the observed date range
and thresholds for each retained policy fingerprint. It is evidence of what was
evaluated, not a substitute for an approved source-of-truth policy registry.

Run the focused checks after loading:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_limit_policy_schedule_consistency_checks.sql
```

The checks cover event-period inclusion, fingerprint-to-period uniqueness,
configured-period overlap, latest-row preservation, breach counts and snapshot
status reconciliation.

## Compatibility Boundary

Flat policies remain accepted as `legacy_unbounded` to avoid breaking existing
callers. New production-like examples should use explicit versions. Historical
configuration approval, maker-checker workflow, authorisation, exception expiry,
alert routing and regulatory retention remain outside this increment.
