# Portfolio Risk-Limit Monitoring

## Outcome

This path evaluates the rolling portfolio-attribution series against a versioned
local policy and retains deterministic evidence for every metric:

```text
current portfolio_risk_attribution snapshots
  -> selected portfolio definition and covariance window
  -> versioned risk-limit policy
  -> portfolio-volatility evaluation
  -> largest absolute component-contribution-share evaluation
  -> versioned portfolio_risk_limit_evaluations Parquet
  -> PostgreSQL history, current breach and snapshot-status views
  -> append-only human acknowledgement evidence
  -> open and acknowledged breach views
```

It provides monitoring and review evidence only. It does not send alerts, block
trades, change positions or make an approval decision.

## Policy

Policies are configured in `config/portfolio_risk_limits.yaml`:

```yaml
policies:
  us-tech-standard:
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

Each policy receives a deterministic fingerprint from its portfolio, window,
annualisation basis and thresholds. Changing a threshold creates a new policy
version rather than silently relabelling prior evidence.

The first two metrics are deliberately narrow:

- annualised portfolio volatility from the attribution snapshot;
- the absolute value of the largest Euler component-contribution share.

The signed component share and constituent key remain in the output so a
risk-reducing negative contribution is not misrepresented.

## Evaluation Semantics

For each current attribution snapshot, two records are emitted. Status is:

```text
observed < warning                 -> ok
warning <= observed < critical     -> warning
observed >= critical               -> critical
```

Warning and critical rows record the applicable threshold and non-negative
excess. `ok` rows retain a null breach threshold and zero excess.

The evaluator filters to the policy portfolio, exact definition fingerprint,
covariance window, annualisation basis and supported attribution methods. When
several attribution calculations exist for one event date, current input is
selected by:

```text
ts_ingest DESC
calculation_id DESC
```

Identical duplicate calculations are ignored. Reusing one attribution
calculation ID for conflicting content fails closed.

Evaluation identity binds:

```text
portfolio-risk-limits-v1
+ policy fingerprint
+ attribution calculation ID
+ metric name
```

An identical rerun therefore writes no duplicate evidence. A corrected
attribution snapshot or policy change creates a distinguishable evaluation.

## Operator Flow

Generate rolling attribution first, then evaluate the requested date range:

```bash
.venv/bin/python -m src.orchestration.run_portfolio_attribution_history \
  --portfolio-id us-tech-equal \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --covariance-window 20

.venv/bin/python -m src.orchestration.run_portfolio_risk_limits \
  --policy-id us-tech-standard \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --summary-json .demo/portfolio-risk-limits-summary.json
```

The result dataset is:

```text
data/curated/portfolio_risk_limit_evaluations
```

One request is capped at 10,000 evaluation records. Because two metrics are
emitted per current attribution date, a single request covers at most 5,000
snapshots. Larger histories must be split into bounded ranges.

## PostgreSQL Serving

Apply `sql/portfolio_risk_limits_schema.sql` after the attribution schema, then
load the Parquet evidence:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_limits_schema.sql

.venv/bin/python -m src.warehouse.portfolio_risk_limits_loader \
  --dsn postgresql://risk_user:risk_password@localhost:5433/risk_platform

docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_limits_consistency_checks.sql
```

The history table is:

```text
risk_platform.portfolio_risk_limit_evaluations
```

Current and reporting views are:

```text
risk_platform.latest_portfolio_risk_limit_evaluations
risk_platform.portfolio_risk_limit_breaches
risk_platform.portfolio_risk_limit_snapshot_status
```

The current grain includes policy fingerprint, portfolio definition, event date,
models, methods, covariance window, annualisation basis and metric. It excludes
the constituent subject key so a corrected snapshot that changes the largest
component can replace the previous current metric while retaining both history
rows.

`portfolio_risk_limit_snapshot_status` combines the two current metrics into one
portfolio-date status using critical > warning > ok precedence.

## Breach Acknowledgement

A human reviewer can append an acknowledgement to one current breach:

```bash
.venv/bin/python -m src.warehouse.portfolio_risk_limit_acknowledger \
  --calculation-id portfolio-risk-limits-v1-example \
  --request-id INC-2026-001 \
  --acknowledged-by reviewer@example.com \
  --disposition investigating \
  --reason 'Reviewing the component concentration and source inputs.'
```

The supported dispositions are:

```text
investigating
accepted
false_positive
```

The acknowledgement model is `portfolio-risk-limit-ack-v1`. Its deterministic
identity binds the target evaluation and caller-supplied request ID. Retrying the
same request with the same immutable content is idempotent. Reusing the request
ID with different actor, disposition or reason is rejected.

Acknowledgements are stored in:

```text
risk_platform.portfolio_risk_limit_acknowledgements
```

The table is append-only. PostgreSQL triggers reject updates and deletes, and an
insert trigger requires the target evaluation to be a breach with a
timezone-aware acknowledgement time on or after the breach event.

The reporting contracts are:

```text
risk_platform.portfolio_risk_limit_acknowledgement_history
risk_platform.portfolio_risk_limit_breach_status
risk_platform.portfolio_risk_limit_open_breaches
risk_platform.portfolio_risk_limit_acknowledged_breaches
```

`portfolio_risk_limit_breach_status` attaches the latest acknowledgement to each
current breach while retaining an acknowledgement count. The open and
acknowledged views partition current breaches for operator triage.

An acknowledgement **does not change the breach status**, rewrite a risk-limit
evaluation, approve a trade or remove historical evidence. It records that a
named human reviewed the current breach. If a corrected attribution creates a
new current evaluation, that replacement requires its own acknowledgement.

## Reconciliation Evidence

`sql/portfolio_risk_limits_consistency_checks.sql` validates both the monitoring
and acknowledgement contracts. In addition to the evaluation checks, it verifies:

- every acknowledgement references a retained breach evaluation;
- acknowledgement timestamps do not precede the breach event;
- request IDs are unique within each evaluation;
- acknowledgement history matches the append-only table;
- breach status selects the latest acknowledgement deterministically;
- open and acknowledged views partition current breaches; and
- all three validation and mutation-prevention triggers are enabled.

The required `PostgreSQL contract` CI job applies the schema and executes these
checks against PostgreSQL 16 on every pull request.

## Safety And Boundary

The input and warehouse readers retain the existing bounded local pattern:

- at most 4,096 Parquet files;
- at most 1 GB of physical input;
- at most 250,000 rows;
- symbolic-link and unsafe-file rejection; and
- no provider request or cloud mutation.

Acknowledgement actor and reason fields are bounded and reject control
characters. The CLI reports identifiers and disposition but does not echo the
free-text reason.

The policy is a transparent demonstration, not a regulatory limit framework.
Production ownership would additionally require authorised policy lifecycle,
maker-checker approval, effective dating, alert routing, escalation, exception
expiry, access control and formal audit-retention controls.
