# Controlled Portfolio-Risk Workflow Plan

## Outcome

This increment produces a deterministic, reviewable JSON plan for the local
portfolio-risk workflow without executing any planned step:

```text
configuration files
  -> size and SHA-256 evidence
  -> effective mandate selection
  -> effective risk-limit policy selection
  -> bounded date and parameter validation
  -> ordered step graph with declared reads and writes
  -> non-authorizing plan JSON
```

The implementation is split between:

```text
src/orchestration/portfolio_risk_workflow_plan.py
src/orchestration/plan_portfolio_risk_workflow.py
```

The plan is control-plane evidence. It is not an executor, approval, schedule, or
background task.

## Generate A Plan

```bash
.venv/bin/python -m src.orchestration.plan_portfolio_risk_workflow \
  --portfolio-id us-tech-equal \
  --policy-id us-tech-standard \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --vol-window 20 \
  --var-window 60 \
  --var-confidence 0.95 \
  --max-snapshots 100 \
  --max-evaluations 200 \
  --output .demo/portfolio-risk-workflow-plan.json
```

The command only validates and writes JSON. It does not import a subprocess
executor, invoke `make`, run the governed cycle, open PostgreSQL, call a provider,
deliver notifications, deploy infrastructure, or apply Terraform.

## Temporal Governance

The requested end date selects:

- one effective-dated `PortfolioMandate`; and
- one effective-dated `PortfolioRiskLimitPolicy` version.

The start and end dates must remain inside both selected inclusive/exclusive
ranges. A broad request crossing a mandate or policy boundary fails before a plan
is written and must be split explicitly.

The selected mandate and policy metadata are copied into the plan, including:

- mandate ID and definition fingerprint;
- mandate effective bounds;
- policy-version ID;
- temporal policy fingerprint;
- threshold-definition fingerprint; and
- policy effective bounds.

## Configuration Evidence

The planner requires the portfolio, risk-limit, and storage configuration paths
to be regular non-symbolic-link files no larger than 1 MB each. It records for
each file:

```text
path
byte count
SHA-256 digest
```

The plan therefore identifies the reviewed configuration bytes without copying
credentials or unrestricted source data into the artifact.

An operator should compare these hashes immediately before separately authorizing
a mutating step. The plan does not perform that later authorization or
verification itself.

## Plan Identity

The `plan_id` is a SHA-256-derived identity over the canonical plan body,
including:

- temporal governance metadata;
- parameters and bounds;
- configuration paths and hashes;
- ordered steps and dependencies;
- declared preconditions;
- command argument arrays; and
- safety controls.

Generating the same plan from the same inputs reproduces the same `plan_id` and
writes no new file content. Changed dates, thresholds, mandates, configuration
bytes, bounds, or commands produce a different identity.

## Planned Steps

The artifact declares four ordered steps.

### 1. Review the governed dry run

The first command calls `run_governed_portfolio_cycle --dry-run`. It validates
mandate and policy governance and describes analytics stages without taking a
lease or writing analytical datasets.

### 2. Run the governed local cycle

The second command removes `--dry-run` and declares local Parquet writes for:

```text
portfolio_daily_returns
portfolio_daily_risk_summary
portfolio_risk_attribution
portfolio_risk_limit_evaluations
```

Execution is explicitly outside this plan and requires separate operator
authorization. The governed cycle itself holds one outer local lease.

### 3. Load the local warehouse

The third step declares:

```text
make portfolio-risk-limits-warehouse-load
```

It reads local curated Parquet and writes to the operator-controlled local
PostgreSQL `risk_platform` schema. The plan does not start PostgreSQL or execute
the target.

### 4. Reconcile local serving

The final step declares:

```text
make check-portfolio-risk-limits-consistency
```

It reads the local PostgreSQL serving contract and produces operator-visible
reconciliation output. The plan does not interpret a result as approval.

## Command Representation

Commands are represented as structured `program` and `argv` arrays, not shell
strings. This makes argument boundaries inspectable and avoids embedding shell
expansion, pipes, redirects, or credentials in the plan.

## Safety Controls

Every generated plan contains:

```json
{
  "execution_authorized": false,
  "requires_human_review": true,
  "controls": {
    "provider_requests": 0,
    "external_delivery_attempts": 0,
    "cloud_mutations": 0,
    "terraform_apply": false,
    "deployment": false,
    "trading_mutation": false,
    "acknowledgement_mutation": false,
    "executor_included": false
  }
}
```

These values describe the planner implementation, not the future safety of an
operator who independently runs a listed command. The operator remains
responsible for reviewing the plan, verifying configuration hashes, confirming
local data availability, and separately authorizing each mutating step.

## Publication

The plan writer:

- requires a `.json` suffix;
- rejects symbolic-link output paths and symbolic-link ancestors;
- writes through a temporary file in the destination directory;
- flushes and fsyncs before atomic replacement;
- applies owner-only `0600` permissions; and
- reports zero writes when identical content is already present.

Generated plans belong under `.demo/`, which is ignored by Git.

## Boundary

This increment does not include a plan executor. It also intentionally excludes:

- a scheduler or background worker;
- provider ingestion;
- notification delivery;
- acknowledgement or lifecycle mutation;
- trade placement or blocking;
- automatic PostgreSQL startup;
- managed cloud persistence;
- deployment; or
- `terraform apply`.

A later increment may add a local/manual executor, but only with explicit plan-ID
matching, configuration re-verification, overlap protection, per-step evidence,
and no implicit external delivery.
