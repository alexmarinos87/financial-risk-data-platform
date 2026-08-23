# Model Approval Evidence

## Outcome

The repository records append-only model approval evidence and enforces it at the
method-aware portfolio risk-limit boundary:

```text
supported attribution model + fixed parameters
  -> deterministic contract fingerprint
  -> idempotent approval event
  -> optional targeted revocation event
  -> immutable PostgreSQL history
  -> deterministic current approval status
  -> pre-read method-policy gate
  -> risk-limit evaluation only when permitted
```

The pure identity and validation contract is implemented in
`src/warehouse/model_approval_contract.py`. PostgreSQL mutation is implemented in
`src/warehouse/model_approval_registry.py`. Runtime enforcement is implemented
in `src/warehouse/model_approval_gate.py` and called by
`src/orchestration/run_method_aware_portfolio_risk_limits.py`.

This is evidence governance, not an external approval service and not a claim
that a model is suitable for every use.

## Supported Contracts

The first version deliberately supports only the two analytical contracts that
exist in the repository.

### Sample covariance baseline

```text
attribution_model_version = portfolio-attribution-v1
weighting_method = constant_weight_daily_rebalanced
covariance_method = sample_annualized
correlation_method = pearson
fixed_parameters = {
  annualization_days: 252,
  degrees_of_freedom: 1,
  estimator: sample
}
```

### Fixed-decay EWMA

```text
attribution_model_version = portfolio-attribution-ewma-v1
weighting_method = constant_weight_daily_rebalanced
covariance_method = ewma_zero_mean_lambda_0_94_annualized
correlation_method = implied_from_ewma_covariance
fixed_parameters = {
  annualization_days: 252,
  decay: 0.94,
  mean_assumption: zero_daily
}
```

Mixed or unknown model/method tuples fail before database mutation or analytical
input access. The fixed-parameter document is canonical JSON and participates in
the deterministic `model-contract-v1-*` fingerprint.

## Approval Identity

An approval event records:

- use case;
- complete model and method tuple;
- canonical fixed parameters;
- contract fingerprint;
- operator request ID;
- reviewer identity;
- review timestamp; and
- bounded review reason.

The deterministic `model-approval-v1-*` identity binds the use case, contract
fingerprint, request ID and stored approval timestamp.

The registry first resolves the unique request grain. Retrying the same request
with omitted timestamps returns the original persisted event. Reusing the
request ID with different reviewer, reason, timestamp or contract content fails
closed.

Approval never updates an analytical record or risk-limit policy.

## Revocation Identity

A revocation is a separate append-only event that targets one approval. It
records:

- approval ID;
- request ID;
- reviewer identity;
- revocation timestamp; and
- bounded reason.

The `model-approval-revocation-v1-*` identity binds the target approval, request
ID and stored revocation timestamp. A revocation timestamp cannot predate its
approval. Reapproval requires a new approval request and therefore creates a new
history row rather than removing the revocation.

## Runtime Approval Gate

Method-aware risk-limit execution resolves a deterministic
`model-approval-gate-v1` decision after policy and portfolio validation but
**before attribution Parquet is read and before any risk-limit record is
published**.

The baseline policy is explicit:

```text
portfolio-attribution-v1 + sample_annualized + pearson
  -> baseline_exempt
  -> no PostgreSQL approval query
```

The non-baseline fixed-decay EWMA policy is explicit:

```text
portfolio-attribution-ewma-v1
  + ewma_zero_mean_lambda_0_94_annualized
  + implied_from_ewma_covariance
  -> query current_model_approval_status
  -> approved: continue
  -> absent or revoked: fail before attribution read
```

The gate uses the declared use case:

```text
portfolio-risk-limit-evaluation
```

It queries one current row by `use_case + contract_fingerprint`. Duplicate,
unknown or incompatible status evidence fails closed. The DSN is selected from:

```text
--approval-dsn
MODEL_APPROVAL_POSTGRES_DSN
WAREHOUSE_POSTGRES_DSN
local Docker PostgreSQL default
```

The sample baseline never opens that connection.

A successful run summary includes a `model_approval_gate` object containing:

- deterministic gate evidence ID;
- contract and method-policy fingerprints;
- decision (`baseline_exempt` or `approved`);
- whether approval was required;
- approval ID, timestamp and reviewer when applicable.

It excludes the free-text review reason and fixed-parameter document. Approval
identity remains run-level governance evidence; it does not alter analytical
attribution or risk-limit calculation IDs. Reapproval therefore changes the gate
evidence ID without manufacturing duplicate analytical facts.

## Operator Commands

Apply the schema to an existing local PostgreSQL database:

```bash
docker compose exec -T postgres \
  psql -U risk_user -d risk_platform \
  < sql/model_approval_schema.sql
```

Approve the fixed-decay EWMA contract for risk-limit evaluation:

```bash
.venv/bin/python -m src.warehouse.model_approval_registry approve \
  --use-case portfolio-risk-limit-evaluation \
  --attribution-model-version portfolio-attribution-ewma-v1 \
  --weighting-method constant_weight_daily_rebalanced \
  --covariance-method ewma_zero_mean_lambda_0_94_annualized \
  --correlation-method implied_from_ewma_covariance \
  --request-id MODEL-2026-001 \
  --approved-by reviewer@example.test \
  --reason "Reviewed for bounded local portfolio risk-limit evidence."
```

Run the approved EWMA policy:

```bash
MODEL_APPROVAL_POSTGRES_DSN="postgresql://risk_user:risk_password@localhost:5433/risk_platform" \
.venv/bin/python -m src.orchestration.run_method_aware_portfolio_risk_limits \
  --method-policy-id us-tech-ewma \
  --end-date 2026-03-31 \
  --summary-json .demo/ewma-risk-limits.json
```

Revoke one exact approval:

```bash
.venv/bin/python -m src.warehouse.model_approval_registry revoke \
  --approval-id model-approval-v1-0123456789abcdef01234567 \
  --request-id MODEL-REVOKE-2026-001 \
  --revoked-by reviewer@example.test \
  --reason "Approval withdrawn after model review."
```

A subsequent EWMA risk-limit run then fails before reading attribution data. A
new approval request can reapprove the same immutable contract without deleting
history.

The JSON registry summaries omit the free-text reason and fixed-parameter
document. They return deterministic identifiers and non-sensitive contract
metadata.

## PostgreSQL Contract

The history tables are:

```text
risk_platform.model_approvals
risk_platform.model_approval_revocations
```

Database triggers reject `UPDATE` and `DELETE` against both tables. Revocation
inserts are validated against the target approval and its timestamp.

Queryable views are:

```text
risk_platform.model_approval_event_history
risk_platform.current_model_approval_status
risk_platform.current_model_approvals
risk_platform.revoked_model_approvals
```

The current grain is:

```text
use_case + contract_fingerprint
```

Within that grain, the newest approval is selected by:

```text
approved_at DESC, approval_id DESC
```

A revocation of that approval makes the current status `revoked`. A later
approval request creates a new current `approved` row. Older approvals and
revocations remain queryable in history.

## Executable Evidence

The PostgreSQL CI job:

1. initializes `sql/model_approval_schema.sql` after the method-aware schemas;
2. proves the EWMA gate rejects an absent approval;
3. creates an EWMA approval and proves an identical retry is idempotent;
4. proves the gate binds that approval;
5. revokes it, proves revocation replay is idempotent and proves the gate rejects
   the revoked state;
6. creates a later reapproval and proves the gate binds the new approval with a
   new deterministic gate evidence ID;
7. rejects conflicting request reuse;
8. proves direct update and delete attempts fail; and
9. runs `sql/model_approval_consistency_checks.sql`.

Unit tests separately prove the sample baseline returns `baseline_exempt`
without invoking an approval reader and that gate failure occurs before the
attribution reader or publisher is called.

The consistency suite checks history counts, references, timestamps, unique
request grains, latest-selection semantics, status partitioning, supported
contracts and enabled append-only triggers.

## Boundary

The implemented gate is deliberately narrow. It does not provide:

- external maker-checker workflow;
- authentication or directory integration;
- digital signatures;
- model validation documents;
- automated policy activation;
- trade or position controls;
- deployment or Terraform apply.

It governs only method-aware portfolio risk-limit execution. Other analytical
commands remain evidence-producing tools and do not imply approval for a
business use.
