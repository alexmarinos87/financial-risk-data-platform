# Model Approval Evidence

## Outcome

This increment adds append-only evidence that one supported analytical model
contract was reviewed for a declared use:

```text
supported attribution model + fixed parameters
  -> deterministic contract fingerprint
  -> idempotent approval event
  -> optional targeted revocation event
  -> immutable PostgreSQL history
  -> deterministic current approval status
```

The pure identity and validation contract is implemented in
`src/warehouse/model_approval_contract.py`. PostgreSQL mutation is implemented in
`src/warehouse/model_approval_registry.py`.

This is evidence governance, not an external approval service and not a claim
that the model is suitable for every use.

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

Mixed or unknown model/method tuples fail before database mutation. The fixed
parameter document is canonical JSON and participates in the deterministic
`model-contract-v1-*` fingerprint.

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

Revoke one exact approval:

```bash
.venv/bin/python -m src.warehouse.model_approval_registry revoke \
  --approval-id model-approval-v1-0123456789abcdef01234567 \
  --request-id MODEL-REVOKE-2026-001 \
  --revoked-by reviewer@example.test \
  --reason "Approval withdrawn after model review."
```

The JSON summaries omit the free-text reason and fixed-parameter document. They
return deterministic identifiers and non-sensitive contract metadata.

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
2. creates an EWMA approval and proves an identical retry is idempotent;
3. revokes it and proves revocation replay is idempotent;
4. creates a later reapproval and verifies deterministic current status;
5. rejects conflicting request reuse;
6. proves direct update and delete attempts fail; and
7. runs `sql/model_approval_consistency_checks.sql`.

The consistency suite checks history counts, references, timestamps, unique
request grains, latest-selection semantics, status partitioning, supported
contracts and enabled append-only triggers.

## Boundary

This increment does **not** yet require an approval during risk-limit execution.
That enforcement is the next dependency-safe slice. It also does not provide:

- external maker-checker workflow;
- authentication or directory integration;
- digital signatures;
- model validation documents;
- automated policy activation;
- trade or position controls;
- deployment or Terraform apply.

The existing sample model remains the baseline. The following increment will
require current approval only when a governed method policy targets the
non-baseline EWMA contract.
