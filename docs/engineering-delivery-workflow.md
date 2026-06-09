# Engineering Delivery Workflow

Use this note to explain how larger repo changes are prepared, reviewed, and
validated.

## Interview Position

The value is not that implementation work becomes unchecked. The value is that
routine implementation can move faster while the engineer stays responsible for
the important work:

1. Defining the objective and acceptance criteria.
2. Choosing the smallest useful scope.
3. Reviewing the design and diff.
4. Running validation before merge.
5. Keeping generated data, secrets, and cloud deploys out of accidental commits.
6. Deciding when a change is safe enough to merge or deploy.

In this repo, that model is visible through small modules, focused tests,
repeatable demo commands, local database walkthroughs, and disabled-by-default
AWS infrastructure.

## What Can Be Delegated

Good delegated tasks in this codebase:

1. Add a focused test around validation, deduplication, storage, or loading.
2. Extend a walkthrough with commands and expected output.
3. Implement a small loader or adapter with tests.
4. Refactor a module while preserving public CLI behaviour.
5. Prepare disabled-by-default infrastructure scaffolding.
6. Investigate a failed check and propose the smallest fix.

Poor delegated tasks without close review:

1. IAM and secret handling.
2. Production deploys.
3. Terraform apply or cloud resource creation.
4. Destructive database migrations.
5. Broad rewrites before the target behaviour is pinned down.
6. Business-critical risk logic without tests and manual review.

## Standard Task Brief

```text
Objective:
<What outcome should exist after the change?>

Scope:
<Which files or areas are in bounds?>

Acceptance criteria:
<What must be true in the repo?>

Validation:
<Which commands must run?>

Git workflow:
<Branch, commit, PR, merge expectations>

Do not:
<Anything destructive, costly, or out of scope>
```

## Validation Ladder

Use the smallest validation set that matches the risk.

| Change Type | Validation |
| --- | --- |
| Docs only | `git diff --check` |
| Python logic | `make lint && make test` |
| Demo path | `make readiness-check` |
| PostgreSQL loader | `make clean-generated && make run-demo && make load-postgres-dry-run` |
| Local database walkthrough | `make local-db-up && make consistency-demo && make local-db-down` |
| Terraform scaffold | `terraform -chdir=infra/terraform fmt -check -diff && terraform -chdir=infra/terraform init -backend=false && terraform -chdir=infra/terraform validate` |

If Docker or Terraform is not installed locally, state that clearly and run the
nearest available check.

## Monitoring Evidence

The repo has lightweight monitoring signals that are useful in a demo:

1. Pipeline summary output from `make run-demo`.
2. Late-event and duplicate-rate status in `.demo/pipeline-summary.json`.
3. Volatility status and value-at-risk output in the pipeline summary.
4. PostgreSQL consistency checks in `sql/consistency_checks.sql`.
5. Operational inspection queries in `sql/ops_queries.sql`.
6. Local source-to-warehouse reconciliation in `docs/data-consistency-walkthrough.md`.

In a managed AWS version, the same model would become CloudWatch metrics,
pipeline failure alarms, RDS/Aurora query checks, DocumentDB collection counts,
and S3 partition freshness checks. The current repo prepares that path without
creating billable resources by default.

## Review Questions

Before accepting a change, ask:

1. Does the diff solve the stated objective?
2. Are unrelated files untouched?
3. Are generated outputs excluded?
4. Are tests targeted at the risky behaviour?
5. Did the validation commands actually run?
6. Are unavailable checks disclosed?
7. Are cloud resources still disabled by default?
8. Are secrets absent?
9. Is the explanation accurate without overstating production usage?

## Short Talk Track

I use delegated implementation for bounded tasks, but I keep engineering
ownership. My job is to frame the problem, constrain scope, review the diff,
run validation, and make the merge or deployment decision. This repo shows that
model through repeatable tests, demo commands, source-to-warehouse consistency
checks, and infrastructure that is prepared but not deployed by accident.
