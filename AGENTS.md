# Agent Operating Guide

Use this file as durable project guidance for coding agents working in this
repository.

## Project Purpose

This is a portfolio-grade data engineering platform focused on pipeline
reliability:

1. Ingest market-like source events and external signals.
2. Validate, normalise, and deduplicate records.
3. Write raw and curated partitioned parquet outputs.
4. Load curated outputs into PostgreSQL-style warehouse tables.
5. Reconcile source, raw, curated, and warehouse counts.
6. Document AWS deployment and managed database paths without deploying by
   default.

Prioritise clear engineering evidence over broad feature sprawl.

## Default Commands

Use these commands for normal validation:

```bash
make lint
make test
make security-check
make readiness-check
```

Expected test suite result:

```text
42 passed
```

When Docker is available, validate the local source-to-warehouse loop:

```bash
make local-db-up
make consistency-demo
make local-db-down
```

When Terraform is available, validate infrastructure changes:

```bash
terraform -chdir=infra/terraform fmt -check -diff
terraform -chdir=infra/terraform init -backend=false
terraform -chdir=infra/terraform validate
```

If Docker or Terraform is unavailable, state that clearly in the final summary
and run the closest local validation that does not require those tools.

## Engineering Rules

1. Prefer small, defensible changes with tests.
2. Keep generated local outputs out of git.
3. Do not commit `data/`, `.demo/`, caches, virtual environments, or local
   database volumes.
4. Keep AWS resources disabled by default unless the user explicitly asks to
   deploy.
5. Do not run `terraform apply`, cloud deploy workflows, or destructive cloud
   commands without explicit user confirmation.
6. Do not add streaming/Kinesis features unless specifically requested; the
   stronger interview story is Airbyte-style ELT, PostgreSQL, MongoDB/DocumentDB
   mapping, S3/Lambda, data quality, and backfills.
7. When adding a new feature, update the relevant walkthrough or preparation
   doc if it changes how the project should be explained.
8. Use `docs/agent-roles.md` when splitting work across multiple delegated
   tasks.
9. Use `docs/overnight-sandbox.md` for unattended validation loops.
10. Use `docs/security-protocols.md` for cloud, deploy, secret, and ownership
   controls.

## Git Workflow

Unless the user asks only for analysis:

1. Inspect `git status --short --branch` before editing.
2. Preserve unrelated user changes.
3. Use a neutral branch name when asked to create a branch.
4. Stage only files that belong to the task.
5. Use neutral commit and pull request wording.
6. Run checks before pushing.

Do not mention internal tool names in commit messages or pull request titles
unless the user explicitly asks for that.

## Overnight Sandbox Rules

Unattended overnight work must be validation-first. It can run local checks and
write logs under `.sandbox/`, but it must not push, merge, deploy, run
`terraform apply`, or use cloud credentials unless the user explicitly approves
that exact action.

Use:

```bash
make sandbox-once
make overnight-sandbox
```

## Validation Expectations

For Python or pipeline changes:

```bash
make lint
make test
make security-check
```

For demo-path changes:

```bash
make readiness-check
```

For PostgreSQL/MongoDB local-playground changes:

```bash
.venv/bin/python -c "import yaml; yaml.safe_load(open('docker-compose.yml', encoding='utf-8'))"
make lint
make test
```

Also run Docker-based checks if Docker is available.

For Terraform changes, run Terraform format and validate if Terraform is
available. Keep optional managed databases behind explicit `create_*` flags that
default to `false`.

## Documentation Style

Docs should be interview-useful:

1. Explain the engineering decision.
2. State the trade-off.
3. Point to concrete files or commands.
4. Avoid overclaiming production use of tools that are only scaffolded or
   locally demonstrated.

Preferred wording for tool gaps:

```text
The repo demonstrates the engineering pattern around the tool: source landing,
validation, deduplication, idempotent loading, data quality, and backfill
behaviour. It does not claim production ownership of every managed service.
```

## Risk Guardrails

High-risk changes require extra care:

1. Terraform/cloud resources.
2. Database schemas and load logic.
3. Deployment workflows.
4. Data quality thresholds.
5. Backfill and locking behaviour.
6. Authentication, secrets, IAM, and network access.

For these, include tests or a concrete validation note. If a tool is unavailable
locally, say what was not run.
