# Agentic Workflow Guide

This guide explains how to use coding agents effectively in this repository.
The aim is to delegate larger tasks while keeping engineering control through
branching, tests, review, and clear acceptance criteria.

## Autonomy Levels

Use the smallest autonomy level that fits the task.

| Level | Use When | Agent Can Do | Human Must Do |
| --- | --- | --- | --- |
| 0: Advice | You are deciding direction | Explain options, risks, and next steps | Choose the path |
| 1: Local Change | Scope is clear and low-risk | Edit files and run tests locally | Review and accept the final diff |
| 2: Branch And PR | Work is multi-file but bounded | Branch, commit, push, open PR | Accept the change before merge |
| 3: PR Follow-Through | PR has CI/review feedback | Inspect failures, patch, rerun checks | Accept the final revision before merge |
| 4: Cloud/Infra | AWS, Terraform, deploys, secrets | Prepare scaffolding and plans | Approve apply/deploy/secret changes |

For this repo, default to Level 1 or 2. Use Level 4 only for disabled-by-default
infrastructure scaffolding unless explicitly deploying.

## Good Task Shape

Strong prompts include:

1. Objective.
2. Scope boundaries.
3. Files or areas to inspect first.
4. Acceptance criteria.
5. Validation commands.
6. Git expectations.
7. What not to do.

Template:

```text
Objective:
Implement <specific outcome>.

Scope:
Touch only <directories/files>.

Acceptance criteria:
- <observable behaviour>
- <tests/docs updated>
- <commands pass>

Validation:
Run <commands>.

Git:
Create a neutral branch, commit, push, open a PR, wait for checks, and stop for
human acceptance before merge.

Do not:
- <risky or out-of-scope action>
```

## Ready-To-Use Prompts

### Multi-File Feature

```text
Implement a source-to-warehouse consistency improvement.

Scope:
- src/warehouse/
- sql/
- docs/
- tests/

Acceptance criteria:
- Existing pipeline demo still writes 6 raw events and 9 curated records.
- Loader dry-run prints counts for raw, returns, volatility, risk, and quality.
- Reconciliation SQL has pass/fail output.
- Tests cover batch collection and upsert SQL generation.

Validation:
- make security-check
- make lint
- make test
- make readiness-check

Do not deploy AWS resources or run terraform apply.
```

### Refactor

```text
Refactor the pipeline module to reduce duplication without changing behaviour.

Acceptance criteria:
- Public CLI arguments remain the same.
- Demo output counts remain the same.
- Tests pass.
- No unrelated formatting churn.

Validation:
- make security-check
- make lint
- make test
- make readiness-check
```

### Test Gap

```text
Find one meaningful missing test around data quality, storage idempotency, or
warehouse loading, then implement the smallest focused test.

Acceptance criteria:
- Test fails for a plausible regression.
- No production logic changes unless needed.
- Test name explains the behaviour.

Validation:
- make test
```

### Pull Request Review

```text
Review this branch for correctness risks, missing tests, and overclaiming in
documentation. Prioritise findings by severity with file references. Do not
make edits unless asked.
```

### Independent Correctness Review

```text
Do not modify files. Review this change against its task brief. Prioritise
correctness, maintainability, observability, performance, security,
testability, unnecessary complexity, and missing tests. Cite files and explain
the failure each finding could cause. Say explicitly when no finding is proven.
```

### Production Failure Challenge

```text
Do not modify files. Assume this passed its current tests. What can still fail
in production? Challenge input boundaries, partial failure, retries,
idempotency, concurrency, data volume, permissions, network dependencies,
rollout, rollback, monitoring, and cost. Separate demonstrated defects from
questions or optional hardening.
```

### Morning Package Synthesis

```text
Do not modify files. Use the local acceptance package, task brief, reviewer
findings, and final diff to prepare a human review summary. Explain the change,
architecture and important data/control paths, state and side effects,
assumptions, failure and recovery behaviour, security and permissions,
operational/performance/cost implications, test evidence, and unresolved
questions. Rank no more than ten concrete file locations I should inspect.
Separate recorded evidence from inference and leave the decision pending.
```

### CI Fix

```text
Inspect the failing CI check, identify the root cause, implement the smallest
fix, rerun the relevant local check, commit, push, and report the result.
```

### Infrastructure Scaffold

```text
Add disabled-by-default Terraform scaffolding for <resource>.

Requirements:
- Creation flag defaults to false.
- No public access by default.
- Use private subnet and security group variables.
- Include cost and teardown notes in docs.
- Do not run terraform apply.

Validation:
- make infrastructure-check
- make security-check
- make test
```

## Role Split

Use `docs/agent-roles.md` to split larger work into bounded roles. Keep write
scopes separate and require a final lead-engineer review before merge.

For unattended validation, use `docs/overnight-sandbox.md`; it never publishes.
For an explicitly approved scheduled candidate, use
`docs/overnight-development.md`. Its publishing mode remains disabled until the
runbook's repository-protection and least-privilege credential gates are proven.

For continued improvement work, use `docs/iteration-loop.md` and pick one item
from `docs/iteration-backlog.md`. Keep each iteration to one coherent change
set and stop after validation for review.

For the full implement, independent-review, production-challenge,
automated-gate, and human-acceptance sequence, use
`docs/engineering-delivery-workflow.md`. Run `make morning-review` after the
checks to create an ignored local evidence package; the package supports human
review and never approves a change.

## Acceptance Policy

Use `docs/engineering-delivery-workflow.md` as the single source of truth for
delegation boundaries, evidence freshness, risk review, and human acceptance.
This guide supplies prompts and autonomy mechanics; it does not weaken or
replace that policy.
