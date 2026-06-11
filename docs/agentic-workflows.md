# Agentic Workflow Guide

This guide explains how to use coding agents effectively in this repository.
The aim is to delegate larger tasks while keeping engineering control through
branching, tests, review, and clear acceptance criteria.

## Autonomy Levels

Use the smallest autonomy level that fits the task.

| Level | Use When | Agent Can Do | Human Must Do |
| --- | --- | --- | --- |
| 0: Advice | You are deciding direction | Explain options, risks, and next steps | Choose the path |
| 1: Local Change | Scope is clear and low-risk | Edit files and run tests locally | Review final diff |
| 2: Branch And PR | Work is multi-file but bounded | Branch, commit, push, open PR | Review PR before merge unless explicitly delegated |
| 3: PR Follow-Through | PR has CI/review feedback | Inspect failures, patch, rerun checks | Approve risky changes |
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
Create a neutral branch, commit, push, open PR, wait for checks, merge if green.

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

For overnight work, use `docs/overnight-sandbox.md`. The overnight flow is
validation-first and must not push, merge, deploy, or run cloud commands.

For continued improvement work, use `docs/iteration-loop.md` and pick one item
from `docs/iteration-backlog.md`. Keep each iteration to one coherent change
set and stop after validation for review.

## Review Checklist

Before accepting agent-authored changes, check:

1. Does the change solve the stated problem?
2. Are unrelated files untouched?
3. Are generated files excluded?
4. Do tests cover the risky behaviour?
5. Did validation actually run?
6. Are unavailable checks clearly disclosed?
7. Are docs accurate and not overstated?
8. Are cloud resources disabled by default?
9. Are secrets absent?
10. Is the diff small enough to review confidently?

## What To Delegate

Good candidates:

1. Adding focused tests.
2. Writing runbooks and walkthroughs.
3. Implementing small loaders or adapters.
4. Refactoring with stable behaviour.
5. Creating disabled-by-default infrastructure scaffolds.
6. Debugging CI failures.
7. Drafting PR descriptions and validation summaries.

Poor candidates without close review:

1. IAM permissions.
2. Secrets and credential handling.
3. Production deploys.
4. Destructive migrations.
5. Broad rewrites before requirements are clear.
6. Business-critical logic without tests.

## Human Review Model

Do not treat agent output as automatically correct. Treat it as a fast junior or
mid-level implementation draft that still needs engineering ownership.

For low-risk documentation or tests, review can be quick. For data loading,
schema, infrastructure, auth, or deployment work, review the diff carefully,
run checks, and reason through failure modes before merge.

The practical target is not "no human checks code". The target is:

```text
humans spend less time typing boilerplate
and more time reviewing design, risk, tests, and production impact
```
