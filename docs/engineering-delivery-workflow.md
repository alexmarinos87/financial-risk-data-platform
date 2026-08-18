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

Risk and failure behaviour:
<State, side effects, recovery, permissions, and cost constraints>

Validation:
<Which commands must run?>

Git workflow:
<Branch, commit, PR, merge expectations>

Do not:
<Anything destructive, costly, or out of scope>
```

## Controlled Agent Loop

Optimise for the amount of code the lead engineer can safely accept, not the
number of agent passes. Prefer one coherent change of roughly 200 to 500
non-generated changed lines. This is a review heuristic rather than a quality
target: split sooner when a change crosses contracts, schemas, IAM, deployment,
or unrelated modules, and require an explicit reason for a larger diff.

Use this sequence for agent-authored logic and other material changes:

1. The delivery lead drafts the task brief and records the starting branch and
   status. The human engineer confirms the brief before one writer receives a
   bounded scope.
2. The writer implements the smallest change that meets the acceptance criteria
   and reports assumptions and checks actually run.
3. A separate read-only reviewer checks correctness, maintainability,
   observability, performance, security, testability, unnecessary complexity,
   and missing tests.
4. Another read-only pass assumes the tests pass and challenges production
   failure modes: invalid inputs, partial failure, retries, idempotency,
   concurrency, volume, permissions, dependencies, rollout, rollback,
   monitoring, and cost.
5. The delivery lead separates demonstrated defects from questions and optional
   hardening, then assigns accepted fixes to the writer.
6. The relevant automated gates run and the lead verifies their evidence.
7. `make morning-review` creates an ignored local evidence package for the final
   human review.

Reviewers do not approve their own changes, and repeated agreement between
agents is not an acceptance signal. The delivery lead triages and assembles
evidence; the human engineer alone accepts the final diff, merges, or authorises
deployment. Use the prompts in
`docs/agentic-workflows.md` and the role boundaries in `docs/agent-roles.md`.
Any fix invalidates review or validation evidence affected by that fix; repeat
the relevant pass before acceptance.

## Validation Ladder

Use the smallest validation set that matches the risk.

| Change Type | Validation |
| --- | --- |
| Docs only | `make security-check && git diff --check` |
| Python logic | `make security-check && make quality-check` |
| Demo path | `make readiness-check` |
| PostgreSQL loader | `make clean-generated && make run-demo && make load-postgres-dry-run` |
| Local database walkthrough | `make local-db-up && make consistency-demo && make local-db-down` |
| Terraform or Kubernetes scaffold | `make infrastructure-check` |
| Whole-repo iteration | `make iteration-check` |

If Docker or Terraform is not installed locally, state that clearly and run the
nearest available check.

## Morning Acceptance Package

After review and validation, run:

```bash
make morning-review
```

The command writes a Markdown report and machine-readable JSON evidence under
ignored `.sandbox/review-packages/`. It records the current Git state,
changed-file risk flags, the latest overnight summary when present, up to ten
files to inspect first, and the human acceptance questions. Automated evidence
is kept separate from reviewer assertions and the human decision remains
pending. Use `make morning-review REVIEW_BASE_REF=main` when the local `main`
branch, rather than the default `origin/main`, is the intended comparison base;
the command never fetches.

The command deliberately does not include a full diff, run validation, approve
the change, commit, push, merge, or deploy. Overnight evidence is historical
unless it can be tied to the reviewed Git state, so its timestamp and limits
must be considered before relying on it.

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

For unattended local validation, use `docs/overnight-sandbox.md`. The sandbox
runs readiness and security checks repeatedly, writes logs under `.sandbox/`,
and does not push, merge, deploy, or create cloud resources.

Scheduled candidate mode is currently disabled. Only after every activation
gate in `docs/overnight-development.md` passes may an explicitly approved run
prepare one arc42-bounded change. External publication is a separate capability
and requires the same protected-branch, CI-concurrency, least-privilege
credential, isolation, scope, and evidence gates. Even then it may open only a
draft PR; human acceptance and merge remain outside the scheduled run.

This scheduled-candidate runbook adds isolation, authorization, recurrence, and
publication gates only. The controlled agent loop in this document remains the
single source of truth for implementation, review, validation, and human
acceptance.

For continued improvement, use `docs/iteration-loop.md` and
`docs/iteration-backlog.md`. The queue keeps work small enough to review.

## Review Questions

Before accepting a change, ask:

1. Does the diff solve the stated objective?
2. Can I explain the important control and data paths?
3. Do I understand state, side effects, idempotency, and failure recovery?
4. Are unrelated files untouched and generated outputs excluded?
5. Are permissions, secrets, and network exposure appropriate?
6. Are operational, performance, and cost implications understood?
7. Are tests targeted at the risky behaviour?
8. Did the reported validation commands actually run for the state reviewed?
9. Are unavailable or stale checks disclosed?
10. Are cloud resources still disabled by default?
11. Is the explanation accurate without overstating production usage?
12. Can I defend the change without relying on an agent's approval?

## Short Talk Track

I use delegated implementation for bounded tasks, but I keep engineering
ownership. My job is to frame the problem, constrain scope, review the diff,
run validation, and make the merge or deployment decision. This repo shows that
model through repeatable tests, demo commands, source-to-warehouse consistency
checks, and infrastructure that is prepared but not deployed by accident.
