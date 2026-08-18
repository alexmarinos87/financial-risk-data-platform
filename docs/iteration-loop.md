# Iteration Loop

Use this loop when continuing work across the repo. The goal is steady,
reviewable improvement rather than unattended broad rewrites.

## Operating Rule

Each iteration should produce one bounded change set. Stop after the validation
gate and review the diff before starting another item.

## Loop

1. Pick one item from `docs/iteration-backlog.md`.
2. Confirm the objective, acceptance criteria, scope, risk, and forbidden
   actions.
3. Record the starting status and create a neutral branch if the change will be
   published.
4. Give one writer a bounded implementation scope.
5. Run independent read-only correctness and production-failure reviews for
   agent-authored logic or other material changes.
6. Triage findings, have the assigned writer make only accepted fixes, and
   repeat any affected read-only review.
7. Run the validation gate.
8. Generate the local evidence package with `make morning-review`.
9. Review the diff for behaviour, failure modes, unrelated changes, generated
   files, secrets, and overclaiming.
10. Open a pull request only after validation passes.
11. Merge only after checks pass and the human engineer understands and accepts
    the change.

Use `docs/engineering-delivery-workflow.md` for the change-budget heuristic and
human acceptance model, and `docs/agentic-workflows.md` for the review prompts.
A docs-only or trivial change may combine the two read-only review roles, but
the writer must not be the sole reviewer.

## Validation Gate

Use the full local gate for normal iterations:

```bash
make iteration-check
git diff --check
```

For docs-only changes:

```bash
make security-check
git diff --check
```

For Docker-backed database changes, add:

```bash
make local-db-up
make consistency-demo
make local-db-down
```

For Python-only changes:

```bash
make security-check
make quality-check
make readiness-check
```

## Stop Conditions

Stop and ask for review before continuing when:

1. A validation command fails.
2. The change touches AWS credentials, IAM, OIDC, deployment workflows, or
   database schemas.
3. A tool is missing and the closest substitute is not enough.
4. The diff grows beyond one coherent change.
5. The task would need `terraform apply`, a cloud deploy, or destructive data
   operations.

## Morning Or End-Of-Run Review

Before publishing:

1. Read `git status --short --branch`.
2. Check `git diff --stat`.
3. Confirm `.demo/`, `.sandbox/`, `data/`, `.terraform/`, and caches are not
   staged.
4. Confirm `make iteration-check` passed or document exactly what was not run.
5. Run `make morning-review` and inspect its risk-prioritised file list.
6. Use neutral branch, commit, and pull request wording.

## Short Prompt Template

```text
Objective:
Pick the next item from docs/iteration-backlog.md and implement only that item.

Scope:
Stay within the files listed for the selected item.

Acceptance criteria:
Use the selected backlog item.

Validation:
Run make iteration-check and git diff --check.

Review:
Use a separate read-only correctness review and production-failure challenge.
Return accepted fixes to the writer and repeat affected evidence. Then run
make morning-review.

Do not:
Deploy AWS, run terraform apply, change secrets, delete unrelated files, or
start a second backlog item. Stop for human acceptance before merge or deploy.
```
