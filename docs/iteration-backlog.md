# Iteration Backlog

This backlog keeps future work bounded. Pick one item at a time and use
`docs/iteration-loop.md`.

## 1. Infrastructure Validation Evidence

Status: validated

Scope:

1. `Makefile`
2. `deploy/kubernetes/`
3. `infra/terraform/`
4. `deploy/README.md`
5. `docs/security-protocols.md`

Acceptance criteria:

1. `make infrastructure-check` renders dev/prod Kubernetes overlays.
2. Terraform fmt/init/validate runs without applying infrastructure.
3. Terraform provider lock file is tracked.
4. Kustomize config files are loaded without disabling load restrictions.
5. Deploy workflow validates and applies the same rendered manifest.

Validation:

```bash
make security-check
make infrastructure-check
make readiness-check
git diff --check
```

Why it matters:

This turns previously scaffolded infrastructure into something that can be
validated locally without creating cloud resources.

## 2. Local Database End-To-End Evidence

Status: validated without Docker; run the Docker-backed validation on a machine
with Docker installed

Scope:

1. `docker-compose.yml`
2. `sql/`
3. `mongo/init/`
4. `docs/postgres-mongodb-walkthrough.md`
5. `docs/data-consistency-walkthrough.md`

Acceptance criteria:

1. Docker Compose starts PostgreSQL and MongoDB locally.
2. Seeded MongoDB and PostgreSQL examples match the documented source story.
3. `make consistency-demo` produces clear pass/fail reconciliation output.
4. Walkthrough docs include expected commands and outputs.

Validation:

```bash
make security-check
make local-db-up
make consistency-demo
make local-db-down
make readiness-check
```

Why it matters:

This gives a concrete answer for source-system handling, warehouse loading, and
source-to-curated reconciliation.

## 3. Data Quality Test Expansion

Status: validated

Scope:

1. `src/analytics/data_quality.py`
2. `src/processing/deduplicator.py`
3. `tests/unit/`
4. `docs/demo-script.md`

Acceptance criteria:

1. Add focused tests for late-event and duplicate-rate boundary behaviour.
2. Confirm critical/warn/ok status transitions are covered.
3. Demo script explains why the sample intentionally produces critical quality
   statuses.

Validation:

```bash
make security-check
make test
make readiness-check
```

Why it matters:

It improves the risk-control story: the platform does not just process data, it
detects quality issues and makes them visible.

## 4. Operational Runbook

Status: validated

Scope:

1. `docs/`
2. `sql/ops_queries.sql`
3. `Makefile`

Acceptance criteria:

1. Add a runbook for failed pipeline runs, late-data spikes, duplicate spikes,
   and warehouse count mismatches.
2. Link each failure mode to an inspection query or command.
3. Avoid claiming production alerting that is not implemented.

Validation:

```bash
make security-check
git diff --check
```

Why it matters:

It helps explain how an engineer would operate the platform, not just build it.

## 5. Backfill And Idempotency Story

Status: validated

Scope:

1. `src/orchestration/backfill.py`
2. `src/orchestration/locks.py`
3. `tests/integration/`
4. `docs/failure-scenarios.md`

Acceptance criteria:

1. Confirm tests cover repeated backfill runs and lock handling.
2. Add a small missing test if a realistic regression is uncovered.
3. Document the idempotency trade-off clearly.

Validation:

```bash
make security-check
make test
make readiness-check
```

Why it matters:

Backfills, retries, and idempotency are core data-platform interview topics.

## 6. Demo Narrative Tightening

Status: validated

Scope:

1. `docs/demo-script.md`
2. `docs/interview-stories.md`
3. `docs/architecture.md`
4. `README.md`

Acceptance criteria:

1. Create a concise 5-minute walkthrough path.
2. Map each command to the engineering decision it proves.
3. Keep wording accurate for scaffolded services versus locally validated
   behaviour.

Validation:

```bash
make security-check
make readiness-check
git diff --check
```

Why it matters:

It makes the repo easier to explain under interview pressure.

## 7. CI Coverage Split

Status: validated

Scope:

1. `.github/workflows/ci.yml`
2. `Makefile`
3. `docs/security-protocols.md`

Acceptance criteria:

1. CI separates security, Python readiness, and infrastructure validation into
   readable jobs.
2. Infrastructure validation still does not deploy or apply infrastructure.
3. PR failures are easier to diagnose from job names.

Validation:

```bash
make security-check
make infrastructure-check
make readiness-check
```

Why it matters:

It makes the repository look closer to a real team workflow and improves
reviewability.
