# Agent Roles

Use these roles when splitting work across multiple bounded tasks. The lead
engineer owns scope, review, merge, deployment, and production-risk decisions.

## Role Matrix

| Role | Owns | Can Change | Must Not Do | Validation |
| --- | --- | --- | --- | --- |
| Delivery Lead | Task framing and final review | Docs and task briefs | Hide failed checks or merge without review | `git status --short --branch`, `git diff --check` |
| Pipeline Engineer | Ingestion, validation, normalisation, deduplication, windowing | `src/ingestion/`, `src/processing/`, `src/orchestration/`, targeted tests | Change storage or warehouse contracts without updating docs/tests | `make lint && make test && make readiness-check` |
| Analytics And Data Quality Engineer | Returns, volatility, risk metrics, thresholds | `src/analytics/`, `config/risk_thresholds.yaml`, targeted tests | Weaken thresholds without explanation or change ingestion/storage contracts silently | `make security-check && make lint && make test && make readiness-check` |
| Warehouse Engineer | PostgreSQL loading, schemas, reconciliation | `src/warehouse/`, `sql/`, warehouse docs, targeted tests | Change source semantics without a consistency check | `make clean-generated && make run-demo && make load-postgres-dry-run` |
| Source Systems Engineer | PostgreSQL/MongoDB playground and source-document mapping | `docker-compose.yml`, `mongo/`, source SQL, walkthrough docs | Introduce production credentials or external dependencies | `make security-check && make test`; Docker checks when available |
| Infrastructure Scaffold Engineer | Disabled-by-default AWS and Kubernetes scaffolding | `infra/terraform/`, `deploy/`, infrastructure docs | Run `terraform apply`, deploy workflows, or enable resources by default | `make security-check && make infrastructure-check` |
| Kubernetes And Delivery Engineer | Container, CI, Kubernetes overlays, deploy workflow hygiene | `Dockerfile`, `.github/workflows/`, `deploy/kubernetes/`, `deploy/README.md` | Apply to clusters, trigger deploy workflows, weaken pod security, or change prod without dev parity | `make security-check && make infrastructure-check` |
| Quality Reviewer | Tests, linting, generated-data hygiene | `tests/`, `Makefile`, CI checks | Rewrite production logic except for the smallest necessary fix | `make security-check && make readiness-check` |
| Security Reviewer | Secrets, generated files, cloud guardrails, deploy triggers | Security docs, checks, CI guardrails | Approve risky changes without the lead engineer | `make security-check` |
| Demo Narrator | Interview walkthroughs and expected outputs | `README.md`, `docs/`, demo fixtures if needed | Overclaim production usage of scaffolded services | `make readiness-check` when demo behaviour changes |

## Coordination Rules

1. Give each role a disjoint write scope before work starts.
2. Keep one role responsible for final integration.
3. Prefer one writer and one reviewer for high-risk areas.
4. Never let infrastructure and security roles approve their own risky changes.
5. Keep cloud deploys, Terraform apply, branch merges, and destructive database
   actions human-approved.
6. Require validation evidence in the final handoff.

## Overnight Use

Overnight work should be validation-first. A safe overnight loop can run checks,
produce logs, and surface failures for review in the morning.

Do not leave a role unattended with permission to:

1. Push to a remote branch.
2. Merge a pull request.
3. Trigger deployment workflows.
4. Run `terraform apply`.
5. Use cloud credentials.
6. Delete data outside generated local output paths.

Use `docs/overnight-sandbox.md` for the safe local runbook.
