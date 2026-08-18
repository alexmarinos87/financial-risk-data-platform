# Financial Risk Data Platform

A production-style data platform that ingests market data and external risk
signals, validates and normalises events, and produces decision-ready analytics
such as returns, volatility, data quality and risk summaries.

This repository is intentionally structured like a small internal platform at a
bank or fintech. It prioritises reproducibility, replay safety, explicit data
grains and interview-ready engineering evidence.

## What It Does

1. Ingests market data and external signals.
2. Validates schemas, normalises symbols and deduplicates events.
3. Retains immutable, partitioned raw Parquet for replay.
4. Builds minute-oriented demo analytics and daily market-risk analytics.
5. Stores curated Parquet and demonstrates PostgreSQL serving and reconciliation.

## Quickstart

```bash
make setup
make test
make security-check
```

The Makefile uses `.venv/bin/python` by default, so commands work without
activating the environment after `make setup`. Dependency resolution is
constrained by `requirements.lock`, which is refreshed deliberately after a full
green CI run.

The equivalent manual setup is:

```bash
python3 -m venv .venv
PIP_CONSTRAINT=requirements.lock .venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pip check
.venv/bin/python -m pytest -q
```

## Run The Demo Pipeline

Run the sample event-to-curated path:

```bash
.venv/bin/python -m src.orchestration.run_pipeline
```

Provide a JSON list of market events:

```bash
.venv/bin/python -m src.orchestration.run_pipeline \
  --input tests/fixtures/sample_events.json
```

For the fuller local walkthrough with duplicates, a late event and curated
metrics:

```bash
make readiness-check
```

The demo writes `.demo/pipeline-summary.json` and `.demo/lineage.json`. The
lineage manifest traces source inventory, transformations, raw and curated
outputs, data-quality checks and the reporting-view dependency. See
`docs/demo-script.md` for the five-minute interview walkthrough.

Include landed external risk signals when running the sample pipeline:

```bash
.venv/bin/python -m src.orchestration.run_pipeline \
  --input tests/fixtures/sample_events.json \
  --signals path/to/signals.json
```

## Alpha Vantage Daily Risk Path

With an Alpha Vantage API key already present in the environment, run one
bounded source-to-curated daily-risk cycle:

```bash
export ALPHA_VANTAGE_API_KEY='set-locally-do-not-commit'
make daily-risk-demo SYMBOL=IBM
```

The command defaults to completed daily bars ending yesterday in UTC. Optional
bounds and model parameters can be supplied without changing code:

```bash
make daily-risk-demo \
  SYMBOL=IBM \
  START_DATE=2026-01-01 \
  END_DATE=2026-03-31 \
  MAX_RECORDS=100 \
  VOL_WINDOW=20 \
  VAR_WINDOW=60 \
  VAR_CONFIDENCE=0.95
```

The flow is deliberately separate from the minute-labelled demo pipeline:

```text
Alpha Vantage TIME_SERIES_DAILY
  -> validated, immutable raw market events
  -> one-day returns
  -> annualised rolling volatility
  -> historical VaR loss and maximum drawdown
  -> versioned curated Parquet
```

Curated datasets are written under `data/curated/daily_returns`,
`data/curated/daily_volatility` and `data/curated/daily_risk_summary`. Replaying
the same source history and parameters writes zero duplicate records. A late
historical observation produces a new calculation fingerprint rather than
silently replacing earlier analytical evidence. The command writes a
credential-free summary to `.demo/daily-risk-summary.json`.

No live provider request runs in CI or `make readiness-check`; tests inject or
land deterministic Alpha Vantage-shaped events. See
`docs/daily-risk-pipeline.md` for grains, formulas, replay behaviour and current
boundaries.

## Data Sources

The implemented external market source is Alpha Vantage daily time series. The
repository also demonstrates provider-neutral landed files and external risk
signals. Stooq, FRED and exchange-calendar integrations remain useful future
exercises rather than production-owned claims.

See `docs/data-model.md` for the shared event schemas.

## Repository Layout

See `docs/architecture.md` for the end-to-end design.

Additional preparation and operating notes:

0. `AGENTS.md` for durable project instructions used by coding agents
1. `docs/demo-script.md` for a short technical walkthrough
2. `docs/daily-risk-pipeline.md` for the real daily source-to-curated path
3. `docs/preparation-plan.md` for interview preparation
4. `docs/interview-stories.md` for interview story rehearsal
5. `docs/mock-interview.md` for timed interview practice
6. `docs/elt-mapping.md` for connector-based ELT mapping
7. `docs/source-document-mapping.md` for nested source document flattening
8. `docs/postgres-mongodb-walkthrough.md` for local source-to-warehouse inspection
9. `docs/data-consistency-walkthrough.md` for source-to-warehouse reconciliation
10. `docs/aws-managed-databases.md` for disabled-by-default database IaC
11. `docs/lambda-s3-orchestration.md` for AWS orchestration mapping
12. `sql/postgres_schema.sql` and `sql/ops_queries.sql` for warehouse examples
13. `docs/agentic-workflows.md` for larger delegated development workflows
14. `docs/engineering-delivery-workflow.md` for the controlled agent loop
15. `docs/agent-roles.md` for splitting work across bounded roles
16. `docs/overnight-sandbox.md` for safe unattended validation runs
17. `docs/overnight-development.md` for guarded candidate-branch controls
18. `docs/security-protocols.md` for local and cloud safety controls
19. `docs/operational-runbook.md` for local failure investigation
20. `docs/iteration-loop.md` and `docs/iteration-backlog.md` for continued iterations

## Local Database Playground

An optional Docker Compose playground starts PostgreSQL and MongoDB with seeded
demo data:

```bash
make local-db-up
make consistency-demo
make postgres-shell
make mongo-shell
make local-db-down
```

See `docs/postgres-mongodb-walkthrough.md` and
`docs/data-consistency-walkthrough.md` for the full inspection and reconciliation
flow. The PostgreSQL seed includes:

```text
symbol_dimension_history -> current_symbol_dimension -> finance_risk_semantic_model
```

This demonstrates SCD Type 2 history, current-dimension serving and a
dashboard-ready view over the original minute-oriented risk outputs. The daily
curated datasets are local Parquet contracts in the current slice; adding daily
warehouse tables is a separate schema decision.

## Performance Benchmark

Compare CSV scans with partitioned Parquet scans:

```bash
make benchmark-io
```

See `docs/performance-benchmark.md` for details.

## Generated Files

Pipeline runs write generated Parquet under `data/`; demo summaries can be
written under `.demo/`. Both are ignored by Git. Keep reusable sample inputs
under `tests/fixtures/` and run `make clean-generated` before a fresh demo.

## Security And Readiness Checks

```bash
make security-check
make quality-check
make readiness-check
```

`make quality-check` includes linting, type checking, the test suite and
`pip check`. CI installs through the reviewed constraints file before running
the same checks.

For validation-only unattended loops:

```bash
make sandbox-once
make overnight-sandbox
```

The sandbox writes ignored logs under `.sandbox/` and never pushes, merges,
deploys or runs `terraform apply`. For a bounded improvement iteration:

```bash
make iteration-check
make morning-review
```

The review package is evidence for human inspection, not approval.

## Deployment

The deployment scaffold uses Docker, GitHub Actions, Amazon ECR and Kubernetes
CronJobs with separate `dev` and `prod` overlays. See `deploy/README.md`.
Managed cloud resources remain disabled by default.

## Notes

This is a portfolio-grade platform. Local behaviour and tests substantiate its
claims; scaffolded cloud services are not described as production-owned.
