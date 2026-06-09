# Financial Risk Data Platform

A production-style data platform that ingests market data and external risk signals, validates and normalizes events, and produces decision-ready analytics (returns, volatility, data quality, and risk summaries).

This repo is intentionally structured to resemble real internal data platforms at banks and fintechs. It is designed for clarity, reproducibility, and interview readiness.

## What It Does

1. Ingests market data and external signals
2. Validates schema, deduplicates, and normalizes events
3. Aggregates into time windows
4. Computes risk analytics and data quality metrics
5. Stores raw and curated datasets with partitioning

## Quickstart

1. Create a virtual environment and install dependencies.
2. Review configuration in `config/`.
3. Run unit tests to validate core logic.

Example commands:

```bash
make setup
make test
```

The Makefile uses `.venv/bin/python` by default, so commands can be run
without activating the virtual environment after `make setup`.

If you prefer to run the commands manually:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pytest -q
```

## Run The Demo Pipeline

Run a local end-to-end pipeline with a small sample dataset:

```bash
.venv/bin/python -m src.orchestration.run_pipeline
```

Provide your own JSON events (list of objects) if desired:

```bash
.venv/bin/python -m src.orchestration.run_pipeline --input tests/fixtures/sample_events.json
```

For a fuller local walkthrough with duplicates, a late event, and curated
metrics:

```bash
make readiness-check
```

Include landed external risk signals to enrich the risk summary:

```bash
.venv/bin/python -m src.orchestration.run_pipeline \
  --input tests/fixtures/sample_events.json \
  --signals path/to/signals.json
```

## Data Sources

This project is designed for free, legal, and finance-credible sources. Examples:

1. Stooq for historical market data
2. FRED for macroeconomic indicators
3. Exchange calendars for trading day logic

See `docs/data-model.md` for event schemas.

## Repository Layout

See `docs/architecture.md` for the end-to-end design.

Additional preparation notes:

0. `AGENTS.md` for durable project instructions used by coding agents
1. `docs/demo-script.md` for a short technical walkthrough
2. `docs/preparation-plan.md` for interview preparation
3. `docs/interview-stories.md` for interview story rehearsal
4. `docs/mock-interview.md` for timed interview practice
5. `docs/elt-mapping.md` for connector-based ELT mapping
6. `docs/source-document-mapping.md` for nested source document flattening
7. `docs/postgres-mongodb-walkthrough.md` for local source-to-warehouse inspection
8. `docs/data-consistency-walkthrough.md` for source-to-warehouse reconciliation
9. `docs/aws-managed-databases.md` for disabled-by-default RDS/Aurora/DocumentDB IaC
10. `docs/lambda-s3-orchestration.md` for AWS orchestration mapping
11. `sql/postgres_schema.sql` and `sql/ops_queries.sql` for PostgreSQL examples
12. `docs/agentic-workflows.md` for larger delegated development workflows
13. `docs/engineering-delivery-workflow.md` for the engineer-owned delivery model

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

See `docs/postgres-mongodb-walkthrough.md` for inspection commands and
`docs/data-consistency-walkthrough.md` for the full reconciliation flow.

## Performance Benchmark

Run the local I/O benchmark to compare CSV scans with partitioned Parquet scans:

```bash
make benchmark-io
```

See `docs/performance-benchmark.md` for details.

## Generated Files

Local pipeline runs write generated parquet output under `data/`. Demo summaries
can be written under `.demo/`. Both paths are ignored by git so repeatable runs
do not pollute commits. Keep reusable sample inputs under `tests/fixtures/`.
Use `make clean-generated` to remove local outputs and caches before a fresh
demo run.

## Deployment

The deployment scaffold uses Docker, GitHub Actions, Amazon ECR, and Kubernetes
CronJobs with separate `dev` and `prod` overlays. See `deploy/README.md`.

## Notes

This is a portfolio-grade platform scaffold. All modules are intentionally small but structured for expansion.
