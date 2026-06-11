# Architecture

The platform is organized into four layers:

1. Ingestion: market data and external risk signals
2. Raw storage: immutable, partitioned event storage
3. Processing: validation, deduplication, normalization, and windowing
4. Analytics: returns, volatility, external signal summaries, data quality, and risk summaries

The design emphasizes:

1. Reproducibility and deterministic backfills
2. Explicit trade-offs between cost and latency
3. Strong schema validation at ingestion
4. Measurable storage and query performance improvements

## Evidence Map

| Architecture decision | Local evidence |
| --- | --- |
| Validate before transformation | `src/orchestration/run_pipeline.py`, `src/analytics/data_quality.py`, `tests/unit/test_data_quality.py` |
| Keep raw data replayable | `src/storage/s3_writer.py`, `tests/integration/test_s3_writer.py` |
| Make backfills deterministic | `src/orchestration/backfill.py`, `src/orchestration/locks.py`, `tests/integration/test_backfill.py` |
| Serve stable warehouse outputs | `src/warehouse/postgres_loader.py`, `sql/postgres_schema.sql`, `sql/ops_queries.sql` |
| Reconcile source to warehouse | `sql/consistency_checks.sql`, `docs/data-consistency-walkthrough.md` |
| Validate infrastructure without deploying | `Makefile`, `deploy/kubernetes/`, `infra/terraform/` |

## Deployment Model

The deploy scaffold packages the pipeline as a Docker image and runs it as a
Kubernetes CronJob. GitHub Actions builds and pushes immutable images to ECR,
then applies the matching Kustomize overlay for the selected environment.

For this portfolio repo, cloud resources stay disabled and deployment remains
manual by default. The repository demonstrates the engineering pattern around
packaging, validation, deploy guardrails, data quality, and backfill behaviour;
it does not claim production ownership of every managed service.
