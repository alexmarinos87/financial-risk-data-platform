# Data Engineering Demo Script

Use this as a short walkthrough for a technical conversation. The goal is to
show pipeline engineering judgement: clear inputs, deterministic processing,
observable data quality, repeatable storage layout, and recovery behaviour.

## Setup Check

```bash
make setup
make test
```

Expected result: the test suite passes from a clean checkout after dependency
installation.

## Five-Minute Walkthrough Path

Use this when the conversation is time-boxed:

1. State the contract: source events become validated raw parquet, curated risk
   datasets, and PostgreSQL-style serving tables.
2. Run `make readiness-check`.
3. Point to the summary values: 7 source records become 6 raw events, 9 curated
   records, 1 duplicate, and 1 late event.
4. Show `src/orchestration/run_pipeline.py` for validation, deduplication,
   partition locking, and writes.
5. Show `src/storage/s3_writer.py` for deterministic partitioned output.
6. Show `sql/consistency_checks.sql` or `docs/data-consistency-walkthrough.md`
   for the source-to-warehouse reconciliation story.
7. Close with the trade-off: the repo prioritises repeatable batch reliability
   and local evidence over a live cloud deployment.

## Command Evidence Map

| Command | Decision it proves |
| --- | --- |
| `make security-check` | Generated output, obvious secrets, deploy guardrails, Kubernetes defaults, and Terraform creation flags are checked locally. |
| `make readiness-check` | Linting, tests, demo pipeline output, and warehouse dry-run loading work together. |
| `make infrastructure-check` | Kubernetes overlays render and Terraform validates without applying cloud resources. |
| `make consistency-demo` | With Docker available, source audit counts, raw records, curated rows, and warehouse checks reconcile. |

## Three-Minute Walkthrough

1. Start with the purpose.

   This project is a production-style batch data pipeline. It takes market
   events and optional external risk signals, validates and normalises them,
   writes immutable raw records, and publishes curated outputs for downstream
   consumption.

2. Show the pipeline entry point.

   Point to `src/orchestration/run_pipeline.py`. The important sequence is:
   load input, validate required fields, reject null or invalid numeric values,
   normalise symbols, deduplicate by `event_id`, compute curated metrics, lock
   affected partitions, and write partitioned parquet outputs.

3. Run the sample pipeline.

   ```bash
   make clean-generated
   .venv/bin/python -m src.orchestration.run_pipeline \
     --input tests/fixtures/demo_events.json \
     --late-seconds 60 \
     --vol-window 2 \
     --summary-json .demo/pipeline-summary.json
   ```

   Call out the summary values: raw events written, curated records written,
   duplicate rate, late rate, data quality status, affected partitions, and
   latest risk metrics.

   The demo fixture intentionally includes one duplicate business event and one
   late event. With the strict demo thresholds, those rates are reported as
   critical so the walkthrough shows visible data quality evidence instead of a
   happy-path-only run.

4. Show storage reliability.

   Point to `src/storage/s3_writer.py`. Writes are grouped by ingest-time
   partitions, use stable content-derived batch names, write to a temporary file
   first, and skip an existing target file. The same input can be replayed
   without duplicating files.

5. Show recovery and backfills.

   Point to `src/orchestration/backfill.py` and `src/orchestration/locks.py`.
   Backfills replay raw hourly partitions, maintain resume state, and use
   partition locks to avoid overlapping live and backfill writes.

6. Show operational readiness.

   Point to `.github/workflows/ci.yml`, `Dockerfile`, and `deploy/README.md`.
   The repo has automated tests, container packaging, and a Kubernetes CronJob
   deployment model with separate dev and prod overlays.

7. Show the warehouse and orchestration mapping if the conversation goes there.

   Point to `sql/postgres_schema.sql`, `sql/ops_queries.sql`,
   `docs/elt-mapping.md`, `docs/source-document-mapping.md`, and
   `docs/lambda-s3-orchestration.md`. These files explain how the local
   pipeline maps to a connector-based ELT flow, nested upstream records,
   PostgreSQL serving tables, and an S3/Lambda orchestration pattern.

## Questions To Be Ready For

Use `docs/interview-stories.md` for the longer rehearsal versions and
`docs/mock-interview.md` for timed practice.

What happens if the upstream sends duplicates?

The pipeline deduplicates on `event_id`, tracks duplicate rate in
`data_quality_metrics`, and uses deterministic batch filenames so replaying the
same payload does not create duplicate files.

What happens if a required field is missing or null?

The pipeline calculates data quality metrics first, then raises a validation
error with field-level counts. Bad records do not silently flow into curated
outputs.

How would this map to a PostgreSQL warehouse?

The raw parquet layer would remain the durable landing zone. Curated outputs
such as returns, volatility, data quality metrics, and risk summaries would be
loaded into warehouse tables with primary keys based on business keys and
window timestamps. Ops-facing SQL would read from those curated tables, not
from raw source payloads.

How would this map to an ELT connector tool?

The connector would handle extraction and landing from source systems. This
repo covers the downstream contract: schema checks, normalisation,
deduplication, partitioning, transformation, data quality evidence, and
repeatable backfill behaviour.

What would you improve next?

Add PostgreSQL DDL and warehouse load tests, add a source-document flattening
example for nested upstream records, and expose data quality alerts from the
pipeline summary.
