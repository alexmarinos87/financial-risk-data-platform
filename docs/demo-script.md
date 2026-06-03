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

## Questions To Be Ready For

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
