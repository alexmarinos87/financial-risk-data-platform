# Operational Runbook

This runbook explains how to inspect local pipeline failures and data quality
issues. It is intentionally local and evidence-focused. The repo does not claim
production alerting, paging, or managed-service ownership.

## First Checks

Start with the smallest reproducible checks:

```bash
make security-check
make readiness-check
```

If local databases are available:

```bash
make local-db-up
make consistency-demo
make local-db-down
```

Use `make local-db-down` after database investigation so local volumes do not
linger between demo runs.

## Failed Pipeline Run

Symptoms:

1. `make readiness-check` fails during `run-demo`.
2. No new curated parquet output is written.
3. The pipeline raises a validation error before warehouse loading.

Inspect:

```bash
.venv/bin/python -m src.orchestration.run_pipeline \
  --input tests/fixtures/demo_events.json \
  --late-seconds 60 \
  --vol-window 2 \
  --summary-json .demo/pipeline-summary.json
```

What to check:

1. Required-field failures point to missing source contract fields.
2. Null-field failures point to incomplete required source values.
3. Numeric-value failures point to non-positive prices, negative volumes, or
   non-numeric payloads.
4. Lock errors point to overlapping live and backfill writes for the same
   partition.

Useful evidence:

```text
src/orchestration/run_pipeline.py
src/analytics/data_quality.py
tests/integration/test_run_pipeline_curated.py
```

## Late-Data Spike

Symptoms:

1. `late_status` is not `ok`.
2. The latest run summary reports a high late rate.
3. Downstream windows may need replay or review.

Inspect the latest local demo summary:

```bash
cat .demo/pipeline-summary.json
```

With PostgreSQL running, inspect the latest data quality status:

```bash
make postgres-shell
```

Then run query 1 from `sql/ops_queries.sql`:

```sql
SELECT
    ts_ingest,
    total_events,
    late_events,
    ROUND((late_rate * 100)::numeric, 2) AS late_rate_pct,
    late_status
FROM risk_platform.latest_data_quality_status;
```

Decision:

If the late data is expected source delay, keep the run and explain the
correction window. If the source delay is unexpected, rerun from raw partitions
after the source issue is understood.

## Duplicate Spike

Symptoms:

1. `duplicate_status` is not `ok`.
2. Source records exceed distinct event IDs.
3. Raw output remains deduplicated, but the quality metric is critical.

Inspect source-side duplicates in MongoDB:

```bash
make mongo-shell
```

```javascript
db.market_events_source.aggregate([
  { $group: { _id: "$eventId", count: { $sum: 1 } } },
  { $match: { count: { $gt: 1 } } }
])
```

Inspect warehouse quality status with query 2 from `sql/ops_queries.sql`.

Decision:

Duplicates are not automatically a failed run when deduplication succeeds. They
are still operational evidence: the upstream may be retrying sends or replaying
old batches.

## Warehouse Count Mismatch

Symptoms:

1. `make check-postgres-consistency` returns a row with `status = fail`.
2. Raw warehouse counts do not match deduped data quality counts.
3. Curated table counts do not match expected demo counts.

Run:

```bash
make check-postgres-consistency
```

The checks in `sql/consistency_checks.sql` compare source audit counts, raw
events, curated tables, and data quality metrics.

Decision:

If source and raw disagree, inspect deduplication and raw loading. If raw and
curated disagree, inspect transformation output and warehouse upsert keys. If
only expected demo counts fail, confirm whether the fixture changed and update
the documented expectation in the same change.

## Freshness Issue

Symptoms:

1. Curated tables have old `ts_ingest` values.
2. Risk summaries are missing for recent source data.
3. A downstream user sees stale warehouse output.

With PostgreSQL running, use query 5 from `sql/ops_queries.sql` to compare
freshness across curated tables.

Decision:

If all tables are stale, inspect orchestration and source landing. If only one
curated table is stale, inspect that transformation and its load specification
in `src/warehouse/postgres_loader.py`.

## Backfill Or Replay

Use backfill when a raw partition needs deterministic replay after a source
delay, bug fix, or transformation change.

Evidence to inspect:

```text
src/orchestration/backfill.py
src/orchestration/locks.py
tests/integration/test_backfill.py
docs/failure-scenarios.md
```

Guardrail:

Do not create a second correction path for demos. Replay from raw partitions so
the recovery path uses the same validation, transformation, and write logic as
the normal pipeline.
