# Failure Scenarios

This note explains how the project handles realistic data-platform failures.
The common theme is to keep recovery deterministic: raw data remains replayable,
curated outputs are idempotent, and live/backfill overlap is blocked by
partition locks.

## Late-Arriving Data

Scenario:

An upstream provider sends an event after the expected ingest window. Windowed
metrics may need review because the event belongs to an earlier event-time
window.

Current behaviour:

1. The pipeline calculates late-event counts and late rate.
2. The run summary and `data_quality_metrics` record `late_status`.
3. Curated outputs carry the same quality context through `risk_summary`.

Trade-off:

The pipeline does not silently hide late data. It allows the run to complete
when records are otherwise valid, but marks the quality status so downstream
users can decide whether to trust, replay, or annotate the output.

Evidence:

```text
src/orchestration/run_pipeline.py
src/analytics/data_quality.py
tests/integration/test_run_pipeline_curated.py
```

## Duplicate Source Events

Scenario:

An upstream source retries a batch or replays a previously sent event.

Current behaviour:

1. Records are validated before deduplication.
2. The pipeline deduplicates by `event_id`.
3. Duplicate count and duplicate rate are emitted as data quality metrics.
4. Stable storage file names keep replayed writes from creating duplicate
   parquet files.

Trade-off:

Deduplication makes downstream output stable, but duplicate spikes still matter
operationally. They point to source retry behaviour, connector replay, or
upstream idempotency gaps.

Evidence:

```text
src/processing/deduplicator.py
src/storage/s3_writer.py
tests/integration/test_s3_writer.py
tests/unit/test_data_quality.py
```

## Schema Or Value Drift

Scenario:

A source omits a required field, sends nulls in required columns, or changes a
numeric field into a string or invalid value.

Current behaviour:

1. Required-field, null-field, and numeric-range checks run before curated
   output is written.
2. Validation errors include field-level counts.
3. Bad required inputs stop the run instead of producing partial curated
   output.

Trade-off:

Failing fast can interrupt a refresh, but publishing invalid risk data is
worse. The field-level counts keep the failure explainable.

Evidence:

```text
src/analytics/data_quality.py
src/orchestration/run_pipeline.py
tests/unit/test_data_quality.py
tests/integration/test_run_pipeline_curated.py
```

## Storage Write Retry

Scenario:

A job retries after writing some outputs, or the same input is replayed during
recovery.

Current behaviour:

1. Writes are partitioned by ingest time.
2. Output file names are derived from record content.
3. Each write uses a temporary file before replacing the final target.
4. Existing target files are skipped.

Trade-off:

This avoids duplicate files and makes replay safe, but it is a simple local
object-storage pattern rather than a full transactional table format.

Evidence:

```text
src/storage/s3_writer.py
tests/integration/test_s3_writer.py
docs/performance-benchmark.md
```

## Backfill Overlap With Live Ingestion

Scenario:

A historical replay tries to write the same partition as a live run.

Current behaviour:

1. Live and backfill jobs acquire partition locks before writing.
2. A conflicting active lock raises an overlap error.
3. Backfill records a `blocked_overlap` summary and stops.
4. After the overlap clears, rerunning backfill resumes from the last
   successful partition.

Trade-off:

Partition-level locking is coarse. It is easier to reason about than row-level
repair, and it fits the project goal: deterministic replay from raw partitions.

Evidence:

```text
src/orchestration/backfill.py
src/orchestration/locks.py
tests/integration/test_backfill.py
tests/unit/test_locks.py
```

## Resume And Idempotency

Scenario:

A backfill is interrupted after one partition succeeds.

Current behaviour:

1. Resume state is written after each successful partition.
2. A normal rerun skips already successful windows.
3. A forced rerun can replay the window range again.
4. Deterministic writes mean a forced rerun does not duplicate existing raw or
   curated files.

Trade-off:

The resume state is intentionally simple local JSON state. That is enough for a
portfolio-grade demonstration. A production system would usually put this state
in an orchestrator metadata store, warehouse audit table, or durable lock
service.

Evidence:

```text
src/orchestration/backfill.py
tests/integration/test_backfill.py
docs/operational-runbook.md
```
