# Interview Stories

Use these as rehearsal notes. The aim is not to recite them word for word, but
to answer with a clear structure: situation, decision, trade-off, and result.

## Story 1: Pipeline Reliability

Situation:

The pipeline needs to handle repeated source events, late arrivals, and replayed
batches without producing confusing downstream output.

Decision:

The project validates source payloads before transformation, normalises symbols,
deduplicates by `event_id`, writes data into ingest-time partitions, and uses
stable content-derived file names. Partition locks prevent live and backfill
jobs from writing the same partition at the same time.

Trade-off:

This keeps the batch path simple and replayable, but it accepts higher latency
than a continuous streaming design. For a small operational pipeline, that is a
reasonable trade-off because correctness and recovery are easier to reason
about.

Result:

The same input can be replayed without duplicating output files. The test suite
covers deduplication, partitioning, idempotent writes, stale lock replacement,
and blocked live/backfill overlap.

Evidence to point to:

```text
src/orchestration/run_pipeline.py
src/storage/s3_writer.py
src/orchestration/locks.py
tests/integration/test_s3_writer.py
tests/unit/test_locks.py
```

Short version:

> I designed the batch path around replay safety: validate first, deduplicate by
> source key, write deterministic partitioned output, and lock partitions when
> live and backfill work could overlap.

## Story 2: Data Quality Failure

Situation:

Operational users need to know whether data can be trusted, not just whether a
job technically completed.

Decision:

The pipeline calculates required-field, null-rate, numeric-validity, duplicate,
and late-arrival metrics. Bad required fields or invalid values stop the
pipeline before curated outputs are written. Quality metrics are also stored as
a curated dataset for inspection.

Trade-off:

Failing early can interrupt downstream refreshes, but silently publishing bad
data is worse. The project keeps field-level counts so failures are specific
enough to debug.

Result:

Demo data shows duplicate and late-rate metrics in the run summary. Tests cover
missing required fields, nulls, invalid numeric values, and data quality metric
materialisation.

Evidence to point to:

```text
src/analytics/data_quality.py
src/orchestration/run_pipeline.py
tests/unit/test_data_quality.py
tests/integration/test_run_pipeline_curated.py
sql/ops_queries.sql
```

Short version:

> I treat data quality as part of the pipeline contract. A successful job should
> tell us what was loaded, what was rejected, and whether late or duplicate data
> crossed a threshold.

## Story 3: Backfill And Recovery

Situation:

Historical data needs to be replayed safely after a bug fix, source delay, or
logic change. Backfills must not collide with live ingestion.

Decision:

The backfill flow reads raw hourly partitions, replays them through the same
pipeline path, writes resume state after each successful partition, and stops if
an active lock indicates overlap with live work.

Trade-off:

Partition-level replay is less granular than row-level repair, but it is easier
to operate and verify. Resume state keeps recovery simple after interruption.

Result:

Backfills can resume after a blocked or interrupted run. Tests cover successful
hourly replay, forced rerun behaviour, blocked overlap, and resume after the
overlap clears.

Evidence to point to:

```text
src/orchestration/backfill.py
src/orchestration/locks.py
tests/integration/test_backfill.py
tests/integration/test_run_pipeline_curated.py
docs/failure-scenarios.md
```

Short version:

> I would rather backfill through the same tested path as live ingestion than
> create a separate correction path. The raw layer is the source for replay, and
> locks protect partitions from concurrent writes.

## Story 4: Stakeholder Translation

Situation:

An ops team usually does not want raw event payloads. They want clear tables and
queries that answer freshness, quality, and status questions.

Decision:

The project separates raw storage from curated outputs, then documents a
PostgreSQL serving model with risk summaries, data quality status, freshness
queries, and idempotent load patterns.

Trade-off:

This adds a warehouse contract to maintain, but it gives non-specialist users
stable outputs and avoids exposing raw source complexity.

Result:

The repo has SQL examples for latest pipeline health, recent non-OK checks,
warehouse freshness, risk summary consumption, and conflict-safe loads from
staging tables.

Evidence to point to:

```text
sql/postgres_schema.sql
sql/ops_queries.sql
docs/elt-mapping.md
docs/source-document-mapping.md
docs/lambda-s3-orchestration.md
```

Short version:

> I try to translate requirements into a small warehouse contract: what tables
> exist, what keys make loads idempotent, what quality checks users can see, and
> how freshness is measured.

## Likely Follow-Up Questions

What would you change for production?

> I would add a real warehouse load step, alerting from data quality status, and
> environment-specific secrets and access controls. I would also decide whether
> the workload belongs on a scheduled container, Lambda, or managed batch job
> based on data volume and runtime.

Why not deploy the full Kubernetes path for a portfolio project?

> The repo includes the deployment scaffold, but a live cluster would add cost
> without much extra interview signal. For a real environment I would deploy
> only after setting budget controls, least-privilege roles, and teardown rules.

How does this map to an ELT connector?

> The connector owns extraction and raw landing. This project owns the
> downstream contract: validation, normalisation, deduplication, curated
> outputs, data quality evidence, and backfill behaviour.

How would you handle schema drift?

> I would validate required fields, quarantine or reject incompatible records,
> track drift in data quality output, and version the downstream contract when a
> source change is intentional.

How would you explain this to a non-technical stakeholder?

> The pipeline turns source events into trusted operational tables. It checks
> whether records are complete, avoids double-counting duplicates, tracks late
> data, and gives the team a clear view of freshness and quality.

## Practice Checklist

Before the interview:

1. Run the demo once from a clean state.
2. Explain each story out loud in under two minutes.
3. For each story, name one trade-off.
4. For each story, point to one file as evidence.
5. Prepare two questions about the current warehouse and pipeline reliability.

Demo command:

```bash
make clean-generated
.venv/bin/python -m src.orchestration.run_pipeline \
  --input tests/fixtures/demo_events.json \
  --late-seconds 60 \
  --vol-window 2 \
  --summary-json .demo/pipeline-summary.json
```
