# Mock Interview Prep

Use this as a timed practice run before the interview. Keep answers concise,
evidence-based, and focused on pipeline engineering.

## Thirty-Minute Run Plan

1. Opening pitch: 2 minutes.
2. Project walkthrough: 5 minutes.
3. Technical deep dive: 10 minutes.
4. Behavioural questions: 8 minutes.
5. Questions for the interviewer: 5 minutes.

Record the practice run if possible. The goal is to remove rambling and make
each answer point to a concrete example.

## Opening Pitch

Short version:

> I am strongest where data engineering meets ownership: taking source data,
> making it reliable, shaping it into warehouse-ready outputs, and giving users
> clear signals on quality and freshness. This project is a small example of
> that thinking: validation, deduplication, partitioned storage, data quality
> metrics, backfills, CI, and deployment scaffolding.

What to avoid:

1. Do not lead with risk analytics.
2. Do not claim direct production use of tools that are only mapped in docs.
3. Do not spend too long explaining financial formulas.
4. Do not turn the answer into a repository tour.

## Five-Minute Project Walkthrough

Use this order:

1. Problem: source events can be duplicated, late, malformed, or replayed.
2. Contract: validate required fields, nulls, and numeric ranges before curated
   outputs are produced.
3. Processing: normalise symbols, deduplicate by `event_id`, compute curated
   outputs, and emit data quality metrics.
4. Storage: write partitioned parquet with stable file names for replay safety.
5. Recovery: replay raw hourly partitions through the same tested pipeline path.
6. Serving: map curated outputs to PostgreSQL tables and ops-facing SQL.
7. Operations: CI, Docker, Kubernetes CronJob scaffold, and Lambda/S3 design
   notes show how it would move toward production.

Evidence files:

```text
src/orchestration/run_pipeline.py
src/storage/s3_writer.py
src/orchestration/backfill.py
sql/postgres_schema.sql
sql/ops_queries.sql
docs/demo-script.md
```

## Technical Questions

Why did you choose batch instead of streaming?

> The aim here is reliability and replayability. Batch gives a simpler path for
> validation, deduplication, partitioned writes, and backfills. If freshness
> requirements became tighter, I would move extraction or landing closer to
> streaming, but I would keep the downstream contract and quality checks.

How do you prevent duplicates?

> The pipeline deduplicates source records by `event_id`, tracks duplicate rate
> in data quality output, and writes batches using stable content-derived file
> names. For PostgreSQL serving tables, the same principle maps to primary keys
> and `ON CONFLICT` load patterns.

What happens when data is late?

> Late arrivals are measured by comparing event time with ingest time. The
> pipeline emits late-rate metrics and status. In production, that status could
> drive alerts or decide whether a downstream table should be refreshed.

How do you handle bad records?

> Required-field, null, and numeric-validity checks run before transformation.
> Missing or invalid required values stop the run rather than silently producing
> bad curated data. The counts are specific enough to identify what failed.

How would this work with PostgreSQL?

> The parquet layer remains the durable raw and curated storage layer. PostgreSQL
> becomes the operational serving layer. I would load through staging tables,
> validate row counts and keys, then use idempotent `INSERT ... ON CONFLICT`
> patterns into curated tables.

How would this work with an ELT connector?

> The connector owns extraction, source authentication, cursor state, and raw
> landing. This project owns the downstream contract: validation,
> normalisation, deduplication, data quality evidence, curated tables, and
> replay behaviour.

How would this work with nested document data?

> I would preserve the source document id, flatten nested fields into a clear raw
> contract, validate timestamps and required fields, then load curated warehouse
> tables from that contract. The mapping document shows that pattern.

Would you deploy this to a live cloud environment?

> I would not keep an always-on cluster running for a portfolio project. The
> repo shows the deployment shape, but live deployment should start with budget
> controls, least-privilege access, and a teardown plan. For this workload I
> would usually prefer a scheduled batch job unless freshness requirements
> justify more infrastructure.

## Behavioural Questions

Tell me about owning a problem end to end.

> I would frame ownership as taking a vague requirement, turning it into a data
> contract, building the pipeline, testing failure modes, and making sure users
> can tell whether the output is fresh and trustworthy.

Tell me about communicating with non-technical stakeholders.

> I would avoid leading with implementation detail. I would explain what the
> data means, how freshness is measured, what quality checks exist, and what
> happens when a source sends bad or late records.

Tell me about prioritising reliability vs speed.

> I would move quickly on the simplest pipeline that is testable and observable.
> I would not skip basic validation, idempotency, or recovery because those are
> the parts that decide whether users can trust the system.

Tell me about handling ambiguity.

> I would clarify the downstream decision first: who uses the data, what they
> need to decide, and how fresh it must be. Then I would define the source
> contract, failure handling, and minimal serving model.

## Questions To Ask

1. What are the most common reliability issues in the current pipelines?
2. How do ops users consume data today: SQL, internal tools, scheduled exports,
   or something else?
3. Where does transformation currently live: Airbyte, PostgreSQL SQL, dbt, or
   application code?
4. What would success look like for this role in the first 90 days?
5. Which data source causes the most schema drift or operational noise?

## Scoring Checklist

After a practice run, score each area from 1 to 5:

| Area | Score | Notes |
| --- | --- | --- |
| Opening pitch is under 2 minutes |  |  |
| Project walkthrough is clear |  |  |
| Answers mention trade-offs |  |  |
| Answers point to evidence |  |  |
| PostgreSQL mapping is credible |  |  |
| ELT connector mapping is not overstated |  |  |
| AWS answer is cost-aware |  |  |
| Questions for interviewer are specific |  |  |

Target: mostly 4s before the interview. A 5 is not needed; concise and credible
is better than over-polished.

## Final Polish

Before stopping prep:

1. Run the demo once.
2. Re-read `docs/interview-stories.md`.
3. Practise the opening pitch twice.
4. Pick three files to show if asked.
5. Prepare the repo locally with generated output cleaned.

Demo command:

```bash
make clean-generated
.venv/bin/python -m src.orchestration.run_pipeline \
  --input tests/fixtures/demo_events.json \
  --late-seconds 60 \
  --vol-window 2 \
  --summary-json .demo/pipeline-summary.json
```
