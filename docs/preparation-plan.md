# Interview Preparation Plan

This plan is focused on presenting the project as evidence of practical data
engineering judgement: reliable pipelines, warehouse-ready outputs, operational
recovery, and clear stakeholder communication.

## Positioning

Use the repo as a production-style data pipeline showcase, not as a finance or
analytics project.

Core message:

> This project shows how I think about pipeline reliability: schema validation,
> deduplication, partitioned storage, data quality evidence, idempotent writes,
> backfills, and deployment readiness.

Avoid overstating direct experience with tools that are not implemented here.
For Airbyte, PostgreSQL, MongoDB, and dbt, explain the transferable concepts and
where they would fit into the design.

## Today: Thursday 4 June 2026

Goal: make the repo clean, runnable, and easy to demonstrate.

Completed preparation items:

1. Add reproducible local setup through `make setup`.
2. Make tests runnable through `make test`.
3. Ignore generated local output under `data/` and `.demo/`.
4. Add `make clean-generated` for fresh demo runs.
5. Add a richer demo fixture with duplicates, a late event, and curated metrics.
6. Add a walkthrough in `docs/demo-script.md`.
7. Fix summary JSON output so parent directories are created automatically.
8. Add regression coverage for the CLI summary output path.

Validation commands:

```bash
make setup
make format
make lint
make clean-generated
.venv/bin/python -m src.orchestration.run_pipeline \
  --input tests/fixtures/demo_events.json \
  --late-seconds 60 \
  --vol-window 2 \
  --summary-json .demo/pipeline-summary.json
make test
```

Expected result:

```text
38 passed
```

## Friday 5 June 2026

Goal: close the Zinc-specific gaps without overclaiming.

Completed early on Thursday 4 June 2026:

1. PostgreSQL warehouse DDL for curated outputs.
2. Ops-facing SQL queries for data quality, recent pipeline status, and curated
   summary consumption.
3. A short note mapping the project to an Airbyte-style ELT flow.
4. A short note showing how nested upstream MongoDB-style documents would be
   flattened into warehouse-ready records.
5. A Lambda and S3 orchestration note showing how the local pipeline maps to a
   small cloud workflow.

Suggested files:

```text
sql/postgres_schema.sql
sql/ops_queries.sql
docs/elt-mapping.md
docs/source-document-mapping.md
docs/lambda-s3-orchestration.md
```

Validation focus:

1. The SQL should be explainable as a warehouse contract, not as a claim that a
   managed PostgreSQL instance is already deployed.
2. The ELT mapping should explain where a connector fits without claiming direct
   production use.
3. The source document mapping should show clear thinking about nested source
   data, primary keys, timestamps, and validation.
4. The Lambda/S3 note should show orchestration judgement and the limits of
   Lambda for larger workloads.

## Weekend: 6-7 June 2026

Goal: rehearse concise stories rather than memorising code.

Completed early on Friday 5 June 2026:

1. Pipeline reliability.

   Explain validation, deduplication, partition locks, idempotent writes, and
   why these choices reduce operational risk.

2. Data quality failure.

   Explain how missing fields, null values, invalid numeric values, duplicate
   rate, and late rate are detected and surfaced.

3. Backfill and recovery.

   Explain replaying raw hourly partitions, resume state, and preventing
   overlap between live and backfill jobs.

4. Stakeholder translation.

   Explain how an ops requirement becomes a warehouse-ready curated table and a
   small set of SQL queries.

For each story, use this structure:

```text
Situation: what problem existed
Decision: what design choice was made
Trade-off: what was gained and what was accepted
Result: how it can be observed or tested
```

Detailed rehearsal notes are in:

```text
docs/interview-stories.md
```

Sunday 7 June 2026 preparation completed early on Saturday 6 June 2026:

1. Add a timed mock interview run plan.
2. Prepare concise answers for common technical and behavioural questions.
3. Add a self-scoring checklist for practice runs.
4. Add a final polish checklist before Monday.

Detailed mock-interview notes are in:

```text
docs/mock-interview.md
```

## Monday 8 June 2026

Goal: light review only.

Do:

1. Run the demo once from a clean checkout state.
2. Re-read `docs/demo-script.md`.
3. Review the four interview stories.
4. Run through `docs/mock-interview.md`.
5. Prepare two questions for the interviewer.

Do not:

1. Add new features immediately before the interview.
2. Change dependencies unless something is broken.
3. Lead with finance analytics. Lead with engineering reliability.

## Demo Flow

Run:

```bash
make clean-generated
.venv/bin/python -m src.orchestration.run_pipeline \
  --input tests/fixtures/demo_events.json \
  --late-seconds 60 \
  --vol-window 2 \
  --summary-json .demo/pipeline-summary.json
```

Talk through:

1. Input events include duplicates and a late event.
2. Validation rejects bad payloads before curated outputs are produced.
3. Deduplication reduces repeated upstream records.
4. Partitioned parquet output gives deterministic local storage.
5. Data quality metrics expose late and duplicate rates.
6. Backfill logic can replay raw partitions and resume safely.
7. CI and deployment scaffolding show how the workload would be operated.

## Interview Questions To Prepare

Good questions to ask:

1. What are the most common failure modes in the current data pipelines?
2. How do ops users consume warehouse data today: direct SQL, internal tools, or
   scheduled extracts?
3. Which part of the warehouse needs the most ownership in the first 90 days?
4. How much transformation currently lives in Airbyte, PostgreSQL SQL, dbt, or
   application code?
5. What does good pipeline reliability look like at Zinc: freshness, accuracy,
   observability, recovery time, or cost?

## Key Reminder

The strongest angle is not knowing every tool in the advert. The strongest
angle is showing that the fundamentals are solid: SQL thinking, reliable data
movement, validation, ownership, Git discipline, AWS/S3 familiarity, and clear
communication with non-technical stakeholders.
