# Portfolio Case Study: Financial Risk Data Platform

## Executive summary

This repository models a small internal data platform for market and portfolio risk analysis. It turns provider-neutral market events and bounded daily-price inputs into replayable raw evidence, versioned analytical datasets and governed PostgreSQL serving views.

The project is designed to demonstrate data-engineering judgement rather than claim operation of a live bank or fintech platform. Its strongest themes are explicit data grains, deterministic replay, late-data versioning, source-to-serving reconciliation and guarded delivery.

## The engineering problem

Market data pipelines need to answer more than “did the job run?” They need to preserve what arrived, distinguish duplicates from corrections, explain how an analytical result was produced and prevent a late observation or changed model parameter from silently rewriting historical evidence.

The repository addresses that problem for three related paths:

1. provider-neutral market events and minute-oriented analytics;
2. single-symbol daily returns and risk summaries; and
3. multi-symbol portfolio risk, covariance and volatility attribution.

The intended consumers are data engineers, analytics engineers and reviewers who need reproducible analytical outputs and a clear route from source evidence to reporting views.

## Architecture

```text
provider-neutral events / bounded daily prices / external signals
                              ↓
              validation · UTC normalisation · deduplication
                              ↓
                 immutable partitioned raw Parquet
                              ↓
       minute · daily · portfolio · covariance/attribution models
                              ↓
              versioned curated Parquet and calculation IDs
                              ↓
               append-only PostgreSQL analytical history
                              ↓
          current-version views · semantic models · reconciliation
```

The data plane is separated into ingestion, processing, analytics, storage, orchestration and warehouse-serving components. Minute, daily, portfolio and attribution datasets keep their own explicit grains rather than being treated as interchangeable outputs.

## Key engineering decisions

### 1. Preserve source evidence before optimising consumption

Validated source events are retained as immutable, partitioned Parquet. Stable event and content identities make replay observable and allow downstream outputs to be traced back to their inputs.

### 2. Make replay behaviour explicit

Identical inputs use deterministic identities and do not create duplicate files or analytical records. This lets a failed or repeated batch be rerun without relying on a human to remember whether an earlier write completed.

### 3. Version corrections instead of overwriting history

Late observations, changed model parameters, changed portfolio weights and changed covariance windows create new calculation versions. Consumer-facing views select the current version while append-only history preserves the evidence that existed previously.

### 4. Keep analytical and serving contracts separate

Curated Parquet represents analytical publication; PostgreSQL represents the demonstrated serving contract. Version-preserving tables, current views and row-level semantic models allow reporting consumers to query useful grains without rebuilding version-selection logic.

### 5. Treat reconciliation and lineage as product behaviour

The demo writes a lineage manifest covering source inventory, transformations, raw and curated outputs, quality checks and reporting dependencies. SQL consistency checks compare source, analytical and warehouse expectations rather than assuming that successful writes imply correct results.

### 6. Keep external side effects opt-in

Credential-free checks are the default. Managed cloud resources, deployment and notification paths remain disabled or explicitly human-controlled unless separately configured and reviewed.

## Five-minute walkthrough

### 1. Prepare and validate

```bash
make setup
make readiness-check
```

The readiness path combines linting, typing, tests, a deterministic pipeline run, lineage generation and a warehouse dry-run.

### 2. Explain the event flow

The current demo fixture processes seven input events and retains six after event-ID deduplication, including one duplicate and one late observation. On a clean first run it writes six raw and nine curated records.

Inspect:

```text
.demo/pipeline-summary.json
.demo/lineage.json
```

The summary separates input and deduplication counts from physical write counts. The lineage document identifies the source inventory, transformations, affected outputs, quality checks and reporting dependency.

### 3. Demonstrate replay safety

Run the same walkthrough again. The logical event flow is unchanged, while deterministic target identities cause the raw and curated write counts to become zero rather than creating duplicate files.

### 4. Show the implementation boundaries

Useful entry points are:

- `src/orchestration/run_pipeline.py` for validation, deduplication, partition locking and publication;
- `src/storage/s3_writer.py` for deterministic partitioned writes;
- `src/orchestration/backfill.py` and `src/orchestration/locks.py` for bounded replay and overlap control; and
- `sql/consistency_checks.sql` for source-to-warehouse reconciliation.

### 5. Extend to the local warehouse when Docker is available

```bash
make local-db-up
make consistency-demo
make local-db-down
```

This demonstrates the PostgreSQL serving and reconciliation contract locally; it is not evidence of a production database deployment.

## Evidence map

| Question | Repository evidence |
| --- | --- |
| How is the system structured? | [`docs/architecture.md`](docs/architecture.md) |
| How can it be demonstrated quickly? | [`docs/demo-script.md`](docs/demo-script.md) |
| How are daily-risk grains and versions defined? | [`docs/daily-risk-pipeline.md`](docs/daily-risk-pipeline.md) |
| How are portfolio returns and risk calculated? | [`docs/portfolio-risk.md`](docs/portfolio-risk.md) |
| How is covariance and component attribution represented? | [`docs/portfolio-attribution.md`](docs/portfolio-attribution.md) |
| How are source and warehouse outputs reconciled? | [`docs/data-consistency-walkthrough.md`](docs/data-consistency-walkthrough.md) |
| What are the operational and security controls? | [`docs/operational-runbook.md`](docs/operational-runbook.md), [`docs/security-protocols.md`](docs/security-protocols.md) |
| Where is the complete command reference? | [`PROJECT_REFERENCE.md`](PROJECT_REFERENCE.md) |

## Trade-offs and boundaries

- The implemented runtime is batch or micro-batch, not production streaming.
- Local Parquet models an object-storage layout; it is not presented as a transactional lakehouse.
- PostgreSQL is the demonstrated serving contract, but provider availability, production scheduling and durable managed-cloud operation remain outside the implemented boundary.
- The risk calculations are engineering demonstrations, not financial advice or a claim of a validated institutional risk model.
- Green local checks and CI provide technical evidence; they do not independently establish production approval.

## Interview discussion prompts

- Why retain append-only calculation history when current views are easier for consumers?
- How would deterministic replay change when the platform moved from local Parquet to object storage?
- Which controls would be needed before enabling production scheduling and external alerts?
- How would the design evolve for higher-frequency or streaming market data?
- How should model-version, parameter-version and source-correction identities interact?
