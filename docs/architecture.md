# Architecture

This document follows the twelve-section arc42 structure. It describes the
implemented local platform, the disabled-by-default deployment scaffold, and
the boundaries used to keep changes reviewable.

## 1. Introduction And Goals

The platform demonstrates a reliable market-risk data path:

1. Ingest market-like events and optional external signals.
2. Validate, normalise, and deduplicate them.
3. Retain replayable raw parquet and derived curated parquet.
4. Load stable PostgreSQL-style warehouse tables.
5. Reconcile source, raw, curated, and warehouse evidence.
6. Show a guarded AWS/Kubernetes deployment shape without deploying by default.

The primary stakeholders are the engineer operating and extending the repo, a
reviewer assessing engineering evidence, and downstream consumers of the
warehouse views.

The leading quality goals are:

| Priority | Quality goal | Concrete evidence |
| --- | --- | --- |
| 1 | Deterministic replay and recovery | Content-addressed parquet writes, partition locks, and resumable backfills |
| 2 | Explicit data contracts and quality | Pydantic ingestion schemas, field/range checks, late and duplicate metrics |
| 3 | Traceable source-to-serving behaviour | Run summaries, lineage manifests, and reconciliation SQL |
| 4 | Safe local and cloud operation | Local-first commands, manual deployment, disabled optional databases, and security checks |
| 5 | Reviewable evolution | Bounded modules, targeted tests, independent review, and human acceptance |

The design emphasizes:

1. Reproducibility and deterministic backfills.
2. Explicit trade-offs between cost and latency.
3. Strong schema validation at ingestion.
4. Measurable storage and query performance improvements.

## 2. Architecture Constraints

1. Python 3.10 or newer is the implementation language.
2. The demonstrated runtime is batch or micro-batch, not a production streaming
   service.
3. Local parquet paths model an S3-style raw and curated layout; the writer is
   not an AWS S3 client.
4. PostgreSQL is the demonstrated warehouse contract; MongoDB represents a
   source-system document shape.
5. Cloud resources and managed databases stay disabled unless explicitly
   requested. Deployment is manually dispatched.
6. Generated data, local state, caches, and evidence stay outside Git.
7. Tests and local evidence must substantiate claims; scaffolded services are
   not described as production-owned.
8. Kinesis or another streaming expansion is outside the default architecture.

## 3. Context And Scope

### Business Context

```text
Market-event files ----+
                       |
External signals ------+--> Financial risk data platform
                                  |        |         |
                                  v        v         v
                             raw parquet  curated  warehouse views
                                  |        |         |
                                  +--------+---------+
                                           |
                                           v
                                  engineer / reviewer
```

The platform owns validation, normalisation, deduplication, analytical
derivation, local storage layout, warehouse loading, and reconciliation. Source
provider operation, production alert delivery, persistent cloud data storage,
and live cloud ownership remain outside the demonstrated boundary.

### Technical Context

| Neighbour | Interface | Direction | Contract |
| --- | --- | --- | --- |
| Market-event source | JSON records | Inbound | `MarketEvent` schema and stable `event_id` |
| External-signal source | CSV, JSON, JSONL, or NDJSON | Inbound | Signal ID, name, value, event/ingest timestamps, and source |
| Local filesystem | Partitioned parquet in an S3-style layout | Outbound and replay | Storage paths from `config/storage.yaml` |
| PostgreSQL | SQL and Psycopg | Outbound | Tables and views in `sql/` |
| MongoDB demo | Seeded documents | Playground only | Documents in `mongo/init/`; not connected to the pipeline |
| Operator | Make targets and Python CLIs | Bidirectional | Run output, quality status, lineage, and review evidence |
| GitHub Actions | CI and manual deploy workflows | Outbound control | Validation on change; deployment only by explicit dispatch |

## 4. Solution Strategy

The four data-transformation layers are:

1. Ingestion: market data and external risk signals.
2. Raw storage: immutable, partitioned event storage.
3. Processing: validation, deduplication, normalization, and windowing.
4. Analytics: returns, volatility, external signal summaries, data quality, and risk summaries.

Orchestration, warehouse serving, deployment, and engineering controls support
these layers; they do not redefine the transformation flow.

The solution uses a layered Python pipeline coordinated by one orchestration
entry point. It validates source contracts before transformation, separates
pure processing and analytics from I/O, writes replayable partitioned parquet,
and loads warehouse tables with stable conflict keys. Deterministic filenames,
partition locks, and resume checkpoints make retries and backfills explainable.

Configuration is externalised in YAML. Local Docker services provide database
evidence. The image, Kubernetes manifests, ECR/IAM Terraform, optional managed
database resources, and manual deployment workflow demonstrate only the
intended deployment shape. Repository controls and review-package tooling form
a separate engineering-control plane; they never enter the data runtime.

## 5. Building Block View

### Level 1: System Building Blocks

```text
                         +-----------------------+
                         | Orchestration         |
                         | live run / backfill   |
                         +-----------+-----------+
                                     |
        +-------------+--------------+---------------+-------------+
        |             |              |               |             |
        v             v              v               v             v
   Ingestion      Processing      Analytics       Storage       Warehouse
        \             |              /               |             /
         +------------+-------------+----------------+------------+
                                      |
                                Common foundation

   Deployment plane                    Engineering-control plane
   Docker / Kubernetes / Terraform     checks / tests / review evidence / docs
```

| Block ID | Responsibility | Owned paths | May depend on |
| --- | --- | --- | --- |
| `ingestion` | Source adapters and inbound schemas | `src/ingestion/` | `common` and the processing symbol normaliser |
| `processing` | Validation, normalisation, deduplication, and windowing | `src/processing/` | `common` |
| `analytics` | Returns, volatility, risk metrics, and data-quality calculations | `src/analytics/`, `config/risk_thresholds.yaml` | `common` |
| `storage` | Partition layout, storage configuration, and idempotent local parquet writes | `src/storage/`, `config/storage.yaml` | `common` |
| `orchestration` | Pipeline sequencing, locks, backfills, lineage, and placeholder Glue/replay entry points | `src/orchestration/`, `scripts/replay_historical_as_live.py` | ingestion, processing, analytics, storage, common |
| `warehouse` | PostgreSQL loading, reference data, schemas, views, checks, and operational queries | `src/warehouse/`, `sql/`, `scripts/seed_reference_data.py`, `config/symbols.yaml` | storage configuration plus raw and curated dataset contracts |
| `common` | Shared configuration, time, logging, and exceptions | `src/common/` | no data-plane block |
| `source-systems` | Local PostgreSQL/MongoDB fixtures and source-document mapping | `docker-compose.yml`, `mongo/`, relevant walkthroughs | warehouse and ingestion contracts |
| `benchmarking` | CSV-versus-partitioned-parquet measurement | `src/benchmarks/`, `docs/performance-benchmark.md` | storage contracts |
| `deployment` | Image, manual deploy workflow, Kubernetes, and Terraform | `Dockerfile`, `.github/workflows/deploy.yml`, `deploy/`, `infra/` | published runtime and config contracts |
| `engineering-controls` | CI, ownership, validation, repository security, agent workflow, and local review evidence | `.github/workflows/ci.yml`, `.github/CODEOWNERS`, `Makefile`, control scripts and tests, `AGENTS.md`, workflow docs | all blocks as evidence subjects, never as runtime dependencies |

Tests belong to the block whose behaviour they prove. Configuration and
documentation accompany a block only when the same change alters that block's
contract or operating instructions. `Makefile` is a shared command surface:
classify a target change by the block it executes rather than automatically as
an engineering-control change.

### Change Modularity Rules

Use the level-1 blocks above to define branches and pull requests:

1. Name one primary block in every task brief and PR description.
2. Keep one branch and PR to one behavioural outcome in that block, plus its
   tests and directly affected documentation.
3. A vertical slice may cross blocks only when its acceptance criterion requires
   an end-to-end contract change. List each interface crossed and keep unrelated
   refactoring out.
4. Treat schemas, warehouse keys, quality thresholds, locks, security, and
   deployment as explicit contract boundaries. Split them from ordinary feature
   work unless they are the stated objective.
5. Keep data-plane changes separate from deployment-plane changes. A runtime
   change may include its image/config compatibility update, but infrastructure
   provisioning belongs in a separate high-risk PR.
6. Keep engineering-control changes separate from pipeline behaviour. Controls
   can inspect the data plane but must not become a runtime dependency.
7. If a proposed diff has two plausible primary blocks or cannot be explained
   with one runtime scenario, split it before implementation.
8. Use `agent/<block>-<outcome>` for automated branches and record the base SHA.
   A branch never writes directly to `main`.

The normal 200–500 non-generated changed-line budget remains a review heuristic,
not a target. A larger atomic contract plus its tests may stay together when a
split would create an invalid or unverified intermediate state; record that
rationale explicitly.

| Primary block | Minimum validation |
| --- | --- |
| ingestion, processing, analytics, storage, orchestration, common, or benchmarking | `make security-check && make quality-check && make readiness-check` |
| warehouse | `make security-check && make quality-check && make readiness-check`; add Docker reconciliation when available |
| source-systems | Compose YAML parse, lint, tests, and relevant Docker checks when available |
| deployment | `make security-check && make infrastructure-check` |
| Python engineering-controls | Focused tests, `make security-check`, `make quality-check`, and `git diff --check` |
| Documentation-only controls | `make security-check && git diff --check` |

## 6. Runtime View

### Normal Pipeline Run

```text
load inputs
  -> validate and normalise
  -> deduplicate
  -> calculate returns, volatility, quality, and risk summaries
  -> acquire affected partition locks
  -> write raw and curated parquet
  -> release locks
  -> emit run summary and optional lineage

separate operator step
  -> optionally load PostgreSQL with idempotent upserts
```

Invalid required fields or values stop curated publication. Late and duplicate
records remain visible in quality evidence rather than being silently ignored.
The individual parquet dataset writes are idempotent but are not one
multi-dataset transaction.

### Backfill

The backfill runner reads raw hourly partitions, starts after the last successful
checkpoint when resuming, obtains the same partition locks as a live run, and
invokes the normal pipeline. An overlap blocks the run; a successful partition
updates local resume state. See `docs/failure-scenarios.md`.

### Change Acceptance

```text
bounded implementation
  -> independent correctness review
  -> production-failure challenge
  -> automated validation
  -> ignored local evidence package
  -> human acceptance decision
```

This control flow does not run in the container or touch pipeline data.

## 7. Deployment View

| Environment | Nodes and stores | Purpose |
| --- | --- | --- |
| Developer workstation | Python virtual environment, local filesystem parquet, optional Docker PostgreSQL/MongoDB | Primary implemented and tested path |
| GitHub Actions | Ephemeral runners | Security, Python, and infrastructure validation; manual deployment workflow |
| Container runtime | Non-root Python image | Runs `src.orchestration.run_pipeline` with packaged config and SQL |
| Kubernetes scaffold | CronJob, ConfigMaps, service account, network policy, dev/prod overlays | Packaging and security shape; data is currently an `emptyDir`, not persistent storage |
| AWS scaffold | ECR and GitHub OIDC IAM; false-by-default RDS, Aurora, and DocumentDB | Managed deployment examples; S3 and Glue files are placeholders and no EKS cluster is provisioned |

The deploy workflow is `workflow_dispatch` only and expects a pre-existing EKS
cluster. It can build, validate, and apply Kubernetes resources after explicit
invocation. When configured and authorised, it pushes an image tagged with the
commit SHA and workflow attempt to ECR, injects that same image into the selected
dev or prod overlay, then server-side dry-runs, diffs, and applies the same
rendered manifest. Normal validation never deploys or runs `terraform apply`.

## 8. Cross-Cutting Concepts

| Concept | Approach | Evidence |
| --- | --- | --- |
| Data contracts | Pydantic at ingestion; explicit dataset grain and keys | `src/ingestion/schemas.py`, `docs/data-model.md` |
| Time | Event and ingest time remain distinct; loaders normalise to UTC, but direct schema input can currently remain timezone-naive | `src/common/time.py`, `src/ingestion/market_data_loader.py`, `src/processing/windowing.py` |
| Idempotency | Stable event/table keys and content-addressed parquet filenames | `src/storage/s3_writer.py`, `src/warehouse/postgres_loader.py` |
| Concurrency | Partition locks prevent live/backfill overlap | `src/orchestration/locks.py` |
| Recovery | Replayable raw data, resume checkpoints, and deterministic reruns | `src/orchestration/backfill.py` |
| Configuration | YAML config loaded through shared helpers and mounted into deployments | `config/`, `src/common/config.py` |
| Observability | Structured run summaries, lineage, quality metrics, and ops SQL | `src/orchestration/lineage.py`, `sql/ops_queries.sql` |
| Security | Local-only defaults, secret scanning, CODEOWNERS, restricted pod defaults, and manual deploy | `scripts/security_check.py`, `docs/security-protocols.md` |
| Human authority | Automated output is evidence; acceptance, merge, and deploy remain human decisions | `docs/engineering-delivery-workflow.md` |

## 9. Architecture Decisions

| Decision | Consequence and trade-off | Evidence |
| --- | --- | --- |
| Batch/micro-batch before streaming | Lower operational complexity at the cost of latency | `docs/tradeoffs.md` |
| Retry/replay-tolerant local processing with idempotent outputs | Repeated local inputs are safe; no message-delivery or acknowledgement semantics are implemented | Storage and warehouse tests |
| Replayable local parquet before curated publication | Backfills are explainable; local files are not a transactional lakehouse or S3 integration | `src/storage/s3_writer.py` |
| One orchestrator over small functional blocks | End-to-end flow is easy to follow; the coordinator requires disciplined boundaries | `src/orchestration/run_pipeline.py` |
| PostgreSQL serving contract | Familiar upserts and reconciliation; not a distributed analytical warehouse | `src/warehouse/postgres_loader.py`, `sql/` |
| Coarse partition locks | Simple overlap protection; not a distributed lock service | `src/orchestration/locks.py` |
| Manual, disabled-by-default cloud paths | Prevents surprise cost or mutation; live deployment requires explicit setup | `infra/terraform/variables.tf`, `.github/workflows/deploy.yml` |
| arc42 blocks define change modules | PRs align to runtime ownership; deliberate vertical slices must describe crossed interfaces | This document |

## 10. Quality Requirements

| ID | Scenario | Required response | Evidence |
| --- | --- | --- | --- |
| Q1 | The same source batch is replayed | Do not create duplicate parquet files or warehouse facts | S3 writer and loader tests |
| Q2 | An event arrives late or duplicated | Complete valid processing and expose the condition in quality metrics | Data-quality tests and demo output |
| Q3 | A required field or numeric range is invalid | Fail before curated publication with explainable validation detail | Ingestion and pipeline tests |
| Q4 | A backfill is interrupted or overlaps a live partition | Resume after the last success or stop on the active lock | Backfill and lock tests |
| Q5 | Default infrastructure validation runs | Validate without deploying or creating optional managed databases | Security and infrastructure checks |
| Q6 | An agent-authored change is reviewed | Keep automated evidence distinct and the final decision pending | Morning-review tests and workflow docs |
| Q7 | A proposed change spans unrelated blocks | Split it or record the end-to-end contract and cohesion rationale | Change modularity rules above |

### Evidence Map

| Architecture decision | Local evidence |
| --- | --- |
| Validate before transformation | `src/orchestration/run_pipeline.py`, `src/analytics/data_quality.py`, `tests/unit/test_data_quality.py` |
| Keep raw data replayable | `src/storage/s3_writer.py`, `tests/integration/test_s3_writer.py` |
| Make backfills deterministic | `src/orchestration/backfill.py`, `src/orchestration/locks.py`, `tests/integration/test_backfill.py` |
| Serve stable warehouse outputs | `src/warehouse/postgres_loader.py`, `sql/postgres_schema.sql`, `sql/ops_queries.sql` |
| Trace source-to-report lineage | `src/orchestration/lineage.py`, `.demo/lineage.json`, `tests/unit/test_lineage.py` |
| Reconcile source to warehouse | `sql/consistency_checks.sql`, `docs/data-consistency-walkthrough.md` |
| Validate infrastructure without deploying | `Makefile`, `deploy/kubernetes/`, `infra/terraform/` |

## 11. Risks And Technical Debt

1. Locks and backfill checkpoints are local files, not distributed coordination.
2. Parquet writes are idempotent files, not atomic multi-table transactions or
   AWS S3 writes.
3. The Kubernetes CronJob uses ephemeral `emptyDir` storage; it is not a durable
   cloud data path.
4. Terraform contains S3, Glue, and Kinesis placeholders and does not provision
   the EKS cluster expected by the deploy workflow.
5. `config/environments.yaml` is not consumed by the current runtime.
6. Direct `MarketEvent` validation does not currently require timezone-aware
   timestamps even though UTC is the intended convention.
7. Some analytical names imply fixed time windows while the current calculations
   can be observation-count based; `docs/data-model.md` records the distinction.
8. `data_quality_metrics` uses the latest ingest timestamp as a run key rather
   than a dedicated pipeline-run identifier.
9. Historical dimension intervals are checked by tooling but not fully excluded
   by a database constraint.
10. Production alert delivery, dashboards, capacity tests, and live cloud
   operation are not implemented claims.
11. The orchestration coordinator is intentionally direct; extract additional
    services only when a second runtime or independently evolving contract makes
    the boundary valuable.
12. `infra/diagrams/architecture.png` is currently a placeholder rather than
    architectural evidence.

## 12. Glossary

| Term | Meaning in this repository |
| --- | --- |
| Raw | Validated, normalised, deduplicated event-level parquet retained for replay; not the untouched provider payload |
| Curated | Derived analytical parquet datasets such as returns, volatility, quality, and risk summaries |
| Warehouse | PostgreSQL tables and views used for stable serving and reconciliation |
| Event time | When the source event occurred (`ts_event`) |
| Ingest time | When the platform received the event (`ts_ingest`) |
| Backfill | Deterministic replay of historical raw partitions through the normal pipeline |
| Engineering-control plane | Repository checks, tests, review evidence, and delivery policy outside the data runtime |
| Human acceptance | The engineer's explicit decision after understanding the diff and its evidence; never an automated status |
