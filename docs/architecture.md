# Architecture

This document uses the arc42 structure to describe the behaviour that is
implemented and tested in this repository. Cloud resources and deployment
manifests are described only as scaffolding unless the repository contains
executable evidence for them.

## 1. Introduction And Goals

The platform demonstrates two related market-risk paths:

```text
provider-neutral market events
  -> validation, normalisation and deduplication
  -> replayable raw Parquet
  -> minute-oriented demo analytics
  -> PostgreSQL serving and reconciliation

Alpha Vantage daily closes
  -> canonical timezone-aware market events
  -> immutable raw Parquet
  -> versioned daily returns and risk analytics
  -> version-preserving PostgreSQL tables
  -> current-version and semantic views

current daily-return versions
  -> configured long-only portfolio weights
  -> complete-date alignment
  -> versioned portfolio returns and risk summaries
```

The leading quality goals are:

| Priority | Quality goal | Evidence |
| --- | --- | --- |
| 1 | Deterministic replay | Stable event IDs, content-addressed Parquet and calculation IDs |
| 2 | Explicit contracts | Pydantic schemas, dataset grains, SQL constraints and loader specs |
| 3 | Source-to-serving traceability | Lineage, input fingerprints and reconciliation SQL |
| 4 | Safe operation | Local-first commands, bounded scans, manual deployment and security checks |
| 5 | Reviewable evolution | Arc42 change blocks, focused tests, CI and squash-merged PRs |

## 2. Architecture Constraints

1. Python **3.11** is the supported and CI-tested runtime.
2. `MarketEvent` requires timezone-aware event and ingest timestamps and
   normalises accepted values to UTC through Pydantic `AwareDatetime`.
3. The implemented runtime is batch or micro-batch, not a production streaming
   service.
4. Local Parquet models raw and curated object-storage layouts; it is not an AWS
   S3 client or transactional lakehouse.
5. PostgreSQL is the demonstrated serving contract. MongoDB is a seeded
   source-shape playground.
6. Dependency resolution is constrained by `requirements.lock`.
7. Managed cloud resources remain disabled by default, and deployment is manual.
8. Generated data, local state, credentials and evidence are excluded from Git.
9. Tests must substantiate repository claims; scaffolding is not described as
   production ownership.

## 3. Context And Scope

### Business context

```text
market-data source ----+
                       |
external signals ------+--> financial risk platform
                       |        |          |
                       v        v          v
                    raw data  analytics  warehouse views
                                             |
                                             v
                                    engineer / reviewer
```

The platform owns validation, immutable local landing, analytical derivation,
local warehouse loading and reconciliation. Production alert delivery,
persistent cloud storage, provider availability and live cloud operations remain
outside the implemented boundary.

### Technical neighbours

| Neighbour | Interface | Contract |
| --- | --- | --- |
| Alpha Vantage | HTTPS `TIME_SERIES_DAILY` | Bounded adapter, stable daily event ID and completed dates |
| Landed files | CSV, JSON, JSONL or NDJSON | Provider-neutral market and signal loaders |
| Local filesystem | Partitioned Parquet | Paths and datasets from `config/storage.yaml` |
| PostgreSQL | Psycopg and SQL | Tables, views and checks in `sql/` |
| MongoDB | Seeded local documents | Source-system modelling playground |
| Operator | Make targets and Python CLIs | Summaries, dry runs, loads and reconciliation |
| GitHub Actions | CI and manual deploy | Read-only validation; explicit deployment dispatch |

## 4. Solution Strategy

The data plane is split into:

1. **Ingestion** — source adapters and canonical schemas.
2. **Processing** — validation, normalisation, deduplication and windowing.
3. **Analytics** — minute metrics, daily risk, portfolio risk and data quality.
4. **Storage** — bounded, idempotent raw and curated Parquet publication.
5. **Orchestration** — sequencing, locks, backfills and summaries.
6. **Warehouse** — PostgreSQL loading, version retention, views and checks.

Daily and minute-oriented semantics stay separate. Alpha Vantage daily closes do
not enter datasets named `returns_1m` or `volatility_5m`. Daily calculations use
explicit model versions and deterministic calculation IDs so late source history
can create a new traceable version without overwriting earlier evidence.

## 5. Building Block View

| Block | Responsibility | Principal paths |
| --- | --- | --- |
| `common` | Configuration, time, logging and exceptions | `src/common/` |
| `ingestion` | Alpha Vantage and provider-neutral inbound contracts | `src/ingestion/` |
| `processing` | Validation, normalisation, deduplication and windowing | `src/processing/` |
| `analytics` | Returns, volatility, VaR, drawdown and quality | `src/analytics/` |
| `storage` | Dataset configuration and local Parquet publication | `src/storage/`, `config/storage.yaml` |
| `orchestration` | Demo pipeline, daily risk, locks and backfills | `src/orchestration/` |
| `warehouse` | Loader specs, SQL schema, views and reconciliation | `src/warehouse/`, `sql/` |
| `source-systems` | Local PostgreSQL and MongoDB examples | `docker-compose.yml`, `mongo/` |
| `deployment` | Image, Kubernetes and Terraform scaffold | `Dockerfile`, `deploy/`, `infra/` |
| `engineering-controls` | CI, security checks and review evidence | `.github/`, `scripts/`, `AGENTS.md` |

### Change modularity

Each PR names one primary block. A vertical slice may cross interfaces only when
the acceptance criterion requires an end-to-end contract. Data-plane,
deployment-plane and engineering-control changes remain separate unless a
runtime compatibility change makes the crossing unavoidable.

## 6. Runtime View

### Minute-oriented demo

```text
JSON events
  -> required-field, null and numeric validation
  -> UTC-normalised MarketEvent rows
  -> event-ID deduplication
  -> returns, rolling volatility, quality and risk summaries
  -> partition locks
  -> raw and curated Parquet
  -> optional PostgreSQL load
```

Late and duplicate records remain visible in quality metrics. Individual dataset
writes are idempotent, but the local filesystem does not provide one
multi-dataset transaction.

### Alpha Vantage daily risk

```text
bounded provider request
  -> completed daily closes
  -> immutable raw MarketEvent Parquet
  -> daily-risk-v2 calculations
  -> daily_returns
  -> daily_volatility
  -> daily_risk_summary
```

The daily calculation contract records:

- close-to-close return;
- annualised sample volatility using 252 trading days;
- historical VaR loss;
- maximum drawdown;
- window parameters and history readiness;
- source event and calculation fingerprints.

Identical inputs and parameters reproduce identical output files. Late history
changes the fingerprint and creates a distinguishable calculation version.

### Daily warehouse serving

```text
daily curated Parquet
  -> calculation-ID upserts
  -> version-preserving history tables
  -> parameter-aware latest view
  -> source-aware symbol enrichment
  -> reconciliation checks
```

The daily tables are:

- `risk_platform.daily_returns`
- `risk_platform.daily_volatility`
- `risk_platform.daily_risk_summary`

`latest_daily_risk_summary` selects one current row per source, symbol, event
date, model version and parameter set. `daily_risk_semantic_model` joins symbol
reference data on both `source` and `symbol`.

### Portfolio daily risk

```text
current daily_returns versions
  -> portfolio definition and weight validation
  -> complete-date alignment across constituents
  -> weighted portfolio returns
  -> annualised volatility, historical VaR and drawdown
  -> portfolio_daily_returns
  -> portfolio_daily_risk_summary
```

The portfolio path selects the latest component calculation per source, symbol
and event date using ingest time and calculation ID. Only dates with every
configured constituent are emitted. Weights are positive, unique by
source/symbol and sum to one. Component returns, contributions and calculation
IDs remain embedded as stable JSON evidence.

### Backfill

The hourly backfill runner reads immutable raw partitions, resumes after the last
successful checkpoint, uses the same partition locks as live processing and
updates local resume state only after success. Daily Alpha Vantage observations
are excluded from the minute-oriented backfill path.

## 7. Deployment View

| Environment | Implemented purpose |
| --- | --- |
| Developer workstation | Python, local Parquet and optional Docker databases |
| GitHub Actions | Security, Python readiness and infrastructure validation |
| Container | Non-root packaging of the default demo runtime |
| Kubernetes | CronJob, ConfigMaps, service account, network policy and overlays |
| AWS scaffold | ECR/OIDC plus disabled optional database examples |

Kubernetes currently uses ephemeral storage. Terraform does not provision the
EKS cluster expected by the manual deployment workflow. Normal validation never
deploys and never runs `terraform apply`.

## 8. Cross-Cutting Concepts

| Concept | Approach |
| --- | --- |
| Time | Aware timestamps only at the canonical event boundary; UTC internally |
| Identity | Stable source event IDs and deterministic calculation IDs |
| Replay | Immutable raw data and content-addressed curated files |
| Versioning | Daily calculation versions are retained rather than overwritten |
| Configuration | YAML for storage, thresholds, symbols, portfolios and operator choices |
| Data quality | Required fields, nulls, ranges, late and duplicate evidence |
| Concurrency | Local partition locks and repository-global lease fencing |
| Recovery | Resume checkpoints and safe reruns |
| Security | No committed secrets, bounded I/O and disabled cloud defaults |
| Human authority | Automated output is evidence; merge and deployment remain explicit decisions |

## 9. Architecture Decisions

| Decision | Consequence |
| --- | --- |
| Batch before streaming | Lower operational complexity at the cost of latency |
| Immutable raw before analytics | Explainable replay, but no table-format transactions |
| Separate daily and minute semantics | Clear grains and names, with two orchestration paths |
| Calculation ID as daily warehouse key | Retains corrections and parameter versions |
| PostgreSQL serving | Familiar constraints and views, not a distributed warehouse |
| Local locks | Simple overlap protection, not distributed coordination |
| Manual cloud activation | Prevents surprise cost and mutation |
| Arc42 blocks for PR scope | Improves reviewability and limits mixed concerns |

## 10. Quality Scenarios

| ID | Scenario | Required response |
| --- | --- | --- |
| Q1 | The same source batch is replayed | Write no duplicate raw, curated or warehouse facts |
| Q2 | A source event is corrected late | Preserve the earlier version and create traceable new calculations |
| Q3 | A timestamp lacks a timezone | Reject it at the canonical schema boundary |
| Q4 | A required field or numeric range is invalid | Stop before curated publication |
| Q5 | Live and backfill work overlap | Block on the active partition lock |
| Q6 | Daily history is insufficient | Emit `partial` rather than implying full readiness |
| Q7 | A warehouse consumer asks for current daily risk | Return one row per parameterised grain |
| Q8 | Portfolio constituent dates are incomplete | Exclude the date, report the count and fail if alignment is insufficient |
| Q9 | Default infrastructure checks run | Validate without deployment or resource creation |

## 11. Risks And Technical Debt

1. `main` still requires repository-level protection to enforce the CI gates;
   issue #51 tracks the administrative setting.
2. Local Parquet is not durable cloud storage or a transactional table format.
3. Kubernetes storage is ephemeral.
4. S3, Glue and Kinesis Terraform files remain placeholders.
5. The default container command runs the minute-oriented demo, not the daily
   operator sequence.
6. Production scheduling, alerts and dashboards are not implemented.
7. Portfolio outputs are curated Parquet only; PostgreSQL portfolio serving is
   not yet implemented.
8. Marginal risk attribution, covariance matrices, FX conversion, short
   positions and rebalancing schedules are not implemented.
9. Exchange-specific trading calendars are not used; daily observations are
   consecutive available closes, not guaranteed calendar-day intervals.
10. PostgreSQL reconciliation is an explicit local operator step rather than a
    CI database service.
11. Repository control documentation is extensive relative to the compact data
    runtime and should not grow ahead of product evidence.

## 12. Glossary

| Term | Meaning |
| --- | --- |
| Raw | Validated, normalised event-level Parquet retained for replay |
| Curated | Derived analytical Parquet |
| Event time | When the market observation occurred |
| Ingest time | When the platform accepted the observation |
| Calculation ID | Deterministic identity of model version, parameters and inputs |
| Current version | Latest ingest/calculation within a declared serving grain |
| Partial history | Valid output without enough observations for every configured window |
| Portfolio definition | Long-only source/symbol constituents and weights that sum to one |
| Human acceptance | Explicit engineer decision after reviewing code and evidence |
