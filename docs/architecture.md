# Architecture

This document uses the arc42 structure to describe behaviour that is implemented
and tested in this repository. Cloud resources and deployment manifests are
described only as scaffolding unless executable evidence exists for them.

## 1. Introduction And Goals

The platform demonstrates three related market-risk paths:

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
  -> version-preserving PostgreSQL tables
  -> current-version, semantic and return-contribution views
  -> latest-window covariance and correlation
  -> Euler component-volatility attribution
  -> versioned portfolio_risk_attribution Parquet
  -> version-preserving PostgreSQL attribution snapshots
  -> current matrix and volatility-contribution views
```

The leading quality goals are:

| Priority | Quality goal | Evidence |
| --- | --- | --- |
| 1 | Deterministic replay | Stable event IDs, content-addressed Parquet and calculation IDs |
| 2 | Explicit contracts | Pydantic schemas, dataset grains, SQL constraints and loader contracts |
| 3 | Source-to-serving traceability | Input fingerprints, JSONB evidence, views and reconciliation SQL |
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
| PostgreSQL | Psycopg and SQL | Version history, current views and reconciliation in `sql/` |
| MongoDB | Seeded local documents | Source-system modelling playground |
| Operator | Make targets and Python CLIs | Summaries, dry runs, loads and reconciliation |
| GitHub Actions | CI and manual deploy | Read-only validation; explicit deployment dispatch |

## 4. Solution Strategy

The data plane is split into:

1. **Ingestion** — source adapters and canonical schemas.
2. **Processing** — validation, normalisation, deduplication and windowing.
3. **Analytics** — minute metrics, daily risk, portfolio aggregation, covariance,
   volatility attribution and data quality.
4. **Storage** — bounded, idempotent raw and curated Parquet publication.
5. **Orchestration** — sequencing, locks, backfills and run summaries.
6. **Warehouse** — PostgreSQL loading, version retention, row-expansion views and
   reconciliation.

Minute, single-symbol daily, portfolio and attribution semantics stay separate.
Daily, portfolio and attribution calculations use explicit model versions and
deterministic calculation IDs so late source history, changed parameters,
changed weights or changed covariance windows create new traceable versions
without overwriting earlier evidence.

## 5. Building Block View

| Block | Responsibility | Principal paths |
| --- | --- | --- |
| `common` | Configuration, time, logging and exceptions | `src/common/` |
| `ingestion` | Alpha Vantage and provider-neutral inbound contracts | `src/ingestion/` |
| `processing` | Validation, normalisation, deduplication and windowing | `src/processing/` |
| `analytics` | Returns, VaR, drawdown, covariance, aggregation, attribution and quality | `src/analytics/` |
| `storage` | Dataset configuration and local Parquet publication | `src/storage/`, `config/storage.yaml` |
| `orchestration` | Demo, daily, portfolio and attribution runners plus locks and backfills | `src/orchestration/` |
| `warehouse` | Loaders, SQL schemas, current views and reconciliation | `src/warehouse/`, `sql/` |
| `source-systems` | Local PostgreSQL and MongoDB examples | `docker-compose.yml`, `mongo/` |
| `deployment` | Image, Kubernetes and Terraform scaffold | `Dockerfile`, `deploy/`, `infra/` |
| `engineering-controls` | CI, security checks and review evidence | `.github/`, `scripts/`, `AGENTS.md` |

Each PR names one primary block. A vertical slice may cross interfaces only when
the acceptance criterion requires an end-to-end contract. Data-plane,
deployment-plane and engineering-control changes remain separate unless runtime
compatibility requires otherwise.

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

The daily contract records close-to-close return, annualised sample volatility,
historical VaR loss, maximum drawdown, window parameters, history readiness and
source fingerprints. Identical inputs and parameters reproduce identical files.
Late history changes the fingerprint and creates a distinguishable version.

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
  -> constant-weight daily-rebalanced portfolio returns
  -> annualised volatility, historical VaR and drawdown
  -> portfolio_daily_returns
  -> portfolio_daily_risk_summary
```

The portfolio path selects the latest component calculation per source, symbol
and event date using ingest time and calculation ID. Only dates with every
configured constituent are emitted. Weights are positive, unique by
source/symbol and sum to one. Component returns, contributions and calculation
IDs remain embedded as stable JSON evidence.

### Portfolio warehouse serving

```text
portfolio curated Parquet
  -> JSON-aware calculation-ID upserts
  -> version-preserving portfolio history
  -> definition- and parameter-aware current views
  -> portfolio summary and constituent return-contribution views
  -> portfolio reconciliation checks
```

The portfolio tables are:

- `risk_platform.portfolio_daily_returns`
- `risk_platform.portfolio_daily_risk_summary`

`latest_portfolio_daily_returns` ranks corrections within a portfolio definition,
event date, model version and weighting method.
`latest_portfolio_daily_risk_summary` adds volatility and VaR parameters to that
grain. The `definition_fingerprint` remains part of both grains, so two weight
definitions using the same human-readable `portfolio_id` stay independently
queryable.

`portfolio_risk_semantic_model` exposes current portfolio summaries.
`portfolio_daily_contribution_model` expands each current return into one row per
constituent with weight, component return, component calculation ID and return
contribution.

### Portfolio covariance and volatility attribution

```text
current portfolio_daily_returns versions
  -> selected portfolio definition fingerprint
  -> bounded latest covariance window
  -> annualised sample covariance
  -> Pearson correlation
  -> Euler component contributions to portfolio volatility
  -> portfolio_risk_attribution
```

The attribution path validates the persisted portfolio-return evidence against
the selected definition, ranks corrections by ingest time and calculation ID,
and requires the full configured window. It writes one latest-window snapshot
that contains covariance and correlation matrices plus constituent volatility,
marginal contribution, component contribution and contribution-share mappings.
Undefined zero-variance correlations are stored as JSON `null`, never
non-standard `NaN`. Component contributions reconcile to total portfolio
volatility within a strict numerical tolerance.

### Attribution warehouse serving

```text
portfolio_risk_attribution Parquet
  -> bounded JSON-aware calculation-ID upsert
  -> risk_platform.portfolio_risk_attribution history
  -> latest_portfolio_risk_attribution
  -> portfolio_attribution_semantic_model
  -> portfolio_covariance_model
  -> portfolio_correlation_model
  -> portfolio_volatility_contribution_model
  -> attribution reconciliation checks
```

The base table preserves every deterministic snapshot. The current grain includes
the portfolio definition, event date, model and weighting method, covariance and
correlation methods, covariance window and annualisation basis. Within the grain,
`ts_ingest DESC, calculation_id DESC` selects the current version.

The matrix views expose one row per ordered constituent pair. The contribution
view exposes one row per constituent without recalculating covariance or
volatility. `portfolio-attribution-warehouse-load` first loads prerequisite
portfolio-return facts and then the attribution snapshot.
`check-portfolio-attribution-consistency` verifies input references, matrix
shape and symmetry, correlation null semantics, variance/volatility identity and
Euler reconciliation.

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
| Versioning | Daily, portfolio and attribution versions are retained rather than overwritten |
| Configuration | YAML for storage, thresholds, symbols, portfolios and operator choices |
| Evidence | Component IDs, weights, returns, matrices and contributions persist as JSONB-ready mappings |
| Serving | JSON history remains intact while SQL views expose queryable row grains |
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
| Separate minute, daily, portfolio and attribution semantics | Clear grains and explicit orchestration paths |
| Calculation ID as warehouse version key | Retains corrections, definitions and parameter versions |
| Portfolio definition fingerprint in current grains | Weight configurations do not silently overwrite one another |
| JSONB evidence plus row-expansion views | Preserves deterministic documents while enabling SQL analysis |
| Latest-window attribution snapshot | Bounded output and simple replay, but not a full historical matrix series |
| Sample covariance and Euler volatility contribution | Transparent decomposition without a factor or shrinkage model |
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
| Q9 | A portfolio component calculation ID conflicts | Fail rather than make a file-order-dependent choice |
| Q10 | A portfolio definition or parameter changes | Retain both versions and keep both current grains queryable |
| Q11 | A consumer asks for portfolio return contributions | Return one row per constituent without multiplying facts |
| Q12 | A portfolio covariance window is incomplete | Fail without publishing a partial attribution snapshot |
| Q13 | A zero-variance constituent has undefined correlation | Persist JSON nulls and explicit status, not NaN |
| Q14 | Component volatility contributions are calculated | Reconcile their sum to portfolio volatility |
| Q15 | An attribution correction is loaded | Retain history and select the newest row in its declared grain |
| Q16 | A consumer queries a matrix or attribution vector | Return one row per pair or constituent with no analytical recomputation |
| Q17 | Default infrastructure checks run | Validate without deployment or resource creation |

## 11. Risks And Technical Debt

1. `main` still requires repository-level protection to enforce the CI gates;
   issue #51 tracks the administrative setting.
2. Local Parquet is not durable cloud storage or a transactional table format.
3. Kubernetes storage is ephemeral.
4. S3, Glue and Kinesis Terraform files remain placeholders.
5. The default container command runs the minute-oriented demo, not the daily,
   portfolio or attribution operator sequences.
6. Production scheduling, alerts and dashboards are not implemented.
7. Attribution is a latest-window snapshot only; historical snapshots for every
   event date are not calculated.
8. Shrinkage, exponentially weighted and factor covariance estimators, marginal
   VaR, FX conversion, short positions, leverage, transaction costs and
   non-daily rebalancing are not implemented.
9. Portfolio definitions do not yet carry effective-from/to dates; the definition
   fingerprint distinguishes weight sets but does not model a scheduled mandate.
10. Exchange-specific trading calendars are not used; daily observations are
    consecutive available closes, not guaranteed calendar-day intervals.
11. PostgreSQL reconciliation is an explicit local operator step rather than a
    CI database service.
12. Repository control documentation is extensive relative to the compact data
    runtime and should not grow ahead of product evidence.

## 12. Glossary

| Term | Meaning |
| --- | --- |
| Raw | Validated, normalised event-level Parquet retained for replay |
| Curated | Derived analytical Parquet |
| Event time | When the market observation occurred |
| Ingest time | When the platform accepted the observation |
| Calculation ID | Deterministic identity of model version, parameters and inputs |
| Definition fingerprint | Deterministic identity of portfolio constituents, weights and base currency |
| Current version | Latest ingest/calculation within a declared serving grain |
| Partial history | Valid output without enough observations for every configured window |
| Portfolio definition | Long-only source/symbol constituents and weights that sum to one |
| Return contribution | Weight multiplied by the constituent daily return |
| Covariance window | Latest current portfolio-return observations used to estimate the matrix |
| Attribution snapshot | One retained covariance, correlation and volatility-decomposition calculation |
| Component volatility contribution | Euler allocation of portfolio volatility to one constituent |
| Human acceptance | Explicit engineer decision after reviewing code and evidence |
