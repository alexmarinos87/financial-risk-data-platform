# Architecture

This arc42 document describes behaviour that is implemented and tested in this
repository. Cloud manifests are described as scaffolding unless executable
evidence exists for them.

## 1. Introduction And Goals

The platform demonstrates three related market-risk paths:

```text
provider-neutral market events
  -> validation, normalisation and deduplication
  -> replayable raw Parquet
  -> minute-oriented analytics
  -> PostgreSQL serving and reconciliation

Alpha Vantage daily closes
  -> timezone-aware canonical market events
  -> immutable raw Parquet
  -> daily-risk-v2 returns and risk analytics
  -> version-preserving PostgreSQL tables
  -> current-version and semantic views

current daily-return versions
  -> configured long-only portfolio weights
  -> complete-date alignment
  -> portfolio_daily_returns and portfolio_daily_risk_summary
  -> PostgreSQL portfolio serving
  -> rolling historical covariance and correlation
  -> Euler component-volatility attribution
  -> versioned portfolio_risk_attribution Parquet
  -> PostgreSQL attribution history and row-level views
```

| Priority | Quality goal | Evidence |
| --- | --- | --- |
| 1 | Deterministic replay | Stable event IDs, content-addressed Parquet and calculation IDs |
| 2 | Explicit contracts | Pydantic schemas, dataset grains, SQL constraints and loader contracts |
| 3 | Traceability | Input fingerprints, JSONB evidence, current views and reconciliation SQL |
| 4 | Safe operation | Local-first commands, bounded scans and disabled cloud defaults |
| 5 | Reviewable evolution | Focused tests, CI and squash-merged PRs |

## 2. Architecture Constraints

1. Python **3.11** is the supported and CI-tested runtime.
2. `MarketEvent` uses Pydantic `AwareDatetime` and normalises accepted timestamps
   to UTC.
3. Runtime execution is batch or micro-batch, not production streaming.
4. Local Parquet models object-storage layouts; it is not a transactional
   lakehouse.
5. PostgreSQL is the demonstrated serving contract.
6. Dependency resolution is constrained by `requirements.lock`.
7. Managed cloud resources remain disabled by default and deployment is manual.
8. Generated data, local state and credentials are excluded from Git.
9. Repository claims require tests or executable evidence.

## 3. Context And Scope

```text
market data ----+
                |
external risk --+--> ingestion -> raw -> analytics -> warehouse views
                                                |
                                                v
                                         engineer / reviewer
```

The repository owns validation, immutable local landing, analytical derivation,
local warehouse loading and reconciliation. Provider availability, durable cloud
storage, production scheduling, alert delivery and live cloud operations remain
outside the implemented boundary.

| Neighbour | Interface | Contract |
| --- | --- | --- |
| Alpha Vantage | HTTPS `TIME_SERIES_DAILY` | Bounded adapter and completed dates |
| Landed files | CSV, JSON, JSONL or NDJSON | Provider-neutral event inputs |
| Local filesystem | Partitioned Parquet | Paths from `config/storage.yaml` |
| PostgreSQL | Psycopg and SQL | Version history, views and checks under `sql/` |
| Operator | Make targets and Python CLIs | Summaries, dry runs, loads and reconciliation |
| GitHub Actions | CI and manual deploy | Validation without implicit deployment |

## 4. Solution Strategy

The data plane is separated into:

1. **Ingestion** — source adapters and canonical schemas.
2. **Processing** — validation, normalisation, deduplication and windowing.
3. **Analytics** — returns, VaR, drawdown, portfolio aggregation, covariance and
   volatility attribution.
4. **Storage** — bounded, idempotent raw and curated Parquet publication.
5. **Orchestration** — daily, portfolio, latest-attribution and history runners.
6. **Warehouse** — version-preserving PostgreSQL loading, row-expansion views and
   reconciliation.

Minute, single-symbol daily, portfolio and attribution semantics remain separate.
Late source history, changed parameters, changed weights or changed covariance
windows create new deterministic versions rather than overwriting evidence.

## 5. Building Block View

| Block | Responsibility | Principal paths |
| --- | --- | --- |
| `common` | Configuration, time and exceptions | `src/common/` |
| `ingestion` | External and landed input contracts | `src/ingestion/` |
| `processing` | Validation, deduplication and windows | `src/processing/` |
| `analytics` | Daily risk, portfolio risk and attribution | `src/analytics/` |
| `storage` | Parquet configuration and publication | `src/storage/`, `config/storage.yaml` |
| `orchestration` | Pipeline and bounded operator runners | `src/orchestration/` |
| `warehouse` | Loaders, schemas, current views and checks | `src/warehouse/`, `sql/` |
| `deployment` | Container, Kubernetes and Terraform scaffold | `Dockerfile`, `deploy/`, `infra/` |
| `engineering-controls` | CI, security and review evidence | `.github/`, `scripts/`, `AGENTS.md` |

Each PR names one primary block. Interface crossings are included only when an
acceptance criterion requires an end-to-end contract.

## 6. Runtime View

### Minute-oriented demo

```text
landed events
  -> required-field and range validation
  -> UTC-normalised MarketEvent rows
  -> event-ID deduplication
  -> returns, volatility, quality and risk summaries
  -> raw and curated Parquet
  -> optional PostgreSQL load
```

### Single-symbol daily risk

```text
Alpha Vantage daily closes
  -> immutable raw events
  -> daily_returns
  -> daily_volatility
  -> daily_risk_summary
```

The warehouse retains:

- `risk_platform.daily_returns`
- `risk_platform.daily_volatility`
- `risk_platform.daily_risk_summary`

`latest_daily_risk_summary` selects current rows per source, symbol, event date,
model and parameter set. `daily_risk_semantic_model` joins reference data on both
source and symbol.

### Portfolio risk

```text
current daily_returns
  -> portfolio definition validation
  -> complete-date alignment
  -> constant-weight daily-rebalanced returns
  -> rolling volatility, historical VaR and drawdown
  -> portfolio_daily_returns
  -> portfolio_daily_risk_summary
```

The warehouse retains:

- `risk_platform.portfolio_daily_returns`
- `risk_platform.portfolio_daily_risk_summary`

`latest_portfolio_daily_risk_summary` preserves definition and parameter grains.
`portfolio_risk_semantic_model` exposes current summaries.
`portfolio_daily_contribution_model` exposes one constituent return contribution
per row.

### Covariance and volatility attribution

```text
current portfolio_daily_returns
  -> exact definition fingerprint
  -> complete covariance windows
  -> annualised sample covariance
  -> Pearson correlation
  -> Euler component-volatility attribution
  -> portfolio_risk_attribution
```

`portfolio-attribution-demo` writes only the latest complete window.
`portfolio-attribution-history-demo` expands the same canonical current-version
input into one snapshot per eligible rolling end date. `START_DATE` filters
emitted snapshot dates while preserving earlier observations needed by the first
window. One request is bounded by `MAX_HISTORY_SNAPSHOTS = 2_500`.

A late correction is applied to the current portfolio-return version for its
business date. Every affected rolling window receives a new deterministic
calculation ID; earlier attribution versions remain retained.

Undefined zero-variance correlations are stored as JSON `null`, not NaN.
Component volatility contributions reconcile to portfolio volatility within a
strict tolerance.

### Attribution warehouse serving

```text
portfolio_risk_attribution Parquet
  -> bounded JSON-aware upsert
  -> risk_platform.portfolio_risk_attribution
  -> latest_portfolio_risk_attribution
  -> portfolio_attribution_semantic_model
  -> portfolio_covariance_model
  -> portfolio_correlation_model
  -> portfolio_volatility_contribution_model
```

The current grain includes portfolio definition, event date, model, methods,
covariance window and annualisation basis. Within the grain,
`ts_ingest DESC, calculation_id DESC` selects the current version.

`portfolio_covariance_model` and `portfolio_correlation_model` expose one ordered
constituent pair per row. `portfolio_volatility_contribution_model` exposes one
constituent per row without analytical recomputation.

`portfolio-attribution-warehouse-load` loads prerequisite portfolio facts and all
retained attribution snapshots. `check-portfolio-attribution-consistency`
verifies input references, matrix shape and symmetry, correlation null semantics,
variance/volatility identity and Euler reconciliation.

## 7. Deployment View

| Environment | Implemented purpose |
| --- | --- |
| Developer workstation | Python, local Parquet and optional Docker databases |
| GitHub Actions | Security, Python readiness and infrastructure validation |
| Container | Non-root packaging of the default demo |
| Kubernetes | CronJob, ConfigMaps, service account, network policy and overlays |
| AWS scaffold | Disabled-by-default examples and manual deployment workflow |

Kubernetes storage is ephemeral. Terraform does not provision the EKS cluster
expected by the manual deployment workflow. Normal validation never runs
`terraform apply`.

## 8. Cross-Cutting Concepts

| Concept | Approach |
| --- | --- |
| Time | Aware UTC timestamps at canonical boundaries |
| Identity | Stable event IDs and deterministic calculation IDs |
| Replay | Immutable raw data and content-addressed curated files |
| Versioning | Daily, portfolio and attribution versions are retained |
| Current state | Explicit grains ranked by ingest time and calculation ID |
| Evidence | IDs, weights, returns, matrices and contributions persist as JSONB-ready documents |
| Serving | JSON history remains intact while SQL views expose row grains |
| Safety | File, byte, row and snapshot caps; no implicit provider or cloud action |
| Human authority | Automated evidence does not imply merge or deployment approval |

## 9. Architecture Decisions

| Decision | Consequence |
| --- | --- |
| Batch before streaming | Lower operational complexity at the cost of latency |
| Immutable raw before analytics | Explainable replay without table-format transactions |
| Calculation ID as version key | Corrections and parameter changes remain distinguishable |
| Portfolio definition fingerprint | Weight configurations do not overwrite one another |
| Sample covariance and Euler allocation | Transparent decomposition without a factor model |
| Bounded rolling attribution history | Queryable risk trends with explicit output limits |
| JSONB documents plus row-expansion views | Preserve deterministic evidence and support SQL analysis |
| PostgreSQL serving | Familiar constraints, not a distributed warehouse |
| Manual cloud activation | Prevents surprise cost and mutation |

## 10. Quality Scenarios

| ID | Scenario | Required response |
| --- | --- | --- |
| Q1 | The same source batch is replayed | Write no duplicate raw, curated or warehouse facts |
| Q2 | A source event is corrected late | Preserve prior versions and create traceable replacements |
| Q3 | A timestamp lacks a timezone | Reject it at the canonical boundary |
| Q4 | A portfolio date is incomplete | Exclude it and report the alignment gap |
| Q5 | A covariance window is incomplete | Publish no partial attribution snapshot |
| Q6 | Historical attribution is requested | Emit every complete selected window in date order |
| Q7 | A start date is supplied | Filter outputs while retaining prior window context |
| Q8 | A history request exceeds 2,500 snapshots | Fail before publication and require a bounded split |
| Q9 | A zero-variance constituent has undefined correlation | Persist JSON null and explicit status |
| Q10 | Component contributions are calculated | Reconcile their sum to portfolio volatility |
| Q11 | A corrected attribution is loaded | Retain history and select the newest declared grain |
| Q12 | A consumer queries a matrix or vector | Return one pair or constituent per row without recomputation |
| Q13 | Default infrastructure checks run | Validate without deployment or resource creation |

## 11. Risks And Technical Debt

1. `main` still requires repository-level protection; issue #51 tracks the
   administrative setting.
2. Local Parquet is not durable cloud storage or a transactional table format.
3. Kubernetes storage is ephemeral.
4. S3, Glue and Kinesis Terraform files remain placeholders.
5. The default container command runs the minute-oriented demo rather than the
   daily, portfolio or attribution flows.
6. Production scheduling, alerts and dashboards are not implemented.
7. Shrinkage, exponentially weighted and factor covariance estimators, marginal
   VaR, FX conversion, short positions, leverage, transaction costs and
   non-daily rebalancing are not implemented.
8. Portfolio definitions do not model effective-from/to mandate periods.
9. Exchange-specific trading calendars are not used.
10. PostgreSQL reconciliation remains an explicit local operator action rather
    than a CI database service.

## 12. Glossary

| Term | Meaning |
| --- | --- |
| Raw | Validated event-level Parquet retained for replay |
| Curated | Derived analytical Parquet |
| Calculation ID | Deterministic identity of model, parameters and inputs |
| Definition fingerprint | Identity of portfolio constituents, weights and currency |
| Current version | Latest ingest/calculation inside an explicit grain |
| Covariance window | Current return observations used to estimate one matrix |
| Attribution snapshot | One covariance, correlation and volatility-decomposition result |
| Rolling attribution history | One complete attribution snapshot per selected window end date |
| Component volatility contribution | Euler allocation of portfolio volatility to one constituent |
| Human acceptance | Explicit engineer decision after reviewing code and evidence |
