# Financial Risk Data Platform

A production-style data engineering portfolio project for ingesting market data and external risk signals, retaining replayable evidence, calculating versioned risk analytics, and serving governed PostgreSQL views.

**Python · SQL · PostgreSQL · Parquet · Docker · GitHub Actions**

## At a glance

| Area | Implementation focus |
| --- | --- |
| Ingestion | Schema validation, symbol normalisation, deterministic deduplication and immutable raw Parquet |
| Analytics | Daily returns, rolling volatility, historical VaR, maximum drawdown, portfolio risk, covariance and attribution |
| Serving | Append-only PostgreSQL history with current-version and semantic reporting views |
| Reliability | Explicit grains, replay safety, late-data versioning, reconciliation and data-quality evidence |
| Delivery | Reproducible dependency resolution, automated tests, security checks and disabled-by-default cloud scaffolding |

## Architecture

```text
market data + external signals
            ↓
validation · normalisation · deduplication
            ↓
immutable partitioned raw Parquet
            ↓
versioned daily, portfolio and attribution models
            ↓
curated Parquet + append-only PostgreSQL history
            ↓
current views · semantic models · reconciliation evidence
```

## What this project demonstrates

- Designing data contracts with explicit business and calculation grains.
- Replaying identical inputs without creating duplicate analytical evidence.
- Preserving corrected or late observations as new calculation versions rather than silently overwriting history.
- Separating immutable storage, curated analytical models and consumer-facing database views.
- Treating lineage, reconciliation, readiness and security checks as part of the product rather than optional documentation.
- Building a project that can be demonstrated locally without claiming that scaffolded cloud services are production-deployed.

## Run the credential-free walkthrough

```bash
make setup
make readiness-check
```

The walkthrough exercises the sample ingestion-to-curated path, including duplicate and late-event behaviour, and writes credential-free evidence under `.demo/`.

For focused commands and a five-minute walkthrough, see [`docs/demo-script.md`](docs/demo-script.md).

## Explore the evidence

| Topic | Starting point |
| --- | --- |
| End-to-end architecture | [`docs/architecture.md`](docs/architecture.md) |
| Daily market-risk path | [`docs/daily-risk-pipeline.md`](docs/daily-risk-pipeline.md) |
| Portfolio returns and risk | [`docs/portfolio-risk.md`](docs/portfolio-risk.md) |
| Covariance and attribution | [`docs/portfolio-attribution.md`](docs/portfolio-attribution.md) |
| Source-to-warehouse reconciliation | [`docs/data-consistency-walkthrough.md`](docs/data-consistency-walkthrough.md) |
| Security controls | [`docs/security-protocols.md`](docs/security-protocols.md) |
| Operating and failure investigation | [`docs/operational-runbook.md`](docs/operational-runbook.md) |
| Complete command and capability reference | [`PROJECT_REFERENCE.md`](PROJECT_REFERENCE.md) |

## Project boundary

This is a portfolio-grade engineering project. Local behaviour, tests and generated evidence substantiate the implemented claims. Managed cloud resources and external notification paths remain explicitly disabled or human-controlled unless separately configured.
