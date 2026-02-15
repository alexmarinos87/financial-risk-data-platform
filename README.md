# Financial Risk Data Platform

A production-style **Data Engineering** project for financial risk analytics. The platform ingests market events, applies contract-driven validation, writes partitioned bronze/silver/gold datasets, computes risk metrics, and enforces data quality thresholds.

## Data Engineer Focus

This repository is structured to demonstrate Data Engineer ownership areas:

1. Data contracts and schema governance (`config/data_contracts.yaml`)
2. Partitioned and idempotent storage writes (`src/storage/s3_writer.py`)
3. Layered modeling (`bronze`, `silver`, `gold`)
4. Data quality gates and run metadata (`src/orchestration/run_pipeline.py`)
5. Deterministic backfills (`src/orchestration/backfill.py`)

## What It Does

1. Ingests market data and external signals
2. Validates schema, deduplicates, and normalizes events
3. Aggregates into time windows
4. Computes risk analytics and DQ metrics
5. Writes partitioned Parquet datasets by layer

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pytest -q
```

## Run Pipeline

Run a local end-to-end pipeline with sample data:

```bash
python -m src.orchestration.run_pipeline --run-id demo-001
```

Provide your own JSON events:

```bash
python -m src.orchestration.run_pipeline \
  --input tests/fixtures/sample_events.json \
  --run-id demo-002
```

Allow DQ threshold breaches without failing the run:

```bash
python -m src.orchestration.run_pipeline --allow-dq-breach --run-id demo-soft-001
```

## Run Backfill

```bash
python -m src.orchestration.backfill \
  --start-date 2025-01-01 \
  --end-date 2025-01-03 \
  --allow-dq-breach
```

## Output Layout

```
data_lake/
  bronze/market_events/contract_version=v1/year=.../month=.../day=.../hour=.../run_id=....parquet
  silver/market_events/contract_version=v1/year=.../month=.../day=.../hour=.../run_id=....parquet
  gold/risk_summary/contract_version=v1/year=.../month=.../day=.../hour=.../run_id=....parquet
  gold/data_quality_metrics/contract_version=v1/year=.../month=.../day=.../hour=.../run_id=....parquet
  _runs/<run_id>.json
  _runs/backfill-<start>-<end>.json
```

## Data Sources

Examples of finance-credible sources:

1. Stooq for historical market data
2. FRED for macroeconomic indicators
3. Exchange calendars for trading day logic

See `docs/data-model.md` for schema details and `docs/architecture.md` for the full flow.
