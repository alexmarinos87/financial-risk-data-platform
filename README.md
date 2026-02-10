# Financial Risk Data Platform

A production-style data platform that ingests market data and external risk signals, validates and normalizes events, and produces decision-ready analytics (returns, volatility, data quality, and risk summaries).

This repo is intentionally structured to resemble real internal data platforms at banks and fintechs. It is designed for clarity, reproducibility, and interview readiness.

## What It Does

1. Ingests market data and external signals
2. Validates schema, deduplicates, and normalizes events
3. Aggregates into time windows
4. Computes risk analytics and data quality metrics
5. Stores raw and curated datasets with partitioning

## Quickstart

1. Create a virtual environment and install dependencies.
2. Review configuration in `config/`.
3. Run unit tests to validate core logic.

Example commands:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pytest -q
```

For local quality checks (recommended before every PR):

```bash
pip install -e .[dev]
make check
pre-commit install
pre-commit run --all-files
```

## Run The Demo Pipeline

Run a local end-to-end pipeline with a small sample dataset:

```bash
python -m src.orchestration.run_pipeline
```

Provide your own JSON events (list of objects) if desired:

```bash
python -m src.orchestration.run_pipeline --input tests/fixtures/sample_events.json
```

## Data Sources

This project is designed for free, legal, and finance-credible sources. Examples:

1. Stooq for historical market data
2. FRED for macroeconomic indicators
3. Exchange calendars for trading day logic

See `docs/data-model.md` for event schemas.

## Repository Layout

See `docs/architecture.md` for the end-to-end design.

## Notes

This is a portfolio-grade platform scaffold. All modules are intentionally small but structured for expansion.
