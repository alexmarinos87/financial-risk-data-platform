# Architecture

The platform is organized into five layers:

1. Contract: versioned schema and compatibility rules in `config/data_contracts.yaml`
2. Ingestion: market data and external risk signals
3. Bronze: immutable, partitioned raw events
4. Silver: validated, normalized, deduplicated events
5. Gold: risk summaries and data quality metrics

The design emphasizes:

1. Reproducible, idempotent reruns via explicit `run_id`
2. Deterministic backfills with day-by-day run manifests
3. Contract-first validation before persistence
4. Data quality gates that can fail runs in CI/production
