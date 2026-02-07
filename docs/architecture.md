# Architecture

The platform is organized into four layers:

1. Ingestion: market data and external risk signals
2. Raw storage: immutable, partitioned event storage
3. Processing: validation, deduplication, normalization, and windowing
4. Analytics: returns, volatility, data quality, and risk summaries

The design emphasizes:

1. Reproducibility and deterministic backfills
2. Explicit trade-offs between cost and latency
3. Strong schema validation at ingestion
