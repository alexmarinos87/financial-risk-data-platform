# Daily-risk publication and replay evidence

Primary arc42 block: `orchestration`, with an end-to-end contract across the
existing analytical builder and local Parquet writer. No product code changes.

```bash
python -m pytest -q tests/integration/test_daily_risk_publication_recovery.py
```

## Reject invalid calculations without publishing

Four cases land actual raw Parquet containing finite positive prices whose
returns or warmed-up volatility overflow. They run with both empty and already
populated curated storage. The real reader, builder, writer configuration and
CLI are used: rejection must preserve every raw/curated file byte and an existing
summary file, with no success output. Earlier valid history remains replayable
by choosing its end date; later invalid raw observations are not deleted.

## Recover after publication but before durability confirmation

Three cases fail directory fsync after the first, fourth or eighth curated file
has been linked. The write raises even though its complete final file exists.
Only this durability-confirmation boundary is injected; raw reads, serialization,
validation, hard links and subsequent replay use the real implementations.

A retry must recognize already-published rows, write only the missing rows and
report correct per-dataset counts. Reading every resulting Parquet row must
match the analytical builder, with exactly one occurrence of each calculation
ID. Timestamp fields are read as exact epoch microseconds and reconstructed as
aware UTC datetimes, matching the reader without optional decoding dependencies.
Published file bytes and raw history remain unchanged; another full replay
writes zero rows. The eighth-file case covers a failed acknowledgement after
all expected rows have become visible.

These tests require DuckDB and the repository's normal test dependencies. They
prove local replay convergence under the injected failure, not atomic publication
across datasets, power-loss durability without successful fsync, concurrent
writer safety, a cloud deployment or a PostgreSQL transaction. Consumers can
still observe partial output before recovery; deterministic replay is not rollback.
