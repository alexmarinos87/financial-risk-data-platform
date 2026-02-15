# Failure Scenarios

1. Contract violation: required fields are missing or types are incompatible.
2. DQ threshold breach: late or duplicate rates exceed configured thresholds and run fails.
3. Storage write failure: partitioned Parquet write fails on local/remote filesystem issues.
4. Backfill interruption: one day fails; manifest captures completed and failed ranges.
5. Idempotent rerun overwrite: same `run_id` intentionally replaces prior files for deterministic replay.
