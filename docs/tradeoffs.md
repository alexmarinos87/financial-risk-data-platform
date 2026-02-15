# Trade-offs

Key design decisions and trade-offs:

1. Streaming vs batch: micro-batch lowers operational complexity at the cost of latency.
2. Exactly-once vs at-least-once: at-least-once is acceptable with idempotent `run_id` writes.
3. Contract strictness: strict required-field enforcement catches errors early but can drop malformed producer payloads.
4. DQ hard-fail vs soft-fail: hard-fail protects downstream consumers; soft-fail supports exploratory/local runs.
5. File layout vs table format: plain Parquet keeps tooling simple, but lacks built-in ACID and schema evolution controls found in Iceberg/Delta/Hudi.
