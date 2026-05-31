# Performance Benchmark

This benchmark measures a practical data engineering question: how much scan
work can be avoided when market events are stored as partitioned Parquet instead
of a single CSV file.

The benchmark generates deterministic synthetic market events, writes them in
two layouts, and runs the same filtered aggregate through DuckDB:

1. A single row-oriented CSV file.
2. A Hive-partitioned Parquet dataset partitioned by `symbol` and `trade_date`.

The result is a reproducible summary with scan timings, bytes on disk, file
counts, matching row counts, and the calculated Parquet speedup ratio.

## Run

```bash
python -m src.benchmarks.io_engine_benchmark \
  --rows 50000 \
  --symbols 20 \
  --days 10 \
  --summary-json .benchmarks/io_engine/summary.json
```

Or use:

```bash
make benchmark-io
```

## Why This Matters

Financial risk pipelines often repeatedly scan high-volume market data for a
small symbol, date or window. A partitioned columnar layout reduces unnecessary
I/O, improves predicate pruning, and creates a clearer boundary between Python
orchestration and vectorised SQL execution.

This is a first step toward a broader performance programme:

1. Establish a reproducible baseline.
2. Compare storage layouts and query shapes.
3. Track throughput and scan cost as data volume grows.
4. Use the benchmark results to guide pipeline storage and compute choices.

The benchmark is intentionally local and deterministic so it can run in CI or on
a developer machine before larger cloud or distributed experiments are added.
