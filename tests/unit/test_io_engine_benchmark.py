from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.benchmarks.io_engine_benchmark import generate_market_events, run_io_benchmark


def test_generate_market_events_is_deterministic() -> None:
    first = generate_market_events(100, symbol_count=5, day_count=2)
    second = generate_market_events(100, symbol_count=5, day_count=2)

    pd.testing.assert_frame_equal(first, second)
    assert set(first.columns) == {
        "event_id",
        "symbol",
        "price",
        "volume",
        "trade_date",
        "ts_event",
        "ts_ingest",
        "source",
    }
    assert first["symbol"].nunique() == 5
    assert first["trade_date"].nunique() == 2


def test_run_io_benchmark_reports_matching_scan_results(tmp_path: Path) -> None:
    summary = run_io_benchmark(
        output_dir=tmp_path / "benchmark",
        row_count=1_000,
        symbol_count=5,
        day_count=2,
        repetitions=1,
    )

    assert summary.csv.matched_rows == summary.partitioned_parquet.matched_rows
    assert summary.csv.average_price == summary.partitioned_parquet.average_price
    assert summary.csv.total_volume == summary.partitioned_parquet.total_volume
    assert summary.csv.bytes_on_disk > 0
    assert summary.partitioned_parquet.bytes_on_disk > 0
    assert summary.partitioned_parquet.file_count > 1
    assert summary.parquet_speedup_ratio >= 0.0
    assert "csv" in summary.to_dict()
    assert "partitioned_parquet" in summary.to_dict()
