from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from src.storage.partitioning import partition_path
from src.storage.s3_writer import write_records


def _build_storage_config(tmp_path: Path) -> dict:
    return {
        "storage": {
            "base_dir": str(tmp_path),
            "raw": {
                "base_path": str(tmp_path / "raw"),
                "dataset": "market_events",
            },
            "curated": {
                "base_path": str(tmp_path / "curated"),
                "datasets": {
                    "returns_1m": "returns_1m",
                    "volatility_5m": "volatility_5m",
                    "data_quality_metrics": "data_quality_metrics",
                    "risk_summary": "risk_summary",
                },
            },
            "format": "parquet",
            "partitioning": {
                "granularity": "hourly",
            },
        }
    }


def _parquet_row_count(path: Path) -> int:
    escaped = str(path).replace("'", "''")
    with duckdb.connect() as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM read_parquet('{escaped}')").fetchone()[0])


def test_write_records_raw_partitioned_and_idempotent(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    ts_ingest = datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc)
    records = [
        {
            "event_id": "evt-1",
            "symbol": "AAPL",
            "price": 100.0,
            "volume": 10,
            "ts_event": datetime(2025, 1, 20, 10, 0, tzinfo=timezone.utc),
            "ts_ingest": ts_ingest,
            "source": "stooq",
        },
        {
            "event_id": "evt-2",
            "symbol": "AAPL",
            "price": 101.0,
            "volume": 11,
            "ts_event": datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc),
            "ts_ingest": ts_ingest.isoformat(),
            "source": "stooq",
        },
    ]

    written = write_records(records, kind="raw", storage_config=config)
    assert written == 2

    partition = partition_path(ts_ingest)
    dataset_dir = tmp_path / "raw" / "market_events" / partition
    parquet_files = list(dataset_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    assert _parquet_row_count(parquet_files[0]) == 2

    written_again = write_records(records, kind="raw", storage_config=config)
    assert written_again == 0
    assert len(list(dataset_dir.glob("*.parquet"))) == 1


def test_write_records_curated_without_ts_ingest(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    records = [{"late_rate": 0.2, "duplicate_rate": 0.1}]

    written = write_records(
        records,
        kind="curated",
        dataset="risk_summary",
        storage_config=config,
    )
    assert written == 1

    dataset_dir = tmp_path / "curated" / "risk_summary"
    parquet_files = list(dataset_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    assert _parquet_row_count(parquet_files[0]) == 1
