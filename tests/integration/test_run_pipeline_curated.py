from __future__ import annotations

import json
from pathlib import Path

import duckdb
import yaml

from src.orchestration.run_pipeline import run_pipeline


def _count_parquet_rows(dataset_dir: Path) -> int:
    parquet_files = sorted(dataset_dir.rglob("*.parquet"))
    if not parquet_files:
        return 0
    escaped_paths = [str(path).replace("'", "''") for path in parquet_files]
    quoted = ", ".join(f"'{path}'" for path in escaped_paths)
    with duckdb.connect() as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM read_parquet([{quoted}])").fetchone()[0])


def test_run_pipeline_materializes_all_curated_datasets(tmp_path: Path) -> None:
    storage_config = {
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
    storage_config_path = tmp_path / "storage.yaml"
    with storage_config_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(storage_config, handle, sort_keys=False)

    events = [
        {
            "event_id": "evt-1",
            "symbol": "AAPL",
            "price": 100.0,
            "volume": 10,
            "ts_event": "2025-01-20T10:01:00Z",
            "ts_ingest": "2025-01-20T10:01:05Z",
            "source": "stooq",
        },
        {
            "event_id": "evt-2",
            "symbol": "AAPL",
            "price": 101.0,
            "volume": 11,
            "ts_event": "2025-01-20T10:02:00Z",
            "ts_ingest": "2025-01-20T10:02:04Z",
            "source": "stooq",
        },
        {
            "event_id": "evt-3",
            "symbol": "AAPL",
            "price": 102.0,
            "volume": 12,
            "ts_event": "2025-01-20T10:03:00Z",
            "ts_ingest": "2025-01-20T10:03:04Z",
            "source": "stooq",
        },
        {
            "event_id": "evt-4",
            "symbol": "MSFT",
            "price": 240.0,
            "volume": 9,
            "ts_event": "2025-01-20T10:01:00Z",
            "ts_ingest": "2025-01-20T10:01:03Z",
            "source": "stooq",
        },
        {
            "event_id": "evt-5",
            "symbol": "MSFT",
            "price": 242.0,
            "volume": 10,
            "ts_event": "2025-01-20T10:02:00Z",
            "ts_ingest": "2025-01-20T10:07:00Z",
            "source": "stooq",
        },
        {
            "event_id": "evt-6",
            "symbol": "MSFT",
            "price": 241.0,
            "volume": 8,
            "ts_event": "2025-01-20T10:03:00Z",
            "ts_ingest": "2025-01-20T10:03:05Z",
            "source": "stooq",
        },
        {
            "event_id": "evt-6",
            "symbol": "MSFT",
            "price": 241.0,
            "volume": 8,
            "ts_event": "2025-01-20T10:03:00Z",
            "ts_ingest": "2025-01-20T10:03:05Z",
            "source": "stooq",
        },
    ]
    input_path = tmp_path / "events.json"
    with input_path.open("w", encoding="utf-8") as handle:
        json.dump(events, handle)

    summary = run_pipeline(
        input_path=input_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        late_seconds=60,
        window_minutes=5,
        vol_window=2,
        storage_config_path=storage_config_path,
    )

    assert summary["raw_events"] == 6
    assert summary["curated_records_by_dataset"] == {
        "returns_1m": 4,
        "volatility_5m": 2,
        "data_quality_metrics": 1,
        "risk_summary": 2,
    }
    assert summary["curated_records"] == 9
    assert set(summary["volatility_latest"]) == {"AAPL", "MSFT"}
    assert set(summary["value_at_risk"]) == {"AAPL", "MSFT"}

    assert _count_parquet_rows(tmp_path / "curated" / "returns_1m") == 4
    assert _count_parquet_rows(tmp_path / "curated" / "volatility_5m") == 2
    assert _count_parquet_rows(tmp_path / "curated" / "data_quality_metrics") == 1
    assert _count_parquet_rows(tmp_path / "curated" / "risk_summary") == 2
