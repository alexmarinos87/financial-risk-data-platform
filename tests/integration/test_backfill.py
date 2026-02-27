from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import yaml

from src.orchestration.backfill import run_backfill
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


def _seed_raw_partitions(storage_config: dict) -> None:
    records = [
        {
            "event_id": "evt-1",
            "symbol": "AAPL",
            "price": 100.0,
            "volume": 10,
            "ts_event": datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc),
            "ts_ingest": datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc),
            "source": "stooq",
        },
        {
            "event_id": "evt-2",
            "symbol": "AAPL",
            "price": 101.0,
            "volume": 11,
            "ts_event": datetime(2025, 1, 20, 10, 2, tzinfo=timezone.utc),
            "ts_ingest": datetime(2025, 1, 20, 10, 2, tzinfo=timezone.utc),
            "source": "stooq",
        },
        {
            "event_id": "evt-3",
            "symbol": "AAPL",
            "price": 102.0,
            "volume": 12,
            "ts_event": datetime(2025, 1, 20, 10, 3, tzinfo=timezone.utc),
            "ts_ingest": datetime(2025, 1, 20, 10, 3, tzinfo=timezone.utc),
            "source": "stooq",
        },
        {
            "event_id": "evt-4",
            "symbol": "MSFT",
            "price": 240.0,
            "volume": 9,
            "ts_event": datetime(2025, 1, 20, 11, 1, tzinfo=timezone.utc),
            "ts_ingest": datetime(2025, 1, 20, 11, 1, tzinfo=timezone.utc),
            "source": "stooq",
        },
        {
            "event_id": "evt-5",
            "symbol": "MSFT",
            "price": 242.0,
            "volume": 10,
            "ts_event": datetime(2025, 1, 20, 11, 2, tzinfo=timezone.utc),
            "ts_ingest": datetime(2025, 1, 20, 11, 2, tzinfo=timezone.utc),
            "source": "stooq",
        },
        {
            "event_id": "evt-6",
            "symbol": "MSFT",
            "price": 241.0,
            "volume": 8,
            "ts_event": datetime(2025, 1, 20, 11, 3, tzinfo=timezone.utc),
            "ts_ingest": datetime(2025, 1, 20, 11, 3, tzinfo=timezone.utc),
            "source": "stooq",
        },
    ]
    write_records(records, kind="raw", storage_config=storage_config)


def test_run_backfill_replays_hourly_partitions_and_is_idempotent(tmp_path: Path) -> None:
    storage_config = _build_storage_config(tmp_path)
    storage_config_path = tmp_path / "storage.yaml"
    with storage_config_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(storage_config, handle, sort_keys=False)

    _seed_raw_partitions(storage_config)

    first = run_backfill(
        "2025-01-20T10:00:00Z",
        "2025-01-20T11:00:00Z",
        "hourly",
        storage_config_path=storage_config_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        vol_window=2,
    )
    assert len(first) == 2
    assert {summary["partition"] for summary in first} == {
        "year=2025/month=01/day=20/hour=10",
        "year=2025/month=01/day=20/hour=11",
    }
    first_run_ids = {summary["run_id"] for summary in first}
    assert len(first_run_ids) == 1
    UUID(next(iter(first_run_ids)))
    assert all(summary["status"] == "success" for summary in first)
    assert all(summary["records_replayed"] == 3 for summary in first)
    assert all(summary["raw_events"] == 0 for summary in first)
    assert all(summary["curated_records"] > 0 for summary in first)
    assert len({summary["run_started_at"] for summary in first}) == 1
    assert len({summary["run_ended_at"] for summary in first}) == 1
    assert all(summary["started_at"] <= summary["ended_at"] for summary in first)

    second = run_backfill(
        "2025-01-20T10:00:00Z",
        "2025-01-20T11:00:00Z",
        "hourly",
        storage_config_path=storage_config_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        vol_window=2,
    )
    assert len(second) == 2
    second_run_ids = {summary["run_id"] for summary in second}
    assert len(second_run_ids) == 1
    assert second_run_ids != first_run_ids
    assert all(summary["status"] == "success" for summary in second)
    assert all(summary["raw_events"] == 0 for summary in second)
    assert all(summary["curated_records"] == 0 for summary in second)
