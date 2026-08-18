from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from src.ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from src.orchestration.backfill import _load_partition_records, run_backfill
from src.orchestration.locks import acquire_partition_locks, release_partition_locks
from src.storage.s3_writer import write_records
from tests.storage_config_helpers import build_storage_config, write_storage_config


def _seed_raw_partitions(storage_config: dict[str, Any]) -> None:
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


def test_run_backfill_replays_hourly_partitions_resumes_and_can_force_rerun(tmp_path: Path) -> None:
    storage_config = build_storage_config(tmp_path, include_external_signal_summary=False)
    storage_config_path = write_storage_config(tmp_path, include_external_signal_summary=False)

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
    assert second == []

    third = run_backfill(
        "2025-01-20T10:00:00Z",
        "2025-01-20T11:00:00Z",
        "hourly",
        storage_config_path=storage_config_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        vol_window=2,
        resume=False,
    )
    assert len(third) == 2
    third_run_ids = {summary["run_id"] for summary in third}
    assert len(third_run_ids) == 1
    assert third_run_ids != first_run_ids
    assert all(summary["status"] == "success" for summary in third)
    assert all(summary["raw_events"] == 0 for summary in third)
    assert all(summary["curated_records"] == 0 for summary in third)


def test_run_backfill_resumes_after_blocked_overlap(tmp_path: Path) -> None:
    storage_config = build_storage_config(tmp_path, include_external_signal_summary=False)
    storage_config_path = write_storage_config(tmp_path, include_external_signal_summary=False)

    _seed_raw_partitions(storage_config)
    locked_partition = "year=2025/month=01/day=20/hour=10"
    lock_paths = acquire_partition_locks(tmp_path, [locked_partition], owner="live:test")
    try:
        first = run_backfill(
            "2025-01-20T10:00:00Z",
            "2025-01-20T11:00:00Z",
            "hourly",
            storage_config_path=storage_config_path,
            thresholds_path=Path("config/risk_thresholds.yaml"),
            vol_window=2,
        )
    finally:
        release_partition_locks(lock_paths)

    assert len(first) == 1

    blocked = first[0]
    assert blocked["partition"] == locked_partition
    assert blocked["status"] == "blocked_overlap"
    assert blocked["records_replayed"] == 0
    assert blocked["raw_events"] == 0
    assert blocked["curated_records"] == 0
    assert "already locked" in blocked["overlap_error"]

    second = run_backfill(
        "2025-01-20T10:00:00Z",
        "2025-01-20T11:00:00Z",
        "hourly",
        storage_config_path=storage_config_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        vol_window=2,
    )
    assert len(second) == 2
    assert {summary["partition"] for summary in second} == {
        "year=2025/month=01/day=20/hour=10",
        "year=2025/month=01/day=20/hour=11",
    }
    assert all(summary["status"] == "success" for summary in second)
    assert all(summary["records_replayed"] == 3 for summary in second)
    assert all(summary["curated_records"] > 0 for summary in second)

    third = run_backfill(
        "2025-01-20T10:00:00Z",
        "2025-01-20T11:00:00Z",
        "hourly",
        storage_config_path=storage_config_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        vol_window=2,
    )
    assert third == []


def test_backfill_preserves_raw_identity_precision(tmp_path: Path) -> None:
    storage_config = build_storage_config(tmp_path, include_external_signal_summary=False)
    storage_config_path = write_storage_config(
        tmp_path,
        include_external_signal_summary=False,
    )
    precise_event = {
        "event_id": "precise-event",
        "symbol": "AAPL",
        "price": 100.1234567890123,
        "volume": 10,
        "ts_event": datetime(
            2025,
            1,
            20,
            10,
            1,
            2,
            123456,
            tzinfo=timezone.utc,
        ),
        "ts_ingest": datetime(2025, 1, 20, 10, 2, tzinfo=timezone.utc),
        "source": "stooq",
    }
    assert write_records([precise_event], kind="raw", storage_config=storage_config) == 1

    result = run_backfill(
        "2025-01-20T10:00:00Z",
        "2025-01-20T10:00:00Z",
        "hourly",
        storage_config_path=storage_config_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        vol_window=2,
    )

    assert len(result) == 1
    assert result[0]["status"] == "success"
    assert result[0]["records_replayed"] == 1
    assert result[0]["raw_events"] == 0


def test_backfill_partition_read_does_not_require_pytz(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage_config = build_storage_config(tmp_path, include_external_signal_summary=False)
    _seed_raw_partitions(storage_config)
    partition_dir = (
        Path(storage_config["storage"]["raw"]["base_path"])
        / storage_config["storage"]["raw"]["dataset"]
        / "year=2025/month=01/day=20/hour=10"
    )

    monkeypatch.setitem(sys.modules, "pytz", None)

    records = _load_partition_records(partition_dir)
    assert len(records) == 3
    assert records[0]["ts_event"].endswith("+00:00")


def test_backfill_excludes_alpha_vantage_daily_events_from_minute_analytics(
    tmp_path: Path,
) -> None:
    storage_config = build_storage_config(tmp_path, include_external_signal_summary=False)
    storage_config_path = write_storage_config(
        tmp_path,
        include_external_signal_summary=False,
    )
    common = {
        "symbol": "IBM",
        "price": 101.5,
        "volume": 1_200,
        "ts_ingest": datetime(2025, 2, 2, 12, 30, tzinfo=timezone.utc),
    }
    events = [
        {
            **common,
            "event_id": alpha_vantage_daily_event_id("IBM", date(2025, 1, 3)),
            "ts_event": datetime(2025, 1, 3, tzinfo=timezone.utc),
            "source": "alpha_vantage",
        },
        {
            **common,
            "event_id": "future-alpha-identity",
            "ts_event": datetime(2025, 1, 4, tzinfo=timezone.utc),
            "source": "alpha_vantage",
        },
        {
            **common,
            "event_id": "av-daily-legacy-marker",
            "ts_event": datetime(2025, 1, 5, tzinfo=timezone.utc),
            "source": "legacy-provider",
        },
    ]
    assert write_records(events, kind="raw", storage_config=storage_config) == 3

    result = run_backfill(
        "2025-02-02T12:00:00Z",
        "2025-02-02T12:00:00Z",
        "hourly",
        storage_config_path=storage_config_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        resume=False,
    )

    assert len(result) == 1
    assert result[0]["status"] == "skipped_no_records"
    assert result[0]["records_replayed"] == 0
    assert not (tmp_path / "curated").exists()
