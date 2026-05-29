from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest
import yaml

from src.common.exceptions import OverlapError, ValidationError
from src.orchestration.locks import acquire_partition_locks, release_partition_locks
from src.orchestration.run_pipeline import run_pipeline


def _count_parquet_rows(dataset_dir: Path) -> int:
    parquet_files = sorted(dataset_dir.rglob("*.parquet"))
    if not parquet_files:
        return 0
    escaped_paths = [str(path).replace("'", "''") for path in parquet_files]
    quoted = ", ".join(f"'{path}'" for path in escaped_paths)
    with duckdb.connect() as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM read_parquet([{quoted}])").fetchone()[0])


def _read_parquet_rows(dataset_dir: Path) -> list[dict]:
    parquet_files = sorted(dataset_dir.rglob("*.parquet"))
    escaped_paths = [str(path).replace("'", "''") for path in parquet_files]
    quoted = ", ".join(f"'{path}'" for path in escaped_paths)
    with duckdb.connect() as conn:
        return conn.execute(f"SELECT * FROM read_parquet([{quoted}])").df().to_dict("records")


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
    assert summary["required_fields_status"] == "ok"
    assert summary["missing_required_field_count"] == 0
    assert summary["missing_required_record_count"] == 0
    assert summary["null_rate_status"] == "ok"
    assert summary["null_field_count"] == 0
    assert summary["null_record_count"] == 0
    assert summary["max_null_rate"] == 0.0
    assert summary["value_validity_status"] == "ok"
    assert summary["invalid_value_count"] == 0
    assert summary["invalid_value_record_count"] == 0
    assert set(summary["volatility_latest"]) == {"AAPL", "MSFT"}
    assert set(summary["value_at_risk"]) == {"AAPL", "MSFT"}

    assert _count_parquet_rows(tmp_path / "curated" / "returns_1m") == 4
    assert _count_parquet_rows(tmp_path / "curated" / "volatility_5m") == 2
    assert _count_parquet_rows(tmp_path / "curated" / "data_quality_metrics") == 1
    assert _count_parquet_rows(tmp_path / "curated" / "risk_summary") == 2

    data_quality_rows = _read_parquet_rows(tmp_path / "curated" / "data_quality_metrics")
    assert data_quality_rows[0]["required_fields_status"] == "ok"
    assert data_quality_rows[0]["required_fields_checked"] == 7
    assert data_quality_rows[0]["missing_required_field_count"] == 0
    assert data_quality_rows[0]["missing_required_record_count"] == 0
    assert data_quality_rows[0]["null_rate_status"] == "ok"
    assert data_quality_rows[0]["null_fields_checked"] == 7
    assert data_quality_rows[0]["null_field_count"] == 0
    assert data_quality_rows[0]["null_record_count"] == 0
    assert data_quality_rows[0]["max_null_rate"] == 0.0
    assert data_quality_rows[0]["value_validity_status"] == "ok"
    assert data_quality_rows[0]["value_fields_checked"] == 2
    assert data_quality_rows[0]["invalid_value_count"] == 0
    assert data_quality_rows[0]["invalid_value_record_count"] == 0


def test_run_pipeline_rejects_missing_required_fields(tmp_path: Path) -> None:
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
        },
    ]
    input_path = tmp_path / "events.json"
    with input_path.open("w", encoding="utf-8") as handle:
        json.dump(events, handle)

    with pytest.raises(ValidationError, match="Missing required fields in 1 records"):
        run_pipeline(
            input_path=input_path,
            thresholds_path=Path("config/risk_thresholds.yaml"),
            late_seconds=60,
            window_minutes=5,
            vol_window=2,
            storage_config_path=storage_config_path,
        )


def test_run_pipeline_rejects_null_required_fields(tmp_path: Path) -> None:
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
            "price": None,
            "volume": 11,
            "ts_event": "2025-01-20T10:02:00Z",
            "ts_ingest": "2025-01-20T10:02:04Z",
            "source": "stooq",
        },
    ]
    input_path = tmp_path / "events.json"
    with input_path.open("w", encoding="utf-8") as handle:
        json.dump(events, handle)

    with pytest.raises(ValidationError, match="Null required field values in 1 records"):
        run_pipeline(
            input_path=input_path,
            thresholds_path=Path("config/risk_thresholds.yaml"),
            late_seconds=60,
            window_minutes=5,
            vol_window=2,
            storage_config_path=storage_config_path,
        )


def test_run_pipeline_rejects_invalid_numeric_field_values(tmp_path: Path) -> None:
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
            "price": 0.0,
            "volume": 11,
            "ts_event": "2025-01-20T10:02:00Z",
            "ts_ingest": "2025-01-20T10:02:04Z",
            "source": "stooq",
        },
        {
            "event_id": "evt-3",
            "symbol": "MSFT",
            "price": 240.0,
            "volume": -1,
            "ts_event": "2025-01-20T10:03:00Z",
            "ts_ingest": "2025-01-20T10:03:04Z",
            "source": "stooq",
        },
    ]
    input_path = tmp_path / "events.json"
    with input_path.open("w", encoding="utf-8") as handle:
        json.dump(events, handle)

    with pytest.raises(ValidationError, match="Invalid numeric field values in 2 records"):
        run_pipeline(
            input_path=input_path,
            thresholds_path=Path("config/risk_thresholds.yaml"),
            late_seconds=60,
            window_minutes=5,
            vol_window=2,
            storage_config_path=storage_config_path,
        )


def test_run_pipeline_blocks_when_partition_is_locked(tmp_path: Path) -> None:
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
    ]
    input_path = tmp_path / "events.json"
    with input_path.open("w", encoding="utf-8") as handle:
        json.dump(events, handle)

    locked_partition = "year=2025/month=01/day=20/hour=10"
    lock_paths = acquire_partition_locks(tmp_path, [locked_partition], owner="live:test")
    try:
        with pytest.raises(OverlapError):
            run_pipeline(
                input_path=input_path,
                thresholds_path=Path("config/risk_thresholds.yaml"),
                late_seconds=60,
                window_minutes=5,
                vol_window=2,
                storage_config_path=storage_config_path,
                lock_owner="backfill:test",
            )
    finally:
        release_partition_locks(lock_paths)


def test_run_pipeline_replaces_stale_partition_lock(tmp_path: Path) -> None:
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
    ]
    input_path = tmp_path / "events.json"
    with input_path.open("w", encoding="utf-8") as handle:
        json.dump(events, handle)

    locked_partition = "year=2025/month=01/day=20/hour=10"
    lock_path = tmp_path / ".orchestration_locks" / locked_partition / ".lock"
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text(
        json.dumps(
            {
                "owner": "crashed:test",
                "partition": locked_partition,
                "acquired_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            }
        ),
        encoding="utf-8",
    )

    summary = run_pipeline(
        input_path=input_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        late_seconds=60,
        window_minutes=5,
        vol_window=2,
        storage_config_path=storage_config_path,
        lock_owner="backfill:test",
        lock_stale_seconds=3600,
    )

    assert summary["raw_events"] == 2
    assert not lock_path.exists()


def test_run_pipeline_materializes_external_signal_summary(tmp_path: Path) -> None:
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
                    "external_signal_summary": "external_signal_summary",
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
    ]
    signals = [
        {
            "signal_id": "sig-vix-1",
            "name": "vix",
            "value": 18.4,
            "ts_event": "2025-01-20T10:01:00Z",
            "ts_ingest": "2025-01-20T10:01:10Z",
            "source": "cboe",
        },
        {
            "signal_id": "sig-vix-2",
            "name": "vix",
            "value": 19.1,
            "ts_event": "2025-01-20T10:04:00Z",
            "ts_ingest": "2025-01-20T10:04:10Z",
            "source": "cboe",
        },
        {
            "signal_id": "sig-spread-1",
            "name": "credit-spread",
            "value": 1.25,
            "ts_event": "2025-01-20T10:02:00Z",
            "ts_ingest": "2025-01-20T10:02:10Z",
            "source": "fred",
        },
    ]
    input_path = tmp_path / "events.json"
    signals_path = tmp_path / "signals.json"
    with input_path.open("w", encoding="utf-8") as handle:
        json.dump(events, handle)
    with signals_path.open("w", encoding="utf-8") as handle:
        json.dump(signals, handle)

    summary = run_pipeline(
        input_path=input_path,
        signals_path=signals_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        late_seconds=60,
        window_minutes=5,
        vol_window=2,
        storage_config_path=storage_config_path,
    )

    assert summary["external_signal_count"] == 3
    assert summary["external_signals_latest"] == {
        "CREDIT_SPREAD": 1.25,
        "VIX": 19.1,
    }
    assert summary["curated_records_by_dataset"]["external_signal_summary"] == 2

    signal_rows = _read_parquet_rows(tmp_path / "curated" / "external_signal_summary")
    signal_by_name = {row["name"]: row for row in signal_rows}
    assert set(signal_by_name) == {"CREDIT_SPREAD", "VIX"}
    assert signal_by_name["VIX"]["latest_signal_id"] == "sig-vix-2"
    assert signal_by_name["VIX"]["latest_value"] == 19.1

    risk_rows = _read_parquet_rows(tmp_path / "curated" / "risk_summary")
    assert risk_rows[0]["external_signal_count"] == 3
    assert risk_rows[0]["latest_external_signal_name"] == "VIX"
    assert risk_rows[0]["latest_external_signal_value"] == 19.1
    assert risk_rows[0]["latest_external_signal_source"] == "cboe"
