from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import StorageError
from src.storage.s3_writer import write_records
from src.warehouse.market_freshness_loader import (
    COLUMNS,
    TABLE_NAME,
    build_upsert_sql,
    collect_market_freshness_records,
)
from tests.storage_config_helpers import build_storage_config


def _record() -> dict[str, Any]:
    return {
        "model_version": "market-freshness-v1",
        "calculation_id": "freshness-1",
        "calendar_id": "XNYS",
        "calendar_fingerprint": "calendar-1",
        "calendar_timezone": "America/New_York",
        "calendar_valid_from": date(2026, 1, 1),
        "calendar_valid_to": date(2027, 1, 1),
        "source": "alpha_vantage",
        "symbol": "AAPL",
        "as_of_date": date(2026, 1, 10),
        "as_of_day_type": "weekend",
        "ts_event": datetime(2026, 1, 10, tzinfo=timezone.utc),
        "ts_ingest": datetime(2026, 1, 9, 12, tzinfo=timezone.utc),
        "first_observation_date": date(2026, 1, 8),
        "latest_observation_date": date(2026, 1, 9),
        "expected_latest_session_date": date(2026, 1, 9),
        "expected_session_close_time": "16:00",
        "expected_session_is_early_close": False,
        "observation_count": 2,
        "expected_session_count": 2,
        "missing_session_count": 0,
        "trailing_missing_session_count": 0,
        "missing_sessions_json": "[]",
        "freshness_status": "current",
        "input_fingerprint": "input-1",
    }


def _write_config(tmp_path: Path, storage: dict[str, Any]) -> Path:
    path = tmp_path / "storage.yaml"
    path.write_text(
        yaml.safe_dump(storage, sort_keys=False),
        encoding="utf-8",
    )
    return path


def test_loader_collects_parquet_and_builds_calculation_id_upsert(
    tmp_path: Path,
) -> None:
    storage = build_storage_config(tmp_path)
    path = _write_config(tmp_path, storage)
    assert write_records(
        [_record()],
        kind="curated",
        dataset="daily_market_freshness",
        storage_config=storage,
    ) == 1

    records = collect_market_freshness_records(path)
    assert len(records) == 1
    assert set(records[0]) == set(COLUMNS)
    assert records[0]["calculation_id"] == "freshness-1"

    statement = build_upsert_sql()
    assert f'"{TABLE_NAME}"' in statement
    assert 'ON CONFLICT ("calculation_id")' in statement
    assert "DO UPDATE SET" in statement


def test_loader_returns_zero_when_dataset_is_absent(tmp_path: Path) -> None:
    storage = build_storage_config(tmp_path)
    path = _write_config(tmp_path, storage)
    assert collect_market_freshness_records(path) == []


def test_loader_rejects_invalid_missing_session_json(tmp_path: Path) -> None:
    storage = build_storage_config(tmp_path)
    path = _write_config(tmp_path, storage)
    record = _record()
    record["missing_sessions_json"] = "{}"
    assert write_records(
        [record],
        kind="curated",
        dataset="daily_market_freshness",
        storage_config=storage,
    ) == 1

    with pytest.raises(StorageError, match="JSON string array"):
        collect_market_freshness_records(path)


def test_loader_rejects_symbolic_link_dataset(tmp_path: Path) -> None:
    storage = build_storage_config(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    dataset_path = (
        Path(storage["storage"]["curated"]["base_path"])
        / storage["storage"]["curated"]["datasets"][
            "daily_market_freshness"
        ]
    )
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    dataset_path.symlink_to(target, target_is_directory=True)
    path = _write_config(tmp_path, storage)

    with pytest.raises(StorageError, match="symbolic link"):
        collect_market_freshness_records(path)
