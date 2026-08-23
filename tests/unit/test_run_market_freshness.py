from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.market_calendar import parse_market_calendar
from src.common.exceptions import StorageError
from src.ingestion.schemas import MarketEvent
from src.orchestration.run_market_freshness import (
    OUTPUT_DATASET,
    run_market_freshness,
)


def _calendar():
    return parse_market_calendar(
        {
            "calendars": {
                "XNYS": {
                    "timezone": "America/New_York",
                    "valid_from": "2026-01-01",
                    "valid_to": "2027-01-01",
                    "session_weekdays": [0, 1, 2, 3, 4],
                    "regular_close_time": "16:00",
                    "holidays": ["2026-01-01"],
                    "early_closes": {},
                }
            }
        },
        "XNYS",
    )


def _event(day: int) -> MarketEvent:
    timestamp = datetime(2026, 1, day, tzinfo=timezone.utc)
    return MarketEvent(
        event_id=f"event-{day}",
        symbol="AAPL",
        price=100.0 + day,
        volume=100,
        ts_event=timestamp,
        ts_ingest=timestamp.replace(hour=12),
        source="alpha_vantage",
    )


def _storage() -> dict[str, Any]:
    return {
        "storage": {
            "base_dir": "unused",
            "raw": {
                "base_path": "unused/raw",
                "dataset": "market_events",
            },
            "curated": {
                "base_path": "unused/curated",
                "datasets": {
                    OUTPUT_DATASET: OUTPUT_DATASET,
                },
            },
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }


def test_runner_publishes_one_credential_free_freshness_record() -> None:
    writes: list[dict[str, Any]] = []

    def writer(records: list[dict[str, Any]], **kwargs: Any) -> int:
        assert kwargs["dataset"] == OUTPUT_DATASET
        writes.extend(records)
        return 1

    summary = run_market_freshness(
        symbol="aapl",
        as_of_date=date(2026, 1, 10),
        calendar_id="XNYS",
        calendar_config_path=Path("calendar.yaml"),
        storage_config_path=Path("storage.yaml"),
        reader=lambda **_: [_event(8), _event(9)],
        writer=writer,
        storage_config_loader=lambda _: _storage(),
        calendar_loader=lambda *_: _calendar(),
    )

    assert summary["provider_request_performed"] is False
    assert summary["latest_status"]["freshness_status"] == "current"
    assert summary["curated_output"][OUTPUT_DATASET] == {
        "records_selected": 1,
        "records_written": 1,
        "records_already_present": 0,
    }
    assert len(writes) == 1


def test_runner_reports_replay_and_invalid_writer_results() -> None:
    replay = run_market_freshness(
        symbol="AAPL",
        as_of_date=date(2026, 1, 10),
        calendar_id="XNYS",
        calendar_config_path=Path("calendar.yaml"),
        storage_config_path=Path("storage.yaml"),
        reader=lambda **_: [_event(8), _event(9)],
        writer=lambda *_args, **_kwargs: 0,
        storage_config_loader=lambda _: _storage(),
        calendar_loader=lambda *_: _calendar(),
    )
    assert replay["curated_output"][OUTPUT_DATASET][
        "records_already_present"
    ] == 1

    with pytest.raises(StorageError, match="invalid result"):
        run_market_freshness(
            symbol="AAPL",
            as_of_date=date(2026, 1, 10),
            calendar_id="XNYS",
            calendar_config_path=Path("calendar.yaml"),
            storage_config_path=Path("storage.yaml"),
            reader=lambda **_: [_event(8), _event(9)],
            writer=lambda *_args, **_kwargs: 2,
            storage_config_loader=lambda _: _storage(),
            calendar_loader=lambda *_: _calendar(),
        )


def test_runner_requires_configured_output_dataset() -> None:
    storage = _storage()
    storage["storage"]["curated"]["datasets"].clear()

    with pytest.raises(StorageError, match=OUTPUT_DATASET):
        run_market_freshness(
            symbol="AAPL",
            as_of_date=date(2026, 1, 10),
            calendar_id="XNYS",
            calendar_config_path=Path("calendar.yaml"),
            storage_config_path=Path("storage.yaml"),
            reader=lambda **_: [_event(8), _event(9)],
            writer=lambda *_args, **_kwargs: 1,
            storage_config_loader=lambda _: storage,
            calendar_loader=lambda *_: _calendar(),
        )
