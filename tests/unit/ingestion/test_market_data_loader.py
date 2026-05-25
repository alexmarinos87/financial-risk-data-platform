from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.common.exceptions import IngestionError
from src.ingestion.market_data_loader import (
    build_market_event_from_row,
    load_market_events,
    market_events_to_records,
)


def test_load_market_events_from_csv_generates_stable_ids(tmp_path: Path) -> None:
    path = tmp_path / "market.csv"
    path.write_text(
        "ticker,close,volume,timestamp\n"
        " aapl ,189.32,1200,2025-01-20T10:01:00Z\n"
        "MSFT,410.5,900,2025-01-20T10:02:00Z\n",
        encoding="utf-8",
    )
    ingested_at = datetime(2025, 1, 20, 10, 3, tzinfo=timezone.utc)

    first = load_market_events(path, ingested_at=ingested_at)
    second = load_market_events(path, ingested_at=ingested_at)

    assert [event.symbol for event in first] == ["AAPL", "MSFT"]
    assert [event.event_id for event in first] == [event.event_id for event in second]
    assert first[0].event_id.startswith("market-")
    assert first[0].source == "stooq"
    assert first[0].ts_ingest == ingested_at


def test_load_market_events_from_json_lines_preserves_supplied_ids(tmp_path: Path) -> None:
    path = tmp_path / "market.jsonl"
    rows = [
        {
            "event_id": "evt-1",
            "symbol": "AAPL",
            "price": 189.32,
            "volume": 1200,
            "ts_event": "2025-01-20T10:01:00Z",
            "ts_ingest": "2025-01-20T10:01:03Z",
            "source": "polygon",
        },
        {
            "id": "evt-2",
            "instrument": "msft",
            "last_price": "410.5",
            "trade_volume": "900",
            "event_time": "2025-01-20T10:02:00+00:00",
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    events = load_market_events(path)

    assert [event.event_id for event in events] == ["evt-1", "evt-2"]
    assert events[0].source == "polygon"
    assert events[1].symbol == "MSFT"
    assert events[1].price == 410.5
    assert events[1].volume == 900


def test_market_events_to_records_returns_pipeline_records(tmp_path: Path) -> None:
    path = tmp_path / "market.json"
    path.write_text(
        json.dumps(
            [
                {
                    "symbol": "AAPL",
                    "price": 189.32,
                    "volume": 1200,
                    "ts_event": "2025-01-20T10:01:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )
    ingested_at = datetime(2025, 1, 20, 10, 3, tzinfo=timezone.utc)

    records = market_events_to_records(load_market_events(path, ingested_at=ingested_at))

    assert records == [
        {
            "event_id": records[0]["event_id"],
            "symbol": "AAPL",
            "price": 189.32,
            "volume": 1200,
            "ts_event": datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc),
            "ts_ingest": ingested_at,
            "source": "stooq",
        }
    ]


def test_build_market_event_from_row_rejects_missing_required_fields() -> None:
    with pytest.raises(IngestionError, match="Missing required market data fields"):
        build_market_event_from_row({"symbol": "AAPL", "price": 189.32})


def test_load_market_events_rejects_unsupported_file_type(tmp_path: Path) -> None:
    path = tmp_path / "market.txt"
    path.write_text("symbol=AAPL", encoding="utf-8")

    with pytest.raises(IngestionError, match="Unsupported market data file type"):
        load_market_events(path)
