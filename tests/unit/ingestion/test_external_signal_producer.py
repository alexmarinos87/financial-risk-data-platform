from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.common.exceptions import IngestionError
from src.ingestion.external_signal_producer import (
    build_external_signal_from_row,
    external_signals_to_records,
    load_external_signals,
)


def test_load_external_signals_from_json_lines_generates_stable_ids(tmp_path: Path) -> None:
    path = tmp_path / "signals.jsonl"
    rows = [
        {
            "indicator": " vix ",
            "reading": "18.4",
            "timestamp": "2025-01-20T10:00:00Z",
        },
        {
            "signal_id": "sig-fed-1",
            "name": "fed-funds",
            "value": 4.5,
            "ts_event": "2025-01-20T10:01:00Z",
            "provider": "fred",
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    ingested_at = datetime(2025, 1, 20, 10, 3, tzinfo=timezone.utc)

    first = load_external_signals(path, ingested_at=ingested_at)
    second = load_external_signals(path, ingested_at=ingested_at)

    assert [signal.name for signal in first] == ["VIX", "FED_FUNDS"]
    assert [signal.signal_id for signal in first] == [signal.signal_id for signal in second]
    assert first[0].signal_id.startswith("signal-")
    assert first[0].source == "fred"
    assert first[0].ts_ingest == ingested_at
    assert first[1].signal_id == "sig-fed-1"


def test_external_signals_to_records_returns_pipeline_records(tmp_path: Path) -> None:
    path = tmp_path / "signals.csv"
    path.write_text(
        "name,value,ts_event\n"
        "credit-spread,1.25,2025-01-20T10:01:00Z\n",
        encoding="utf-8",
    )
    ingested_at = datetime(2025, 1, 20, 10, 3, tzinfo=timezone.utc)

    records = external_signals_to_records(load_external_signals(path, ingested_at=ingested_at))

    assert records == [
        {
            "signal_id": records[0]["signal_id"],
            "name": "CREDIT_SPREAD",
            "value": 1.25,
            "ts_event": datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc),
            "ts_ingest": ingested_at,
            "source": "fred",
        }
    ]


def test_build_external_signal_from_row_rejects_missing_required_fields() -> None:
    with pytest.raises(IngestionError, match="Missing required external signal fields"):
        build_external_signal_from_row({"name": "VIX", "value": 18.4})


def test_load_external_signals_rejects_unsupported_file_type(tmp_path: Path) -> None:
    path = tmp_path / "signals.txt"
    path.write_text("name=VIX", encoding="utf-8")

    with pytest.raises(IngestionError, match="Unsupported external signal file type"):
        load_external_signals(path)
