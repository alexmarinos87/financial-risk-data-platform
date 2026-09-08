import json
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration import run_pipeline as pipeline
from src.processing.deduplicator import dedupe_events
from tests.storage_config_helpers import write_storage_config


def _event() -> dict[str, Any]:
    return {
        "event_id": "same-event",
        "symbol": "AAPL",
        "price": 100.0,
        "volume": 10,
        "ts_event": "2026-01-01T12:00:00Z",
        "ts_ingest": "2026-01-01T12:00:01Z",
        "source": "stooq",
    }


def test_conflict_rejected_before_any_pipeline_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    event = _event()
    input_path = tmp_path / "events.json"
    input_path.write_text(json.dumps([event, {**event, "price": 101.0}]), encoding="utf-8")
    storage_path = write_storage_config(tmp_path)

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("A conflicting batch must fail before locks or writes")

    monkeypatch.setattr(pipeline, "write_records", forbidden)
    monkeypatch.setattr(pipeline, "acquire_partition_locks", forbidden)
    with pytest.raises(ValidationError, match="Conflicting duplicate event identity"):
        pipeline.run_pipeline(
            input_path=input_path,
            thresholds_path=tmp_path / "thresholds-must-not-be-read.yaml",
            late_seconds=60,
            window_minutes=5,
            vol_window=2,
            storage_config_path=storage_path,
        )
    assert not list(tmp_path.rglob("*.parquet"))


def test_normalised_equivalent_events_still_collapse() -> None:
    first = _event()
    equivalent = {
        **first,
        "symbol": "aapl",
        "ts_event": "2026-01-01T13:00:00+01:00",
        "ts_ingest": "2026-01-01T13:00:01+01:00",
    }
    normalised = [pipeline._validate_and_normalize(event) for event in (first, equivalent)]
    assert dedupe_events(normalised) == [normalised[0]]
