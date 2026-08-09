from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.orchestration.run_pipeline import _load_input, run_pipeline


def _write_payload(tmp_path: Path, payload: Any) -> Path:
    input_path = tmp_path / "events.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    return input_path


def test_load_input_accepts_a_list_of_event_objects(tmp_path: Path) -> None:
    payload = [{"event_id": "evt-1"}, {"event_id": "evt-2"}]

    assert _load_input(_write_payload(tmp_path, payload)) == payload


@pytest.mark.parametrize(
    "payload",
    [
        {"event_id": "evt-1"},
        [42],
        ["evt-1"],
        [[]],
        [None],
        [{"event_id": "evt-1"}, 42],
    ],
)
def test_run_pipeline_rejects_input_that_is_not_a_list_of_event_objects(
    tmp_path: Path,
    payload: Any,
) -> None:
    input_path = _write_payload(tmp_path, payload)

    with pytest.raises(ValueError, match="Input JSON must be a list of event objects"):
        run_pipeline(
            input_path=input_path,
            thresholds_path=tmp_path / "thresholds-are-not-loaded.yaml",
            late_seconds=60,
            window_minutes=5,
            vol_window=2,
            storage_config_path=tmp_path / "storage-is-not-loaded.yaml",
        )
