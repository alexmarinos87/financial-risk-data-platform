"""Backfill selection and saved progress follow UTC storage hours, not wall time."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import OverlapError
from src.orchestration import backfill
from src.storage.partitioning import partition_path

UTC = timezone.utc
START = datetime(2025, 12, 31, 23, 40, tzinfo=UTC)
END = datetime(2026, 1, 1, 1, tzinfo=UTC)
HOURS = [datetime(2025, 12, 31, 23, tzinfo=UTC),
         datetime(2026, 1, 1, 0, tzinfo=UTC), END]


def _configure(root: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    raw = root / "raw/events"
    state = root / ".orchestration_state/backfill_resume.json"
    monkeypatch.setattr(backfill, "load_storage_config", lambda path: {"storage": {
        "base_dir": str(root), "raw": {"base_path": str(root / "raw"), "dataset": "events"},
    }})
    return raw, state


@pytest.mark.parametrize("offset_minutes", [0, 60, 330, 345, -210])
def test_equivalent_offsets_scan_the_same_complete_inclusive_utc_hours(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, offset_minutes: int,
) -> None:
    raw, state = _configure(tmp_path, monkeypatch)
    visited: list[Path] = []

    def load(path: Path) -> list[dict[str, Any]]:
        visited.append(path)
        return []

    monkeypatch.setattr(backfill, "_load_partition_records", load)
    offset = timezone(timedelta(minutes=offset_minutes))
    results = backfill.run_backfill(
        START.astimezone(offset).isoformat(), END.astimezone(offset).isoformat(),
        "hourly", resume=False,
    )
    expected = [partition_path(value) for value in HOURS]
    assert visited == [raw / partition for partition in expected]
    assert [item["partition"] for item in results] == expected
    assert [item["window_start"] for item in results] == [value.isoformat() for value in HOURS]
    assert all(item["status"] == "skipped_no_records" for item in results)
    assert not state.exists()


@pytest.mark.parametrize("checkpoint", [
    "2025-12-31T23:00:00Z", "2026-01-01T05:00:00+05:45",
    "2025-12-31T20:00:00-03:30",
])
def test_legacy_checkpoint_resumes_after_its_utc_partition_not_its_wall_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, checkpoint: str,
) -> None:
    raw, state = _configure(tmp_path, monkeypatch)
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"hourly": {
        "last_successful_window_start": checkpoint,
        "last_successful_partition": partition_path(HOURS[0]),
    }}), encoding="utf-8")
    before = state.read_bytes()
    visited: list[Path] = []

    def load(path: Path) -> list[dict[str, Any]]:
        visited.append(path)
        return []

    monkeypatch.setattr(backfill, "_load_partition_records", load)
    results = backfill.run_backfill(START.isoformat(), HOURS[1].isoformat(), "hourly")
    assert visited == [raw / partition_path(HOURS[1])]
    assert [item["window_start"] for item in results] == [HOURS[1].isoformat()]
    assert state.read_bytes() == before  # Empty partitions do not advance saved progress.


def test_checkpoint_and_overlap_summary_keep_the_actual_utc_hour(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, state = _configure(tmp_path, monkeypatch)
    monkeypatch.setattr(backfill, "_load_partition_records", lambda path: [{"event_id": "one"}])
    replay_files: list[str] = []

    def run(**kwargs: Any) -> dict[str, Any]:
        replay_files.append(kwargs["input_path"].name)
        assert json.loads(kwargs["input_path"].read_text()) == [{"event_id": "one"}]
        if len(replay_files) == 2:
            raise OverlapError("held by another run")
        return {"raw_events": 0, "curated_records": 1}

    monkeypatch.setattr(backfill, "run_pipeline", run)
    results = backfill.run_backfill(
        "2026-01-01T05:10:00+05:30", "2026-01-01T06:30:00+05:30", "hourly",
        resume=False,
    )
    assert [row["status"] for row in results] == ["success", "blocked_overlap"]
    assert [row["window_start"] for row in results] == [v.isoformat() for v in HOURS[:2]]
    assert replay_files == ["20251231T230000Z.json", "20260101T000000Z.json"]
    saved = json.loads(state.read_text())["hourly"]
    assert saved["last_successful_window_start"] == HOURS[0].isoformat()
    assert saved["last_successful_partition"] == partition_path(HOURS[0])


def test_naive_inputs_still_mean_utc(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(tmp_path, monkeypatch)
    monkeypatch.setattr(backfill, "_load_partition_records", lambda path: [])
    results = backfill.run_backfill("2026-01-01T00:30:00", "2026-01-01T01:00:00", "hour")
    assert [row["window_start"] for row in results] == [v.isoformat() for v in HOURS[1:]]


@pytest.mark.parametrize("text", ["0001-01-01T00:00:00+01:00", "9999-12-31T23:00:00-02:00"])
def test_unrepresentable_utc_instants_reject_before_storage(
    monkeypatch: pytest.MonkeyPatch, text: str,
) -> None:
    calls: list[Path] = []

    def loader(path: Path) -> dict[str, Any]:
        calls.append(path)
        raise AssertionError("invalid UTC instant must not load storage")

    monkeypatch.setattr(backfill, "load_storage_config", loader)
    with pytest.raises(ValueError, match="outside the supported UTC range"):
        backfill.run_backfill(text, text, "hourly")
    assert calls == []
