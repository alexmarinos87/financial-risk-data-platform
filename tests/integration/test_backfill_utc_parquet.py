"""Equivalent offset requests replay the same actual UTC-partitioned raw files."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from src.orchestration.backfill import run_backfill
from src.storage.s3_writer import write_records
from tests.storage_config_helpers import build_storage_config, write_storage_config


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*.parquet")}


@pytest.mark.parametrize("start,end", [
    ("2026-01-01T05:45:00+05:45", "2026-01-01T06:45:00+05:45"),
    ("2025-12-31T20:30:00-03:30", "2025-12-31T21:30:00-03:30"),
])
def test_offset_backfill_replays_both_utc_hours_and_preserves_replay_bytes(
    tmp_path: Path, start: str, end: str,
) -> None:
    config = build_storage_config(tmp_path)
    config_path = write_storage_config(tmp_path)
    records: list[dict[str, Any]] = [
        {
            "event_id": f"offset-{hour}-{minute}", "symbol": "IBM", "price": 100.0 + minute,
            "volume": 10, "source": "fixture",
            "ts_event": datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc),
            "ts_ingest": datetime(2026, 1, 1, hour, minute, 1, tzinfo=timezone.utc),
        }
        for hour in (0, 1) for minute in (0, 1)
    ]
    assert write_records(records, kind="raw", storage_config=config) == 4
    raw_before = _snapshot(tmp_path / "raw")
    first = run_backfill(start, end, "hourly", storage_config_path=config_path, resume=False)
    expected = [f"year=2026/month=01/day=01/hour={hour:02d}" for hour in (0, 1)]
    assert [row["partition"] for row in first] == expected
    assert [row["status"] for row in first] == ["success", "success"]
    assert [row["records_replayed"] for row in first] == [2, 2]
    assert _snapshot(tmp_path / "raw") == raw_before
    curated_before = _snapshot(tmp_path / "curated")
    assert curated_before
    replay = run_backfill(
        "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", "hourly",
        storage_config_path=config_path, resume=False,
    )
    assert [row["partition"] for row in replay] == expected
    assert all(row["status"] == "success" for row in replay)
    assert _snapshot(tmp_path / "raw") == raw_before
    assert _snapshot(tmp_path / "curated") == curated_before
    assert not list((tmp_path / ".orchestration_locks").rglob(".lock"))
