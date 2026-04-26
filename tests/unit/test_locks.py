from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.common.exceptions import OverlapError
from src.orchestration.locks import acquire_partition_locks, release_partition_locks


def _lock_path(base_dir: Path, partition: str) -> Path:
    return base_dir / ".orchestration_locks" / partition / ".lock"


def test_active_partition_lock_blocks(tmp_path: Path) -> None:
    partition = "year=2025/month=01/day=20/hour=10"
    lock_paths = acquire_partition_locks(tmp_path, [partition], owner="live:test")

    try:
        with pytest.raises(OverlapError):
            acquire_partition_locks(
                tmp_path,
                [partition],
                owner="backfill:test",
                stale_after_seconds=3600,
            )
    finally:
        release_partition_locks(lock_paths)


def test_stale_partition_lock_is_replaced(tmp_path: Path) -> None:
    partition = "year=2025/month=01/day=20/hour=10"
    path = _lock_path(tmp_path, partition)
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "owner": "crashed:test",
                "partition": partition,
                "acquired_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            }
        ),
        encoding="utf-8",
    )

    lock_paths = acquire_partition_locks(
        tmp_path,
        [partition],
        owner="backfill:test",
        stale_after_seconds=3600,
    )

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["owner"] == "backfill:test"
    finally:
        release_partition_locks(lock_paths)


def test_malformed_partition_lock_still_blocks(tmp_path: Path) -> None:
    partition = "year=2025/month=01/day=20/hour=10"
    path = _lock_path(tmp_path, partition)
    path.parent.mkdir(parents=True)
    path.write_text("not-json", encoding="utf-8")

    with pytest.raises(OverlapError):
        acquire_partition_locks(
            tmp_path,
            [partition],
            owner="backfill:test",
            stale_after_seconds=3600,
        )
