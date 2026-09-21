"""Every emitted lock body must fit the existing bounded metadata reader."""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration import locks

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
ERROR = "^Partition lock metadata exceeds the size limit$"


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> type[datetime]:
    class Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return NOW if tz is None else NOW.astimezone(tz)

    monkeypatch.setattr(locks, "datetime", Clock)
    return Clock


def _body(owner: str, partition: str = "a") -> bytes:
    return json.dumps({
        "owner": owner, "partition": partition, "acquired_at": NOW.isoformat(),
    }).encode("utf-8")


def _owner_for_size(size: int, partition: str = "a") -> str:
    return "x" * (size - len(_body("", partition)))


@pytest.mark.parametrize("extra", [1, 1024])
def test_oversized_body_rejects_before_exclusive_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: type[datetime], extra: int,
) -> None:
    owner = _owner_for_size(locks._MAX_LOCK_METADATA_BYTES + extra)
    calls: list[str] = []
    original_open = os.open

    def opened(path: str, flags: int, *args: Any, **kwargs: Any) -> int:
        if flags & os.O_EXCL:
            calls.append(path)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", opened)
    with pytest.raises(ValidationError, match=ERROR):
        locks.acquire_partition_locks(tmp_path, ["a"], owner)
    assert calls == []
    assert list(tmp_path.rglob(".lock")) == []


@pytest.mark.parametrize("remaining", [0, 1])
def test_boundary_body_round_trips_through_stale_inspection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: type[datetime], remaining: int,
) -> None:
    size = locks._MAX_LOCK_METADATA_BYTES - remaining
    owner = _owner_for_size(size)
    paths = locks.acquire_partition_locks(tmp_path, ["a"], owner)
    try:
        assert paths[0].read_bytes() == _body(owner)
        assert paths[0].stat().st_size == size

        class Later(datetime):
            @classmethod
            def now(cls, tz: Any = None) -> datetime:
                return NOW + timedelta(hours=2)

        monkeypatch.setattr(locks, "datetime", Later)
        assert locks._is_stale_lock(paths[0], 3600) is True
    finally:
        locks.release_partition_locks(paths)


@pytest.mark.parametrize(
    "owner", ["é" * 11000, "\n" * 33000, '"' * 33000],
    ids=["unicode-escaping", "newline-escaping", "quote-escaping"],
)
def test_budget_counts_serialized_bytes_not_owner_characters(
    tmp_path: Path, clock: type[datetime], owner: str,
) -> None:
    assert len(owner) < locks._MAX_LOCK_METADATA_BYTES
    assert len(_body(owner)) > locks._MAX_LOCK_METADATA_BYTES
    with pytest.raises(ValidationError, match=ERROR):
        locks.acquire_partition_locks(tmp_path, ["a"], owner)
    assert list(tmp_path.rglob(".lock")) == []


def test_oversized_request_does_not_reclaim_existing_stale_owner(
    tmp_path: Path, clock: type[datetime],
) -> None:
    path = tmp_path / ".orchestration_locks/a/.lock"
    path.parent.mkdir(parents=True)
    content = json.dumps({"owner": "existing", "acquired_at": "2000-01-01T00:00:00Z"}).encode()
    path.write_bytes(content)
    inode = path.stat().st_ino
    owner = _owner_for_size(locks._MAX_LOCK_METADATA_BYTES + 1)
    with pytest.raises(ValidationError, match=ERROR):
        locks.acquire_partition_locks(tmp_path, ["a"], owner, stale_after_seconds=1)
    assert path.read_bytes() == content
    assert path.stat().st_ino == inode


def test_later_oversized_partition_rolls_back_earlier_acquisition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: type[datetime],
) -> None:
    owner = "ordinary-owner"
    monkeypatch.setattr(locks, "_MAX_LOCK_METADATA_BYTES", len(_body(owner, "a")))
    with pytest.raises(ValidationError, match=ERROR):
        locks.acquire_partition_locks(tmp_path, ["bbbb", "a"], owner)
    assert list(tmp_path.rglob(".lock")) == []
    retry = locks.acquire_partition_locks(tmp_path, ["a"], owner)
    locks.release_partition_locks(retry)


def test_normal_payload_and_duplicate_partition_behavior_is_unchanged(
    tmp_path: Path, clock: type[datetime],
) -> None:
    paths = locks.acquire_partition_locks(tmp_path, ["b", "a", "a"], "ordinary-owner")
    try:
        assert [path.parent.name for path in paths] == ["a", "b"]
        for path in paths:
            assert path.read_bytes() == _body("ordinary-owner", path.parent.name)
    finally:
        locks.release_partition_locks(paths)
