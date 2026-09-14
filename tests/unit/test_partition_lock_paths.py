"""Partition validation must precede every filesystem operation in acquisition."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import OverlapError, ValidationError
from src.orchestration import locks


@pytest.mark.parametrize("partition", [
    "", ".", "..", "../outside", "a/../../outside", "a/../b", "./a",
    "a/./b", "a//b", "a/", "/absolute", "//server/share", "C:/outside",
    "C:outside", r"a\b", "a\x00b", "a\nb", "a\x7fb", None, 17, ["a"],
])
def test_unsafe_partition_rejects_before_any_filesystem_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, partition: Any,
) -> None:
    calls: list[str] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append("filesystem")
        pytest.fail("invalid partition reached the filesystem")

    with monkeypatch.context() as patch:
        patch.setattr(Path, "mkdir", forbidden)
        patch.setattr(locks.os, "open", forbidden)
        patch.setattr(Path, "unlink", forbidden)
        with pytest.raises(ValidationError) as error:
            locks.acquire_partition_locks(tmp_path / "data", ["safe", partition], "test")
    assert calls == []
    assert str(error.value) == "Partition must be a non-empty relative path without traversal"
    assert not (tmp_path / "data").exists()


@pytest.mark.parametrize("absolute", [False, True])
def test_escaped_stale_lock_is_never_replaced(tmp_path: Path, absolute: bool) -> None:
    base = tmp_path / "data"
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / ".lock"
    payload = b'{"owner":"other","acquired_at":"2000-01-01T00:00:00+00:00"}'
    sentinel.write_bytes(payload)
    partition = str(outside) if absolute else "../../outside"
    with pytest.raises(ValidationError):
        locks.acquire_partition_locks(base, [partition], "request", stale_after_seconds=3600)
    assert sentinel.read_bytes() == payload
    assert not base.exists()


def test_complete_request_is_checked_before_any_valid_lock_is_created(tmp_path: Path) -> None:
    base = tmp_path / "data"
    with pytest.raises(ValidationError):
        locks.acquire_partition_locks(base, ["a-valid", "z/../invalid"], "request")
    assert not base.exists()


@pytest.mark.parametrize("partition", ["a", "part=a", "year=2026/month=09/day=14/hour=10"])
def test_valid_partitions_retain_payload_order_overlap_and_release(
    tmp_path: Path, partition: str,
) -> None:
    requested = [partition, "z-last", partition]
    before = list(requested)
    paths = locks.acquire_partition_locks(tmp_path, requested, "same-owner")
    try:
        assert requested == before
        assert paths == [tmp_path / ".orchestration_locks" / p / ".lock"
                         for p in sorted(set(requested))]
        payload = json.loads(paths[0].read_text(encoding="utf-8"))
        assert payload["owner"] == "same-owner"
        assert payload["partition"] == sorted(set(requested))[0]
        with pytest.raises(OverlapError):
            locks.acquire_partition_locks(tmp_path, [partition], "contender")
    finally:
        locks.release_partition_locks(paths)
    assert all(not p.exists() for p in paths)


def test_empty_request_remains_a_noop(tmp_path: Path) -> None:
    base = tmp_path / "data"
    assert locks.acquire_partition_locks(base, [], "test") == []
    assert not base.exists()
