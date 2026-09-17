"""Stale inspection must be bounded and must not follow special lock entries."""

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import OverlapError
from src.orchestration import locks

LIMIT = 65_536
OLD = "2000-01-01T00:00:00+00:00"


def _lock(base: Path, content: bytes) -> Path:
    path = base / ".orchestration_locks" / "b" / ".lock"
    path.parent.mkdir(parents=True)
    path.write_bytes(content)
    return path


def _payload(size: int) -> bytes:
    content = json.dumps({"acquired_at": OLD, "owner": ""}).encode()
    return content[:-2] + b"x" * (size - len(content)) + content[-2:]


def test_oversized_metadata_never_reaches_json_or_replaces_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    content = _payload(LIMIT + 1)
    path = _lock(tmp_path, content)
    parsed: list[int] = []
    original = json.loads

    def loads(value: Any, *args: Any, **kwargs: Any) -> Any:
        parsed.append(len(value))
        return original(value, *args, **kwargs)

    monkeypatch.setattr(locks.json, "loads", loads)
    with pytest.raises(OverlapError):
        locks.acquire_partition_locks(tmp_path, ["a", "b"], "contender", stale_after_seconds=1)
    assert parsed == []
    assert path.read_bytes() == content
    assert not (tmp_path / ".orchestration_locks/a/.lock").exists()


@pytest.mark.parametrize("size,expected", [(LIMIT - 1, True), (LIMIT, True), (LIMIT + 1, False)])
def test_metadata_limit_is_inclusive(tmp_path: Path, size: int, expected: bool) -> None:
    path = _lock(tmp_path, _payload(size))
    assert locks._is_stale_lock(path, 1) is expected


def test_stale_symlink_is_not_followed_or_replaced(tmp_path: Path) -> None:
    target = tmp_path / "other-owner.json"
    content = json.dumps({"acquired_at": OLD}).encode()
    target.write_bytes(content)
    path = tmp_path / ".orchestration_locks/b/.lock"
    path.parent.mkdir(parents=True)
    path.symlink_to(target)
    with pytest.raises(OverlapError):
        locks.acquire_partition_locks(tmp_path, ["a", "b"], "contender", stale_after_seconds=1)
    assert path.is_symlink()
    assert target.read_bytes() == content
    assert not (tmp_path / ".orchestration_locks/a/.lock").exists()


def test_fifo_does_not_block_inspection_or_leave_earlier_lock(tmp_path: Path) -> None:
    path = tmp_path / ".orchestration_locks/b/.lock"
    path.parent.mkdir(parents=True)
    os.mkfifo(path)
    script = """
import sys
from pathlib import Path
from src.common.exceptions import OverlapError
from src.orchestration.locks import acquire_partition_locks
try:
    acquire_partition_locks(Path(sys.argv[1]), ['a', 'b'], 'child', stale_after_seconds=1)
except OverlapError:
    print('blocked')
else:
    raise AssertionError('FIFO must not be replaced')
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True,
        timeout=5, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "blocked"
    assert path.is_fifo()
    assert not (tmp_path / ".orchestration_locks/a/.lock").exists()


@pytest.mark.parametrize("content", [
    b"[" * 2000 + b"0" + b"]" * 2000, b"not-json", b"\xff", b"[]", b"{}",
])
def test_unusable_metadata_remains_blocked(tmp_path: Path, content: bytes) -> None:
    path = _lock(tmp_path, content)
    with pytest.raises(OverlapError):
        locks.acquire_partition_locks(tmp_path, ["a", "b"], "contender", stale_after_seconds=1)
    assert path.read_bytes() == content
    assert not (tmp_path / ".orchestration_locks/a/.lock").exists()


@pytest.mark.parametrize("timestamp,expected", [
    (OLD, True), ("2000-01-01T00:00:00", True),
    ((datetime.now(timezone.utc) + timedelta(days=1)).isoformat(), False),
])
def test_regular_metadata_retains_time_policy(
    tmp_path: Path, timestamp: str, expected: bool,
) -> None:
    path = _lock(tmp_path, json.dumps({"acquired_at": timestamp}).encode())
    assert locks._is_stale_lock(path, 1) is expected
    assert locks._is_stale_lock(path, None) is False


@pytest.mark.parametrize("error_type", [OSError, KeyboardInterrupt])
def test_read_failure_closes_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_type: type[BaseException],
) -> None:
    path = _lock(tmp_path, json.dumps({"acquired_at": OLD}).encode())
    descriptors: list[int] = []

    def read(fd: int, count: int) -> bytes:
        assert count == LIMIT + 1
        descriptors.append(fd)
        raise error_type("synthetic read failure")

    monkeypatch.setattr(locks.os, "read", read)
    if error_type is KeyboardInterrupt:
        with pytest.raises(KeyboardInterrupt):
            locks._is_stale_lock(path, 1)
    else:
        assert locks._is_stale_lock(path, 1) is False
    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


@pytest.mark.parametrize("flag", ["O_NONBLOCK", "O_NOFOLLOW"])
def test_missing_safe_open_primitive_disables_takeover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flag: str,
) -> None:
    path = _lock(tmp_path, json.dumps({"acquired_at": OLD}).encode())
    monkeypatch.delattr(locks.os, flag)
    assert locks._is_stale_lock(path, 1) is False


def test_decoder_depth_failure_remains_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _lock(tmp_path, b"{}")

    def fail_depth(*args: Any, **kwargs: Any) -> Any:
        raise RecursionError("synthetic decoder depth failure")

    monkeypatch.setattr(locks.json, "loads", fail_depth)
    assert locks._is_stale_lock(path, 1) is False
