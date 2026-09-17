"""A short read is not EOF and must not authorize takeover from a JSON prefix."""

import json
import os
from pathlib import Path

import pytest

from src.common.exceptions import OverlapError
from src.orchestration import locks

LIMIT = 65_536
OLD = json.dumps({"acquired_at": "2000-01-01T00:00:00+00:00"}).encode()


def _lock(root: Path, content: bytes) -> Path:
    path = root / ".orchestration_locks" / "b" / ".lock"
    path.parent.mkdir(parents=True)
    path.write_bytes(content)
    return path


@pytest.mark.parametrize("chunk_size", [1, 7, 31])
def test_valid_metadata_can_arrive_in_short_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, chunk_size: int,
) -> None:
    # Multibyte UTF-8 can span reads: decode only after collecting the body.
    content = json.dumps({"acquired_at": "2000-01-01T00:00:00+00:00", "owner": "α"},
                         ensure_ascii=False).encode()
    path = _lock(tmp_path, content)
    real_read = os.read
    calls: list[tuple[int, bytes]] = []

    def read(fd: int, count: int) -> bytes:
        data = real_read(fd, min(count, chunk_size))
        calls.append((fd, data))
        return data

    monkeypatch.setattr(locks.os, "read", read)
    assert locks._is_stale_lock(path, 1) is True
    assert b"".join(data for _, data in calls) == content
    assert calls[-1][1] == b""
    with pytest.raises(OSError):
        os.fstat(calls[0][0])
    assert path.read_bytes() == content


@pytest.mark.parametrize("suffix", [b"\nnot-json", b'\n{"acquired_at":"2099-01-01"}'])
def test_complete_json_prefix_cannot_hide_an_invalid_tail_or_replace_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, suffix: bytes,
) -> None:
    content = OLD + suffix
    path = _lock(tmp_path, content)
    real_read = os.read

    def read(fd: int, count: int) -> bytes:
        return real_read(fd, min(count, len(OLD)))

    monkeypatch.setattr(locks.os, "read", read)
    with pytest.raises(OverlapError):
        locks.acquire_partition_locks(tmp_path, ["a", "b"], "contender", stale_after_seconds=1)
    assert path.read_bytes() == content
    assert not (tmp_path / ".orchestration_locks/a/.lock").exists()


def test_error_after_valid_prefix_does_not_authorize_takeover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _lock(tmp_path, OLD)
    real_read = os.read
    descriptors: list[int] = []

    def read(fd: int, count: int) -> bytes:
        descriptors.append(fd)
        if len(descriptors) > 1:
            raise OSError("synthetic failure before confirmed EOF")
        return real_read(fd, count)

    monkeypatch.setattr(locks.os, "read", read)
    assert locks._is_stale_lock(path, 1) is False
    assert len(descriptors) == 2
    with pytest.raises(OSError):
        os.fstat(descriptors[0])
    assert path.read_bytes() == OLD


def test_growth_after_stat_still_obeys_one_total_byte_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _lock(tmp_path, OLD)
    real_read = os.read
    total = 0
    requested: list[int] = []

    def read(fd: int, count: int) -> bytes:
        nonlocal total
        assert count == LIMIT + 1 - total
        if not requested:
            with path.open("ab") as handle:
                handle.write(b" " * (LIMIT + 1 - len(OLD)))
        requested.append(count)
        # The first read returns a well-formed prefix, but the whole file is too big.
        size = len(OLD) if len(requested) == 1 else 4096
        data = real_read(fd, min(count, size))
        total += len(data)
        return data

    monkeypatch.setattr(locks.os, "read", read)
    assert locks._is_stale_lock(path, 1) is False
    assert total == LIMIT + 1
    assert requested[-1] > 0


@pytest.mark.parametrize("size", [len(OLD), LIMIT])
def test_exact_limit_and_complete_body_are_accepted_after_eof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, size: int,
) -> None:
    content = OLD + b" " * (size - len(OLD))
    path = _lock(tmp_path, content)
    real_read = os.read
    total = 0
    reached_eof = False

    def read(fd: int, count: int) -> bytes:
        nonlocal total, reached_eof
        assert 0 < count <= LIMIT + 1 - total
        data = real_read(fd, min(count, 257))
        total += len(data)
        reached_eof = not data
        return data

    monkeypatch.setattr(locks.os, "read", read)
    assert locks._is_stale_lock(path, 1) is True
    assert total == size
    assert reached_eof
