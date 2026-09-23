"""Exercise acquisition failures with real temporary lock files and descriptors."""

from __future__ import annotations

import errno
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


def lock_path(root: Path, partition: str) -> Path:
    return root / ".orchestration_locks" / partition / ".lock"


def inject_failure(
    patch: pytest.MonkeyPatch, root: Path, phase: str, failure: BaseException,
) -> list[int]:
    target = lock_path(root, "b")
    real_open, real_fdopen = os.open, os.fdopen
    real_mkdir, real_dumps = Path.mkdir, json.dumps
    descriptors: list[int] = []
    targets: dict[int, Path] = {}

    def open_file(path: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        if Path(path) == target and phase == "open":
            raise failure
        fd = real_open(path, flags, *args, **kwargs)
        descriptors.append(fd)
        targets[fd] = Path(path)
        return fd

    def mkdir(path: Path, *args: Any, **kwargs: Any) -> None:
        if path == target.parent and phase == "mkdir":
            raise failure
        real_mkdir(path, *args, **kwargs)

    def dumps(payload: Any, *args: Any, **kwargs: Any) -> str:
        if isinstance(payload, dict) and payload.get("partition") == "b" and phase == "encode":
            raise failure
        return real_dumps(payload, *args, **kwargs)

    def fdopen(fd: int, *args: Any, **kwargs: Any) -> Any:
        selected = targets.get(fd) == target
        if selected and phase == "fdopen":
            raise failure
        handle = real_fdopen(fd, *args, **kwargs)
        if not selected or phase not in {"write", "close"}:
            return handle

        class FailingHandle:
            def __enter__(self) -> FailingHandle:
                handle.__enter__()
                return self

            def write(self, payload: bytes) -> int:
                if phase == "write":
                    handle.write(payload[:1])
                    handle.flush()
                    raise failure
                return handle.write(payload)

            def __exit__(self, *exception: Any) -> None:
                handle.__exit__(*exception)
                if phase == "close":
                    raise failure

        return FailingHandle()

    patch.setattr(os, "open", open_file)
    patch.setattr(os, "fdopen", fdopen)
    patch.setattr(Path, "mkdir", mkdir)
    patch.setattr(json, "dumps", dumps)
    return descriptors


def close_baseline_leaks(descriptors: list[int]) -> None:
    # Also make running these regression tests against the old code safe.
    for fd in set(descriptors):
        try:
            os.close(fd)
        except OSError as error:
            if error.errno != errno.EBADF:
                raise


@pytest.mark.parametrize("phase", ["mkdir", "encode", "open", "fdopen", "write", "close"])
def test_failure_rolls_back_all_created_paths_and_allows_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str,
) -> None:
    original = OSError(errno.ENOSPC, "synthetic acquisition failure")
    descriptors: list[int] = []
    try:
        with monkeypatch.context() as patch:
            descriptors = inject_failure(patch, tmp_path, phase, original)
            with pytest.raises(OSError) as caught:
                locks.acquire_partition_locks(tmp_path, ["b", "a"], "failed")
            assert caught.value is original
        assert list(tmp_path.rglob(".lock")) == []
        for fd in set(descriptors):
            with pytest.raises(OSError) as closed:
                os.fstat(fd)
            assert closed.value.errno == errno.EBADF
        acquired = locks.acquire_partition_locks(tmp_path, ["b", "a", "a"], "retry")
        assert acquired == [lock_path(tmp_path, "a"), lock_path(tmp_path, "b")]
        locks.release_partition_locks(acquired)
    finally:
        close_baseline_leaks(descriptors)


@pytest.mark.parametrize("phase", ["fdopen", "write", "close"])
@pytest.mark.parametrize("failure_type", [KeyboardInterrupt, SystemExit])
def test_handled_unwinding_preserves_interruptions_and_cleans_registered_locks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str, failure_type: type[BaseException],
) -> None:
    original = failure_type("synthetic interruption")
    descriptors: list[int] = []
    try:
        with monkeypatch.context() as patch:
            descriptors = inject_failure(patch, tmp_path, phase, original)
            with pytest.raises(failure_type) as caught:
                locks.acquire_partition_locks(tmp_path, ["a", "b"], "interrupted")
            assert caught.value is original
        assert list(tmp_path.rglob(".lock")) == []
        for fd in set(descriptors):
            with pytest.raises(OSError) as closed:
                os.fstat(fd)
            assert closed.value.errno == errno.EBADF
    finally:
        close_baseline_leaks(descriptors)


def test_overlap_removes_earlier_acquisition_but_preserves_existing_owner(tmp_path: Path) -> None:
    existing = locks.acquire_partition_locks(tmp_path, ["b"], "existing")
    before = existing[0].read_bytes()
    try:
        with pytest.raises(OverlapError):
            locks.acquire_partition_locks(tmp_path, ["a", "b"], "blocked")
        assert not lock_path(tmp_path, "a").exists()
        assert existing[0].read_bytes() == before
    finally:
        locks.release_partition_locks(existing)


def test_losing_stale_recreation_does_not_remove_the_new_contender(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = lock_path(tmp_path, "b")
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps({
        "acquired_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
    }), encoding="utf-8")
    real_open = os.open
    attempts = 0

    def competing_open(path: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        nonlocal attempts
        # Race only with exclusive creation, not read-only metadata inspection.
        if Path(path) == target and flags & os.O_EXCL:
            attempts += 1
            if attempts == 2:
                target.write_bytes(b"other-owner")
        return real_open(path, flags, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(os, "open", competing_open)
        with pytest.raises(FileExistsError):
            locks.acquire_partition_locks(tmp_path, ["a", "b"], "loser", stale_after_seconds=3600)
    assert attempts == 2
    assert not lock_path(tmp_path, "a").exists()
    assert target.read_bytes() == b"other-owner"


def test_cleanup_failure_attempts_remaining_paths_and_keeps_original_in_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = OSError(errno.ENOSPC, "synthetic write failure")
    cleanup = PermissionError("synthetic cleanup failure")
    real_unlink = Path.unlink
    attempted: list[Path] = []

    def unlink(path: Path, *args: Any, **kwargs: Any) -> None:
        attempted.append(path)
        if path == lock_path(tmp_path, "b"):
            raise cleanup
        real_unlink(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        inject_failure(patch, tmp_path, "write", original)
        patch.setattr(Path, "unlink", unlink)
        with pytest.raises(PermissionError) as caught:
            locks.acquire_partition_locks(tmp_path, ["a", "b"], "failed")
    assert caught.value is cleanup
    chain: list[BaseException] = []
    error: BaseException | None = caught.value
    while error is not None:
        chain.append(error)
        error = error.__context__
    assert original in chain
    assert attempted == [lock_path(tmp_path, "b"), lock_path(tmp_path, "a")]
    assert not lock_path(tmp_path, "a").exists()
    assert lock_path(tmp_path, "b").exists()


def test_success_retains_locks_until_release_and_blocks_another_process(tmp_path: Path) -> None:
    child = """
import sys
from pathlib import Path
from src.common.exceptions import OverlapError
from src.orchestration.locks import acquire_partition_locks, release_partition_locks
try:
    paths = acquire_partition_locks(Path(sys.argv[1]), ['a', 'b'], 'child')
except OverlapError:
    print('blocked')
else:
    release_partition_locks(paths)
    print('acquired')
"""
    acquired = locks.acquire_partition_locks(tmp_path, ["b", "a", "b"], "parent")
    assert acquired == [lock_path(tmp_path, "a"), lock_path(tmp_path, "b")]
    try:
        for path in acquired:
            assert json.loads(path.read_text(encoding="utf-8"))["owner"] == "parent"
        blocked = subprocess.run(
            [sys.executable, "-c", child, str(tmp_path)],
            cwd=Path(__file__).resolve().parents[2], check=True, capture_output=True,
            text=True, timeout=10,
        )
        assert blocked.stdout.strip() == "blocked"
    finally:
        locks.release_partition_locks(acquired)
    retried = subprocess.run(
        [sys.executable, "-c", child, str(tmp_path)],
        cwd=Path(__file__).resolve().parents[2], check=True, capture_output=True,
        text=True, timeout=10,
    )
    assert retried.stdout.strip() == "acquired"
    assert list(tmp_path.rglob(".lock")) == []
