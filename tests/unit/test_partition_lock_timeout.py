"""Invalid stale timeouts must never enable destructive lock takeover."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import OverlapError, ValidationError
from src.orchestration import locks

INVALID_TIMEOUTS = [
    pytest.param(0, id="zero"), pytest.param(-1, id="negative"),
    pytest.param(True, id="true"), pytest.param(False, id="false"),
    pytest.param(0.5, id="fraction"), pytest.param("60", id="text"),
    pytest.param(float("inf"), id="infinity"), pytest.param(float("nan"), id="nan"),
    pytest.param(10**1000, id="overflow"),
]


@pytest.mark.parametrize("timeout", INVALID_TIMEOUTS)
@pytest.mark.parametrize("partitions", [[], ["part=a", "part=b"]])
def test_invalid_timeout_rejects_before_any_filesystem_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, timeout: Any, partitions: list[str],
) -> None:
    calls: list[str] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append("filesystem access")
        raise AssertionError("invalid timeout reached filesystem")

    with monkeypatch.context() as patch:
        patch.setattr(Path, "mkdir", forbidden)
        patch.setattr(Path, "read_text", forbidden)
        patch.setattr(Path, "unlink", forbidden)
        patch.setattr(locks.os, "open", forbidden)
        with pytest.raises(ValidationError, match="^stale_after_seconds"):
            locks.acquire_partition_locks(
                tmp_path / "not-created", partitions, "candidate", stale_after_seconds=timeout,
            )
    assert calls == []
    assert not (tmp_path / "not-created").exists()


@pytest.mark.parametrize("timeout", [0, -1, False])
def test_invalid_timeout_does_not_replace_an_existing_owner(tmp_path: Path, timeout: Any) -> None:
    paths = locks.acquire_partition_locks(tmp_path, ["part=a"], "existing-owner")
    original = paths[0].read_bytes()
    try:
        with pytest.raises(ValidationError, match="^stale_after_seconds"):
            locks.acquire_partition_locks(
                tmp_path, ["part=a"], "contender", stale_after_seconds=timeout,
            )
        assert paths[0].read_bytes() == original
        with pytest.raises(OverlapError):
            locks.acquire_partition_locks(tmp_path, ["part=a"], "other-contender")
    finally:
        locks.release_partition_locks(paths)


@pytest.mark.parametrize("timeout", [None, 3600, timedelta.max.days * 86_400 + 86_399])
def test_valid_timeout_preserves_active_overlap_blocking(
    tmp_path: Path, timeout: int | None,
) -> None:
    paths = locks.acquire_partition_locks(
        tmp_path, ["part=a"], "owner", stale_after_seconds=timeout,
    )
    try:
        with pytest.raises(OverlapError):
            locks.acquire_partition_locks(
                tmp_path, ["part=a"], "contender", stale_after_seconds=timeout,
            )
    finally:
        locks.release_partition_locks(paths)


def test_valid_stale_timeout_still_allows_existing_takeover_policy(tmp_path: Path) -> None:
    paths = locks.acquire_partition_locks(tmp_path, ["part=a"], "previous-owner")
    payload = json.loads(paths[0].read_text(encoding="utf-8"))
    payload["acquired_at"] = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    paths[0].write_text(json.dumps(payload), encoding="utf-8")
    acquired = locks.acquire_partition_locks(
        tmp_path, ["part=a"], "new-owner", stale_after_seconds=3600,
    )
    try:
        assert json.loads(acquired[0].read_text(encoding="utf-8"))["owner"] == "new-owner"
    finally:
        locks.release_partition_locks(acquired)
