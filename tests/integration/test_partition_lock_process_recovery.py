"""Real-process contention and OS failures must not abandon acquired locks."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from src.orchestration.locks import acquire_partition_locks, release_partition_locks

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CHILD = """
import json
import sys
from pathlib import Path
from src.common.exceptions import OverlapError
from src.orchestration.locks import acquire_partition_locks, release_partition_locks

root = Path(sys.argv[1])
partitions = json.loads(sys.argv[2])
try:
    paths = acquire_partition_locks(root, partitions, owner="child-process")
except OverlapError:
    result = {"status": "blocked"}
except OSError as exc:
    result = {"status": "io_error", "error_type": type(exc).__name__}
else:
    try:
        result = {
            "status": "acquired",
            "partitions": [json.loads(path.read_text())["partition"] for path in paths],
            "owners": [json.loads(path.read_text())["owner"] for path in paths],
        }
    finally:
        release_partition_locks(paths)
print(json.dumps(result))
"""


def _attempt(root: Path, partitions: list[str]) -> dict[str, Any]:
    # run() kills and waits for the child on timeout; no persistent worker or
    # clock-based sleep is used to coordinate lock ownership.
    completed = subprocess.run(
        [sys.executable, "-c", CHILD, str(root), json.dumps(partitions)],
        cwd=REPOSITORY_ROOT, capture_output=True, text=True, timeout=15, check=True,
    )
    assert completed.stderr == ""
    return json.loads(completed.stdout)


def _lock(root: Path, partition: str) -> Path:
    return root / ".orchestration_locks" / partition / ".lock"


def test_other_process_cannot_take_an_active_lock_or_leave_earlier_locks(tmp_path: Path) -> None:
    held = acquire_partition_locks(tmp_path, ["part=z"], owner="parent-process")
    payload = held[0].read_bytes()
    try:
        # Deliberately reversed and duplicated: production code chooses order.
        for _ in range(2):
            assert _attempt(tmp_path, ["part=z", "part=a", "part=a"]) == {"status": "blocked"}
            assert not _lock(tmp_path, "part=a").exists()
            assert held[0].read_bytes() == payload
    finally:
        release_partition_locks(held)

    assert _attempt(tmp_path, ["part=z", "part=a", "part=a"]) == {
        "status": "acquired", "partitions": ["part=a", "part=z"],
        "owners": ["child-process", "child-process"],
    }
    assert not list((tmp_path / ".orchestration_locks").rglob(".lock"))


def test_real_directory_creation_failure_rolls_back_and_allows_process_retry(tmp_path: Path) -> None:
    root = tmp_path / ".orchestration_locks"
    root.mkdir()
    blocker = root / "part=z"
    blocker.write_bytes(b"not a directory; preserve these bytes")

    result = _attempt(tmp_path, ["part=z", "part=a"])
    assert result["status"] == "io_error"
    assert result["error_type"] == "FileExistsError"
    assert blocker.read_bytes() == b"not a directory; preserve these bytes"
    assert not _lock(tmp_path, "part=a").exists()

    # Only the test-created obstruction is removed, not any production lock.
    blocker.unlink()
    assert _attempt(tmp_path, ["part=z", "part=a"])["status"] == "acquired"
    assert not list(root.rglob(".lock"))


def test_lock_ownership_can_pass_from_child_to_parent_without_stale_takeover(tmp_path: Path) -> None:
    partitions = ["year=2026/month=09/day=17/hour=10", "year=2026/month=09/day=17/hour=11"]
    assert _attempt(tmp_path, partitions)["status"] == "acquired"
    held = acquire_partition_locks(tmp_path, partitions, owner="parent-after-child")
    try:
        assert _attempt(tmp_path, partitions) == {"status": "blocked"}
        assert all(json.loads(path.read_text())["owner"] == "parent-after-child" for path in held)
    finally:
        release_partition_locks(held)
    assert _attempt(tmp_path, list(reversed(partitions)))["status"] == "acquired"
