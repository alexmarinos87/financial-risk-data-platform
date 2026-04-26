from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..common.exceptions import OverlapError

_LOCKS_DIR = ".orchestration_locks"


def _lock_path(base_dir: Path, partition: str) -> Path:
    return base_dir / _LOCKS_DIR / partition / ".lock"


def _is_stale_lock(path: Path, stale_after_seconds: int | None) -> bool:
    if stale_after_seconds is None:
        return False

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        acquired_at = datetime.fromisoformat(str(payload["acquired_at"]))
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False

    if acquired_at.tzinfo is None:
        acquired_at = acquired_at.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - acquired_at > timedelta(seconds=stale_after_seconds)


def acquire_partition_locks(
    base_dir: Path,
    partitions: list[str],
    owner: str,
    *,
    stale_after_seconds: int | None = None,
) -> list[Path]:
    lock_paths: list[Path] = []
    for partition in sorted(set(partitions)):
        path = _lock_path(base_dir, partition)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "owner": owner,
                "partition": partition,
                "acquired_at": datetime.now(timezone.utc).isoformat(),
            }
        ).encode("utf-8")
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            if _is_stale_lock(path, stale_after_seconds):
                path.unlink(missing_ok=True)
                fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            else:
                for lock_path in lock_paths:
                    lock_path.unlink(missing_ok=True)
                raise OverlapError(
                    f"Partition '{partition}' is already locked; live and backfill overlap is blocked."
                ) from exc

        try:
            handle = os.fdopen(fd, "wb")
        except Exception:
            for lock_path in lock_paths:
                lock_path.unlink(missing_ok=True)
            raise

        with handle:
            handle.write(payload)
        lock_paths.append(path)

    return lock_paths


def release_partition_locks(lock_paths: list[Path]) -> None:
    for path in lock_paths:
        path.unlink(missing_ok=True)
