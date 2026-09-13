from __future__ import annotations

import json
import os
from contextlib import ExitStack
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
    with ExitStack() as rollback:
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
                    raise OverlapError(
                        f"Partition '{partition}' is already locked; "
                        "live and backfill overlap is blocked."
                    ) from exc

            # Register only after exclusive creation, but before wrapping or
            # writing: even an empty/partially written lock needs rollback.
            rollback.callback(path.unlink, missing_ok=True)
            try:
                # Keep one explicit descriptor owner even if wrapping fails.
                with os.fdopen(fd, "wb", closefd=False) as handle:
                    handle.write(payload)
            finally:
                os.close(fd)
            lock_paths.append(path)

        # Successful acquisition transfers release responsibility to the caller.
        rollback.pop_all()
    return lock_paths


def release_partition_locks(lock_paths: list[Path]) -> None:
    """Attempt every supplied release and retain all cleanup failures."""
    failures: list[BaseException] = []
    for path in lock_paths:
        try:
            path.unlink(missing_ok=True)
        except BaseException as exc:
            # Finish cleanup even on interruption, then propagate without
            # turning a partially released set into a successful result.
            failures.append(exc)
    if len(failures) == 1:
        raise failures[0]
    if failures:
        raise BaseExceptionGroup("Partition lock release failed", failures)
