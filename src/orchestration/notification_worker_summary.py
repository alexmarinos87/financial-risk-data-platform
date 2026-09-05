from __future__ import annotations

import json
import os
import stat
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError

MAX_SUMMARY_BYTES = 1_048_576


def _check_destination(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    if stat.S_ISLNK(mode):
        raise StorageError("notification worker summary must not be a symbolic link")
    if not stat.S_ISREG(mode):
        raise StorageError("notification worker summary must be a regular file")


def write_notification_worker_summary(
    path: Path,
    summary: Mapping[str, Any],
) -> None:
    """Atomically replace a bounded summary in an operator-owned directory.

    The parent directory is trusted. This is not a sandbox against another
    process able to replace its entries. Concurrent writers publish complete
    documents with last-replacement-wins semantics, not compare-and-swap.
    """
    try:
        payload = (
            json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise StorageError("notification worker summary is not valid JSON") from None
    if len(payload) > MAX_SUMMARY_BYTES:
        raise StorageError("notification worker summary exceeds 1 MB")

    temporary: Path | None = None
    try:
        _check_destination(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        # NamedTemporaryFile exclusively creates an unpredictable sibling with
        # restrictive permissions. Never open the old predictable .json.tmp path.
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _check_destination(path)
        temporary.replace(path)
    except OSError:
        raise StorageError("unable to write notification worker summary") from None
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                # Do not hide the original failure. The trusted operator may
                # need to remove a temporary file after a filesystem failure.
                pass
