from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..common.exceptions import StorageError


def _to_stable_json(value: Any) -> Any:
    if isinstance(value, datetime):
        timestamp = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return timestamp.isoformat()
    if isinstance(value, dict):
        return {key: _to_stable_json(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [_to_stable_json(item) for item in value]
    return value


def batch_file_name(records: list[dict[str, Any]], file_format: str) -> str:
    stable_records = [
        json.dumps(
            _to_stable_json(record),
            sort_keys=True,
            separators=(",", ":"),
        )
        for record in records
    ]
    payload = "\n".join(sorted(stable_records))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"batch_{digest}.{file_format}"


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    directory_fd = os.open(path, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def create_parquet_file(records: list[dict[str, Any]], target_path: Path) -> bool:
    """Publish without replacement; an fsync error may leave a valid final link."""

    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.is_symlink():
        raise StorageError("Parquet target must not be a symbolic link")
    if target_path.exists():
        if not target_path.is_file():
            raise StorageError("Parquet target must be a regular file")
        return False

    staging_directory = Path(
        tempfile.mkdtemp(
            dir=target_path.parent,
            prefix=".parquet-stage-",
        )
    )
    staging_directory.chmod(0o700)
    temporary_path = staging_directory / "payload.tmp"

    try:
        frame = pd.DataFrame(records)
        with duckdb.connect() as connection:
            connection.register("records_df", frame)
            connection.execute(
                "COPY records_df TO ? (FORMAT PARQUET)",
                [str(temporary_path)],
            )
            cursor = connection.execute(
                "SELECT * FROM read_parquet(?, hive_partitioning=false) LIMIT 0",
                [str(temporary_path)],
            )
            stored_columns = [description[0] for description in cursor.description]
            count_row = connection.execute(
                "SELECT COUNT(*) FROM read_parquet(?, hive_partitioning=false)",
                [str(temporary_path)],
            ).fetchone()
            if count_row is None:
                raise StorageError("Staged parquet output did not return a row count")
            stored_count = int(count_row[0])
        if stored_columns != list(frame.columns) or stored_count != len(records):
            raise StorageError("Staged parquet output failed validation")
        temporary_path.chmod(0o600)
        with temporary_path.open("rb") as handle:
            os.fsync(handle.fileno())

        try:
            os.link(temporary_path, target_path, follow_symlinks=False)
        except FileExistsError:
            if target_path.is_symlink() or not target_path.is_file():
                raise StorageError("Parquet target appeared with an unsafe file type") from None
            return False

        _fsync_directory(target_path.parent)
        return True
    except StorageError:
        raise
    except (duckdb.Error, OSError, TypeError, ValueError):
        raise StorageError("Unable to publish parquet output") from None
    finally:
        shutil.rmtree(staging_directory, ignore_errors=True)


__all__ = ["batch_file_name", "create_parquet_file"]
