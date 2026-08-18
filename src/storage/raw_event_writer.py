from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import stat
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb

from ..common.exceptions import RawEventConflictError, StorageError
from .parquet_io import batch_file_name, create_parquet_file
from .partitioning import partition_path

RAW_EVENT_FIELDS = (
    "event_id",
    "symbol",
    "price",
    "volume",
    "ts_event",
    "ts_ingest",
    "source",
)
BUSINESS_FIELDS = (
    "event_id",
    "symbol",
    "price",
    "volume",
    "ts_event",
    "source",
)
MAX_SIGNED_64_BIT = 9_223_372_036_854_775_807
LOCK_FILE_NAME = ".raw-write.lock"
LOCK_TIMEOUT_SECONDS = 5.0
MAX_INVENTORY_FILES = 10_000
MAX_INVENTORY_ENTRIES = 60_000
MAX_STAGING_DIRECTORIES = 100
MAX_INVENTORY_BYTES = 2_000_000_000
MAX_FILE_BYTES = 250_000_000
MAX_ROWS_PER_FILE = 100_000
MAX_INVENTORY_ROWS = 1_000_000
MAX_INPUT_RECORDS = 100_000
MAX_TEXT_FIELD_BYTES = 65_536
MAX_SCALAR_TEXT_BYTES = 4_096
MAX_INPUT_TEXT_BYTES = 16_000_000
MAX_INVENTORY_TEXT_BYTES = 512_000_000
BATCH_FILE_PATTERN = re.compile(r"^batch_[0-9a-f]{16}\.parquet$")
PARTITION_PATTERN = re.compile(
    r"^year=([0-9]{1,4})/month=([0-9]{2})/day=([0-9]{2})/hour=([0-9]{2})$"
)
STAGING_DIRECTORY_PREFIX = ".parquet-stage-"
STAGING_PAYLOAD_NAME = "payload.tmp"
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_STRING_FIELDS = {"event_id", "symbol", "source"}
_INTEGER_TYPES = {
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
}


@dataclass(frozen=True)
class _PreparedRawEvent:
    record: dict[str, Any]
    key_digest: str
    business_fingerprint: str
    text_bytes: int
    input_text_bytes: int


@dataclass(frozen=True)
class _RawInventory:
    events: dict[str, _PreparedRawEvent]
    file_count: int
    entry_count: int
    total_bytes: int
    total_text_bytes: int
    row_count: int


def _text(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise StorageError(f"Raw event {field_name} must be a string")
    if len(value) > MAX_TEXT_FIELD_BYTES:
        raise StorageError(f"Raw event {field_name} exceeds the local UTF-8 byte limit")
    utf8_compatible = True
    encoded: bytes | None = None
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        utf8_compatible = False
    if not utf8_compatible:
        raise StorageError(f"Raw event {field_name} must contain valid UTF-8 text")
    assert encoded is not None
    if len(encoded) > MAX_TEXT_FIELD_BYTES:
        raise StorageError(f"Raw event {field_name} exceeds the local UTF-8 byte limit")
    return value


def _bounded_scalar_text(value: str, *, field_name: str) -> None:
    if len(value) > MAX_SCALAR_TEXT_BYTES:
        raise StorageError(f"Raw event {field_name} exceeds the local scalar byte limit")
    encoded: bytes | None = None
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        pass
    if encoded is None or len(encoded) > MAX_SCALAR_TEXT_BYTES:
        raise StorageError(f"Raw event {field_name} exceeds the local scalar byte limit")


def _utc_datetime(value: Any, *, field_name: str) -> datetime:
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        _bounded_scalar_text(value, field_name=field_name)
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (OverflowError, ValueError):
            pass
    if parsed is None:
        raise StorageError(f"Raw event {field_name} must be an ISO-8601 datetime")

    offset_known: bool | None = None
    try:
        offset_known = parsed.tzinfo is not None and parsed.utcoffset() is not None
    except Exception:
        pass
    if offset_known is None:
        raise StorageError(f"Raw event {field_name} has an invalid timezone")
    if not offset_known:
        parsed = parsed.replace(tzinfo=timezone.utc)
    converted: datetime | None = None
    try:
        converted = parsed.astimezone(timezone.utc)
    except Exception:
        pass
    if converted is None:
        raise StorageError(f"Raw event {field_name} is outside the supported UTC range")
    return converted


def _datetime_from_epoch_microseconds(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StorageError(f"Existing raw parquet {field_name} is not a timestamp")
    converted: datetime | None = None
    try:
        converted = UTC_EPOCH + timedelta(microseconds=value)
    except (OverflowError, TypeError, ValueError):
        pass
    if converted is None:
        raise StorageError(f"Existing raw parquet {field_name} is outside the supported range")
    return converted


def _price(value: Any) -> float:
    if isinstance(value, bool):
        raise StorageError("Raw market event price must not be a boolean")
    if not isinstance(value, (Decimal, float, int, str)):
        raise StorageError("Raw market event price must be numeric")
    if isinstance(value, str):
        _bounded_scalar_text(value, field_name="price")
    parsed: float | None = None
    try:
        parsed = float(value)
    except (OverflowError, TypeError, ValueError):
        pass
    if parsed is None or not math.isfinite(parsed) or parsed <= 0:
        raise StorageError("Raw market event price must be finite and greater than zero")
    return parsed


def _volume(value: Any) -> int:
    if isinstance(value, bool):
        raise StorageError("Raw market event volume must not be a boolean")
    if not isinstance(value, (Decimal, float, int, str)):
        raise StorageError("Raw market event volume must be an integer")
    if isinstance(value, str):
        _bounded_scalar_text(value, field_name="volume")
    parsed: int | None = value if isinstance(value, int) else None
    if parsed is None:
        candidate: Decimal | None = None
        try:
            candidate = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            pass
        if (
            candidate is not None
            and candidate.is_finite()
            and Decimal(0) <= candidate <= Decimal(MAX_SIGNED_64_BIT)
        ):
            integral = candidate.to_integral_value()
            if candidate == integral:
                parsed = int(integral)
    if parsed is None or parsed < 0 or parsed > MAX_SIGNED_64_BIT:
        raise StorageError(
            "Raw market event volume must fit a non-negative signed 64-bit integer"
        )
    return parsed


def _canonical_record(record: dict[str, Any]) -> dict[str, Any]:
    if set(record) != set(RAW_EVENT_FIELDS):
        raise StorageError("Raw market event must contain exactly the seven contract fields")

    return {
        "event_id": _text(record["event_id"], field_name="event_id"),
        "symbol": _text(record["symbol"], field_name="symbol"),
        "price": _price(record["price"]),
        "volume": _volume(record["volume"]),
        "ts_event": _utc_datetime(record["ts_event"], field_name="ts_event"),
        "ts_ingest": _utc_datetime(record["ts_ingest"], field_name="ts_ingest"),
        "source": _text(record["source"], field_name="source"),
    }


def _business_fingerprint(record: dict[str, Any]) -> str:
    business_record = {
        field: (
            record[field].isoformat()
            if isinstance(record[field], datetime)
            else record[field]
        )
        for field in BUSINESS_FIELDS
    }
    payload = json.dumps(
        business_record,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _prepare_record(record: dict[str, Any]) -> _PreparedRawEvent:
    canonical = _canonical_record(record)
    key_digest = hashlib.sha256(canonical["event_id"].encode("utf-8")).hexdigest()
    return _PreparedRawEvent(
        record=canonical,
        key_digest=key_digest,
        business_fingerprint=_business_fingerprint(canonical),
        text_bytes=sum(
            len(canonical[field].encode("utf-8")) for field in _STRING_FIELDS
        ),
        input_text_bytes=sum(
            len(value.encode("utf-8"))
            for value in record.values()
            if isinstance(value, str)
        ),
    )


def _conflict(message: str, key_digest: str) -> RawEventConflictError:
    return RawEventConflictError(f"{message} for raw event key {key_digest[:12]}")


def _prepare_incoming(records: list[dict[str, Any]]) -> dict[str, _PreparedRawEvent]:
    if len(records) > MAX_INPUT_RECORDS:
        raise StorageError("Raw input exceeds the supported local record limit")
    prepared: dict[str, _PreparedRawEvent] = {}
    prepared_text_bytes = 0
    for record in records:
        candidate = _prepare_record(record)
        prepared_text_bytes += candidate.input_text_bytes
        if prepared_text_bytes > MAX_INPUT_TEXT_BYTES:
            raise StorageError("Raw input exceeds the local UTF-8 byte limit")
        event_id = candidate.record["event_id"]
        current = prepared.get(event_id)
        if current is None:
            prepared[event_id] = candidate
            continue
        if current.business_fingerprint != candidate.business_fingerprint:
            raise _conflict("Incoming batch contains conflicting versions", candidate.key_digest)
        if candidate.record["ts_ingest"] < current.record["ts_ingest"]:
            prepared[event_id] = candidate
    return prepared


def _validate_partition(partition: str) -> None:
    match = PARTITION_PATTERN.fullmatch(partition)
    if match is None:
        raise StorageError("Raw parquet file is outside the hourly partition layout")
    year, month, day, hour = (int(value) for value in match.groups())
    valid = True
    try:
        datetime(year, month, day, hour, tzinfo=timezone.utc)
    except ValueError:
        valid = False
    if not valid:
        raise StorageError("Raw parquet file has an invalid hourly partition")


def _metadata(path: Path, *, message: str) -> os.stat_result:
    metadata: os.stat_result | None = None
    try:
        metadata = path.lstat()
    except OSError:
        pass
    if metadata is None:
        raise StorageError(message)
    return metadata


def _resolved(path: Path, *, strict: bool, message: str) -> Path:
    resolved: Path | None = None
    try:
        resolved = path.resolve(strict=strict)
    except (OSError, RuntimeError, ValueError):
        pass
    if resolved is None:
        raise StorageError(message)
    return resolved


def _is_beneath(candidate: Path, parent: Path, *, allow_equal: bool = False) -> bool:
    relative: Path | None = None
    try:
        relative = candidate.relative_to(parent)
    except ValueError:
        pass
    if relative is None:
        return False
    return allow_equal or relative != Path(".")


def _filesystem_owner_is_current(metadata: os.stat_result) -> bool:
    return not hasattr(os, "getuid") or metadata.st_uid == os.getuid()


def _lexical_absolute(path: Path, *, message: str) -> Path:
    absolute: Path | None = None
    try:
        absolute = Path(os.path.abspath(path))
    except (OSError, ValueError):
        pass
    if absolute is None:
        raise StorageError(message)
    return absolute


def _assert_no_symlink_components(
    candidate: Path,
    parent: Path,
    *,
    allow_equal: bool = False,
) -> None:
    candidate_absolute = _lexical_absolute(
        candidate,
        message="Raw path could not be normalised",
    )
    parent_absolute = _lexical_absolute(
        parent,
        message="Raw path boundary could not be normalised",
    )
    if not _is_beneath(candidate_absolute, parent_absolute, allow_equal=allow_equal):
        raise StorageError("Raw path escaped its configured lexical boundary")
    relative = candidate_absolute.relative_to(parent_absolute)
    current = parent_absolute
    for part in relative.parts:
        current /= part
        metadata: os.stat_result | None = None
        metadata_failed = False
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            break
        except OSError:
            metadata_failed = True
        if metadata_failed:
            raise StorageError("Raw path metadata is unreadable")
        assert metadata is not None
        if stat.S_ISLNK(metadata.st_mode):
            raise StorageError("Raw path must not contain symbolic links")


def _remove_recoverable_staging_directory(path: Path) -> None:
    directory_metadata = _metadata(
        path,
        message="Raw parquet staging metadata is unreadable",
    )
    if (
        not stat.S_ISDIR(directory_metadata.st_mode)
        or stat.S_IMODE(directory_metadata.st_mode) != 0o700
        or not _filesystem_owner_is_current(directory_metadata)
    ):
        raise StorageError("Raw parquet staging directory has an unsafe owner or type")

    entries: list[os.DirEntry[str]] | None = None
    try:
        with os.scandir(path) as iterator:
            entries = list(iterator)
    except OSError:
        pass
    if entries is None:
        raise StorageError("Raw parquet staging directory is unreadable")
    if len(entries) > 1 or (entries and entries[0].name != STAGING_PAYLOAD_NAME):
        raise StorageError("Raw parquet staging directory contains unmanaged content")

    if entries:
        payload = path / STAGING_PAYLOAD_NAME
        payload_metadata = _metadata(
            payload,
            message="Raw parquet staging payload metadata is unreadable",
        )
        if (
            not stat.S_ISREG(payload_metadata.st_mode)
            or payload_metadata.st_nlink not in {1, 2}
            or not _filesystem_owner_is_current(payload_metadata)
        ):
            raise StorageError("Raw parquet staging payload has an unsafe owner or type")

        if payload_metadata.st_nlink == 2:
            matching_finals = 0
            siblings: list[os.DirEntry[str]] | None = None
            try:
                with os.scandir(path.parent) as iterator:
                    siblings = list(iterator)
            except OSError:
                pass
            if siblings is None:
                raise StorageError("Raw parquet staging parent is unreadable")
            for sibling in siblings:
                if BATCH_FILE_PATTERN.fullmatch(sibling.name) is None:
                    continue
                final_metadata = _metadata(
                    path.parent / sibling.name,
                    message="Raw parquet recovery target metadata is unreadable",
                )
                if (
                    stat.S_ISREG(final_metadata.st_mode)
                    and final_metadata.st_dev == payload_metadata.st_dev
                    and final_metadata.st_ino == payload_metadata.st_ino
                    and final_metadata.st_nlink == 2
                    and stat.S_IMODE(final_metadata.st_mode) == 0o600
                    and _filesystem_owner_is_current(final_metadata)
                ):
                    matching_finals += 1
            if matching_finals != 1:
                raise StorageError("Raw parquet staging link has no unique managed final file")

        removed_payload = False
        try:
            payload.unlink()
            removed_payload = True
        except OSError:
            pass
        if not removed_payload:
            raise StorageError("Raw parquet staging payload could not be recovered")

    removed_directory = False
    try:
        path.rmdir()
        removed_directory = True
    except OSError:
        pass
    if not removed_directory:
        raise StorageError("Raw parquet staging directory could not be recovered")


def _recover_private_staging(root: Path) -> None:
    walk_failed = False
    entry_count = 0
    staging_count = 0

    def record_walk_error(error: OSError) -> None:
        nonlocal walk_failed
        walk_failed = True

    for current_root, directory_names, file_names in os.walk(
        root,
        topdown=True,
        followlinks=False,
        onerror=record_walk_error,
    ):
        staging_names = [
            name for name in directory_names if name.startswith(STAGING_DIRECTORY_PREFIX)
        ]
        staging_count += len(staging_names)
        if staging_count > MAX_STAGING_DIRECTORIES:
            raise StorageError("Raw dataset exceeds the private staging recovery limit")
        entry_count += len(directory_names) - len(staging_names) + len(file_names)
        if entry_count > MAX_INVENTORY_ENTRIES:
            raise StorageError("Raw dataset exceeds the supported local entry count")
        current = Path(current_root)
        directory_names[:] = [name for name in directory_names if name not in staging_names]
        for name in staging_names:
            _remove_recoverable_staging_directory(current / name)
    if walk_failed:
        raise StorageError("Raw parquet staging inventory cannot be traversed")


def _compatible_physical_type(
    field_name: str,
    physical_type: str,
    parquet_converted_type: str | None,
) -> bool:
    normalized = physical_type.upper()
    if field_name in _STRING_FIELDS:
        return normalized == "VARCHAR"
    if field_name == "price":
        return normalized == "DOUBLE"
    if field_name == "volume":
        return normalized in _INTEGER_TYPES
    if field_name in {"ts_event", "ts_ingest"}:
        return (
            normalized in {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}
            and parquet_converted_type == "TIMESTAMP_MICROS"
        ) or (
            normalized == "TIMESTAMP_NS" and parquet_converted_type is None
        )
    return False


def _assert_safe_tree(root: Path) -> tuple[list[Path], int, int]:
    parquet_files: list[Path] = []
    inventory_bytes = 0
    entry_count = 0

    walk_failed = False

    def record_walk_error(error: OSError) -> None:
        nonlocal walk_failed
        walk_failed = True

    for current_root, directory_names, file_names in os.walk(
        root,
        followlinks=False,
        onerror=record_walk_error,
    ):
        entry_count += len(directory_names) + len(file_names)
        if entry_count > MAX_INVENTORY_ENTRIES:
            raise StorageError("Raw dataset exceeds the supported local entry count")
        current = Path(current_root)
        for name in [*directory_names, *file_names]:
            candidate = current / name
            metadata = _metadata(
                candidate,
                message="Raw parquet inventory metadata is unreadable",
            )
            if stat.S_ISLNK(metadata.st_mode):
                raise StorageError("Raw dataset must not contain symbolic links")
        for name in file_names:
            if Path(name).suffix != ".parquet":
                continue
            candidate = current / name
            metadata = _metadata(
                candidate,
                message="Raw parquet inventory metadata is unreadable",
            )
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise StorageError("Raw parquet inventory must contain regular unlinked files")
            if metadata.st_size > MAX_FILE_BYTES:
                raise StorageError("Raw parquet file exceeds the supported local byte limit")
            if BATCH_FILE_PATTERN.fullmatch(name) is None:
                raise StorageError("Raw parquet inventory contains an unmanaged filename")
            relative_partition = candidate.parent.relative_to(root).as_posix()
            _validate_partition(relative_partition)
            inventory_bytes += metadata.st_size
            parquet_files.append(candidate)
    if walk_failed:
        raise StorageError("Raw parquet inventory cannot be traversed")
    if len(parquet_files) > MAX_INVENTORY_FILES:
        raise StorageError("Raw parquet inventory exceeds the supported local file count")
    if inventory_bytes > MAX_INVENTORY_BYTES:
        raise StorageError("Raw parquet inventory exceeds the supported local byte limit")
    return sorted(parquet_files), inventory_bytes, entry_count


def _read_inventory(root: Path) -> _RawInventory:
    parquet_files, inventory_bytes, entry_count = _assert_safe_tree(root)
    if not parquet_files:
        return _RawInventory(
            events={},
            file_count=0,
            entry_count=entry_count,
            total_bytes=0,
            total_text_bytes=0,
            row_count=0,
        )

    inventory: dict[str, _PreparedRawEvent] = {}
    inventory_rows = 0
    inventory_text_bytes = 0
    for path in parquet_files:
        rows: list[tuple[Any, ...]] | None = None
        schema: dict[str, str] | None = None
        parquet_converted_types: dict[str, str | None] | None = None
        try:
            with duckdb.connect() as connection:
                schema_rows = connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
                    [str(path)],
                ).fetchall()
                schema = {str(row[0]): str(row[1]) for row in schema_rows}
                parquet_rows = connection.execute(
                    "SELECT name, converted_type FROM parquet_schema(?) "
                    "WHERE name <> 'duckdb_schema'",
                    [str(path)],
                ).fetchall()
                parquet_converted_types = {
                    str(row[0]): None if row[1] is None else str(row[1])
                    for row in parquet_rows
                }
        except (duckdb.Error, OSError):
            pass
        if schema is None or parquet_converted_types is None:
            raise StorageError("Existing raw parquet inventory is unreadable")
        if set(schema) != set(RAW_EVENT_FIELDS) or len(schema) != len(RAW_EVENT_FIELDS):
            raise StorageError("Existing raw parquet file has an incompatible schema")
        if set(parquet_converted_types) != set(RAW_EVENT_FIELDS):
            raise StorageError("Existing raw parquet file has incompatible physical metadata")
        if any(
            not _compatible_physical_type(
                field_name,
                schema[field_name],
                parquet_converted_types[field_name],
            )
            for field_name in RAW_EVENT_FIELDS
        ):
            raise StorageError("Existing raw parquet file has incompatible physical types")

        nanosecond_fields = [
            field_name
            for field_name in ("ts_event", "ts_ingest")
            if schema[field_name].upper() == "TIMESTAMP_NS"
        ]
        if nanosecond_fields:
            precision_row: tuple[Any, ...] | None = None
            precision_select = ", ".join(
                "COALESCE(SUM(CASE WHEN epoch_ns(\""
                f"{field_name}"
                "\") % 1000 <> 0 THEN 1 ELSE 0 END), 0)"
                for field_name in nanosecond_fields
            )
            try:
                with duckdb.connect() as connection:
                    precision_row = connection.execute(
                        f"SELECT {precision_select} "
                        "FROM read_parquet(?, hive_partitioning=false)",
                        [str(path)],
                    ).fetchone()
            except (duckdb.Error, OSError):
                pass
            if precision_row is None:
                raise StorageError("Existing raw parquet inventory is unreadable")
            if any(int(value) > 0 for value in precision_row):
                raise StorageError("Existing raw parquet timestamps exceed microsecond precision")

        metrics: tuple[Any, ...] | None = None
        try:
            with duckdb.connect() as connection:
                metrics = connection.execute(
                    "SELECT COUNT(*), "
                    "COALESCE(MAX(octet_length(encode(event_id))), 0), "
                    "COALESCE(MAX(octet_length(encode(symbol))), 0), "
                    "COALESCE(MAX(octet_length(encode(source))), 0), "
                    "COALESCE(SUM(COALESCE(octet_length(encode(event_id)), 0) + "
                    "COALESCE(octet_length(encode(symbol)), 0) + "
                    "COALESCE(octet_length(encode(source)), 0)), 0) "
                    "FROM read_parquet(?, hive_partitioning=false)",
                    [str(path)],
                ).fetchone()
        except (duckdb.Error, OSError):
            pass
        if metrics is None:
            raise StorageError("Existing raw parquet inventory is unreadable")
        row_count = int(metrics[0])
        maximum_text_bytes = max(int(value) for value in metrics[1:4])
        file_text_bytes = int(metrics[4])
        if row_count == 0:
            raise StorageError("Existing raw parquet file must not be empty")
        if row_count > MAX_ROWS_PER_FILE:
            raise StorageError("Existing raw parquet file exceeds the supported row limit")
        if maximum_text_bytes > MAX_TEXT_FIELD_BYTES:
            raise StorageError("Existing raw parquet text exceeds the local field byte limit")
        inventory_rows += row_count
        if inventory_rows > MAX_INVENTORY_ROWS:
            raise StorageError("Raw parquet inventory exceeds the supported local row limit")
        inventory_text_bytes += file_text_bytes
        if inventory_text_bytes > MAX_INVENTORY_TEXT_BYTES:
            raise StorageError("Raw parquet inventory exceeds the local UTF-8 byte limit")

        try:
            with duckdb.connect() as connection:
                select_list = ", ".join(
                    f'epoch_us("{field}") AS "{field}"'
                    if field in {"ts_event", "ts_ingest"}
                    else f'"{field}"'
                    for field in RAW_EVENT_FIELDS
                )
                rows = connection.execute(
                    f"SELECT {select_list} "
                    "FROM read_parquet(?, hive_partitioning=false)",
                    [str(path)],
                ).fetchall()
        except (duckdb.Error, OSError):
            pass
        if rows is None or len(rows) != row_count:
            raise StorageError("Existing raw parquet file could not be read completely")

        relative_partition = path.parent.relative_to(root).as_posix()
        for row in rows:
            stored_record = dict(zip(RAW_EVENT_FIELDS, row, strict=True))
            for timestamp_field in ("ts_event", "ts_ingest"):
                stored_record[timestamp_field] = _datetime_from_epoch_microseconds(
                    stored_record[timestamp_field],
                    field_name=timestamp_field,
                )
            candidate = _prepare_record(stored_record)
            if partition_path(candidate.record["ts_ingest"]) != relative_partition:
                raise StorageError("Raw parquet record does not match its ingest-hour partition")
            event_id = candidate.record["event_id"]
            current = inventory.get(event_id)
            if current is None:
                inventory[event_id] = candidate
                continue
            if current.business_fingerprint != candidate.business_fingerprint:
                raise _conflict(
                    "Existing raw dataset contains conflicting versions",
                    candidate.key_digest,
                )
            raise StorageError(
                "Existing raw dataset contains duplicate records for key "
                f"{candidate.key_digest[:12]}"
            )
    return _RawInventory(
        events=inventory,
        file_count=len(parquet_files),
        entry_count=entry_count,
        total_bytes=inventory_bytes,
        total_text_bytes=inventory_text_bytes,
        row_count=inventory_rows,
    )


@contextmanager
def _dataset_lock(root: Path) -> Iterator[None]:
    lock_path = root / LOCK_FILE_NAME
    lock_is_symlink = False
    try:
        lock_is_symlink = lock_path.is_symlink()
    except OSError:
        pass
    if lock_is_symlink:
        raise StorageError("Raw dataset lock must not be a symbolic link")
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW

    descriptor: int | None = None
    open_failed = False
    try:
        descriptor = os.open(lock_path, flags, 0o600)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or not _filesystem_owner_is_current(metadata)
        ):
            raise OSError("unsafe lock file")
        os.fchmod(descriptor, 0o600)
    except OSError:
        open_failed = True
    if open_failed:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise StorageError("Unable to open the raw dataset lock")
    assert descriptor is not None

    acquired = False
    deadline = time.monotonic() + LOCK_TIMEOUT_SECONDS
    try:
        while not acquired:
            blocked = False
            acquire_failed = False
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except BlockingIOError:
                blocked = True
            except OSError:
                acquire_failed = True
            if acquire_failed:
                raise StorageError("Unable to acquire the raw dataset lock")
            if blocked:
                if time.monotonic() >= deadline:
                    raise StorageError("Timed out waiting for the raw dataset lock")
                time.sleep(0.05)
        yield
    finally:
        if acquired:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _ensure_partition_directory(root: Path, partition: str) -> Path:
    target = root / partition
    created = True
    try:
        target.mkdir(parents=True, exist_ok=True)
    except OSError:
        created = False
    if not created:
        raise StorageError("Raw partition directory could not be created")
    current = root
    for part in Path(partition).parts:
        current /= part
        metadata = _metadata(
            current,
            message="Raw partition path metadata is unreadable",
        )
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise StorageError("Raw partition path must contain only regular directories")
        resolved = _resolved(
            current,
            strict=True,
            message="Raw partition path could not be resolved",
        )
        if not _is_beneath(resolved, root):
            raise StorageError("Raw partition path escaped the configured dataset")
    return target


def _path_exists(path: Path) -> bool:
    exists: bool | None = None
    try:
        path.lstat()
        exists = True
    except FileNotFoundError:
        exists = False
    except OSError:
        pass
    if exists is None:
        raise StorageError("Raw parquet target metadata is unreadable")
    return exists


def _publish_raw_batch(
    batch: list[dict[str, Any]],
    target: Path,
    *,
    maximum_bytes: int,
) -> bool:
    published: bool | None = None
    try:
        published = create_parquet_file(
            batch,
            target,
            maximum_bytes=maximum_bytes,
        )
    except StorageError:
        pass
    if published is None:
        raise StorageError("Unable to publish raw parquet output")
    return published


def write_raw_event_records(
    records: list[dict[str, Any]],
    *,
    dataset_path: Path,
    raw_base_path: Path,
    storage_base_dir: Path,
    file_format: str,
) -> int:
    if file_format != "parquet":
        raise StorageError(f"Unsupported storage format '{file_format}'")
    incoming = _prepare_incoming(records)
    if not incoming:
        return 0

    configured_root = _resolved(
        storage_base_dir,
        strict=False,
        message="storage.base_dir could not be resolved",
    )
    configured_raw_base = _resolved(
        raw_base_path,
        strict=False,
        message="Raw base path could not be resolved",
    )
    candidate_root = _resolved(
        dataset_path,
        strict=False,
        message="Raw dataset path could not be resolved",
    )
    if not _is_beneath(configured_raw_base, configured_root, allow_equal=True):
        raise StorageError("Raw base path must remain beneath storage.base_dir")
    if not _is_beneath(candidate_root, configured_raw_base):
        raise StorageError("Raw dataset must remain beneath its configured raw base path")
    _assert_no_symlink_components(
        raw_base_path,
        storage_base_dir,
        allow_equal=True,
    )
    _assert_no_symlink_components(dataset_path, raw_base_path)

    dataset_is_symlink = False
    raw_base_is_symlink = False
    try:
        dataset_is_symlink = dataset_path.is_symlink()
        raw_base_is_symlink = raw_base_path.is_symlink()
    except OSError:
        pass
    if dataset_is_symlink or raw_base_is_symlink:
        raise StorageError("Raw dataset path must not be a symbolic link")

    created = True
    try:
        dataset_path.mkdir(parents=True, exist_ok=True)
    except OSError:
        created = False
    if not created:
        raise StorageError("Raw dataset path could not be created")
    _assert_no_symlink_components(
        raw_base_path,
        storage_base_dir,
        allow_equal=True,
    )
    _assert_no_symlink_components(dataset_path, raw_base_path)
    root = _resolved(
        dataset_path,
        strict=True,
        message="Raw dataset path could not be resolved after creation",
    )
    actual_raw_base = _resolved(
        raw_base_path,
        strict=True,
        message="Raw base path could not be resolved after creation",
    )
    if not _is_beneath(actual_raw_base, configured_root, allow_equal=True):
        raise StorageError("Raw base path escaped storage.base_dir during creation")
    if not _is_beneath(root, actual_raw_base):
        raise StorageError("Raw dataset escaped its configured raw base during creation")

    with _dataset_lock(root):
        _recover_private_staging(root)
        inventory = _read_inventory(root)
        unseen: list[_PreparedRawEvent] = []
        for event_id in sorted(incoming):
            candidate = incoming[event_id]
            current = inventory.events.get(event_id)
            if current is None:
                unseen.append(candidate)
                continue
            if current.business_fingerprint != candidate.business_fingerprint:
                raise _conflict("Incoming event conflicts with the immutable first version", candidate.key_digest)

        if inventory.row_count + len(unseen) > MAX_INVENTORY_ROWS:
            raise StorageError("Raw parquet publication would exceed the local row limit")
        if (
            inventory.total_text_bytes
            + sum(candidate.text_bytes for candidate in unseen)
            > MAX_INVENTORY_TEXT_BYTES
        ):
            raise StorageError("Raw parquet publication would exceed the local UTF-8 byte limit")

        partitioned: dict[str, list[dict[str, Any]]] = {}
        for candidate in unseen:
            partition = partition_path(candidate.record["ts_ingest"])
            partitioned.setdefault(partition, []).append(candidate.record)
        if inventory.file_count + len(partitioned) > MAX_INVENTORY_FILES:
            raise StorageError("Raw parquet publication would exceed the local file limit")
        if inventory.entry_count + (5 * len(partitioned)) > MAX_INVENTORY_ENTRIES:
            raise StorageError("Raw parquet publication would exceed the local entry limit")

        publication_plan: list[tuple[list[dict[str, Any]], Path]] = []
        for partition in sorted(partitioned):
            batch = sorted(partitioned[partition], key=lambda record: record["event_id"])
            target_directory = _ensure_partition_directory(root, partition)
            target = target_directory / batch_file_name(batch, file_format)
            if _path_exists(target):
                raise StorageError("Raw parquet target already exists unexpectedly")
            publication_plan.append((batch, target))

        written = 0
        remaining_bytes = MAX_INVENTORY_BYTES - inventory.total_bytes
        for batch, target in publication_plan:
            publication_byte_limit = min(MAX_FILE_BYTES, remaining_bytes)
            if publication_byte_limit <= 0:
                raise StorageError("Raw parquet publication would exceed the local byte limit")
            if not _publish_raw_batch(
                batch,
                target,
                maximum_bytes=publication_byte_limit,
            ):
                raise StorageError("Raw parquet target appeared during publication")
            remaining_bytes -= _metadata(
                target,
                message="Published raw parquet metadata is unreadable",
            ).st_size
            written += len(batch)
        return written


__all__ = ["write_raw_event_records"]
