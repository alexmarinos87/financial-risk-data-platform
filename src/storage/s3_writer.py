from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..common.exceptions import StorageError
from .partitioning import partition_path
from .storage_config import load_storage_config, validate_storage_config


def _resolve_dataset_name(
    storage: dict[str, Any],
    *,
    kind: str,
    dataset: str | None,
) -> tuple[str, Path]:
    if kind == "raw":
        raw = storage["raw"]
        dataset_name = raw["dataset"]
        if dataset is not None and dataset != dataset_name:
            raise StorageError(f"Raw dataset must be '{dataset_name}', got '{dataset}'")
        base_path = Path(raw["base_path"])
        return dataset_name, base_path

    if kind == "curated":
        curated = storage["curated"]
        datasets = curated["datasets"]
        if dataset is None:
            raise StorageError("Curated writes require a dataset key")
        if dataset in datasets:
            dataset_name = datasets[dataset]
        elif dataset in datasets.values():
            dataset_name = dataset
        else:
            raise StorageError(f"Unknown curated dataset '{dataset}'")
        base_path = Path(curated["base_path"])
        return dataset_name, base_path

    raise StorageError(f"Unknown storage kind '{kind}'")


def _parse_ingest_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise StorageError("Record ts_ingest must be a datetime or ISO-8601 string")
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _to_stable_json(value: Any) -> Any:
    if isinstance(value, datetime):
        ts = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return ts.isoformat()
    if isinstance(value, dict):
        return {key: _to_stable_json(val) for key, val in sorted(value.items())}
    if isinstance(value, list):
        return [_to_stable_json(item) for item in value]
    return value


def _batch_file_name(records: list[dict[str, Any]], file_format: str) -> str:
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


def _write_parquet(records: list[dict[str, Any]], path: Path) -> None:
    df = pd.DataFrame(records)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with duckdb.connect() as conn:
        conn.register("records_df", df)
        escaped = str(tmp_path).replace("'", "''")
        conn.execute(f"COPY records_df TO '{escaped}' (FORMAT PARQUET)")
    tmp_path.replace(path)


def write_records(
    records: list[dict],
    *,
    kind: str,
    dataset: str | None = None,
    storage_config: dict[str, Any] | None = None,
    storage_config_path: Path | None = None,
) -> int:
    if records is None:
        raise StorageError("No records provided")
    if not isinstance(records, list):
        raise StorageError("Records must be a list of dictionaries")
    if any(not isinstance(record, dict) for record in records):
        raise StorageError("Each record must be a dictionary")
    if not records:
        return 0

    if storage_config is None:
        storage_config = load_storage_config(storage_config_path)
    else:
        validate_storage_config(storage_config)

    storage = storage_config["storage"]
    file_format = storage["format"].lower()
    if file_format != "parquet":
        raise StorageError(f"Unsupported storage format '{file_format}'")

    dataset_name, base_path = _resolve_dataset_name(storage, kind=kind, dataset=dataset)
    dataset_path = base_path / dataset_name

    partitioned_batches: dict[str | None, list[dict[str, Any]]] = {}
    for record in records:
        ts_ingest = _parse_ingest_timestamp(record.get("ts_ingest"))
        partition_key = partition_path(ts_ingest) if ts_ingest is not None else None
        partitioned_batches.setdefault(partition_key, []).append(record)

    records_written = 0
    for partition_key, batch in partitioned_batches.items():
        target_dir = dataset_path if partition_key is None else dataset_path / partition_key
        filename = _batch_file_name(batch, file_format)
        target_path = target_dir / filename

        if target_path.exists():
            continue

        _write_parquet(batch, target_path)
        records_written += len(batch)

    return records_written
