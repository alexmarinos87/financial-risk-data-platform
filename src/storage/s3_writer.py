from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..common.exceptions import StorageError
from .partitioning import partition_path


def _dataset_root(root: Path, layer: str, dataset: str, contract_version: str) -> Path:
    return root / layer / dataset / f"contract_version={contract_version}"


def _write_parquet(df: pd.DataFrame, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        target_path.unlink()

    conn = duckdb.connect()
    try:
        conn.register("records_df", df)
        conn.execute(
            "COPY records_df TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(target_path)],
        )
    finally:
        conn.close()


def write_records(
    records: list[dict[str, Any]],
    *,
    layer: str = "bronze",
    dataset: str = "market_events",
    root_path: str | Path = "data_lake",
    partition_field: str | None = "ts_ingest",
    run_id: str = "manual",
    contract_version: str = "v1",
) -> int:
    if records is None:
        raise StorageError("No records provided")
    if not isinstance(records, list):
        raise StorageError("records must be a list of dictionaries")
    if len(records) == 0:
        return 0

    root = Path(root_path)
    df = pd.DataFrame(records).copy()
    dataset_root = _dataset_root(root, layer, dataset, contract_version)

    if partition_field is None:
        file_path = dataset_root / f"run_id={run_id}.parquet"
        _write_parquet(df, file_path)
        return len(df)

    if partition_field not in df.columns:
        raise StorageError(
            f"Partition field '{partition_field}' not found in dataset '{dataset}' columns"
        )

    partition_values = pd.to_datetime(df[partition_field], errors="raise", utc=True)
    df[partition_field] = partition_values

    for partition, subset in df.groupby(partition_values.map(partition_path)):
        file_path = dataset_root / partition / f"run_id={run_id}.parquet"
        _write_parquet(subset.reset_index(drop=True), file_path)

    return len(df)


def write_run_metadata(summary: dict[str, Any], *, root_path: str | Path, run_id: str) -> Path:
    root = Path(root_path)
    metadata_dir = root / "_runs"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = metadata_dir / f"{run_id}.json"
    metadata_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return metadata_path


def dataset_paths(
    *,
    root_path: str | Path,
    layer: str,
    dataset: str,
    contract_version: str,
) -> Path:
    return _dataset_root(Path(root_path), layer, dataset, contract_version)
