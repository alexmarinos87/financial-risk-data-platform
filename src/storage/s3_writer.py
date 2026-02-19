from pathlib import Path
from typing import Any

from ..common.exceptions import StorageError
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
    if storage_config is None:
        storage_config = load_storage_config(storage_config_path)
    else:
        validate_storage_config(storage_config)

    storage = storage_config["storage"]
    dataset_name, base_path = _resolve_dataset_name(storage, kind=kind, dataset=dataset)
    _ = base_path / dataset_name
    return len(records)
