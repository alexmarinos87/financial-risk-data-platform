from __future__ import annotations

from pathlib import Path
from typing import Any

from ..common.config import load_yaml
from ..common.exceptions import StorageError

DEFAULT_STORAGE_CONFIG = Path("config/storage.yaml")


def _require_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise StorageError(f"{label} must be a mapping")
    return value


def _require_keys(section: dict[str, Any], keys: set[str], label: str) -> None:
    missing = [key for key in sorted(keys) if key not in section]
    if missing:
        raise StorageError(f"{label} missing keys: {', '.join(missing)}")


def validate_storage_config(config: dict[str, Any]) -> None:
    root = _require_dict(config, "storage config")
    storage = _require_dict(root.get("storage"), "storage")
    _require_keys(storage, {"base_dir", "raw", "curated", "format", "partitioning"}, "storage")

    raw = _require_dict(storage.get("raw"), "storage.raw")
    _require_keys(raw, {"base_path", "dataset"}, "storage.raw")

    curated = _require_dict(storage.get("curated"), "storage.curated")
    _require_keys(curated, {"base_path", "datasets"}, "storage.curated")
    datasets = _require_dict(curated.get("datasets"), "storage.curated.datasets")
    if not datasets:
        raise StorageError("storage.curated.datasets must not be empty")

    partitioning = _require_dict(storage.get("partitioning"), "storage.partitioning")
    _require_keys(partitioning, {"granularity"}, "storage.partitioning")

    for label, value in (
        ("storage.base_dir", storage.get("base_dir")),
        ("storage.format", storage.get("format")),
        ("storage.raw.base_path", raw.get("base_path")),
        ("storage.raw.dataset", raw.get("dataset")),
        ("storage.curated.base_path", curated.get("base_path")),
        ("storage.partitioning.granularity", partitioning.get("granularity")),
    ):
        if not isinstance(value, str) or not value:
            raise StorageError(f"{label} must be a non-empty string")


def load_storage_config(path: Path | None = None) -> dict[str, Any]:
    config = load_yaml(path or DEFAULT_STORAGE_CONFIG)
    if not isinstance(config, dict):
        raise StorageError("Storage config must be a mapping")
    validate_storage_config(config)
    return config
