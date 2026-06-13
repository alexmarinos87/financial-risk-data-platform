from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

CURATED_DATASETS = {
    "returns_1m": "returns_1m",
    "volatility_5m": "volatility_5m",
    "data_quality_metrics": "data_quality_metrics",
    "risk_summary": "risk_summary",
    "external_signal_summary": "external_signal_summary",
}


def build_storage_config(
    tmp_path: Path,
    *,
    include_external_signal_summary: bool = True,
) -> dict[str, Any]:
    curated_datasets = dict(CURATED_DATASETS)
    if not include_external_signal_summary:
        curated_datasets.pop("external_signal_summary")

    return {
        "storage": {
            "base_dir": str(tmp_path),
            "raw": {
                "base_path": str(tmp_path / "raw"),
                "dataset": "market_events",
            },
            "curated": {
                "base_path": str(tmp_path / "curated"),
                "datasets": curated_datasets,
            },
            "format": "parquet",
            "partitioning": {
                "granularity": "hourly",
            },
        }
    }


def write_storage_config(
    tmp_path: Path,
    *,
    include_external_signal_summary: bool = True,
) -> Path:
    storage_config_path = tmp_path / "storage.yaml"
    storage_config = build_storage_config(
        tmp_path,
        include_external_signal_summary=include_external_signal_summary,
    )
    with storage_config_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(storage_config, handle, sort_keys=False)
    return storage_config_path
