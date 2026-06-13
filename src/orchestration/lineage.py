from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _path_or_label(path: Path | None, fallback: str) -> str:
    return str(path) if path is not None else fallback


def _dataset_path(storage_config: dict[str, Any], *, kind: str, dataset_key: str) -> str:
    storage = storage_config["storage"]
    if kind == "raw":
        return str(Path(storage["raw"]["base_path"]) / storage["raw"]["dataset"])

    dataset_name = storage["curated"]["datasets"][dataset_key]
    return str(Path(storage["curated"]["base_path"]) / dataset_name)


def build_lineage_manifest(
    *,
    run_id: str,
    input_path: Path | None,
    signals_path: Path | None,
    storage_config: dict[str, Any],
    raw_dataset: str,
    raw_payload_count: int,
    deduped_event_count: int,
    external_signal_count: int,
    raw_written: int,
    curated_writes: dict[str, int],
    partitions: list[str],
    quality_summary: dict[str, Any],
) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat()
    curated_outputs = [
        {
            "dataset": dataset_key,
            "path": _dataset_path(storage_config, kind="curated", dataset_key=dataset_key),
            "records_written": record_count,
            "depends_on": [raw_dataset],
        }
        for dataset_key, record_count in sorted(curated_writes.items())
    ]

    return {
        "run_id": run_id,
        "generated_at": generated_at,
        "source_inventory": [
            {
                "name": "market_events",
                "source_type": "json_file",
                "location": _path_or_label(input_path, "built_in_sample_events"),
                "authoritative_key": "event_id",
                "records_received": raw_payload_count,
                "records_after_deduplication": deduped_event_count,
            },
            {
                "name": "external_signals",
                "source_type": "file",
                "location": _path_or_label(signals_path, "not_provided"),
                "authoritative_key": "signal_id",
                "records_received": external_signal_count,
                "records_after_deduplication": external_signal_count,
            },
        ],
        "layers": [
            {
                "name": "raw",
                "datasets": [
                    {
                        "dataset": raw_dataset,
                        "path": _dataset_path(storage_config, kind="raw", dataset_key=raw_dataset),
                        "records_written": raw_written,
                        "partitions": partitions,
                    }
                ],
            },
            {
                "name": "curated",
                "datasets": curated_outputs,
            },
            {
                "name": "reporting",
                "datasets": [
                    {
                        "dataset": "risk_platform.finance_risk_semantic_model",
                        "path": "PostgreSQL view",
                        "depends_on": [
                            "risk_summary",
                            "data_quality_metrics",
                            "symbol_dimension_history",
                        ],
                    }
                ],
            },
        ],
        "transformations": [
            {
                "name": "validate_required_fields",
                "inputs": ["market_events"],
                "outputs": ["validated_events"],
            },
            {
                "name": "normalize_symbols",
                "inputs": ["validated_events"],
                "outputs": ["normalised_events"],
            },
            {
                "name": "deduplicate_events",
                "inputs": ["normalised_events"],
                "outputs": [raw_dataset],
                "business_key": "event_id",
            },
            {
                "name": "compute_curated_metrics",
                "inputs": [raw_dataset, "external_signals"],
                "outputs": sorted(curated_writes),
            },
        ],
        "quality_checks": quality_summary,
    }


def write_lineage_manifest(path: Path, manifest: dict[str, Any]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True, default=str)
