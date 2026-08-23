from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import StorageError, ValidationError
from src.orchestration import replicate_durable_datasets as replication
from src.orchestration.replicate_durable_datasets import (
    MANIFEST_DATASET,
    inventory_local_replication_artifacts,
    replicate_local_datasets,
)
from src.storage.durable_s3 import (
    DurableS3Config,
    content_sha256,
    immutable_object_key,
)


def _durable_config(**overrides: Any) -> DurableS3Config:
    values: dict[str, Any] = {
        "store_id": "primary-s3",
        "enabled": True,
        "bucket_env": "DURABLE_BUCKET",
        "region_env": "AWS_REGION",
        "prefix": "risk-platform",
        "server_side_encryption": "AES256",
        "kms_key_env": None,
        "max_object_bytes": 1_000_000,
        "max_list_objects": 20,
    }
    values.update(overrides)
    return DurableS3Config(**values)


def _storage_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "storage": {
            "base_dir": str(tmp_path),
            "raw": {
                "base_path": str(tmp_path / "raw"),
                "dataset": "market_events",
            },
            "curated": {
                "base_path": str(tmp_path / "curated"),
                "datasets": {
                    "daily_returns": "daily_returns",
                    "portfolio_risk_attribution": "portfolio_risk_attribution",
                },
            },
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }


def _write_storage_config(tmp_path: Path) -> tuple[dict[str, Any], Path]:
    config = _storage_config(tmp_path)
    path = tmp_path / "storage.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config, path


def _write_artifacts(tmp_path: Path) -> None:
    raw = tmp_path / "raw" / "market_events" / "year=2026" / "month=08"
    raw.mkdir(parents=True)
    (raw / "raw-b.parquet").write_bytes(b"raw-b")
    (raw / "raw-a.parquet").write_bytes(b"raw-a")

    daily = tmp_path / "curated" / "daily_returns" / "year=2026"
    daily.mkdir(parents=True)
    (daily / "daily.parquet").write_bytes(b"daily")

    attribution = tmp_path / "curated" / "portfolio_risk_attribution"
    attribution.mkdir(parents=True)
    (attribution / "attribution.parquet").write_bytes(b"attribution")


def _publisher_calls() -> tuple[list[dict[str, Any]], Any]:
    calls: list[dict[str, Any]] = []

    def publisher(_client: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        payload = kwargs["payload"]
        key = immutable_object_key(
            kwargs["config"],
            dataset=kwargs["dataset"],
            payload=payload,
            extension=kwargs["extension"],
        )
        return {
            "bucket_configured": True,
            "content_length": len(payload),
            "key": key,
            "sha256": content_sha256(payload),
            "status": "written",
        }

    return calls, publisher


def test_inventory_is_deterministic_and_preserves_dataset_evidence(
    tmp_path: Path,
) -> None:
    config, _ = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)

    artifacts = inventory_local_replication_artifacts(
        storage_config=config,
        durable_config=_durable_config(),
        max_files=10,
        max_total_bytes=1_000,
    )

    assert [
        (artifact.remote_dataset, artifact.relative_path)
        for artifact in artifacts
    ] == [
        ("curated-daily_returns", "year=2026/daily.parquet"),
        (
            "curated-portfolio_risk_attribution",
            "attribution.parquet",
        ),
        ("raw-market_events", "year=2026/month=08/raw-a.parquet"),
        ("raw-market_events", "year=2026/month=08/raw-b.parquet"),
    ]
    assert all(artifact.extension == "parquet" for artifact in artifacts)
    assert all(
        artifact.content_type == "application/vnd.apache.parquet"
        for artifact in artifacts
    )
    assert all(
        artifact.object_key.startswith("risk-platform/")
        for artifact in artifacts
    )


def test_dataset_selection_is_explicit_and_unknown_names_fail(
    tmp_path: Path,
) -> None:
    config, _ = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)

    selected = inventory_local_replication_artifacts(
        storage_config=config,
        durable_config=_durable_config(),
        selected_datasets=["daily_returns"],
    )
    assert {artifact.dataset for artifact in selected} == {"daily_returns"}

    with pytest.raises(ValidationError, match="not configured"):
        inventory_local_replication_artifacts(
            storage_config=config,
            durable_config=_durable_config(),
            selected_datasets=["unknown"],
        )


def test_plan_only_requires_no_environment_or_client(
    tmp_path: Path,
) -> None:
    _, storage_path = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)
    clients: list[str] = []

    summary = replicate_local_datasets(
        store_id="primary-s3",
        storage_config_path=storage_path,
        durable_config_path=tmp_path / "durable.yaml",
        selected_datasets=["daily_returns"],
        enable_durable_write=False,
        environment={},
        config_loader=lambda *_: _durable_config(),
        client_factory=lambda **_: clients.append("created"),
    )

    assert summary["replication"] == {
        "performed": False,
        "reason": "explicit_enable_flag_required",
    }
    assert summary["plan"]["artifact_count"] == 1
    assert summary["plan"]["dataset_count"] == 1
    assert summary["plan"]["manifest_id"].startswith(
        "durable-replication-"
    )
    assert clients == []


def test_disabled_store_blocks_execution_before_environment_resolution(
    tmp_path: Path,
) -> None:
    _, storage_path = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)
    clients: list[str] = []

    summary = replicate_local_datasets(
        store_id="primary-s3",
        storage_config_path=storage_path,
        durable_config_path=tmp_path / "durable.yaml",
        selected_datasets=["daily_returns"],
        enable_durable_write=True,
        environment={},
        config_loader=lambda *_: _durable_config(enabled=False),
        client_factory=lambda **_: clients.append("created"),
    )

    assert summary["replication"] == {
        "performed": False,
        "reason": "store_disabled_in_configuration",
    }
    assert clients == []


def test_execution_delegates_objects_and_manifest_to_immutable_adapter(
    tmp_path: Path,
) -> None:
    _, storage_path = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)
    calls, publisher = _publisher_calls()
    regions: list[str] = []

    summary = replicate_local_datasets(
        store_id="primary-s3",
        storage_config_path=storage_path,
        durable_config_path=tmp_path / "durable.yaml",
        selected_datasets=["daily_returns", "market_events"],
        max_files=10,
        max_total_bytes=1_000,
        enable_durable_write=True,
        environment={
            "DURABLE_BUCKET": "secret-bucket-name",
            "AWS_REGION": "eu-west-2",
        },
        config_loader=lambda *_: _durable_config(),
        client_factory=lambda *, region_name: regions.append(region_name)
        or object(),
        publisher=publisher,
    )

    assert regions == ["eu-west-2"]
    assert [call["dataset"] for call in calls] == [
        "curated-daily_returns",
        "raw-market_events",
        "raw-market_events",
        MANIFEST_DATASET,
    ]
    assert summary["replication"]["performed"] is True
    assert summary["replication"]["artifacts_written"] == 3
    assert summary["replication"]["artifacts_already_present"] == 0
    assert summary["replication"]["manifest"]["status"] == "written"
    assert "secret-bucket-name" not in str(summary)
    assert "eu-west-2" not in str(summary)
    assert summary["credentials_recorded"] is False


def test_replay_statuses_are_counted_without_duplicate_manifest_identity(
    tmp_path: Path,
) -> None:
    _, storage_path = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)
    statuses = iter(["already_present", "written"])

    def publisher(_client: Any, **kwargs: Any) -> dict[str, Any]:
        payload = kwargs["payload"]
        return {
            "content_length": len(payload),
            "key": immutable_object_key(
                kwargs["config"],
                dataset=kwargs["dataset"],
                payload=payload,
                extension=kwargs["extension"],
            ),
            "sha256": content_sha256(payload),
            "status": next(statuses),
        }

    summary = replicate_local_datasets(
        store_id="primary-s3",
        storage_config_path=storage_path,
        durable_config_path=tmp_path / "durable.yaml",
        selected_datasets=["daily_returns"],
        enable_durable_write=True,
        environment={
            "DURABLE_BUCKET": "bucket",
            "AWS_REGION": "eu-west-2",
        },
        config_loader=lambda *_: _durable_config(),
        client_factory=lambda **_: object(),
        publisher=publisher,
    )

    assert summary["replication"]["artifacts_written"] == 0
    assert summary["replication"]["artifacts_already_present"] == 1
    assert summary["replication"]["manifest"]["status"] == "written"


def test_file_and_total_byte_bounds_fail_before_publication(
    tmp_path: Path,
) -> None:
    config, _ = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)

    with pytest.raises(ValidationError, match="max_files"):
        inventory_local_replication_artifacts(
            storage_config=config,
            durable_config=_durable_config(),
            max_files=1,
            max_total_bytes=1_000,
        )

    with pytest.raises(ValidationError, match="max_total_bytes"):
        inventory_local_replication_artifacts(
            storage_config=config,
            durable_config=_durable_config(),
            max_files=10,
            max_total_bytes=2,
        )


def test_object_size_and_symlink_paths_fail_closed(
    tmp_path: Path,
) -> None:
    config, _ = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)

    with pytest.raises(ValidationError, match="max_object_bytes"):
        inventory_local_replication_artifacts(
            storage_config=config,
            durable_config=_durable_config(max_object_bytes=2),
        )

    real = tmp_path / "real-daily"
    real.mkdir()
    dataset = tmp_path / "curated" / "daily_returns"
    for path in sorted(dataset.rglob("*"), reverse=True):
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            path.rmdir()
    dataset.rmdir()
    try:
        dataset.symlink_to(real, target_is_directory=True)
    except OSError:
        pytest.skip("symbolic links are unavailable")

    with pytest.raises(StorageError, match="symbolic link"):
        inventory_local_replication_artifacts(
            storage_config=config,
            durable_config=_durable_config(),
            selected_datasets=["daily_returns"],
        )


def test_artifact_change_after_planning_fails_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, storage_path = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)
    original = replication._read_regular_file
    reads = 0
    published: list[str] = []

    def changing_read(path: Path, *, expected_size: int) -> bytes:
        nonlocal reads
        reads += 1
        payload = original(path, expected_size=expected_size)
        if reads == 2:
            return b"x" * expected_size
        return payload

    monkeypatch.setattr(replication, "_read_regular_file", changing_read)

    with pytest.raises(StorageError, match="changed after the plan"):
        replicate_local_datasets(
            store_id="primary-s3",
            storage_config_path=storage_path,
            durable_config_path=tmp_path / "durable.yaml",
            selected_datasets=["daily_returns"],
            enable_durable_write=True,
            environment={
                "DURABLE_BUCKET": "bucket",
                "AWS_REGION": "eu-west-2",
            },
            config_loader=lambda *_: _durable_config(),
            client_factory=lambda **_: object(),
            publisher=lambda *_args, **_kwargs: published.append("called")
            or {"status": "written"},
        )

    assert published == []


def test_manifest_identity_is_stable_and_changes_with_content(
    tmp_path: Path,
) -> None:
    _, storage_path = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)

    first = replicate_local_datasets(
        store_id="primary-s3",
        storage_config_path=storage_path,
        durable_config_path=tmp_path / "durable.yaml",
        selected_datasets=["daily_returns"],
        config_loader=lambda *_: _durable_config(),
    )
    second = replicate_local_datasets(
        store_id="primary-s3",
        storage_config_path=storage_path,
        durable_config_path=tmp_path / "durable.yaml",
        selected_datasets=["daily_returns"],
        config_loader=lambda *_: _durable_config(),
    )
    assert first["plan"]["manifest_id"] == second["plan"]["manifest_id"]
    assert first["plan"]["manifest_object_key"] == second["plan"][
        "manifest_object_key"
    ]

    daily = next((tmp_path / "curated" / "daily_returns").rglob("*.parquet"))
    daily.write_bytes(b"changed")
    changed = replicate_local_datasets(
        store_id="primary-s3",
        storage_config_path=storage_path,
        durable_config_path=tmp_path / "durable.yaml",
        selected_datasets=["daily_returns"],
        config_loader=lambda *_: _durable_config(),
    )
    assert changed["plan"]["manifest_id"] != first["plan"]["manifest_id"]


def test_no_artifacts_returns_a_non_mutating_result(
    tmp_path: Path,
) -> None:
    _, storage_path = _write_storage_config(tmp_path)

    summary = replicate_local_datasets(
        store_id="primary-s3",
        storage_config_path=storage_path,
        durable_config_path=tmp_path / "durable.yaml",
        enable_durable_write=True,
        config_loader=lambda *_: replace(_durable_config(), enabled=True),
    )

    assert summary["replication"] == {
        "performed": False,
        "reason": "no_artifacts",
    }


def test_invalid_publisher_status_fails_closed(
    tmp_path: Path,
) -> None:
    _, storage_path = _write_storage_config(tmp_path)
    _write_artifacts(tmp_path)

    with pytest.raises(StorageError, match="invalid status"):
        replicate_local_datasets(
            store_id="primary-s3",
            storage_config_path=storage_path,
            durable_config_path=tmp_path / "durable.yaml",
            selected_datasets=["daily_returns"],
            enable_durable_write=True,
            environment={
                "DURABLE_BUCKET": "bucket",
                "AWS_REGION": "eu-west-2",
            },
            config_loader=lambda *_: _durable_config(),
            client_factory=lambda **_: object(),
            publisher=lambda *_args, **_kwargs: {"status": "unknown"},
        )
