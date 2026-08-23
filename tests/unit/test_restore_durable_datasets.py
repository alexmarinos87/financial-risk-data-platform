from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.replicate_durable_datasets import replicate_local_datasets
from src.orchestration.restore_durable_datasets import (
    RESTORE_CONTRACT,
    restore_durable_replication,
)
from src.storage.durable_s3 import DurableS3Config


class FakeBody:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.closed = False

    def read(self, amount: int) -> bytes:
        return self.payload[:amount]

    def close(self) -> None:
        self.closed = True


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, Any]] = {}
        self.head_calls: list[str] = []
        self.get_calls: list[str] = []

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        key = kwargs["Key"]
        payload = kwargs["Body"]
        assert isinstance(key, str)
        assert isinstance(payload, bytes)
        self.objects[key] = {
            "Body": payload,
            "ContentLength": len(payload),
            "Metadata": dict(kwargs["Metadata"]),
        }
        return {"ETag": f'"{len(self.objects)}"'}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        assert Bucket == "test-bucket"
        self.head_calls.append(Key)
        candidate = self.objects[Key]
        return {
            "ContentLength": candidate["ContentLength"],
            "Metadata": dict(candidate["Metadata"]),
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        assert Bucket == "test-bucket"
        self.get_calls.append(Key)
        candidate = self.objects[Key]
        return {
            "Body": FakeBody(candidate["Body"]),
            "ContentLength": candidate["ContentLength"],
        }


def _durable_config(*, enabled: bool = True) -> DurableS3Config:
    return DurableS3Config(
        store_id="primary-s3",
        enabled=enabled,
        bucket_env="DURABLE_BUCKET",
        region_env="AWS_REGION",
        prefix="risk-platform",
        server_side_encryption="AES256",
        kms_key_env=None,
        max_object_bytes=1_000_000,
        max_list_objects=20,
    )


def _storage_config(root: Path) -> dict[str, Any]:
    return {
        "storage": {
            "base_dir": str(root),
            "raw": {
                "base_path": str(root / "raw"),
                "dataset": "market_events",
            },
            "curated": {
                "base_path": str(root / "curated"),
                "datasets": {
                    "daily_returns": "daily_returns",
                    "portfolio_risk_attribution": (
                        "portfolio_risk_attribution"
                    ),
                },
            },
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }


def _write_source_artifacts(root: Path) -> dict[str, bytes]:
    raw_path = (
        root
        / "raw"
        / "market_events"
        / "year=2026"
        / "month=08"
        / "raw.parquet"
    )
    raw_path.parent.mkdir(parents=True)
    raw_payload = b"raw-market-events"
    raw_path.write_bytes(raw_payload)

    daily_path = (
        root
        / "curated"
        / "daily_returns"
        / "year=2026"
        / "daily.parquet"
    )
    daily_path.parent.mkdir(parents=True)
    daily_payload = b"daily-returns"
    daily_path.write_bytes(daily_payload)
    return {
        "market_events/year=2026/month=08/raw.parquet": raw_payload,
        "daily_returns/year=2026/daily.parquet": daily_payload,
    }


def _replication_fixture(
    tmp_path: Path,
    *,
    execute: bool = True,
) -> tuple[
    FakeS3Client,
    Path,
    dict[str, Any],
    dict[str, bytes],
    dict[str, Any],
]:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    source_config = _storage_config(source_root)
    target_config = _storage_config(target_root)
    payloads = _write_source_artifacts(source_root)
    client = FakeS3Client()
    factories: list[str] = []

    summary = replicate_local_datasets(
        store_id="primary-s3",
        storage_config_path=tmp_path / "source-storage.yaml",
        durable_config_path=tmp_path / "durable.yaml",
        selected_datasets=["daily_returns", "market_events"],
        max_files=20,
        max_total_bytes=1_000_000,
        enable_durable_write=execute,
        environment={
            "DURABLE_BUCKET": "test-bucket",
            "AWS_REGION": "eu-west-2",
        },
        config_loader=lambda *_: _durable_config(),
        storage_config_loader=lambda _: source_config,
        client_factory=lambda **_: factories.append("created") or client,
    )
    assert factories == (["created"] if execute else [])
    summary_path = tmp_path / "replication-summary.json"
    summary_path.write_text(
        json.dumps(summary, sort_keys=True),
        encoding="utf-8",
    )
    return client, summary_path, target_config, payloads, summary


def _restore(
    *,
    summary_path: Path,
    target_config: dict[str, Any],
    client: FakeS3Client,
    enable: bool,
    enabled_store: bool = True,
    max_total_bytes: int = 1_000_000,
    factories: list[str] | None = None,
) -> dict[str, Any]:
    client_factories = factories if factories is not None else []
    return restore_durable_replication(
        replication_summary_path=summary_path,
        store_id="primary-s3",
        storage_config_path=summary_path.parent / "target-storage.yaml",
        durable_config_path=summary_path.parent / "durable.yaml",
        max_total_bytes=max_total_bytes,
        enable_durable_read=enable,
        environment={
            "DURABLE_BUCKET": "test-bucket",
            "AWS_REGION": "eu-west-2",
        },
        config_loader=lambda *_: _durable_config(enabled=enabled_store),
        storage_config_loader=lambda _: target_config,
        client_factory=lambda **_: client_factories.append("created") or client,
    )


def test_plan_only_is_deterministic_and_creates_no_client_or_directory(
    tmp_path: Path,
) -> None:
    client, summary_path, target_config, _, _ = _replication_fixture(tmp_path)
    factories: list[str] = []

    first = _restore(
        summary_path=summary_path,
        target_config=target_config,
        client=client,
        enable=False,
        factories=factories,
    )
    second = _restore(
        summary_path=summary_path,
        target_config=target_config,
        client=client,
        enable=False,
        factories=factories,
    )

    assert first["contract"] == RESTORE_CONTRACT
    assert first["restore_plan_id"] == second["restore_plan_id"]
    assert first["manifest_id"] == second["manifest_id"]
    assert first["plan"]["artifact_count"] == 2
    assert first["plan"]["local_status_counts"] == {
        "missing": 2,
        "already_present": 0,
        "conflict": 0,
    }
    assert first["restore"] == {
        "performed": False,
        "reason": "explicit_enable_flag_required",
    }
    assert factories == []
    assert not Path(target_config["storage"]["base_dir"]).exists()
    assert "test-bucket" not in str(first)
    assert "eu-west-2" not in str(first)
    assert first["credentials_recorded"] is False


def test_disabled_store_blocks_reads_before_client_creation(
    tmp_path: Path,
) -> None:
    client, summary_path, target_config, _, _ = _replication_fixture(tmp_path)
    factories: list[str] = []

    result = _restore(
        summary_path=summary_path,
        target_config=target_config,
        client=client,
        enable=True,
        enabled_store=False,
        factories=factories,
    )

    assert result["restore"] == {
        "performed": False,
        "reason": "store_disabled_in_configuration",
    }
    assert factories == []


def test_verified_restore_and_replay_converge_without_overwrite(
    tmp_path: Path,
) -> None:
    client, summary_path, target_config, payloads, _ = _replication_fixture(
        tmp_path
    )
    factories: list[str] = []

    first = _restore(
        summary_path=summary_path,
        target_config=target_config,
        client=client,
        enable=True,
        factories=factories,
    )
    first_gets = list(client.get_calls)
    client.get_calls.clear()
    second = _restore(
        summary_path=summary_path,
        target_config=target_config,
        client=client,
        enable=True,
        factories=factories,
    )

    assert factories == ["created", "created"]
    assert first["restore"]["performed"] is True
    assert first["restore"]["manifest_verified"] is True
    assert first["restore"]["remote_objects_verified"] == 3
    assert first["restore"]["artifacts_restored"] == 2
    assert first["restore"]["artifacts_already_present"] == 0
    assert second["restore"]["artifacts_restored"] == 0
    assert second["restore"]["artifacts_already_present"] == 2
    assert second["restore_plan_id"] == first["restore_plan_id"]
    assert len(first_gets) == 3
    assert client.get_calls == [first["plan"]["manifest_object_key"]]

    target_root = Path(target_config["storage"]["base_dir"])
    assert (
        target_root
        / "raw"
        / "market_events"
        / "year=2026"
        / "month=08"
        / "raw.parquet"
    ).read_bytes() == payloads["market_events/year=2026/month=08/raw.parquet"]
    assert (
        target_root
        / "curated"
        / "daily_returns"
        / "year=2026"
        / "daily.parquet"
    ).read_bytes() == payloads["daily_returns/year=2026/daily.parquet"]


def test_local_conflict_fails_before_environment_or_client_resolution(
    tmp_path: Path,
) -> None:
    client, summary_path, target_config, _, _ = _replication_fixture(tmp_path)
    target = (
        Path(target_config["storage"]["curated"]["base_path"])
        / "daily_returns"
        / "year=2026"
        / "daily.parquet"
    )
    target.parent.mkdir(parents=True)
    target.write_bytes(b"conflicting-local-data")
    factories: list[str] = []

    with pytest.raises(ValidationError, match="conflicting local targets"):
        _restore(
            summary_path=summary_path,
            target_config=target_config,
            client=client,
            enable=True,
            factories=factories,
        )

    assert factories == []
    assert target.read_bytes() == b"conflicting-local-data"


def test_execution_requires_completed_replication_evidence(
    tmp_path: Path,
) -> None:
    client, summary_path, target_config, _, summary = _replication_fixture(
        tmp_path,
        execute=False,
    )
    assert summary["replication"]["performed"] is False
    factories: list[str] = []

    with pytest.raises(ValidationError, match="completed replication result"):
        _restore(
            summary_path=summary_path,
            target_config=target_config,
            client=client,
            enable=True,
            factories=factories,
        )

    assert factories == []


def test_manifest_metadata_and_payload_are_verified_before_artifacts(
    tmp_path: Path,
) -> None:
    client, summary_path, target_config, _, summary = _replication_fixture(
        tmp_path
    )
    manifest_key = summary["plan"]["manifest_object_key"]
    client.objects[manifest_key]["Metadata"]["sha256"] = "0" * 64

    with pytest.raises(StorageError, match="digest metadata"):
        _restore(
            summary_path=summary_path,
            target_config=target_config,
            client=client,
            enable=True,
        )

    assert not Path(target_config["storage"]["base_dir"]).exists()


def test_artifact_payload_digest_is_verified_before_local_publication(
    tmp_path: Path,
) -> None:
    client, summary_path, target_config, _, summary = _replication_fixture(
        tmp_path
    )
    artifact_key = summary["plan"]["artifacts"][0]["object_key"]
    original = client.objects[artifact_key]["Body"]
    client.objects[artifact_key]["Body"] = b"x" * len(original)

    with pytest.raises(StorageError, match="payload digest"):
        _restore(
            summary_path=summary_path,
            target_config=target_config,
            client=client,
            enable=True,
        )

    assert not Path(target_config["storage"]["base_dir"]).exists()


def test_unsafe_manifest_path_and_total_byte_bound_fail_during_plan(
    tmp_path: Path,
) -> None:
    client, summary_path, target_config, _, summary = _replication_fixture(
        tmp_path
    )
    unsafe = copy.deepcopy(summary)
    unsafe["plan"]["artifacts"][0]["relative_path"] = "../escape.parquet"
    unsafe_path = tmp_path / "unsafe-summary.json"
    unsafe_path.write_text(json.dumps(unsafe), encoding="utf-8")

    with pytest.raises(ValidationError, match="safe relative POSIX path"):
        _restore(
            summary_path=unsafe_path,
            target_config=target_config,
            client=client,
            enable=False,
        )

    with pytest.raises(ValidationError, match="max_total_bytes"):
        _restore(
            summary_path=summary_path,
            target_config=target_config,
            client=client,
            enable=False,
            max_total_bytes=1,
        )


def test_symbolic_link_target_base_fails_closed(tmp_path: Path) -> None:
    client, summary_path, target_config, _, _ = _replication_fixture(tmp_path)
    target_base = Path(target_config["storage"]["curated"]["base_path"])
    real_base = tmp_path / "real-curated"
    real_base.mkdir()
    target_base.parent.mkdir(parents=True, exist_ok=True)
    try:
        target_base.symlink_to(real_base, target_is_directory=True)
    except OSError:
        pytest.skip("symbolic links are unavailable")

    with pytest.raises(StorageError, match="symbolic links"):
        _restore(
            summary_path=summary_path,
            target_config=target_config,
            client=client,
            enable=False,
        )
