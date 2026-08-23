from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import StorageError, ValidationError
from src.storage.s3_replication import (
    MODEL_VERSION,
    inventory_local_parquet,
    parse_s3_replication_config,
    replicate_local_parquet_to_s3,
)
from tests.storage_config_helpers import build_storage_config


class MissingObject(Exception):
    response = {"Error": {"Code": "404"}}


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        self.put_calls: list[dict[str, Any]] = []
        self.head_calls: list[tuple[str, str]] = []

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        key = (kwargs["Bucket"], kwargs["Key"])
        self.head_calls.append(key)
        if key not in self.objects:
            raise MissingObject()
        return dict(self.objects[key])

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        body = kwargs["Body"]
        content = body.read()
        call = {
            key: value
            for key, value in kwargs.items()
            if key != "Body"
        }
        self.put_calls.append(call)
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = {
            "ContentLength": len(content),
            "Metadata": dict(kwargs["Metadata"]),
            "ServerSideEncryption": kwargs["ServerSideEncryption"],
        }
        return {"ETag": "fake"}


def _replication_payload(*, enabled: bool = False, maximum_files: int = 4096):
    return {
        "object_storage": {
            "s3": {
                "enabled": enabled,
                "bucket_env": "RISK_PLATFORM_S3_BUCKET",
                "region_env": "AWS_REGION",
                "prefix": "financial-risk-data-platform",
                "server_side_encryption": "AES256",
                "maximum_files": maximum_files,
                "maximum_bytes": 1_000_000_000,
            }
        }
    }


def _write_configs(
    tmp_path: Path,
    *,
    enabled: bool = False,
    maximum_files: int = 4096,
) -> tuple[Path, Path, dict[str, Any]]:
    storage = build_storage_config(tmp_path)
    storage_path = tmp_path / "storage.yaml"
    storage_path.write_text(
        yaml.safe_dump(storage, sort_keys=False),
        encoding="utf-8",
    )
    replication_path = tmp_path / "object-storage.yaml"
    replication_path.write_text(
        yaml.safe_dump(
            _replication_payload(
                enabled=enabled,
                maximum_files=maximum_files,
            ),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return replication_path, storage_path, storage


def _write_parquet_like_file(
    storage: dict[str, Any],
    *,
    kind: str,
    dataset: str,
    name: str,
    content: bytes,
) -> Path:
    if kind == "raw":
        base = Path(storage["storage"]["raw"]["base_path"])
    else:
        base = Path(storage["storage"]["curated"]["base_path"])
    path = base / dataset / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_config_and_inventory_are_deterministic_and_bounded(tmp_path: Path) -> None:
    replication_path, storage_path, storage = _write_configs(tmp_path)
    raw = _write_parquet_like_file(
        storage,
        kind="raw",
        dataset="market_events",
        name="raw.parquet",
        content=b"raw",
    )
    curated = _write_parquet_like_file(
        storage,
        kind="curated",
        dataset="daily_returns",
        name="curated.parquet",
        content=b"curated",
    )
    config = parse_s3_replication_config(_replication_payload())

    first = inventory_local_parquet(
        storage_config_path=storage_path,
        replication_config=config,
    )
    second = inventory_local_parquet(
        storage_config_path=storage_path,
        replication_config=config,
    )

    assert first == second
    assert [item.relative_path for item in first] == sorted(
        [
            raw.relative_to(tmp_path).as_posix(),
            curated.relative_to(tmp_path).as_posix(),
        ]
    )
    assert all(item.object_key.startswith(config.prefix + "/") for item in first)
    hashes = {item.relative_path: item.sha256 for item in first}
    assert hashes[raw.relative_to(tmp_path).as_posix()] == hashlib.sha256(
        b"raw"
    ).hexdigest()
    assert replication_path.exists()


def test_plan_only_never_constructs_or_calls_s3_client(tmp_path: Path) -> None:
    replication_path, storage_path, storage = _write_configs(tmp_path)
    _write_parquet_like_file(
        storage,
        kind="curated",
        dataset="daily_returns",
        name="one.parquet",
        content=b"one",
    )

    class FailClient:
        def head_object(self, **kwargs: Any) -> dict[str, Any]:
            raise AssertionError("dry run must not call S3")

        def put_object(self, **kwargs: Any) -> dict[str, Any]:
            raise AssertionError("dry run must not call S3")

    summary = replicate_local_parquet_to_s3(
        replication_config_path=replication_path,
        storage_config_path=storage_path,
        execute=False,
        environment={},
        client=FailClient(),
    )

    assert summary["execution"]["performed"] is False
    assert summary["selection"]["files_selected"] == 1
    assert summary["bucket_created"] is False
    assert summary["objects_deleted"] == 0
    assert summary["infrastructure_applied"] is False


def test_disabled_execution_fails_before_remote_calls(tmp_path: Path) -> None:
    replication_path, storage_path, storage = _write_configs(tmp_path)
    _write_parquet_like_file(
        storage,
        kind="curated",
        dataset="daily_returns",
        name="one.parquet",
        content=b"one",
    )
    client = FakeS3Client()

    with pytest.raises(ValidationError, match="disabled"):
        replicate_local_parquet_to_s3(
            replication_config_path=replication_path,
            storage_config_path=storage_path,
            execute=True,
            environment={
                "RISK_PLATFORM_S3_BUCKET": "risk-platform-example",
                "AWS_REGION": "eu-west-2",
            },
            client=client,
        )
    assert client.head_calls == []
    assert client.put_calls == []


def test_upload_verifies_metadata_and_redacts_bucket_name(tmp_path: Path) -> None:
    replication_path, storage_path, storage = _write_configs(
        tmp_path,
        enabled=True,
    )
    _write_parquet_like_file(
        storage,
        kind="curated",
        dataset="daily_returns",
        name="one.parquet",
        content=b"one",
    )
    client = FakeS3Client()
    summary = replicate_local_parquet_to_s3(
        replication_config_path=replication_path,
        storage_config_path=storage_path,
        execute=True,
        environment={
            "RISK_PLATFORM_S3_BUCKET": "risk-platform-example",
            "AWS_REGION": "eu-west-2",
        },
        client=client,
    )

    assert summary["execution"] == {
        "requested": True,
        "performed": True,
        "uploaded": 1,
        "already_present": 0,
    }
    assert client.put_calls[0]["ServerSideEncryption"] == "AES256"
    assert client.put_calls[0]["Metadata"]["model-version"] == MODEL_VERSION
    serialized = json.dumps(summary)
    assert "risk-platform-example" not in serialized
    assert summary["destination"]["bucket_fingerprint"] is not None


def test_matching_remote_object_is_replay_and_conflict_is_not_overwritten(
    tmp_path: Path,
) -> None:
    replication_path, storage_path, storage = _write_configs(
        tmp_path,
        enabled=True,
    )
    _write_parquet_like_file(
        storage,
        kind="curated",
        dataset="daily_returns",
        name="one.parquet",
        content=b"one",
    )
    environment = {
        "RISK_PLATFORM_S3_BUCKET": "risk-platform-example",
        "AWS_REGION": "eu-west-2",
    }
    client = FakeS3Client()
    first = replicate_local_parquet_to_s3(
        replication_config_path=replication_path,
        storage_config_path=storage_path,
        execute=True,
        environment=environment,
        client=client,
    )
    second = replicate_local_parquet_to_s3(
        replication_config_path=replication_path,
        storage_config_path=storage_path,
        execute=True,
        environment=environment,
        client=client,
    )
    assert first["execution"]["uploaded"] == 1
    assert second["execution"] == {
        "requested": True,
        "performed": True,
        "uploaded": 0,
        "already_present": 1,
    }
    assert len(client.put_calls) == 1

    key = next(iter(client.objects))
    client.objects[key]["Metadata"]["sha256"] = "different"
    with pytest.raises(StorageError, match="overwrite is forbidden"):
        replicate_local_parquet_to_s3(
            replication_config_path=replication_path,
            storage_config_path=storage_path,
            execute=True,
            environment=environment,
            client=client,
        )
    assert len(client.put_calls) == 1


def test_inventory_rejects_symlinks_and_file_limit(tmp_path: Path) -> None:
    replication_path, storage_path, storage = _write_configs(
        tmp_path,
        maximum_files=1,
    )
    _write_parquet_like_file(
        storage,
        kind="curated",
        dataset="daily_returns",
        name="one.parquet",
        content=b"one",
    )
    _write_parquet_like_file(
        storage,
        kind="curated",
        dataset="daily_returns",
        name="two.parquet",
        content=b"two",
    )
    config = parse_s3_replication_config(
        _replication_payload(maximum_files=1)
    )
    with pytest.raises(StorageError, match="file limit"):
        inventory_local_parquet(
            storage_config_path=storage_path,
            replication_config=config,
        )

    target = tmp_path / "target.parquet"
    target.write_bytes(b"target")
    symlink = (
        Path(storage["storage"]["curated"]["base_path"])
        / "daily_returns"
        / "linked.parquet"
    )
    symlink.unlink(missing_ok=True)
    symlink.symlink_to(target)
    config = parse_s3_replication_config(_replication_payload())
    with pytest.raises(StorageError, match="unsafe file type"):
        inventory_local_parquet(
            storage_config_path=storage_path,
            replication_config=config,
        )
    assert replication_path.exists()


def test_invalid_bucket_and_destination_requirements_fail_closed(
    tmp_path: Path,
) -> None:
    replication_path, storage_path, storage = _write_configs(
        tmp_path,
        enabled=True,
    )
    _write_parquet_like_file(
        storage,
        kind="curated",
        dataset="daily_returns",
        name="one.parquet",
        content=b"one",
    )
    for environment, pattern in (
        ({"AWS_REGION": "eu-west-2"}, "bucket environment"),
        (
            {"RISK_PLATFORM_S3_BUCKET": "risk-platform-example"},
            "region environment",
        ),
        (
            {
                "RISK_PLATFORM_S3_BUCKET": "Invalid_Bucket",
                "AWS_REGION": "eu-west-2",
            },
            "bucket name",
        ),
    ):
        with pytest.raises(ValidationError, match=pattern):
            replicate_local_parquet_to_s3(
                replication_config_path=replication_path,
                storage_config_path=storage_path,
                execute=True,
                environment=environment,
                client=FakeS3Client(),
            )
