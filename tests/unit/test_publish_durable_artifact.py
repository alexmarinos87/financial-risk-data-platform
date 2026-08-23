from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.publish_durable_artifact import publish_durable_artifact
from src.storage.durable_s3 import DurableS3Config


def _config(*, enabled: bool) -> DurableS3Config:
    return DurableS3Config(
        store_id="primary",
        enabled=enabled,
        bucket_env="BUCKET",
        region_env="REGION",
        prefix="risk-platform",
        server_side_encryption="AES256",
        kms_key_env=None,
        max_object_bytes=1_000_000,
        max_list_objects=100,
    )


def test_missing_explicit_flag_returns_without_reading_file(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing.json"

    summary = publish_durable_artifact(
        store_id="primary",
        dataset="daily-risk",
        file_path=missing,
        durable_config_path=Path("durable.yaml"),
        config_loader=lambda *_: _config(enabled=True),
    )

    assert summary["publication"] == {
        "performed": False,
        "reason": "explicit_enable_flag_required",
    }
    assert summary["credentials_recorded"] is False


def test_disabled_configuration_cannot_be_overridden(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text("{}", encoding="utf-8")

    summary = publish_durable_artifact(
        store_id="primary",
        dataset="daily-risk",
        file_path=artifact,
        durable_config_path=Path("durable.yaml"),
        enable_durable_write=True,
        config_loader=lambda *_: _config(enabled=False),
    )

    assert summary["publication"]["reason"] == (
        "store_disabled_in_configuration"
    )


def test_enabled_publication_resolves_environment_without_recording_values(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text('{"value":1}', encoding="utf-8")
    calls: list[dict[str, Any]] = []
    client = object()

    def publisher(selected_client: object, **kwargs: Any) -> dict[str, Any]:
        calls.append({"client": selected_client, **kwargs})
        return {
            "status": "written",
            "key": "risk-platform/daily-risk/sha256=aa/value.json",
            "sha256": "a" * 64,
            "content_length": 11,
            "bucket_configured": True,
        }

    summary = publish_durable_artifact(
        store_id="primary",
        dataset="daily-risk",
        file_path=artifact,
        durable_config_path=Path("durable.yaml"),
        enable_durable_write=True,
        environment={"BUCKET": "private-bucket", "REGION": "eu-west-2"},
        config_loader=lambda *_: _config(enabled=True),
        client_factory=lambda **kwargs: client,
        publisher=publisher,
    )

    assert calls[0]["client"] is client
    assert calls[0]["bucket"] == "private-bucket"
    assert calls[0]["payload"] == b'{"value":1}'
    assert calls[0]["content_type"] == "application/json"
    assert "private-bucket" not in str(summary)
    assert "eu-west-2" not in str(summary)
    assert summary["publication"]["status"] == "written"


def test_missing_environment_fails_before_client_creation(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text("{}", encoding="utf-8")
    client_created = False

    def client_factory(**_kwargs: Any) -> object:
        nonlocal client_created
        client_created = True
        return object()

    with pytest.raises(ValidationError, match="BUCKET"):
        publish_durable_artifact(
            store_id="primary",
            dataset="daily-risk",
            file_path=artifact,
            durable_config_path=Path("durable.yaml"),
            enable_durable_write=True,
            environment={"REGION": "eu-west-2"},
            config_loader=lambda *_: _config(enabled=True),
            client_factory=client_factory,
        )

    assert client_created is False


def test_symbolic_link_and_empty_artifacts_fail_closed(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "link.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symbolic links are unavailable")

    with pytest.raises(StorageError, match="non-symbolic-link"):
        publish_durable_artifact(
            store_id="primary",
            dataset="daily-risk",
            file_path=link,
            durable_config_path=Path("durable.yaml"),
            enable_durable_write=True,
            environment={"BUCKET": "bucket", "REGION": "eu-west-2"},
            config_loader=lambda *_: _config(enabled=True),
        )

    empty = tmp_path / "empty.json"
    empty.write_bytes(b"")
    with pytest.raises(ValidationError, match="must not be empty"):
        publish_durable_artifact(
            store_id="primary",
            dataset="daily-risk",
            file_path=empty,
            durable_config_path=Path("durable.yaml"),
            enable_durable_write=True,
            environment={"BUCKET": "bucket", "REGION": "eu-west-2"},
            config_loader=lambda *_: _config(enabled=True),
        )
