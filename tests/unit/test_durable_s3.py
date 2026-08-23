from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.storage.durable_s3 import (
    DurableS3Config,
    immutable_object_key,
    inventory_immutable_objects,
    parse_durable_s3_config,
    put_immutable_object,
)


class PreconditionFailed(Exception):
    def __init__(self) -> None:
        self.response = {
            "Error": {"Code": "PreconditionFailed"},
            "ResponseMetadata": {"HTTPStatusCode": 412},
        }


class FakeClient:
    def __init__(self) -> None:
        self.put_calls: list[dict[str, Any]] = []
        self.head_response: dict[str, Any] | None = None
        self.raise_precondition = False
        self.list_responses: list[dict[str, Any]] = []

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        if self.raise_precondition:
            raise PreconditionFailed()
        return {"ETag": '"etag-1"'}

    def head_object(self, **_kwargs: Any) -> dict[str, Any]:
        assert self.head_response is not None
        return self.head_response

    def list_objects_v2(self, **_kwargs: Any) -> dict[str, Any]:
        return self.list_responses.pop(0)


def _config(**overrides: Any) -> DurableS3Config:
    values: dict[str, Any] = {
        "store_id": "primary",
        "enabled": True,
        "bucket_env": "BUCKET",
        "region_env": "REGION",
        "prefix": "risk-platform",
        "server_side_encryption": "AES256",
        "kms_key_env": None,
        "max_object_bytes": 1_000_000,
        "max_list_objects": 10,
    }
    values.update(overrides)
    return DurableS3Config(**values)


def test_config_is_disabled_and_contains_no_bucket_or_credentials() -> None:
    parsed = parse_durable_s3_config(
        {
            "stores": {
                "primary": {
                    "enabled": False,
                    "adapter_type": "s3",
                    "bucket_env": "BUCKET",
                    "region_env": "REGION",
                    "prefix": "risk-platform",
                    "server_side_encryption": "AES256",
                    "kms_key_env": None,
                    "max_object_bytes": 1000,
                    "max_list_objects": 10,
                }
            }
        },
        "primary",
    )

    assert parsed.enabled is False
    assert parsed.bucket_env == "BUCKET"
    assert parsed.region_env == "REGION"


def test_object_key_is_content_addressed_and_deterministic() -> None:
    first = immutable_object_key(
        _config(),
        dataset="daily-risk",
        payload=b"same-content",
        extension="json",
    )
    second = immutable_object_key(
        _config(),
        dataset="daily-risk",
        payload=b"same-content",
        extension="json",
    )
    changed = immutable_object_key(
        _config(),
        dataset="daily-risk",
        payload=b"changed-content",
        extension="json",
    )

    assert first == second
    assert first != changed
    assert first.startswith("risk-platform/daily-risk/sha256=")


def test_put_uses_conditional_write_checksum_and_encryption() -> None:
    client = FakeClient()

    result = put_immutable_object(
        client,
        config=_config(),
        bucket="bucket-name",
        dataset="daily-risk",
        payload=b"payload",
        extension="json",
        content_type="application/json",
    )

    assert result["status"] == "written"
    request = client.put_calls[0]
    assert request["IfNoneMatch"] == "*"
    assert request["ServerSideEncryption"] == "AES256"
    assert request["ChecksumSHA256"]
    assert request["Metadata"]["sha256"] == result["sha256"]
    assert request["Bucket"] == "bucket-name"


def test_existing_matching_object_is_replay_safe() -> None:
    client = FakeClient()
    client.raise_precondition = True
    expected = put_immutable_object
    payload = b"payload"
    from src.storage.durable_s3 import content_sha256

    digest = content_sha256(payload)
    client.head_response = {
        "ContentLength": len(payload),
        "Metadata": {"sha256": digest},
    }

    result = expected(
        client,
        config=_config(),
        bucket="bucket-name",
        dataset="daily-risk",
        payload=payload,
        extension="json",
        content_type="application/json",
    )

    assert result["status"] == "already_present"
    assert result["sha256"] == digest


def test_existing_identity_mismatch_fails_closed() -> None:
    client = FakeClient()
    client.raise_precondition = True
    client.head_response = {
        "ContentLength": 7,
        "Metadata": {"sha256": "different"},
    }

    with pytest.raises(StorageError, match="does not match"):
        put_immutable_object(
            client,
            config=_config(),
            bucket="bucket-name",
            dataset="daily-risk",
            payload=b"payload",
            extension="json",
            content_type="application/json",
        )


def test_kms_configuration_requires_key_and_passes_it_to_s3() -> None:
    client = FakeClient()
    config = replace(
        _config(),
        server_side_encryption="aws:kms",
        kms_key_env="KMS_KEY",
    )

    with pytest.raises(ValidationError, match="kms_key_id"):
        put_immutable_object(
            client,
            config=config,
            bucket="bucket-name",
            dataset="daily-risk",
            payload=b"payload",
            extension="json",
            content_type="application/json",
        )

    put_immutable_object(
        client,
        config=config,
        bucket="bucket-name",
        dataset="daily-risk",
        payload=b"payload",
        extension="json",
        content_type="application/json",
        kms_key_id="alias/risk-platform",
    )
    assert client.put_calls[-1]["SSEKMSKeyId"] == "alias/risk-platform"


def test_inventory_is_prefix_bounded_and_paginated() -> None:
    client = FakeClient()
    client.list_responses = [
        {
            "Contents": [
                {
                    "Key": "risk-platform/daily-risk/a.json",
                    "Size": 10,
                    "ETag": "a",
                    "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc),
                }
            ],
            "IsTruncated": True,
            "NextContinuationToken": "next",
        },
        {
            "Contents": [
                {
                    "Key": "risk-platform/daily-risk/b.json",
                    "Size": 20,
                    "ETag": "b",
                    "LastModified": datetime(2026, 1, 2, tzinfo=timezone.utc),
                }
            ],
            "IsTruncated": False,
        },
    ]

    inventory = inventory_immutable_objects(
        client,
        config=_config(),
        bucket="bucket-name",
        dataset="daily-risk",
    )

    assert [item["key"] for item in inventory] == [
        "risk-platform/daily-risk/a.json",
        "risk-platform/daily-risk/b.json",
    ]


def test_disabled_and_oversized_writes_fail_before_client_use() -> None:
    client = FakeClient()

    with pytest.raises(ValidationError, match="disabled"):
        put_immutable_object(
            client,
            config=_config(enabled=False),
            bucket="bucket-name",
            dataset="daily-risk",
            payload=b"payload",
            extension="json",
            content_type="application/json",
        )

    with pytest.raises(ValidationError, match="max_object_bytes"):
        put_immutable_object(
            client,
            config=_config(max_object_bytes=2),
            bucket="bucket-name",
            dataset="daily-risk",
            payload=b"payload",
            extension="json",
            content_type="application/json",
        )

    assert client.put_calls == []
