from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..common.config import load_yaml
from ..common.exceptions import StorageError, ValidationError

MAX_OBJECT_BYTES = 5_000_000_000
MAX_LIST_OBJECTS = 10_000


@dataclass(frozen=True, slots=True)
class DurableS3Config:
    store_id: str
    enabled: bool
    bucket_env: str
    region_env: str
    prefix: str
    server_side_encryption: str
    kms_key_env: str | None
    max_object_bytes: int
    max_list_objects: int


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    return value.strip()


def _bounded_integer(value: Any, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between 1 and {maximum}"
        )
    return value


def _safe_path(value: Any, label: str) -> str:
    text = _required_text(value, label).strip("/")
    segments = text.split("/")
    if not segments or any(
        segment in {"", ".", ".."}
        or "\\" in segment
        or any(ord(character) < 32 for character in segment)
        for segment in segments
    ):
        raise ValidationError(f"{label} must contain safe path segments")
    return "/".join(segments)


def _safe_dataset(value: Any) -> str:
    text = _required_text(value, "dataset")
    if (
        text in {".", ".."}
        or "/" in text
        or "\\" in text
        or any(ord(character) < 32 for character in text)
    ):
        raise ValidationError("dataset must be one safe path segment")
    return text


def parse_durable_s3_config(
    payload: Mapping[str, Any],
    store_id: str,
) -> DurableS3Config:
    stores = payload.get("stores")
    if not isinstance(stores, Mapping):
        raise ValidationError("durable-storage configuration must define stores")
    candidate = stores.get(store_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(f"durable store '{store_id}' is not configured")
    enabled = candidate.get("enabled")
    if type(enabled) is not bool:
        raise ValidationError("durable store enabled must be true or false")
    if candidate.get("adapter_type") != "s3":
        raise ValidationError("only the s3 durable adapter type is supported")

    encryption = _required_text(
        candidate.get("server_side_encryption"),
        "server_side_encryption",
    )
    if encryption not in {"AES256", "aws:kms"}:
        raise ValidationError(
            "server_side_encryption must be AES256 or aws:kms"
        )
    kms_raw = candidate.get("kms_key_env")
    kms_key_env = (
        None
        if kms_raw is None or kms_raw == ""
        else _required_text(kms_raw, "kms_key_env")
    )
    if encryption == "aws:kms" and kms_key_env is None:
        raise ValidationError("aws:kms storage requires kms_key_env")
    if encryption == "AES256" and kms_key_env is not None:
        raise ValidationError("AES256 storage must not configure kms_key_env")

    return DurableS3Config(
        store_id=_required_text(store_id, "store_id"),
        enabled=enabled,
        bucket_env=_required_text(candidate.get("bucket_env"), "bucket_env"),
        region_env=_required_text(candidate.get("region_env"), "region_env"),
        prefix=_safe_path(candidate.get("prefix"), "prefix"),
        server_side_encryption=encryption,
        kms_key_env=kms_key_env,
        max_object_bytes=_bounded_integer(
            candidate.get("max_object_bytes"),
            "max_object_bytes",
            MAX_OBJECT_BYTES,
        ),
        max_list_objects=_bounded_integer(
            candidate.get("max_list_objects"),
            "max_list_objects",
            MAX_LIST_OBJECTS,
        ),
    )


def load_durable_s3_config(path: Path, store_id: str) -> DurableS3Config:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "durable-storage configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("durable-storage configuration must be a mapping")
    return parse_durable_s3_config(payload, store_id)


def content_sha256(payload: bytes) -> str:
    if not isinstance(payload, bytes):
        raise ValidationError("durable object payload must be bytes")
    return hashlib.sha256(payload).hexdigest()


def immutable_object_key(
    config: DurableS3Config,
    *,
    dataset: str,
    payload: bytes,
    extension: str,
) -> str:
    dataset = _safe_dataset(dataset)
    extension = _required_text(extension, "extension").lstrip(".")
    if not extension.isalnum() or len(extension) > 16:
        raise ValidationError("extension must be short alphanumeric text")
    digest = content_sha256(payload)
    return (
        f"{config.prefix}/{dataset}/sha256={digest[:2]}/"
        f"{digest}.{extension}"
    )


def _already_exists(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return False
    error = response.get("Error")
    metadata = response.get("ResponseMetadata")
    code = error.get("Code") if isinstance(error, Mapping) else None
    status = (
        metadata.get("HTTPStatusCode")
        if isinstance(metadata, Mapping)
        else None
    )
    return code in {"PreconditionFailed", "412"} or status == 412


def _verify_existing_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    digest: str,
    payload_length: int,
) -> None:
    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except Exception:
        raise StorageError(
            "Unable to verify an existing durable object"
        ) from None
    metadata = response.get("Metadata")
    stored_digest = (
        metadata.get("sha256") if isinstance(metadata, Mapping) else None
    )
    if stored_digest != digest or response.get("ContentLength") != payload_length:
        raise StorageError(
            "Existing durable object does not match the immutable content identity"
        )


def put_immutable_object(
    client: Any,
    *,
    config: DurableS3Config,
    bucket: str,
    dataset: str,
    payload: bytes,
    extension: str,
    content_type: str,
    kms_key_id: str | None = None,
) -> dict[str, Any]:
    if not config.enabled:
        raise ValidationError("durable S3 store is disabled")
    bucket = _required_text(bucket, "bucket")
    content_type = _required_text(content_type, "content_type")
    if not isinstance(payload, bytes) or not payload:
        raise ValidationError("durable object payload must be non-empty bytes")
    if len(payload) > config.max_object_bytes:
        raise ValidationError("durable object exceeds max_object_bytes")
    if config.server_side_encryption == "aws:kms":
        kms_key_id = _required_text(kms_key_id, "kms_key_id")
    elif kms_key_id is not None:
        raise ValidationError("kms_key_id is invalid for AES256 storage")

    digest = content_sha256(payload)
    key = immutable_object_key(
        config,
        dataset=dataset,
        payload=payload,
        extension=extension,
    )
    checksum = base64.b64encode(bytes.fromhex(digest)).decode("ascii")
    request: dict[str, Any] = {
        "Body": payload,
        "Bucket": bucket,
        "ChecksumSHA256": checksum,
        "ContentType": content_type,
        "IfNoneMatch": "*",
        "Key": key,
        "Metadata": {
            "sha256": digest,
            "storage-contract": "immutable-content-addressed-v1",
        },
        "ServerSideEncryption": config.server_side_encryption,
    }
    if kms_key_id is not None:
        request["SSEKMSKeyId"] = kms_key_id

    try:
        response = client.put_object(**request)
    except Exception as exc:
        if not _already_exists(exc):
            raise StorageError("Durable S3 object publication failed") from None
        _verify_existing_object(
            client,
            bucket=bucket,
            key=key,
            digest=digest,
            payload_length=len(payload),
        )
        return {
            "bucket_configured": True,
            "content_length": len(payload),
            "etag": None,
            "key": key,
            "sha256": digest,
            "status": "already_present",
        }

    return {
        "bucket_configured": True,
        "content_length": len(payload),
        "etag": response.get("ETag"),
        "key": key,
        "sha256": digest,
        "status": "written",
    }


def inventory_immutable_objects(
    client: Any,
    *,
    config: DurableS3Config,
    bucket: str,
    dataset: str,
) -> tuple[dict[str, Any], ...]:
    if not config.enabled:
        raise ValidationError("durable S3 store is disabled")
    bucket = _required_text(bucket, "bucket")
    dataset = _safe_dataset(dataset)
    prefix = f"{config.prefix}/{dataset}/"
    token: str | None = None
    objects: list[dict[str, Any]] = []

    while True:
        request: dict[str, Any] = {
            "Bucket": bucket,
            "MaxKeys": min(1000, config.max_list_objects),
            "Prefix": prefix,
        }
        if token is not None:
            request["ContinuationToken"] = token
        try:
            response = client.list_objects_v2(**request)
        except Exception:
            raise StorageError("Durable S3 inventory failed") from None
        contents = response.get("Contents", [])
        if not isinstance(contents, list):
            raise StorageError("Durable S3 inventory returned invalid contents")
        for item in contents:
            if not isinstance(item, Mapping):
                raise StorageError(
                    "Durable S3 inventory returned an invalid object"
                )
            key = item.get("Key")
            if not isinstance(key, str) or not key.startswith(prefix):
                raise StorageError(
                    "Durable S3 inventory escaped the configured prefix"
                )
            objects.append(
                {
                    "etag": item.get("ETag"),
                    "key": key,
                    "last_modified": item.get("LastModified"),
                    "size": item.get("Size"),
                }
            )
            if len(objects) > config.max_list_objects:
                raise StorageError(
                    "Durable S3 inventory exceeds max_list_objects"
                )
        if not response.get("IsTruncated"):
            break
        next_token = response.get("NextContinuationToken")
        if not isinstance(next_token, str) or not next_token:
            raise StorageError(
                "Durable S3 inventory is truncated without a continuation token"
            )
        token = next_token

    return tuple(objects)


def create_boto3_s3_client(*, region_name: str) -> Any:
    region_name = _required_text(region_name, "region_name")
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "Durable S3 storage requires boto3 in the enabled runtime."
        ) from exc
    return boto3.client("s3", region_name=region_name)


def canonical_json_payload(value: Mapping[str, Any]) -> bytes:
    if not isinstance(value, Mapping):
        raise ValidationError("canonical JSON payload must be a mapping")
    return json.dumps(
        value,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
