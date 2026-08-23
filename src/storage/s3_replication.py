from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from ..common.config import load_yaml
from ..common.exceptions import StorageError, ValidationError
from .storage_config import load_storage_config, validate_storage_config

MODEL_VERSION = "s3-replication-v1"
MAX_CONFIGURED_FILES = 10_000
MAX_CONFIGURED_BYTES = 10_000_000_000
MAX_PLAN_PREVIEW = 100
CHUNK_SIZE = 1024 * 1024
S3_BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


class S3Client(Protocol):
    def head_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class S3ReplicationConfig:
    enabled: bool
    bucket_env: str
    region_env: str
    prefix: str
    server_side_encryption: str
    maximum_files: int
    maximum_bytes: int

    @property
    def fingerprint(self) -> str:
        payload = {
            "bucket_env": self.bucket_env,
            "enabled": self.enabled,
            "maximum_bytes": self.maximum_bytes,
            "maximum_files": self.maximum_files,
            "model_version": MODEL_VERSION,
            "prefix": self.prefix,
            "region_env": self.region_env,
            "server_side_encryption": self.server_side_encryption,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        return f"s3-replication-config-{digest}"


@dataclass(frozen=True, slots=True)
class LocalObject:
    path: Path
    relative_path: str
    object_key: str
    size_bytes: int
    sha256: str

    def evidence(self) -> dict[str, Any]:
        return {
            "object_key": self.object_key,
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


def _required_text(value: Any, label: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    parsed = value.strip()
    if len(parsed) > maximum or any(ord(character) < 32 for character in parsed):
        raise ValidationError(f"{label} is invalid")
    return parsed


def _bounded_integer(value: Any, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between 1 and {maximum}"
        )
    return value


def _prefix(value: Any) -> str:
    parsed = _required_text(value, "prefix", maximum=512).strip("/")
    segments = parsed.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise ValidationError("prefix must contain safe non-empty path segments")
    return "/".join(segments)


def parse_s3_replication_config(
    payload: Mapping[str, Any],
) -> S3ReplicationConfig:
    if not isinstance(payload, Mapping):
        raise ValidationError("object-storage configuration must be a mapping")
    object_storage = payload.get("object_storage")
    if not isinstance(object_storage, Mapping):
        raise ValidationError("object-storage configuration is missing object_storage")
    candidate = object_storage.get("s3")
    if not isinstance(candidate, Mapping):
        raise ValidationError("object-storage configuration is missing s3")
    enabled = candidate.get("enabled")
    if type(enabled) is not bool:
        raise ValidationError("s3 enabled must be boolean")
    encryption = _required_text(
        candidate.get("server_side_encryption"),
        "server_side_encryption",
    )
    if encryption != "AES256":
        raise ValidationError(
            "this replication contract supports AES256 server-side encryption only"
        )
    return S3ReplicationConfig(
        enabled=enabled,
        bucket_env=_required_text(candidate.get("bucket_env"), "bucket_env"),
        region_env=_required_text(candidate.get("region_env"), "region_env"),
        prefix=_prefix(candidate.get("prefix")),
        server_side_encryption=encryption,
        maximum_files=_bounded_integer(
            candidate.get("maximum_files"),
            "maximum_files",
            MAX_CONFIGURED_FILES,
        ),
        maximum_bytes=_bounded_integer(
            candidate.get("maximum_bytes"),
            "maximum_bytes",
            MAX_CONFIGURED_BYTES,
        ),
    )


def load_s3_replication_config(path: Path) -> S3ReplicationConfig:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "object-storage configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("object-storage configuration must be a mapping")
    return parse_s3_replication_config(payload)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(CHUNK_SIZE):
                digest.update(chunk)
    except OSError:
        raise StorageError("unable to hash local Parquet input") from None
    return digest.hexdigest()


def _ensure_safe_file(path: Path, base: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise StorageError("S3 replication input contains an unsafe file type")
    current = path.parent
    while current != base:
        if current.is_symlink():
            raise StorageError(
                "S3 replication input contains a symbolic-link directory"
            )
        if current == current.parent:
            raise StorageError("S3 replication input escaped its configured base")
        current = current.parent
    if base.is_symlink() or not base.is_dir():
        raise StorageError("S3 replication base must be a regular directory")


def inventory_local_parquet(
    *,
    storage_config_path: Path,
    replication_config: S3ReplicationConfig,
) -> tuple[LocalObject, ...]:
    storage_config = load_storage_config(storage_config_path)
    validate_storage_config(storage_config)
    storage = storage_config["storage"]
    base_dir = Path(storage["base_dir"])
    if base_dir.is_symlink():
        raise StorageError("storage base_dir must not be a symbolic link")

    configured_bases = (
        Path(storage["raw"]["base_path"]),
        Path(storage["curated"]["base_path"]),
    )
    files: list[Path] = []
    for configured_base in configured_bases:
        if not configured_base.exists():
            continue
        if configured_base.is_symlink() or not configured_base.is_dir():
            raise StorageError(
                "configured replication source must be a regular directory"
            )
        try:
            files.extend(sorted(configured_base.rglob("*.parquet")))
        except OSError:
            raise StorageError(
                "unable to inventory local Parquet for S3 replication"
            ) from None

    unique_files = sorted(set(files), key=lambda item: item.as_posix())
    if len(unique_files) > replication_config.maximum_files:
        raise StorageError("S3 replication input exceeds the file limit")

    objects: list[LocalObject] = []
    total_bytes = 0
    for path in unique_files:
        containing_base = next(
            (
                candidate
                for candidate in configured_bases
                if path == candidate or candidate in path.parents
            ),
            None,
        )
        if containing_base is None:
            raise StorageError("S3 replication input escaped configured storage")
        _ensure_safe_file(path, containing_base)
        try:
            size_bytes = path.stat().st_size
            relative = path.relative_to(base_dir).as_posix()
        except (OSError, ValueError):
            raise StorageError(
                "S3 replication input is outside storage base_dir"
            ) from None
        if relative.startswith("../") or relative in {"", ".", ".."}:
            raise StorageError("S3 replication relative path is unsafe")
        total_bytes += size_bytes
        if total_bytes > replication_config.maximum_bytes:
            raise StorageError("S3 replication input exceeds the byte limit")
        objects.append(
            LocalObject(
                path=path,
                relative_path=relative,
                object_key=f"{replication_config.prefix}/{relative}",
                size_bytes=size_bytes,
                sha256=_sha256(path),
            )
        )
    return tuple(objects)


def _manifest_fingerprint(objects: Sequence[LocalObject]) -> str:
    payload = [item.evidence() for item in objects]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _bucket(value: str) -> str:
    parsed = _required_text(value, "S3 bucket", maximum=63)
    if not S3_BUCKET_PATTERN.fullmatch(parsed) or ".." in parsed:
        raise ValidationError("S3 bucket name is invalid")
    return parsed


def _missing_object(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return False
    error = response.get("Error")
    if not isinstance(error, Mapping):
        return False
    return str(error.get("Code")) in {"404", "NoSuchKey", "NotFound"}


def _head(client: S3Client, bucket: str, item: LocalObject) -> Mapping[str, Any] | None:
    try:
        return client.head_object(Bucket=bucket, Key=item.object_key)
    except Exception as exc:
        if _missing_object(exc):
            return None
        raise StorageError("unable to inspect durable S3 object") from None


def _matches_remote(head: Mapping[str, Any], item: LocalObject) -> bool:
    metadata = head.get("Metadata")
    if not isinstance(metadata, Mapping):
        return False
    try:
        content_length = int(head.get("ContentLength"))
    except (TypeError, ValueError):
        return False
    return (
        content_length == item.size_bytes
        and str(metadata.get("sha256")) == item.sha256
        and str(metadata.get("relative-path")) == item.relative_path
    )


def _build_client(region: str) -> S3Client:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "S3 execution requires the optional dependency. "
            "Install with `python -m pip install -e '.[s3]'`."
        ) from exc
    return boto3.client("s3", region_name=region)


def replicate_local_parquet_to_s3(
    *,
    replication_config_path: Path,
    storage_config_path: Path,
    execute: bool = False,
    environment: Mapping[str, str] | None = None,
    client: S3Client | None = None,
) -> dict[str, Any]:
    config = load_s3_replication_config(replication_config_path)
    objects = inventory_local_parquet(
        storage_config_path=storage_config_path,
        replication_config=config,
    )
    total_bytes = sum(item.size_bytes for item in objects)
    manifest_fingerprint = _manifest_fingerprint(objects)
    selected_environment = environment or os.environ
    bucket_value = selected_environment.get(config.bucket_env)
    region_value = selected_environment.get(config.region_env)
    bucket_fingerprint = (
        hashlib.sha256(bucket_value.encode("utf-8")).hexdigest()[:12]
        if bucket_value
        else None
    )
    summary: dict[str, Any] = {
        "run_id": str(uuid4()),
        "model_version": MODEL_VERSION,
        "config_fingerprint": config.fingerprint,
        "manifest_fingerprint": manifest_fingerprint,
        "selection": {
            "files_selected": len(objects),
            "bytes_selected": total_bytes,
            "maximum_files": config.maximum_files,
            "maximum_bytes": config.maximum_bytes,
        },
        "destination": {
            "bucket_configured": bucket_value is not None,
            "bucket_fingerprint": bucket_fingerprint,
            "prefix": config.prefix,
            "region_configured": region_value is not None,
            "server_side_encryption": config.server_side_encryption,
        },
        "plan_preview": [
            item.evidence() for item in objects[:MAX_PLAN_PREVIEW]
        ],
        "plan_preview_truncated": len(objects) > MAX_PLAN_PREVIEW,
        "execution": {
            "requested": execute,
            "performed": False,
            "uploaded": 0,
            "already_present": 0,
        },
        "bucket_created": False,
        "objects_deleted": 0,
        "infrastructure_applied": False,
    }
    if not execute or not objects:
        return summary
    if not config.enabled:
        raise ValidationError(
            "S3 replication is disabled in reviewed configuration"
        )
    if bucket_value is None:
        raise ValidationError(
            f"S3 bucket environment variable {config.bucket_env} is not set"
        )
    if region_value is None or not region_value.strip():
        raise ValidationError(
            f"AWS region environment variable {config.region_env} is not set"
        )
    bucket = _bucket(bucket_value)
    selected_client = client or _build_client(region_value.strip())

    uploaded = 0
    already_present = 0
    for item in objects:
        existing = _head(selected_client, bucket, item)
        if existing is not None:
            if _matches_remote(existing, item):
                already_present += 1
                continue
            raise StorageError(
                f"durable object conflict for key {item.object_key}; overwrite is forbidden"
            )
        try:
            with item.path.open("rb") as body:
                selected_client.put_object(
                    Bucket=bucket,
                    Key=item.object_key,
                    Body=body,
                    ContentLength=item.size_bytes,
                    ContentType="application/vnd.apache.parquet",
                    Metadata={
                        "sha256": item.sha256,
                        "relative-path": item.relative_path,
                        "model-version": MODEL_VERSION,
                    },
                    ServerSideEncryption=config.server_side_encryption,
                )
        except OSError:
            raise StorageError("unable to read local object for S3 replication") from None
        except Exception:
            raise StorageError("unable to upload durable S3 object") from None
        verified = _head(selected_client, bucket, item)
        if verified is None or not _matches_remote(verified, item):
            raise StorageError("uploaded S3 object failed metadata verification")
        uploaded += 1

    summary["execution"] = {
        "requested": True,
        "performed": True,
        "uploaded": uploaded,
        "already_present": already_present,
    }
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plan or explicitly replicate immutable local Parquet into an "
            "existing S3 bucket without overwriting conflicts."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/object_storage.yaml"),
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise StorageError("unable to write S3 replication summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = replicate_local_parquet_to_s3(
            replication_config_path=args.config,
            storage_config_path=args.storage_config,
            execute=args.execute,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "S3 replication failed: configuration, destination, or options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "S3 replication failed: local inventory, remote inspection, upload, "
            "or verification failed; conflicts were not overwritten",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print("S3 replication failed: unexpected local failure", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
