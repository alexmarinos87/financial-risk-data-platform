from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..common.exceptions import StorageError, ValidationError
from ..storage.durable_s3 import (
    DurableS3Config,
    create_boto3_s3_client,
    load_durable_s3_config,
    put_immutable_object,
)

ConfigLoader = Callable[[Path, str], DurableS3Config]
ClientFactory = Callable[..., Any]
Publisher = Callable[..., dict[str, Any]]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Explicitly publish one local artifact to disabled-by-default, "
            "content-addressed S3 storage."
        )
    )
    parser.add_argument("--store-id", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--file", required=True, type=Path)
    parser.add_argument("--content-type")
    parser.add_argument(
        "--durable-config",
        type=Path,
        default=Path("config/durable_storage.yaml"),
    )
    parser.add_argument("--enable-durable-write", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def _environment_value(
    environment: Mapping[str, str],
    name: str,
    *,
    required: bool,
) -> str | None:
    value = environment.get(name)
    if value is None or not value.strip():
        if required:
            raise ValidationError(
                f"required durable-storage environment variable '{name}' is not set"
            )
        return None
    return value.strip()


def _read_artifact(path: Path, *, maximum: int) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise StorageError("durable artifact must be a regular non-symbolic-link file")
    try:
        size = path.stat().st_size
    except OSError:
        raise StorageError("durable artifact could not be inventoried") from None
    if size <= 0:
        raise ValidationError("durable artifact must not be empty")
    if size > maximum:
        raise ValidationError("durable artifact exceeds max_object_bytes")
    try:
        payload = path.read_bytes()
    except OSError:
        raise StorageError("durable artifact could not be read") from None
    if len(payload) != size:
        raise StorageError("durable artifact changed while it was being read")
    return payload


def publish_durable_artifact(
    *,
    store_id: str,
    dataset: str,
    file_path: Path,
    durable_config_path: Path,
    enable_durable_write: bool = False,
    content_type: str | None = None,
    environment: Mapping[str, str] | None = None,
    config_loader: ConfigLoader | None = None,
    client_factory: ClientFactory | None = None,
    publisher: Publisher | None = None,
) -> dict[str, Any]:
    selected_loader = config_loader or load_durable_s3_config
    try:
        config = selected_loader(durable_config_path, store_id)
    except ValidationError:
        raise
    except Exception:
        raise ValidationError("durable-storage configuration is invalid") from None

    base = {
        "run_id": str(uuid4()),
        "store_id": config.store_id,
        "store_enabled": config.enabled,
        "explicit_enable_flag": enable_durable_write,
        "bucket_environment_variable": config.bucket_env,
        "region_environment_variable": config.region_env,
        "kms_key_environment_variable": config.kms_key_env,
        "credentials_recorded": False,
    }
    if not enable_durable_write:
        return {
            **base,
            "publication": {
                "performed": False,
                "reason": "explicit_enable_flag_required",
            },
        }
    if not config.enabled:
        return {
            **base,
            "publication": {
                "performed": False,
                "reason": "store_disabled_in_configuration",
            },
        }

    selected_environment = environment if environment is not None else os.environ
    bucket = _environment_value(
        selected_environment,
        config.bucket_env,
        required=True,
    )
    region = _environment_value(
        selected_environment,
        config.region_env,
        required=True,
    )
    kms_key_id = (
        _environment_value(
            selected_environment,
            config.kms_key_env,
            required=True,
        )
        if config.kms_key_env is not None
        else None
    )
    if bucket is None or region is None:
        raise ValidationError("durable S3 bucket or region is missing")

    payload = _read_artifact(file_path, maximum=config.max_object_bytes)
    extension = file_path.suffix.lstrip(".") or "bin"
    resolved_content_type = content_type or mimetypes.guess_type(file_path.name)[0]
    if resolved_content_type is None:
        resolved_content_type = "application/octet-stream"

    selected_factory = client_factory or create_boto3_s3_client
    client = selected_factory(region_name=region)
    selected_publisher = publisher or put_immutable_object
    result = selected_publisher(
        client,
        config=config,
        bucket=bucket,
        dataset=dataset,
        payload=payload,
        extension=extension,
        content_type=resolved_content_type,
        kms_key_id=kms_key_id,
    )
    if not isinstance(result, dict):
        raise StorageError("durable S3 publisher returned invalid evidence")
    safe_result = {
        key: value
        for key, value in result.items()
        if key not in {"bucket", "credentials", "kms_key_id", "region"}
    }
    return {
        **base,
        "publication": {
            "performed": True,
            **safe_result,
        },
    }


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
        raise StorageError("Unable to write durable publication summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = publish_durable_artifact(
            store_id=args.store_id,
            dataset=args.dataset,
            file_path=args.file,
            durable_config_path=args.durable_config,
            enable_durable_write=args.enable_durable_write,
            content_type=args.content_type,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Durable publication failed: configuration, environment, artifact, "
            "or options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Durable publication failed: bounded local read or S3 operation failed",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print("Durable publication failed: unexpected local failure", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
