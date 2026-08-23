from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..common.exceptions import StorageError, ValidationError
from ..storage.durable_s3 import (
    DurableS3Config,
    canonical_json_payload,
    content_sha256,
    create_boto3_s3_client,
    immutable_object_key,
    load_durable_s3_config,
    put_immutable_object,
)
from ..storage.storage_config import load_storage_config, validate_storage_config

MAX_REPLICATION_FILES = 10_000
MAX_REPLICATION_BYTES = 100_000_000_000
DEFAULT_MAX_REPLICATION_BYTES = 1_000_000_000
MANIFEST_DATASET = "replication-manifests"

ConfigLoader = Callable[[Path, str], DurableS3Config]
StorageConfigLoader = Callable[[Path], dict[str, Any]]
ClientFactory = Callable[..., Any]
Publisher = Callable[..., dict[str, Any]]


@dataclass(frozen=True, slots=True)
class LocalReplicationArtifact:
    dataset: str
    remote_dataset: str
    path: Path
    relative_path: str
    content_length: int
    sha256: str
    object_key: str
    extension: str
    content_type: str


def _bounded_integer(value: int, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValidationError(f"{label} must be an integer between 1 and {maximum}")
    return value


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


def _safe_remote_dataset(kind: str, dataset: str) -> str:
    candidate = f"{kind}-{dataset}"
    if (
        candidate in {"", ".", ".."}
        or "/" in candidate
        or "\\" in candidate
        or any(ord(character) < 32 for character in candidate)
    ):
        raise ValidationError("storage dataset names must form a safe remote dataset")
    return candidate


def _configured_dataset_paths(
    storage_config: dict[str, Any],
) -> tuple[tuple[str, str, Path], ...]:
    validate_storage_config(storage_config)
    storage = storage_config["storage"]
    raw = storage["raw"]
    curated = storage["curated"]

    configured: list[tuple[str, str, Path]] = [
        (
            "raw",
            str(raw["dataset"]),
            Path(raw["base_path"]) / str(raw["dataset"]),
        )
    ]
    datasets = curated["datasets"]
    if not isinstance(datasets, Mapping):
        raise StorageError("curated datasets configuration is invalid")
    for dataset_key in sorted(datasets):
        dataset_name = datasets[dataset_key]
        if not isinstance(dataset_key, str) or not isinstance(dataset_name, str):
            raise StorageError("curated datasets configuration is invalid")
        configured.append(
            (
                "curated",
                dataset_key,
                Path(curated["base_path"]) / dataset_name,
            )
        )
    return tuple(configured)


def _read_regular_file(path: Path, *, expected_size: int) -> bytes:
    try:
        payload = path.read_bytes()
    except OSError:
        raise StorageError("replication artifact could not be read") from None
    if len(payload) != expected_size:
        raise StorageError("replication artifact changed while it was being read")
    return payload


def inventory_local_replication_artifacts(
    *,
    storage_config: dict[str, Any],
    durable_config: DurableS3Config,
    selected_datasets: Sequence[str] | None = None,
    max_files: int | None = None,
    max_total_bytes: int = DEFAULT_MAX_REPLICATION_BYTES,
) -> tuple[LocalReplicationArtifact, ...]:
    requested_max_files = (
        durable_config.max_list_objects if max_files is None else max_files
    )
    requested_max_files = _bounded_integer(
        requested_max_files,
        "max_files",
        min(MAX_REPLICATION_FILES, durable_config.max_list_objects),
    )
    max_total_bytes = _bounded_integer(
        max_total_bytes,
        "max_total_bytes",
        MAX_REPLICATION_BYTES,
    )

    selected = set(selected_datasets or ())
    configured = _configured_dataset_paths(storage_config)
    configured_names = {dataset for _, dataset, _ in configured}
    unknown = sorted(selected - configured_names)
    if unknown:
        raise ValidationError(
            "selected datasets are not configured: " + ", ".join(unknown)
        )

    artifacts: list[LocalReplicationArtifact] = []
    total_bytes = 0
    for kind, dataset, dataset_path in configured:
        if selected and dataset not in selected:
            continue
        base_path = dataset_path.parent
        if base_path.is_symlink() or dataset_path.is_symlink():
            raise StorageError("replication dataset path must not be a symbolic link")
        if not dataset_path.exists():
            continue
        if not dataset_path.is_dir():
            raise StorageError("replication dataset path must be a directory")
        try:
            files = sorted(dataset_path.rglob("*.parquet"))
        except OSError:
            raise StorageError("replication dataset could not be inventoried") from None

        remote_dataset = _safe_remote_dataset(kind, dataset)
        for path in files:
            if path.is_symlink() or not path.is_file():
                raise StorageError(
                    "replication dataset contains an unsafe local file type"
                )
            try:
                size = path.stat().st_size
            except OSError:
                raise StorageError(
                    "replication artifact could not be inventoried"
                ) from None
            if size <= 0:
                raise ValidationError("replication artifacts must not be empty")
            if size > durable_config.max_object_bytes:
                raise ValidationError(
                    "replication artifact exceeds durable max_object_bytes"
                )
            total_bytes += size
            if total_bytes > max_total_bytes:
                raise ValidationError("replication plan exceeds max_total_bytes")
            if len(artifacts) >= requested_max_files:
                raise ValidationError("replication plan exceeds max_files")

            payload = _read_regular_file(path, expected_size=size)
            digest = content_sha256(payload)
            extension = path.suffix.lstrip(".") or "bin"
            object_key = immutable_object_key(
                durable_config,
                dataset=remote_dataset,
                payload=payload,
                extension=extension,
            )
            relative_path = path.relative_to(dataset_path).as_posix()
            content_type = (
                "application/vnd.apache.parquet"
                if extension == "parquet"
                else mimetypes.guess_type(path.name)[0]
                or "application/octet-stream"
            )
            artifacts.append(
                LocalReplicationArtifact(
                    dataset=dataset,
                    remote_dataset=remote_dataset,
                    path=path,
                    relative_path=relative_path,
                    content_length=size,
                    sha256=digest,
                    object_key=object_key,
                    extension=extension,
                    content_type=content_type,
                )
            )

    return tuple(
        sorted(
            artifacts,
            key=lambda artifact: (
                artifact.remote_dataset,
                artifact.relative_path,
                artifact.sha256,
            ),
        )
    )


def _manifest_document(
    artifacts: Sequence[LocalReplicationArtifact],
    *,
    store_id: str,
) -> dict[str, Any]:
    entries = [
        {
            "content_length": artifact.content_length,
            "dataset": artifact.dataset,
            "object_key": artifact.object_key,
            "relative_path": artifact.relative_path,
            "remote_dataset": artifact.remote_dataset,
            "sha256": artifact.sha256,
        }
        for artifact in artifacts
    ]
    identity_payload = {
        "contract": "durable-dataset-replication-v1",
        "entries": entries,
        "store_id": store_id,
    }
    return {
        **identity_payload,
        "manifest_id": (
            "durable-replication-"
            + content_sha256(canonical_json_payload(identity_payload))[:24]
        ),
    }


def _safe_publication_evidence(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in result.items()
        if key not in {"bucket", "credentials", "kms_key_id", "region"}
    }


def replicate_local_datasets(
    *,
    store_id: str,
    storage_config_path: Path,
    durable_config_path: Path,
    selected_datasets: Sequence[str] | None = None,
    max_files: int | None = None,
    max_total_bytes: int = DEFAULT_MAX_REPLICATION_BYTES,
    enable_durable_write: bool = False,
    environment: Mapping[str, str] | None = None,
    config_loader: ConfigLoader | None = None,
    storage_config_loader: StorageConfigLoader | None = None,
    client_factory: ClientFactory | None = None,
    publisher: Publisher | None = None,
) -> dict[str, Any]:
    selected_config_loader = config_loader or load_durable_s3_config
    try:
        durable_config = selected_config_loader(durable_config_path, store_id)
    except ValidationError:
        raise
    except Exception:
        raise ValidationError("durable-storage configuration is invalid") from None

    selected_storage_loader = storage_config_loader or load_storage_config
    try:
        storage_config = selected_storage_loader(storage_config_path)
    except Exception:
        raise StorageError("local storage configuration is invalid") from None
    if not isinstance(storage_config, dict):
        raise StorageError("local storage configuration is invalid")

    artifacts = inventory_local_replication_artifacts(
        storage_config=storage_config,
        durable_config=durable_config,
        selected_datasets=selected_datasets,
        max_files=max_files,
        max_total_bytes=max_total_bytes,
    )
    manifest = _manifest_document(artifacts, store_id=durable_config.store_id)
    manifest_payload = canonical_json_payload(manifest)
    manifest_key = immutable_object_key(
        durable_config,
        dataset=MANIFEST_DATASET,
        payload=manifest_payload,
        extension="json",
    )
    base = {
        "run_id": str(uuid4()),
        "store_id": durable_config.store_id,
        "store_enabled": durable_config.enabled,
        "explicit_enable_flag": enable_durable_write,
        "bucket_environment_variable": durable_config.bucket_env,
        "region_environment_variable": durable_config.region_env,
        "kms_key_environment_variable": durable_config.kms_key_env,
        "credentials_recorded": False,
        "plan": {
            "artifact_count": len(artifacts),
            "dataset_count": len({artifact.remote_dataset for artifact in artifacts}),
            "manifest_id": manifest["manifest_id"],
            "manifest_object_key": manifest_key,
            "total_bytes": sum(artifact.content_length for artifact in artifacts),
            "artifacts": [
                {
                    "content_length": artifact.content_length,
                    "dataset": artifact.dataset,
                    "object_key": artifact.object_key,
                    "relative_path": artifact.relative_path,
                    "remote_dataset": artifact.remote_dataset,
                    "sha256": artifact.sha256,
                }
                for artifact in artifacts
            ],
        },
    }

    if not artifacts:
        return {
            **base,
            "replication": {
                "performed": False,
                "reason": "no_artifacts",
            },
        }
    if not enable_durable_write:
        return {
            **base,
            "replication": {
                "performed": False,
                "reason": "explicit_enable_flag_required",
            },
        }
    if not durable_config.enabled:
        return {
            **base,
            "replication": {
                "performed": False,
                "reason": "store_disabled_in_configuration",
            },
        }

    selected_environment = environment if environment is not None else os.environ
    bucket = _environment_value(
        selected_environment,
        durable_config.bucket_env,
        required=True,
    )
    region = _environment_value(
        selected_environment,
        durable_config.region_env,
        required=True,
    )
    kms_key_id = (
        _environment_value(
            selected_environment,
            durable_config.kms_key_env,
            required=True,
        )
        if durable_config.kms_key_env is not None
        else None
    )
    if bucket is None or region is None:
        raise ValidationError("durable S3 bucket or region is missing")

    selected_factory = client_factory or create_boto3_s3_client
    client = selected_factory(region_name=region)
    selected_publisher = publisher or put_immutable_object
    results: list[dict[str, Any]] = []
    for artifact in artifacts:
        payload = _read_regular_file(
            artifact.path,
            expected_size=artifact.content_length,
        )
        if content_sha256(payload) != artifact.sha256:
            raise StorageError("replication artifact changed after the plan was created")
        result = selected_publisher(
            client,
            config=durable_config,
            bucket=bucket,
            dataset=artifact.remote_dataset,
            payload=payload,
            extension=artifact.extension,
            content_type=artifact.content_type,
            kms_key_id=kms_key_id,
        )
        if not isinstance(result, Mapping):
            raise StorageError("durable S3 publisher returned invalid evidence")
        safe_result = _safe_publication_evidence(result)
        if safe_result.get("status") not in {"written", "already_present"}:
            raise StorageError("durable S3 publisher returned invalid status")
        results.append(
            {
                "dataset": artifact.dataset,
                "relative_path": artifact.relative_path,
                "remote_dataset": artifact.remote_dataset,
                **safe_result,
            }
        )

    manifest_result = selected_publisher(
        client,
        config=durable_config,
        bucket=bucket,
        dataset=MANIFEST_DATASET,
        payload=manifest_payload,
        extension="json",
        content_type="application/json",
        kms_key_id=kms_key_id,
    )
    if not isinstance(manifest_result, Mapping):
        raise StorageError("durable S3 publisher returned invalid manifest evidence")
    safe_manifest_result = _safe_publication_evidence(manifest_result)
    if safe_manifest_result.get("status") not in {"written", "already_present"}:
        raise StorageError("durable S3 publisher returned invalid manifest status")

    written = sum(result["status"] == "written" for result in results)
    already_present = sum(
        result["status"] == "already_present" for result in results
    )
    return {
        **base,
        "replication": {
            "performed": True,
            "artifacts_written": written,
            "artifacts_already_present": already_present,
            "artifact_results": results,
            "manifest": {
                "manifest_id": manifest["manifest_id"],
                **safe_manifest_result,
            },
        },
    }


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plan or explicitly replicate configured local Parquet datasets "
            "through the immutable durable S3 adapter."
        )
    )
    parser.add_argument("--store-id", default="primary-s3")
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument(
        "--durable-config",
        type=Path,
        default=Path("config/durable_storage.yaml"),
    )
    parser.add_argument(
        "--dataset",
        action="append",
        dest="datasets",
        help="Configured raw dataset name or curated dataset key; repeatable.",
    )
    parser.add_argument("--max-files", type=_positive_integer)
    parser.add_argument(
        "--max-total-bytes",
        type=_positive_integer,
        default=DEFAULT_MAX_REPLICATION_BYTES,
    )
    parser.add_argument("--enable-durable-write", action="store_true")
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
        raise StorageError("Unable to write durable replication summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = replicate_local_datasets(
            store_id=args.store_id,
            storage_config_path=args.storage_config,
            durable_config_path=args.durable_config,
            selected_datasets=args.datasets,
            max_files=args.max_files,
            max_total_bytes=args.max_total_bytes,
            enable_durable_write=args.enable_durable_write,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Durable replication failed: configuration, environment, "
            "selection, or bounds were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Durable replication failed: bounded local inventory or "
            "S3 operation failed",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Durable replication failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
