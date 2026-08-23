from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any

from ..common.exceptions import StorageError, ValidationError
from ..storage.durable_s3 import (
    DurableS3Config,
    canonical_json_payload,
    content_sha256,
    create_boto3_s3_client,
    immutable_object_key,
    load_durable_s3_config,
)
from ..storage.storage_config import load_storage_config, validate_storage_config
from .replicate_durable_datasets import MANIFEST_DATASET

RESTORE_CONTRACT = "durable-dataset-restore-v1"
MANIFEST_CONTRACT = "durable-dataset-replication-v1"
IMMUTABLE_STORAGE_CONTRACT = "immutable-content-addressed-v1"
MAX_RESTORE_FILES = 10_000
MAX_RESTORE_BYTES = 100_000_000_000
DEFAULT_MAX_RESTORE_BYTES = 1_000_000_000
MAX_REPLICATION_SUMMARY_BYTES = 16_000_000
MAX_MANIFEST_BYTES = 10_000_000
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
MANIFEST_ID_PATTERN = re.compile(r"^durable-replication-[0-9a-f]{24}$")

ConfigLoader = Callable[[Path, str], DurableS3Config]
StorageConfigLoader = Callable[[Path], dict[str, Any]]
SummaryLoader = Callable[[Path], Mapping[str, Any]]
ClientFactory = Callable[..., Any]


class _DuplicateKeyError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class DurableRestoreArtifact:
    dataset: str
    remote_dataset: str
    relative_path: str
    content_length: int
    sha256: str
    object_key: str
    target_root: Path
    target_path: Path
    local_status: str


@dataclass(frozen=True, slots=True)
class DurableRestorePlan:
    store_id: str
    manifest_id: str
    manifest_object_key: str
    manifest_document: Mapping[str, Any]
    manifest_payload: bytes
    restore_plan_id: str
    artifacts: tuple[DurableRestoreArtifact, ...]
    total_bytes: int
    replication_execution_confirmed: bool


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError
        result[key] = value
    return result


def _reject_nonstandard_constant(_value: str) -> None:
    raise ValueError


def _required_text(value: Any, label: str, maximum: int = 1024) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be text")
    parsed = value.strip()
    if not parsed or len(parsed) > maximum:
        raise ValidationError(
            f"{label} must contain between 1 and {maximum} characters"
        )
    if any(ord(character) < 32 or ord(character) == 127 for character in parsed):
        raise ValidationError(f"{label} must not contain control characters")
    return parsed


def _bounded_integer(value: Any, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between 1 and {maximum}"
        )
    return value


def _sha256(value: Any, label: str) -> str:
    parsed = _required_text(value, label, 64)
    if SHA256_PATTERN.fullmatch(parsed) is None:
        raise ValidationError(f"{label} must be a lowercase SHA-256 digest")
    return parsed


def _safe_segment(value: Any, label: str) -> str:
    parsed = _required_text(value, label, 256)
    if (
        parsed in {".", ".."}
        or "/" in parsed
        or "\\" in parsed
        or any(ord(character) < 32 for character in parsed)
    ):
        raise ValidationError(f"{label} must be one safe path segment")
    return parsed


def _safe_relative_path(value: Any) -> tuple[str, tuple[str, ...]]:
    parsed = _required_text(value, "relative_path", 2048)
    if parsed.startswith("/") or "\\" in parsed or "//" in parsed:
        raise ValidationError("relative_path must be a safe relative POSIX path")
    pure = PurePosixPath(parsed)
    parts = pure.parts
    if (
        not parts
        or len(parts) > 64
        or any(part in {"", ".", ".."} for part in parts)
        or pure.is_absolute()
        or pure.as_posix() != parsed
    ):
        raise ValidationError("relative_path must be a safe relative POSIX path")
    if not parts[-1].endswith(".parquet"):
        raise ValidationError("restore artifacts must use the parquet extension")
    return parsed, tuple(parts)


def _load_replication_summary(path: Path) -> Mapping[str, Any]:
    try:
        if path.is_symlink() or not path.is_file():
            raise StorageError(
                "replication summary must be a regular non-symlink file"
            )
        if path.stat().st_size > MAX_REPLICATION_SUMMARY_BYTES:
            raise ValidationError("replication summary exceeds the size limit")
        payload = path.read_bytes()
    except (ValidationError, StorageError):
        raise
    except OSError:
        raise StorageError("replication summary could not be read") from None
    if len(payload) > MAX_REPLICATION_SUMMARY_BYTES:
        raise ValidationError("replication summary exceeds the size limit")
    try:
        parsed = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonstandard_constant,
        )
    except (
        UnicodeError,
        json.JSONDecodeError,
        _DuplicateKeyError,
        TypeError,
        ValueError,
    ):
        raise ValidationError("replication summary is not strict JSON") from None
    if not isinstance(parsed, Mapping):
        raise ValidationError("replication summary must be a JSON object")
    return parsed


def _environment_value(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name)
    if value is None or not value.strip():
        raise ValidationError(
            f"required durable-storage environment variable '{name}' is not set"
        )
    return value.strip()


def _configured_target_roots(
    storage_config: dict[str, Any],
) -> Mapping[tuple[str, str], Path]:
    validate_storage_config(storage_config)
    storage = storage_config["storage"]
    raw = storage["raw"]
    curated = storage["curated"]

    raw_dataset = _safe_segment(raw.get("dataset"), "raw dataset")
    roots: dict[tuple[str, str], Path] = {
        (
            raw_dataset,
            f"raw-{raw_dataset}",
        ): Path(raw["base_path"]) / raw_dataset
    }
    datasets = curated.get("datasets")
    if not isinstance(datasets, Mapping):
        raise StorageError("curated datasets configuration is invalid")
    for raw_key in sorted(datasets):
        dataset_key = _safe_segment(raw_key, "curated dataset key")
        dataset_name = _safe_segment(
            datasets[raw_key],
            f"curated dataset path for {dataset_key}",
        )
        pair = (dataset_key, f"curated-{dataset_key}")
        if pair in roots:
            raise StorageError("storage configuration contains duplicate datasets")
        roots[pair] = Path(curated["base_path"]) / dataset_name
    return roots


def _assert_safe_local_target(root: Path, target: Path) -> None:
    try:
        target.relative_to(root)
    except ValueError:
        raise StorageError("restore target escaped its configured dataset") from None

    base = root.parent
    for candidate in (base, root):
        if candidate.is_symlink():
            raise StorageError("restore target path must not contain symbolic links")
        if candidate.exists() and not candidate.is_dir():
            raise StorageError("restore dataset path must be a directory")

    relative = target.relative_to(root)
    cursor = root
    for segment in relative.parts[:-1]:
        cursor = cursor / segment
        if cursor.is_symlink():
            raise StorageError("restore target path must not contain symbolic links")
        if cursor.exists() and not cursor.is_dir():
            raise StorageError("restore target parent must be a directory")
    if target.is_symlink():
        raise StorageError("restore target must not be a symbolic link")
    if target.exists() and not target.is_file():
        raise StorageError("restore target must be a regular file")


def _read_local_file(path: Path, expected_size: int) -> bytes:
    try:
        size = path.stat().st_size
        if size != expected_size:
            raise ValidationError("local restore target has conflicting content")
        payload = path.read_bytes()
    except ValidationError:
        raise
    except OSError:
        raise StorageError("local restore target could not be read") from None
    if len(payload) != expected_size:
        raise StorageError("local restore target changed while it was read")
    return payload


def _local_status(
    *,
    target_root: Path,
    target_path: Path,
    expected_size: int,
    expected_sha256: str,
) -> str:
    _assert_safe_local_target(target_root, target_path)
    if not target_path.exists():
        return "missing"
    try:
        payload = _read_local_file(target_path, expected_size)
    except ValidationError:
        return "conflict"
    if content_sha256(payload) != expected_sha256:
        return "conflict"
    return "already_present"


def _expected_artifact_key(
    config: DurableS3Config,
    *,
    remote_dataset: str,
    digest: str,
) -> str:
    dataset = _safe_segment(remote_dataset, "remote_dataset")
    return (
        f"{config.prefix}/{dataset}/sha256={digest[:2]}/"
        f"{digest}.parquet"
    )


def _artifact_from_entry(
    entry: Mapping[str, Any],
    *,
    config: DurableS3Config,
    roots: Mapping[tuple[str, str], Path],
) -> DurableRestoreArtifact:
    expected_keys = {
        "content_length",
        "dataset",
        "object_key",
        "relative_path",
        "remote_dataset",
        "sha256",
    }
    if set(entry) != expected_keys:
        raise ValidationError("replication manifest artifact fields are not exact")

    dataset = _safe_segment(entry.get("dataset"), "dataset")
    remote_dataset = _safe_segment(
        entry.get("remote_dataset"),
        "remote_dataset",
    )
    relative_path, parts = _safe_relative_path(entry.get("relative_path"))
    content_length = _bounded_integer(
        entry.get("content_length"),
        "content_length",
        config.max_object_bytes,
    )
    digest = _sha256(entry.get("sha256"), "sha256")
    object_key = _required_text(entry.get("object_key"), "object_key", 2048)
    if object_key != _expected_artifact_key(
        config,
        remote_dataset=remote_dataset,
        digest=digest,
    ):
        raise ValidationError(
            "artifact object key does not match its immutable content identity"
        )

    target_root = roots.get((dataset, remote_dataset))
    if target_root is None:
        raise ValidationError(
            "replication manifest references an unconfigured local dataset"
        )
    target_path = target_root.joinpath(*parts)
    status = _local_status(
        target_root=target_root,
        target_path=target_path,
        expected_size=content_length,
        expected_sha256=digest,
    )
    return DurableRestoreArtifact(
        dataset=dataset,
        remote_dataset=remote_dataset,
        relative_path=relative_path,
        content_length=content_length,
        sha256=digest,
        object_key=object_key,
        target_root=target_root,
        target_path=target_path,
        local_status=status,
    )


def _manifest_id(identity: Mapping[str, Any]) -> str:
    return (
        "durable-replication-"
        + content_sha256(canonical_json_payload(identity))[:24]
    )


def _replication_execution_confirmed(
    summary: Mapping[str, Any],
    *,
    artifacts: Sequence[DurableRestoreArtifact],
    manifest_id: str,
    manifest_key: str,
    manifest_payload: bytes,
) -> bool:
    replication = summary.get("replication")
    if not isinstance(replication, Mapping) or replication.get("performed") is not True:
        return False

    results = replication.get("artifact_results")
    if not isinstance(results, list) or len(results) != len(artifacts):
        return False
    expected_results = [
        (
            artifact.dataset,
            artifact.remote_dataset,
            artifact.relative_path,
            artifact.object_key,
            artifact.sha256,
            artifact.content_length,
        )
        for artifact in artifacts
    ]
    actual_results: list[tuple[Any, ...]] = []
    for result in results:
        if not isinstance(result, Mapping):
            return False
        if result.get("status") not in {"written", "already_present"}:
            return False
        actual_results.append(
            (
                result.get("dataset"),
                result.get("remote_dataset"),
                result.get("relative_path"),
                result.get("key"),
                result.get("sha256"),
                result.get("content_length"),
            )
        )
    if actual_results != expected_results:
        return False

    written = replication.get("artifacts_written")
    already = replication.get("artifacts_already_present")
    if (
        type(written) is not int
        or type(already) is not int
        or written < 0
        or already < 0
        or written + already != len(artifacts)
    ):
        return False

    manifest = replication.get("manifest")
    if not isinstance(manifest, Mapping):
        return False
    return (
        manifest.get("manifest_id") == manifest_id
        and manifest.get("key") == manifest_key
        and manifest.get("sha256") == content_sha256(manifest_payload)
        and manifest.get("content_length") == len(manifest_payload)
        and manifest.get("status") in {"written", "already_present"}
    )


def _build_restore_plan(
    *,
    summary: Mapping[str, Any],
    config: DurableS3Config,
    storage_config: dict[str, Any],
    max_total_bytes: int,
) -> DurableRestorePlan:
    store_id = _required_text(summary.get("store_id"), "store_id", 128)
    if store_id != config.store_id:
        raise ValidationError("replication summary store_id does not match the request")

    plan = summary.get("plan")
    if not isinstance(plan, Mapping):
        raise ValidationError("replication summary is missing the plan")
    raw_artifacts = plan.get("artifacts")
    if not isinstance(raw_artifacts, list):
        raise ValidationError("replication plan artifacts must be a JSON array")
    maximum_files = min(MAX_RESTORE_FILES, config.max_list_objects)
    if len(raw_artifacts) > maximum_files:
        raise ValidationError("restore plan exceeds the configured artifact limit")

    roots = _configured_target_roots(storage_config)
    artifacts: list[DurableRestoreArtifact] = []
    total_bytes = 0
    seen_targets: set[Path] = set()
    seen_keys: set[str] = set()
    for raw_entry in raw_artifacts:
        if not isinstance(raw_entry, Mapping):
            raise ValidationError("replication plan contains an invalid artifact")
        artifact = _artifact_from_entry(
            raw_entry,
            config=config,
            roots=roots,
        )
        total_bytes += artifact.content_length
        if total_bytes > max_total_bytes:
            raise ValidationError("restore plan exceeds max_total_bytes")
        if artifact.target_path in seen_targets:
            raise ValidationError("restore plan contains duplicate local targets")
        if artifact.object_key in seen_keys:
            raise ValidationError("restore plan contains duplicate object keys")
        seen_targets.add(artifact.target_path)
        seen_keys.add(artifact.object_key)
        artifacts.append(artifact)

    ordered = sorted(
        artifacts,
        key=lambda artifact: (
            artifact.remote_dataset,
            artifact.relative_path,
            artifact.sha256,
        ),
    )
    if artifacts != ordered:
        raise ValidationError("replication plan artifacts are not deterministically ordered")

    if plan.get("artifact_count") != len(artifacts):
        raise ValidationError("replication plan artifact_count is inconsistent")
    if plan.get("dataset_count") != len(
        {artifact.remote_dataset for artifact in artifacts}
    ):
        raise ValidationError("replication plan dataset_count is inconsistent")
    if plan.get("total_bytes") != total_bytes:
        raise ValidationError("replication plan total_bytes is inconsistent")

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
    identity: dict[str, Any] = {
        "contract": MANIFEST_CONTRACT,
        "entries": entries,
        "store_id": store_id,
    }
    manifest_id = _manifest_id(identity)
    if MANIFEST_ID_PATTERN.fullmatch(manifest_id) is None:
        raise StorageError("computed replication manifest ID is invalid")
    manifest_document: dict[str, Any] = {
        **identity,
        "manifest_id": manifest_id,
    }
    manifest_payload = canonical_json_payload(manifest_document)
    if len(manifest_payload) > min(MAX_MANIFEST_BYTES, config.max_object_bytes):
        raise ValidationError("replication manifest exceeds the restore size limit")
    manifest_key = immutable_object_key(
        config,
        dataset=MANIFEST_DATASET,
        payload=manifest_payload,
        extension="json",
    )
    if plan.get("manifest_id") != manifest_id:
        raise ValidationError("replication plan manifest_id is inconsistent")
    if plan.get("manifest_object_key") != manifest_key:
        raise ValidationError("replication plan manifest object key is inconsistent")

    restore_identity = {
        "artifacts": [
            {
                "dataset": artifact.dataset,
                "relative_path": artifact.relative_path,
                "sha256": artifact.sha256,
            }
            for artifact in artifacts
        ],
        "contract": RESTORE_CONTRACT,
        "manifest_id": manifest_id,
        "store_id": store_id,
    }
    restore_plan_id = (
        "durable-restore-"
        + content_sha256(canonical_json_payload(restore_identity))[:24]
    )
    confirmed = _replication_execution_confirmed(
        summary,
        artifacts=artifacts,
        manifest_id=manifest_id,
        manifest_key=manifest_key,
        manifest_payload=manifest_payload,
    )
    return DurableRestorePlan(
        store_id=store_id,
        manifest_id=manifest_id,
        manifest_object_key=manifest_key,
        manifest_document=manifest_document,
        manifest_payload=manifest_payload,
        restore_plan_id=restore_plan_id,
        artifacts=tuple(artifacts),
        total_bytes=total_bytes,
        replication_execution_confirmed=confirmed,
    )


def _head_verified_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    expected_sha256: str,
    expected_length: int,
) -> None:
    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except Exception:
        raise StorageError("Unable to verify a durable restore object") from None
    if not isinstance(response, Mapping):
        raise StorageError("Durable restore object metadata is invalid")
    metadata = response.get("Metadata")
    if not isinstance(metadata, Mapping):
        raise StorageError("Durable restore object metadata is missing")
    if metadata.get("sha256") != expected_sha256:
        raise StorageError("Durable restore object digest metadata does not match")
    if metadata.get("storage-contract") != IMMUTABLE_STORAGE_CONTRACT:
        raise StorageError("Durable restore object storage contract does not match")
    if response.get("ContentLength") != expected_length:
        raise StorageError("Durable restore object content length does not match")


def _read_verified_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    expected_sha256: str,
    expected_length: int,
) -> bytes:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except Exception:
        raise StorageError("Durable restore object read failed") from None
    if not isinstance(response, Mapping):
        raise StorageError("Durable restore object response is invalid")
    response_length = response.get("ContentLength")
    if response_length is not None and response_length != expected_length:
        raise StorageError("Durable restore object read length does not match")
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise StorageError("Durable restore object body is invalid")
    try:
        payload = body.read(expected_length + 1)
    except Exception:
        raise StorageError("Durable restore object body could not be read") from None
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
    if not isinstance(payload, bytes):
        raise StorageError("Durable restore object body did not return bytes")
    if len(payload) != expected_length:
        raise StorageError("Durable restore object payload length does not match")
    if content_sha256(payload) != expected_sha256:
        raise StorageError("Durable restore object payload digest does not match")
    return payload


def _atomic_publish_no_overwrite(
    artifact: DurableRestoreArtifact,
    payload: bytes,
) -> str:
    current = _local_status(
        target_root=artifact.target_root,
        target_path=artifact.target_path,
        expected_size=artifact.content_length,
        expected_sha256=artifact.sha256,
    )
    if current == "already_present":
        return current
    if current == "conflict":
        raise ValidationError("local restore target contains conflicting content")

    try:
        artifact.target_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        raise StorageError("restore target directory could not be created") from None
    _assert_safe_local_target(artifact.target_root, artifact.target_path)

    descriptor: int | None = None
    temporary_path: Path | None = None
    try:
        descriptor, raw_temporary = tempfile.mkstemp(
            prefix=".durable-restore-",
            suffix=".tmp",
            dir=artifact.target_path.parent,
        )
        temporary_path = Path(raw_temporary)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if temporary_path.stat().st_size != artifact.content_length:
            raise StorageError("temporary restore file length is inconsistent")
        try:
            os.link(temporary_path, artifact.target_path)
        except FileExistsError:
            raced = _local_status(
                target_root=artifact.target_root,
                target_path=artifact.target_path,
                expected_size=artifact.content_length,
                expected_sha256=artifact.sha256,
            )
            if raced == "already_present":
                return raced
            raise ValidationError(
                "local restore target appeared with conflicting content"
            ) from None
        except OSError:
            raise StorageError("restore target could not be published atomically") from None
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass

    verified = _local_status(
        target_root=artifact.target_root,
        target_path=artifact.target_path,
        expected_size=artifact.content_length,
        expected_sha256=artifact.sha256,
    )
    if verified != "already_present":
        raise StorageError("published restore target did not verify")
    return "restored"


def _artifact_evidence(artifact: DurableRestoreArtifact) -> dict[str, Any]:
    return {
        "content_length": artifact.content_length,
        "dataset": artifact.dataset,
        "local_status": artifact.local_status,
        "object_key": artifact.object_key,
        "relative_path": artifact.relative_path,
        "remote_dataset": artifact.remote_dataset,
        "sha256": artifact.sha256,
    }


def _base_summary(
    plan: DurableRestorePlan,
    *,
    config: DurableS3Config,
    enable_durable_read: bool,
) -> dict[str, Any]:
    statuses = {
        status: sum(
            artifact.local_status == status for artifact in plan.artifacts
        )
        for status in ("missing", "already_present", "conflict")
    }
    return {
        "contract": RESTORE_CONTRACT,
        "credentials_recorded": False,
        "explicit_enable_flag": enable_durable_read,
        "manifest_id": plan.manifest_id,
        "plan": {
            "artifact_count": len(plan.artifacts),
            "artifacts": [
                _artifact_evidence(artifact) for artifact in plan.artifacts
            ],
            "local_status_counts": statuses,
            "manifest_content_length": len(plan.manifest_payload),
            "manifest_object_key": plan.manifest_object_key,
            "remote_objects_to_verify": len(plan.artifacts) + 1,
            "replication_execution_confirmed": (
                plan.replication_execution_confirmed
            ),
            "total_bytes": plan.total_bytes,
        },
        "restore_plan_id": plan.restore_plan_id,
        "store_enabled": config.enabled,
        "store_id": plan.store_id,
    }


def restore_durable_replication(
    *,
    replication_summary_path: Path,
    store_id: str,
    storage_config_path: Path,
    durable_config_path: Path,
    max_total_bytes: int = DEFAULT_MAX_RESTORE_BYTES,
    enable_durable_read: bool = False,
    environment: Mapping[str, str] | None = None,
    config_loader: ConfigLoader | None = None,
    storage_config_loader: StorageConfigLoader | None = None,
    summary_loader: SummaryLoader | None = None,
    client_factory: ClientFactory | None = None,
) -> dict[str, Any]:
    max_total_bytes = _bounded_integer(
        max_total_bytes,
        "max_total_bytes",
        MAX_RESTORE_BYTES,
    )
    selected_config_loader = config_loader or load_durable_s3_config
    try:
        config = selected_config_loader(durable_config_path, store_id)
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

    selected_summary_loader = summary_loader or _load_replication_summary
    try:
        summary = selected_summary_loader(replication_summary_path)
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise ValidationError("replication summary is invalid") from None
    if not isinstance(summary, Mapping):
        raise ValidationError("replication summary must be a mapping")

    plan = _build_restore_plan(
        summary=summary,
        config=config,
        storage_config=storage_config,
        max_total_bytes=max_total_bytes,
    )
    base = _base_summary(
        plan,
        config=config,
        enable_durable_read=enable_durable_read,
    )
    if not plan.artifacts:
        return {
            **base,
            "restore": {"performed": False, "reason": "no_artifacts"},
        }
    if not enable_durable_read:
        return {
            **base,
            "restore": {
                "performed": False,
                "reason": "explicit_enable_flag_required",
            },
        }
    if not config.enabled:
        return {
            **base,
            "restore": {
                "performed": False,
                "reason": "store_disabled_in_configuration",
            },
        }
    if any(artifact.local_status == "conflict" for artifact in plan.artifacts):
        raise ValidationError("restore plan contains conflicting local targets")
    if not plan.replication_execution_confirmed:
        raise ValidationError(
            "restore execution requires a completed replication result summary"
        )

    selected_environment = environment if environment is not None else os.environ
    bucket = _environment_value(selected_environment, config.bucket_env)
    region = _environment_value(selected_environment, config.region_env)
    selected_factory = client_factory or create_boto3_s3_client
    client = selected_factory(region_name=region)

    manifest_digest = content_sha256(plan.manifest_payload)
    _head_verified_object(
        client,
        bucket=bucket,
        key=plan.manifest_object_key,
        expected_sha256=manifest_digest,
        expected_length=len(plan.manifest_payload),
    )
    remote_manifest_payload = _read_verified_object(
        client,
        bucket=bucket,
        key=plan.manifest_object_key,
        expected_sha256=manifest_digest,
        expected_length=len(plan.manifest_payload),
    )
    if remote_manifest_payload != plan.manifest_payload:
        raise StorageError("remote replication manifest bytes do not match the plan")
    try:
        remote_manifest = json.loads(
            remote_manifest_payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonstandard_constant,
        )
    except Exception:
        raise StorageError("remote replication manifest is invalid") from None
    if remote_manifest != plan.manifest_document:
        raise StorageError("remote replication manifest contract does not match")

    for artifact in plan.artifacts:
        _head_verified_object(
            client,
            bucket=bucket,
            key=artifact.object_key,
            expected_sha256=artifact.sha256,
            expected_length=artifact.content_length,
        )

    results: list[dict[str, Any]] = []
    for planned_artifact in plan.artifacts:
        current_status = _local_status(
            target_root=planned_artifact.target_root,
            target_path=planned_artifact.target_path,
            expected_size=planned_artifact.content_length,
            expected_sha256=planned_artifact.sha256,
        )
        artifact = replace(planned_artifact, local_status=current_status)
        if current_status == "conflict":
            raise ValidationError(
                "local restore target changed to conflicting content"
            )
        if current_status == "already_present":
            status = current_status
        else:
            payload = _read_verified_object(
                client,
                bucket=bucket,
                key=artifact.object_key,
                expected_sha256=artifact.sha256,
                expected_length=artifact.content_length,
            )
            expected_key = immutable_object_key(
                config,
                dataset=artifact.remote_dataset,
                payload=payload,
                extension="parquet",
            )
            if expected_key != artifact.object_key:
                raise StorageError(
                    "downloaded artifact does not match its immutable object key"
                )
            status = _atomic_publish_no_overwrite(artifact, payload)
        results.append(
            {
                "content_length": artifact.content_length,
                "dataset": artifact.dataset,
                "relative_path": artifact.relative_path,
                "remote_dataset": artifact.remote_dataset,
                "sha256": artifact.sha256,
                "status": status,
            }
        )

    restored = sum(result["status"] == "restored" for result in results)
    already_present = sum(
        result["status"] == "already_present" for result in results
    )
    return {
        **base,
        "restore": {
            "artifact_results": results,
            "artifacts_already_present": already_present,
            "artifacts_restored": restored,
            "manifest_verified": True,
            "performed": True,
            "remote_objects_verified": len(plan.artifacts) + 1,
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
            "Plan or explicitly restore one exact immutable replication manifest "
            "through bounded, verified S3 object reads."
        )
    )
    parser.add_argument("--replication-summary", type=Path, required=True)
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
        "--max-total-bytes",
        type=_positive_integer,
        default=DEFAULT_MAX_RESTORE_BYTES,
    )
    parser.add_argument("--enable-durable-read", action="store_true")
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
        raise StorageError("Unable to write durable restore summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = restore_durable_replication(
            replication_summary_path=args.replication_summary,
            store_id=args.store_id,
            storage_config_path=args.storage_config,
            durable_config_path=args.durable_config,
            max_total_bytes=args.max_total_bytes,
            enable_durable_read=args.enable_durable_read,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Durable restore failed: manifest, configuration, environment, "
            "local state or bounds were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Durable restore failed: bounded manifest, S3 read or local "
            "publication verification failed",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print("Durable restore failed: unexpected local failure", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
