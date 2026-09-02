from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError
from src.warehouse.notification_execution_readiness_enforcement import (
    validate_notification_execution_readiness_enforcement,
)
from src.warehouse.notification_retry_execution_contract import (
    canonical_retry_execution_record_bytes,
    validate_retry_execution_record,
)

MODEL_VERSION = "portfolio-risk-notification-retry-readiness-binding-v1"
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _safe_text(value: Any, label: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not SAFE_TEXT.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _aware_utc(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(f"{label} must be ISO-8601") from None
    else:
        raise ValidationError(f"{label} must be timezone-aware")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _exact_mapping(value: Any, label: str, keys: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    actual = set(value)
    if actual != keys:
        raise ValidationError(
            f"{label} fields are invalid; "
            f"missing={sorted(keys - actual)}, unknown={sorted(actual - keys)}"
        )
    return value


def canonical_notification_retry_readiness_binding_bytes(
    binding: Mapping[str, Any],
) -> bytes:
    try:
        return json.dumps(
            binding,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("retry readiness binding is not canonical JSON") from None


def _canonical_enforcement_bytes(enforcement: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            enforcement,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("readiness enforcement is not canonical JSON") from None


def _binding_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        canonical_notification_retry_readiness_binding_bytes(identity)
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-binding-{digest}"


def _terminal_summary(record: Mapping[str, Any]) -> dict[str, Any]:
    canonical = canonical_retry_execution_record_bytes(record)
    return {
        "attempts_persisted": record["attempts_persisted"],
        "document_sha256": hashlib.sha256(canonical).hexdigest(),
        "execution_id": record["execution_id"],
        "finished_at": record["finished_at"],
        "plan_id": record["plan_id"],
        "record_id": record["record_id"],
        "recorded_at": record["recorded_at"],
        "request_count": record["request_count"],
        "request_id": record["request_id"],
        "started_at": record["started_at"],
        "terminal_status": record["terminal_status"],
    }


def _validate_terminal_summary(value: Any) -> dict[str, Any]:
    terminal = _exact_mapping(
        value,
        "terminal execution summary",
        {
            "attempts_persisted",
            "document_sha256",
            "execution_id",
            "finished_at",
            "plan_id",
            "record_id",
            "recorded_at",
            "request_count",
            "request_id",
            "started_at",
            "terminal_status",
        },
    )
    request_count = terminal["request_count"]
    attempts_persisted = terminal["attempts_persisted"]
    if type(request_count) is not int or not 0 <= request_count <= 100:
        raise ValidationError("terminal request_count is invalid")
    if type(attempts_persisted) is not int or not 0 <= attempts_persisted <= 100:
        raise ValidationError("terminal attempts_persisted is invalid")
    if attempts_persisted > request_count:
        raise ValidationError("terminal attempts_persisted exceeds request_count")
    digest = terminal["document_sha256"]
    if not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest):
        raise ValidationError("terminal document_sha256 is invalid")
    started = _aware_utc(terminal["started_at"], "terminal.started_at")
    finished = _aware_utc(terminal["finished_at"], "terminal.finished_at")
    recorded = _aware_utc(terminal["recorded_at"], "terminal.recorded_at")
    if not started <= finished <= recorded:
        raise ValidationError("terminal execution timestamps are not ordered")
    status = terminal["terminal_status"]
    if status not in {
        "completed",
        "failed_before_request",
        "failed_after_request",
        "persistence_uncertain",
    }:
        raise ValidationError("terminal_status is invalid")
    execution_id = _safe_text(
        terminal["execution_id"],
        "terminal.execution_id",
        optional=True,
    )
    if status == "completed" and execution_id is None:
        raise ValidationError("completed terminal evidence requires execution_id")
    return {
        "attempts_persisted": attempts_persisted,
        "document_sha256": digest,
        "execution_id": execution_id,
        "finished_at": finished.isoformat(),
        "plan_id": _safe_text(terminal["plan_id"], "terminal.plan_id"),
        "record_id": _safe_text(terminal["record_id"], "terminal.record_id"),
        "recorded_at": recorded.isoformat(),
        "request_count": request_count,
        "request_id": _safe_text(terminal["request_id"], "terminal.request_id"),
        "started_at": started.isoformat(),
        "terminal_status": status,
    }


def _validate_cross_contract(
    *,
    terminal: Mapping[str, Any],
    terminal_record: Mapping[str, Any],
    enforcement: Mapping[str, Any],
    binding_recorded_at: datetime,
) -> None:
    started = _aware_utc(terminal["started_at"], "terminal.started_at")
    finished = _aware_utc(terminal["finished_at"], "terminal.finished_at")
    terminal_recorded = _aware_utc(
        terminal["recorded_at"],
        "terminal.recorded_at",
    )
    enforced = _aware_utc(enforcement["enforced_at"], "readiness.enforced_at")
    if not started <= enforced <= finished:
        raise ValidationError(
            "readiness enforcement must occur during the terminal execution window"
        )
    if binding_recorded_at < terminal_recorded:
        raise ValidationError("binding recorded_at precedes terminal record persistence")
    if enforcement["execution_kind"] != "retry":
        raise ValidationError("retry terminal history requires retry readiness authority")

    lock = enforcement["lock"]
    terminal_lock_model = terminal_record["lock_model_version"]
    terminal_lock_key = terminal_record["lock_key_fingerprint"]
    if terminal_record["request_count"] > 0 and (
        terminal_lock_model is None or terminal_lock_key is None
    ):
        raise ValidationError(
            "requestful terminal evidence must retain delivery lock identity"
        )
    if terminal_lock_model is not None and terminal_lock_model != lock["model_version"]:
        raise ValidationError("terminal and readiness lock model versions differ")
    if terminal_lock_key is not None and terminal_lock_key != lock["key_fingerprint"]:
        raise ValidationError("terminal and readiness lock fingerprints differ")

    execution_summary = terminal_record["execution_summary"]
    if execution_summary is not None:
        concurrency = execution_summary["concurrency_control"]
        if concurrency["scope"] != lock["scope"]:
            raise ValidationError("terminal and readiness lock scopes differ")


def build_notification_retry_readiness_binding(
    *,
    terminal_record: Mapping[str, Any],
    readiness_enforcement: Mapping[str, Any],
    recorded_at: datetime | str,
) -> dict[str, Any]:
    selected_terminal_record = validate_retry_execution_record(terminal_record)
    enforcement = validate_notification_execution_readiness_enforcement(
        readiness_enforcement
    )
    terminal = _terminal_summary(selected_terminal_record)
    recorded = _aware_utc(recorded_at, "recorded_at")
    _validate_cross_contract(
        terminal=terminal,
        terminal_record=selected_terminal_record,
        enforcement=enforcement,
        binding_recorded_at=recorded,
    )
    enforcement_digest = hashlib.sha256(
        _canonical_enforcement_bytes(enforcement)
    ).hexdigest()
    identity = {
        "model_version": MODEL_VERSION,
        "readiness_enforcement": enforcement,
        "readiness_enforcement_sha256": enforcement_digest,
        "recorded_at": recorded.isoformat(),
        "terminal_execution": terminal,
    }
    return {"binding_id": _binding_id(identity), **identity}


def validate_notification_retry_readiness_binding(
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    exact = _exact_mapping(
        binding,
        "retry readiness binding",
        {
            "binding_id",
            "model_version",
            "readiness_enforcement",
            "readiness_enforcement_sha256",
            "recorded_at",
            "terminal_execution",
        },
    )
    if exact["model_version"] != MODEL_VERSION:
        raise ValidationError("retry readiness binding model_version is unsupported")
    terminal = _validate_terminal_summary(exact["terminal_execution"])
    enforcement = validate_notification_execution_readiness_enforcement(
        exact["readiness_enforcement"]
    )
    if enforcement["execution_kind"] != "retry":
        raise ValidationError("retry readiness binding requires retry authority")
    enforced = _aware_utc(enforcement["enforced_at"], "readiness.enforced_at")
    started = _aware_utc(terminal["started_at"], "terminal.started_at")
    finished = _aware_utc(terminal["finished_at"], "terminal.finished_at")
    recorded = _aware_utc(exact["recorded_at"], "recorded_at")
    terminal_recorded = _aware_utc(
        terminal["recorded_at"],
        "terminal.recorded_at",
    )
    if not started <= enforced <= finished:
        raise ValidationError(
            "readiness enforcement must occur during the terminal execution window"
        )
    if recorded < terminal_recorded:
        raise ValidationError("binding recorded_at precedes terminal record persistence")
    expected_enforcement_digest = hashlib.sha256(
        _canonical_enforcement_bytes(enforcement)
    ).hexdigest()
    digest = exact["readiness_enforcement_sha256"]
    if not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest):
        raise ValidationError("readiness enforcement SHA-256 is invalid")
    if digest != expected_enforcement_digest:
        raise ValidationError("readiness enforcement SHA-256 does not match")
    identity = {
        "model_version": MODEL_VERSION,
        "readiness_enforcement": enforcement,
        "readiness_enforcement_sha256": expected_enforcement_digest,
        "recorded_at": recorded.isoformat(),
        "terminal_execution": terminal,
    }
    if exact["binding_id"] != _binding_id(identity):
        raise ValidationError("retry readiness binding_id does not match content")
    rebuilt = {"binding_id": exact["binding_id"], **identity}
    if dict(exact) != rebuilt:
        raise ValidationError("retry readiness binding is not canonical")
    return rebuilt
