from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError

MODEL_VERSION = "portfolio-risk-notification-retry-destination-binding-v1"
AUTHORITY_MODEL_VERSION = "portfolio-risk-notification-destination-authority-v1"
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
ENVIRONMENT_NAME = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")


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


def canonical_binding_bytes(record: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("destination binding is not canonical JSON") from None


def _binding_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_binding_bytes(identity)).hexdigest()[:24]
    return f"{MODEL_VERSION}-binding-{digest}"


def _canonical_authority(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError("destination_authority must be a mapping")
    required = {
        "authority_id",
        "destination_fingerprint",
        "destination_id",
        "endpoint_environment_variable",
        "evaluated_at",
        "evaluated_event_types",
        "model_version",
        "channel",
        "activation",
        "allowed_event_types",
        "active",
        "endpoint_value_recorded",
        "external_request_performed",
        "delivery_attempt_written",
        "outbox_mutated",
        "acknowledgement_mutated",
    }
    if set(value) != required:
        raise ValidationError("destination_authority fields are invalid")
    if value["model_version"] != AUTHORITY_MODEL_VERSION:
        raise ValidationError("destination_authority model_version is unsupported")
    if value["channel"] != "webhook" or value["active"] is not True:
        raise ValidationError("destination_authority must be active webhook authority")
    endpoint_env = value["endpoint_environment_variable"]
    if not isinstance(endpoint_env, str) or not ENVIRONMENT_NAME.fullmatch(endpoint_env):
        raise ValidationError("destination_authority endpoint environment is invalid")
    event_types = value["evaluated_event_types"]
    allowed = value["allowed_event_types"]
    if not isinstance(event_types, list) or not isinstance(allowed, list):
        raise ValidationError("destination_authority event types must be arrays")
    if event_types != sorted(set(event_types)):
        raise ValidationError("destination_authority event types are not canonical")
    if allowed != sorted(set(allowed)) or not set(event_types).issubset(allowed):
        raise ValidationError("destination_authority allow-list evidence is invalid")
    activation = value["activation"]
    if not isinstance(activation, Mapping) or set(activation) != {
        "enabled",
        "status",
        "change_request_id",
        "reviewed_at",
        "review_expires_at",
    }:
        raise ValidationError("destination_authority activation fields are invalid")
    if activation["enabled"] is not True or activation["status"] != "active":
        raise ValidationError("destination_authority activation is not active")
    reviewed_at = _aware_utc(activation["reviewed_at"], "reviewed_at")
    expires_at = _aware_utc(
        activation["review_expires_at"],
        "review_expires_at",
    )
    evaluated_at = _aware_utc(value["evaluated_at"], "evaluated_at")
    if not reviewed_at <= evaluated_at < expires_at:
        raise ValidationError("destination_authority evaluation is outside review window")
    for flag in (
        "endpoint_value_recorded",
        "external_request_performed",
        "delivery_attempt_written",
        "outbox_mutated",
        "acknowledgement_mutated",
    ):
        if value[flag] is not False:
            raise ValidationError("destination_authority side-effect evidence is invalid")
    canonical = dict(value)
    canonical["evaluated_at"] = evaluated_at.isoformat()
    canonical["evaluated_event_types"] = list(event_types)
    canonical["allowed_event_types"] = list(allowed)
    canonical["activation"] = {
        **dict(activation),
        "reviewed_at": reviewed_at.isoformat(),
        "review_expires_at": expires_at.isoformat(),
    }
    _safe_text(canonical["authority_id"], "authority_id")
    _safe_text(canonical["destination_id"], "destination_id")
    _safe_text(canonical["destination_fingerprint"], "destination_fingerprint")
    _safe_text(activation["change_request_id"], "change_request_id")
    return canonical


def build_retry_destination_binding(
    *,
    record_id: str,
    request_id: str,
    plan_id: str,
    execution_id: str | None,
    destination_authority: Mapping[str, Any],
    recorded_at: datetime | str,
) -> dict[str, Any]:
    selected_record_id = _safe_text(record_id, "record_id")
    selected_request_id = _safe_text(request_id, "request_id")
    selected_plan_id = _safe_text(plan_id, "plan_id")
    selected_execution_id = _safe_text(
        execution_id,
        "execution_id",
        optional=True,
    )
    assert selected_record_id is not None
    assert selected_request_id is not None
    assert selected_plan_id is not None
    authority = _canonical_authority(destination_authority)
    recorded = _aware_utc(recorded_at, "recorded_at")
    evaluated = _aware_utc(authority["evaluated_at"], "authority.evaluated_at")
    if recorded < evaluated:
        raise ValidationError("binding recorded_at precedes authority evaluation")
    identity = {
        "destination_authority": authority,
        "execution_id": selected_execution_id,
        "model_version": MODEL_VERSION,
        "plan_id": selected_plan_id,
        "record_id": selected_record_id,
        "recorded_at": recorded.isoformat(),
        "request_id": selected_request_id,
    }
    return {"binding_id": _binding_id(identity), **identity}


def validate_retry_destination_binding(record: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "binding_id",
        "destination_authority",
        "execution_id",
        "model_version",
        "plan_id",
        "record_id",
        "recorded_at",
        "request_id",
    }
    if set(record) != required:
        raise ValidationError("retry destination binding fields are invalid")
    if record["model_version"] != MODEL_VERSION:
        raise ValidationError("retry destination binding model_version is unsupported")
    rebuilt = build_retry_destination_binding(
        record_id=record["record_id"],
        request_id=record["request_id"],
        plan_id=record["plan_id"],
        execution_id=record["execution_id"],
        destination_authority=record["destination_authority"],
        recorded_at=record["recorded_at"],
    )
    if dict(record) != rebuilt:
        raise ValidationError("retry destination binding is not canonical")
    return rebuilt
