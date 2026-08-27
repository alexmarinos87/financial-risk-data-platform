from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError

MODEL_VERSION = "portfolio-risk-notification-activation-checklist-v1"
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
MAX_REVIEWERS = 16
CONTROL_NAMES = (
    "ambiguous_outcome_runbook_confirmed",
    "credentials_external_to_repository",
    "destination_review_active",
    "endpoint_environment_identity_confirmed",
    "event_allow_list_confirmed",
    "receiver_idempotency_confirmed",
    "recipient_ownership_signed_off",
    "rollback_tested",
)


def _safe_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_TEXT.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _aware_utc(value: datetime | str, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(f"{label} must be ISO-8601") from None
    else:  # pragma: no cover - retained for runtime callers without typing.
        raise ValidationError(f"{label} must be timezone-aware")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _reviewers(values: Sequence[str]) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise ValidationError("reviewed_by must be an array of reviewer identities")
    parsed = [_safe_text(value, "reviewed_by item") for value in values]
    if not parsed or len(parsed) > MAX_REVIEWERS:
        raise ValidationError(
            f"reviewed_by must contain between 1 and {MAX_REVIEWERS} reviewers"
        )
    if len(parsed) != len(set(parsed)):
        raise ValidationError("reviewed_by must contain no duplicates")
    return sorted(parsed)


def _controls(values: Mapping[str, Any]) -> dict[str, bool]:
    if not isinstance(values, Mapping):
        raise ValidationError("activation controls must be a mapping")
    expected = set(CONTROL_NAMES)
    actual = set(values)
    if actual != expected:
        raise ValidationError(
            "activation control fields are invalid; "
            f"missing={sorted(expected - actual)}, "
            f"unknown={sorted(actual - expected)}"
        )
    parsed: dict[str, bool] = {}
    for name in CONTROL_NAMES:
        value = values[name]
        if type(value) is not bool:
            raise ValidationError(f"activation control {name} must be boolean")
        parsed[name] = value
    return parsed


def canonical_activation_checklist_bytes(
    checklist: Mapping[str, Any],
) -> bytes:
    try:
        return json.dumps(
            checklist,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("activation checklist is not canonical JSON") from None


def _checklist_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_activation_checklist_bytes(identity)).hexdigest()
    return f"{MODEL_VERSION}-checklist-{digest[:24]}"


def build_notification_activation_checklist(
    *,
    destination_id: str,
    destination_fingerprint: str,
    authority_id: str,
    reviewed_by: Sequence[str],
    reviewed_at: datetime | str,
    review_expires_at: datetime | str,
    controls: Mapping[str, Any],
) -> dict[str, Any]:
    """Build one deterministic, secret-free notification activation review."""

    reviewed = _aware_utc(reviewed_at, "reviewed_at")
    expires = _aware_utc(review_expires_at, "review_expires_at")
    if expires <= reviewed:
        raise ValidationError("review_expires_at must be after reviewed_at")
    parsed_controls = _controls(controls)
    identity = {
        "authority_id": _safe_text(authority_id, "authority_id"),
        "controls": parsed_controls,
        "destination_fingerprint": _safe_text(
            destination_fingerprint,
            "destination_fingerprint",
        ),
        "destination_id": _safe_text(destination_id, "destination_id"),
        "model_version": MODEL_VERSION,
        "review_expires_at": expires.isoformat(),
        "reviewed_at": reviewed.isoformat(),
        "reviewed_by": _reviewers(reviewed_by),
    }
    return {
        "checklist_id": _checklist_id(identity),
        **identity,
        "activation_ready": all(parsed_controls.values()),
        "credential_recorded": False,
        "endpoint_value_recorded": False,
        "external_request_performed": False,
        "infrastructure_deployed": False,
    }


def validate_notification_activation_checklist(
    checklist: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(checklist, Mapping):
        raise ValidationError("activation checklist must be a mapping")
    expected = {
        "activation_ready",
        "authority_id",
        "checklist_id",
        "controls",
        "credential_recorded",
        "destination_fingerprint",
        "destination_id",
        "endpoint_value_recorded",
        "external_request_performed",
        "infrastructure_deployed",
        "model_version",
        "review_expires_at",
        "reviewed_at",
        "reviewed_by",
    }
    actual = set(checklist)
    if actual != expected:
        raise ValidationError(
            "activation checklist fields are invalid; "
            f"missing={sorted(expected - actual)}, "
            f"unknown={sorted(actual - expected)}"
        )
    if checklist["model_version"] != MODEL_VERSION:
        raise ValidationError("activation checklist model_version is unsupported")
    rebuilt = build_notification_activation_checklist(
        destination_id=checklist["destination_id"],
        destination_fingerprint=checklist["destination_fingerprint"],
        authority_id=checklist["authority_id"],
        reviewed_by=checklist["reviewed_by"],
        reviewed_at=checklist["reviewed_at"],
        review_expires_at=checklist["review_expires_at"],
        controls=checklist["controls"],
    )
    if dict(checklist) != rebuilt:
        raise ValidationError("activation checklist is not canonical")
    return rebuilt
