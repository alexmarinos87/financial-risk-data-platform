from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError
from src.warehouse.notification_execution_readiness_gate import (
    validate_notification_execution_readiness_decision,
)

MODEL_VERSION = "portfolio-risk-notification-execution-readiness-record-v1"
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")


def _safe_text(value: Any, label: str) -> str:
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


def canonical_notification_execution_readiness_record_bytes(
    record: Mapping[str, Any],
) -> bytes:
    try:
        return json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError(
            "notification execution readiness record is not canonical JSON"
        ) from None


def _record_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        canonical_notification_execution_readiness_record_bytes(identity)
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-record-{digest}"


def build_notification_execution_readiness_record(
    *,
    request_id: str,
    recorded_at: datetime | str,
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    selected_request_id = _safe_text(request_id, "request_id")
    selected_decision = validate_notification_execution_readiness_decision(decision)
    recorded = _aware_utc(recorded_at, "recorded_at")
    evaluated = _aware_utc(selected_decision["evaluated_at"], "decision.evaluated_at")
    if recorded < evaluated:
        raise ValidationError("recorded_at must not precede decision evaluation")
    identity = {
        "decision": selected_decision,
        "model_version": MODEL_VERSION,
        "recorded_at": recorded.isoformat(),
        "request_id": selected_request_id,
    }
    return {"record_id": _record_id(identity), **identity}


def validate_notification_execution_readiness_record(
    record: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise ValidationError("notification readiness record must be a mapping")
    expected = {
        "decision",
        "model_version",
        "record_id",
        "recorded_at",
        "request_id",
    }
    actual = set(record)
    if actual != expected:
        raise ValidationError(
            "notification readiness record fields are invalid; "
            f"missing={sorted(expected - actual)}, unknown={sorted(actual - expected)}"
        )
    if record["model_version"] != MODEL_VERSION:
        raise ValidationError("notification readiness record model_version is unsupported")
    rebuilt = build_notification_execution_readiness_record(
        request_id=record["request_id"],
        recorded_at=record["recorded_at"],
        decision=record["decision"],
    )
    if dict(record) != rebuilt:
        raise ValidationError("notification readiness record is not canonical")
    return rebuilt
