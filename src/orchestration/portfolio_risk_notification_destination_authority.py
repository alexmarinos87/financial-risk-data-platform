from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.portfolio_risk_notification_destination_contract import (
    CHANNEL,
    evaluate_destination_activation,
    load_notification_destinations,
)

MODEL_VERSION = "portfolio-risk-notification-destination-authority-v1"
ENVIRONMENT_NAME = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
SAFE_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def _endpoint_environment(value: Any) -> str:
    if not isinstance(value, str) or not ENVIRONMENT_NAME.fullmatch(value):
        raise ValidationError(
            "delivery endpoint environment variable identity is invalid"
        )
    return value


def _event_types(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValidationError("event_types must be a sequence of event type names")
    parsed: list[str] = []
    for value in values:
        if not isinstance(value, str) or not SAFE_EVENT_TYPE.fullmatch(value):
            raise ValidationError("event_types contains an invalid event type")
        parsed.append(value)
    return tuple(sorted(set(parsed)))


def _authority_id(identity: dict[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-authority-{digest}"


def resolve_notification_destination_authority(
    *,
    destination_config_path: Path,
    destination_id: str,
    delivery_endpoint_env: str,
    evaluated_at: datetime | str,
    event_types: Sequence[str] = (),
    require_active: bool = True,
) -> dict[str, Any]:
    """Resolve one secret-free destination authority for notification execution."""

    destinations = load_notification_destinations(destination_config_path)
    destination = destinations.get(destination_id)
    if destination is None:
        raise ValidationError("notification destination does not exist")

    endpoint_env = _endpoint_environment(delivery_endpoint_env)
    if endpoint_env != destination.endpoint_env:
        raise ValidationError(
            "notification destination endpoint environment does not match "
            "delivery configuration"
        )

    evaluated_event_types = _event_types(event_types)
    unsupported = sorted(
        set(evaluated_event_types) - set(destination.allowed_event_types)
    )
    if unsupported:
        raise ValidationError(
            "notification destination does not allow event types: "
            + ", ".join(unsupported)
        )

    activation = evaluate_destination_activation(
        destination,
        evaluated_at=evaluated_at,
    )
    status = activation["activation"]["status"]
    if require_active and status != "active":
        raise ValidationError(
            f"notification destination is not active: {status}"
        )

    identity = {
        "destination_fingerprint": destination.fingerprint,
        "destination_id": destination.destination_id,
        "endpoint_environment_variable": destination.endpoint_env,
        "evaluated_at": activation["evaluated_at"],
        "evaluated_event_types": list(evaluated_event_types),
        "model_version": MODEL_VERSION,
    }
    return {
        "authority_id": _authority_id(identity),
        **identity,
        "channel": CHANNEL,
        "activation": {
            "enabled": destination.activation.enabled,
            "status": status,
            "change_request_id": destination.activation.change_request_id,
            "reviewed_at": (
                None
                if destination.activation.reviewed_at is None
                else destination.activation.reviewed_at.isoformat()
            ),
            "review_expires_at": (
                None
                if destination.activation.review_expires_at is None
                else destination.activation.review_expires_at.isoformat()
            ),
        },
        "allowed_event_types": list(destination.allowed_event_types),
        "active": status == "active",
        "endpoint_value_recorded": False,
        "external_request_performed": False,
        "delivery_attempt_written": False,
        "outbox_mutated": False,
        "acknowledgement_mutated": False,
    }
