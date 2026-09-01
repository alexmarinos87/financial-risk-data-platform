from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.controlled_notification_receiver import (
    ControlledNotificationReceiver,
)
from src.orchestration.notification_activation_checklist import (
    validate_notification_activation_checklist,
)
from src.orchestration.notification_destination_transition_plan import (
    validate_notification_destination_transition_plan,
    validate_target_destination_authority,
)
from src.orchestration.portfolio_risk_notification_destination_authority import (
    MODEL_VERSION as DESTINATION_AUTHORITY_MODEL_VERSION,
)

MODEL_VERSION = "portfolio-risk-notification-destination-transition-rehearsal-v1"
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
MAX_STAGE_REQUESTS = 50
Clock = Callable[[], datetime]


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


def _authority_fields(value: Any, label: str) -> Mapping[str, Any]:
    authority = _exact_mapping(
        value,
        label,
        {
            "acknowledgement_mutated",
            "activation",
            "active",
            "allowed_event_types",
            "authority_id",
            "channel",
            "delivery_attempt_written",
            "destination_fingerprint",
            "destination_id",
            "endpoint_environment_variable",
            "endpoint_value_recorded",
            "evaluated_at",
            "evaluated_event_types",
            "external_request_performed",
            "model_version",
            "outbox_mutated",
        },
    )
    if authority["model_version"] != DESTINATION_AUTHORITY_MODEL_VERSION:
        raise ValidationError(f"{label} model_version is unsupported")
    if authority["channel"] != "webhook" or authority["active"] is not True:
        raise ValidationError(f"{label} must be active webhook authority")
    if any(
        authority[flag] is not False
        for flag in (
            "acknowledgement_mutated",
            "delivery_attempt_written",
            "endpoint_value_recorded",
            "external_request_performed",
            "outbox_mutated",
        )
    ):
        raise ValidationError(f"{label} side-effect evidence is invalid")
    return authority


def _validate_current_authority(
    *,
    plan: Mapping[str, Any],
    authority: Mapping[str, Any],
    label: str,
) -> dict[str, Any]:
    validated_plan = validate_notification_destination_transition_plan(plan)
    current = validated_plan["current"]
    exact = _authority_fields(authority, label)
    expected = {
        "destination_id": validated_plan["destination_id"],
        "destination_fingerprint": current["fingerprint"],
        "endpoint_environment_variable": current["endpoint_environment_variable"],
        "evaluated_at": validated_plan["planned_at"],
    }
    for field, expected_value in expected.items():
        if exact[field] != expected_value:
            raise ValidationError(f"{label} {field} does not match current plan evidence")
    activation = exact["activation"]
    if not isinstance(activation, Mapping) or activation.get("status") != "active":
        raise ValidationError(f"{label} activation is not active")
    return dict(exact)


def _validate_checklist(
    *,
    checklist: Mapping[str, Any],
    authority: Mapping[str, Any],
    label: str,
) -> dict[str, Any]:
    validated = validate_notification_activation_checklist(checklist)
    if validated["activation_ready"] is not True:
        raise ValidationError(f"{label} must be activation-ready")
    expected = {
        "destination_id": authority["destination_id"],
        "destination_fingerprint": authority["destination_fingerprint"],
        "authority_id": authority["authority_id"],
    }
    for field, expected_value in expected.items():
        if validated[field] != expected_value:
            raise ValidationError(f"{label} {field} does not match authority")
    return validated


def _request_bytes(value: Any, label: str) -> tuple[str, bytes]:
    request = _exact_mapping(value, label, {"endpoint", "payload"})
    endpoint = request["endpoint"]
    if not isinstance(endpoint, str) or not endpoint:
        raise ValidationError(f"{label}.endpoint must be non-empty text")
    payload = request["payload"]
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label}.payload must be a mapping")
    try:
        payload_bytes = json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError(f"{label}.payload is not canonical JSON") from None
    return endpoint, payload_bytes


def _event_identity(payload_bytes: bytes, label: str) -> tuple[str, str]:
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeError, ValueError):  # pragma: no cover - encoded above.
        raise ValidationError(f"{label}.payload is invalid JSON") from None
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label}.payload must be an object")
    return (
        _safe_text(payload.get("event_id"), f"{label}.payload.event_id"),
        _safe_text(payload.get("event_type"), f"{label}.payload.event_type"),
    )


def _run_receiver_stage(
    *,
    operation: str,
    plan: Mapping[str, Any],
    authority: Mapping[str, Any],
    checklist: Mapping[str, Any],
    allowed_hosts: Sequence[str],
    requests: Sequence[Mapping[str, Any]],
    response_status: int,
    clock: Clock,
) -> dict[str, Any]:
    if isinstance(requests, (str, bytes)) or not 1 <= len(requests) <= MAX_STAGE_REQUESTS:
        raise ValidationError(
            f"{operation} requests must contain between 1 and {MAX_STAGE_REQUESTS} items"
        )
    validated_plan = validate_notification_destination_transition_plan(plan)
    validated_authority = validate_target_destination_authority(
        plan=validated_plan,
        authority=authority,
    )
    validated_checklist = _validate_checklist(
        checklist=checklist,
        authority=validated_authority,
        label=f"{operation} checklist",
    )
    receiver = ControlledNotificationReceiver(
        activation_checklist=validated_checklist,
        allowed_hosts=allowed_hosts,
        allowed_event_types=validated_authority["allowed_event_types"],
        response_status=response_status,
        max_requests=len(requests),
        clock=clock,
    )
    requested_event_ids: list[str] = []
    requested_event_types: list[str] = []
    for index, request in enumerate(requests, start=1):
        label = f"{operation} request {index}"
        endpoint, payload_bytes = _request_bytes(request, label)
        event_id, event_type = _event_identity(payload_bytes, label)
        receiver(
            endpoint,
            payload_bytes,
            {
                "Content-Type": "application/json",
                "Idempotency-Key": event_id,
                "User-Agent": "financial-risk-data-platform/1",
            },
            5.0,
        )
        requested_event_ids.append(event_id)
        requested_event_types.append(event_type)

    summary = receiver.summary()
    if summary["destination_id"] != validated_plan["destination_id"]:
        raise ValidationError(f"{operation} receiver destination identity changed")
    if summary["destination_fingerprint"] != validated_authority[
        "destination_fingerprint"
    ]:
        raise ValidationError(f"{operation} receiver destination fingerprint changed")
    if summary["authority_id"] != validated_authority["authority_id"]:
        raise ValidationError(f"{operation} receiver authority identity changed")
    if summary["activation_checklist_id"] != validated_checklist["checklist_id"]:
        raise ValidationError(f"{operation} receiver checklist identity changed")
    if summary["request_count"] != len(requests):
        raise ValidationError(f"{operation} receiver request count changed")

    receipts = summary["receipts"]
    first_received_at = receipts[0]["received_at"]
    last_received_at = receipts[-1]["received_at"]
    return {
        "operation": operation,
        "plan_id": validated_plan["plan_id"],
        "authority_id": validated_authority["authority_id"],
        "checklist_id": validated_checklist["checklist_id"],
        "destination_fingerprint": validated_authority["destination_fingerprint"],
        "endpoint_environment_variable": validated_authority[
            "endpoint_environment_variable"
        ],
        "requested_event_ids": requested_event_ids,
        "requested_event_types": requested_event_types,
        "request_count": len(requests),
        "first_received_at": first_received_at,
        "last_received_at": last_received_at,
        "receiver_summary": summary,
    }


def _assert_stale_authority_rejected(
    *,
    plan: Mapping[str, Any],
    authority: Mapping[str, Any],
    label: str,
) -> None:
    try:
        validate_target_destination_authority(plan=plan, authority=authority)
    except ValidationError:
        return
    raise ValidationError(f"{label} unexpectedly authorised a later transition target")


def _chain(
    *,
    rotate_plan: Mapping[str, Any],
    disable_plan: Mapping[str, Any],
    rollback_plan: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    rotate = validate_notification_destination_transition_plan(rotate_plan)
    disable = validate_notification_destination_transition_plan(disable_plan)
    rollback = validate_notification_destination_transition_plan(rollback_plan)
    if [rotate["operation"], disable["operation"], rollback["operation"]] != [
        "rotate",
        "disable",
        "rollback",
    ]:
        raise ValidationError("transition rehearsal operations are not canonical")
    destination_ids = {
        rotate["destination_id"],
        disable["destination_id"],
        rollback["destination_id"],
    }
    if len(destination_ids) != 1:
        raise ValidationError("transition rehearsal destination identities differ")
    plan_times = [
        _aware_utc(rotate["planned_at"], "rotate planned_at"),
        _aware_utc(disable["planned_at"], "disable planned_at"),
        _aware_utc(rollback["planned_at"], "rollback planned_at"),
    ]
    if plan_times != sorted(plan_times):
        raise ValidationError("transition rehearsal plan times are not ordered")
    if rotate["target"] != disable["current"]:
        raise ValidationError("rotation target does not equal disablement current state")
    if disable["target"] != rollback["current"]:
        raise ValidationError("disablement target does not equal rollback current state")
    if rollback["prior_plan_id"] != disable["plan_id"]:
        raise ValidationError("rollback does not reference the exact disablement plan")
    if rollback["target"]["endpoint_environment_variable"] != rotate["current"][
        "endpoint_environment_variable"
    ]:
        raise ValidationError("rollback does not restore the prior endpoint identity")
    return rotate, disable, rollback


def canonical_transition_rehearsal_bytes(summary: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            summary,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("transition rehearsal is not canonical JSON") from None


def _rehearsal_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_transition_rehearsal_bytes(identity)).hexdigest()[:24]
    return f"{MODEL_VERSION}-rehearsal-{digest}"


def rehearse_notification_destination_transition(
    *,
    rotate_plan: Mapping[str, Any],
    disable_plan: Mapping[str, Any],
    rollback_plan: Mapping[str, Any],
    baseline_authority: Mapping[str, Any],
    rotate_authority: Mapping[str, Any],
    rollback_authority: Mapping[str, Any],
    rotate_checklist: Mapping[str, Any],
    rollback_checklist: Mapping[str, Any],
    rotate_allowed_hosts: Sequence[str],
    rollback_allowed_hosts: Sequence[str],
    rotate_requests: Sequence[Mapping[str, Any]],
    rollback_requests: Sequence[Mapping[str, Any]],
    started_at: datetime | str,
    response_status: int = 204,
    clock: Clock | None = None,
) -> dict[str, Any]:
    """Rehearse rotate, disable, and rollback without resolving an endpoint."""

    rotate, disable, rollback = _chain(
        rotate_plan=rotate_plan,
        disable_plan=disable_plan,
        rollback_plan=rollback_plan,
    )
    started = _aware_utc(started_at, "started_at")
    if any(
        _aware_utc(plan["planned_at"], "planned_at") > started
        for plan in (rotate, disable, rollback)
    ):
        raise ValidationError("transition rehearsal starts before a plan was produced")
    baseline = _validate_current_authority(
        plan=rotate,
        authority=baseline_authority,
        label="baseline authority",
    )
    rotated = _validate_current_authority(
        plan=disable,
        authority=rotate_authority,
        label="rotated authority",
    )
    _assert_stale_authority_rejected(
        plan=rotate,
        authority=baseline,
        label="baseline authority",
    )
    _assert_stale_authority_rejected(
        plan=rollback,
        authority=rotated,
        label="rotated authority",
    )

    selected_clock = clock or (lambda: datetime.now(timezone.utc))
    rotate_stage = _run_receiver_stage(
        operation="rotate",
        plan=rotate,
        authority=rotate_authority,
        checklist=rotate_checklist,
        allowed_hosts=rotate_allowed_hosts,
        requests=rotate_requests,
        response_status=response_status,
        clock=selected_clock,
    )
    rotate_last = _aware_utc(rotate_stage["last_received_at"], "rotate received_at")
    if rotate_last < started:
        raise ValidationError("rotation receipt precedes rehearsal start")

    disable_stage = {
        "operation": "disable",
        "plan_id": disable["plan_id"],
        "authority_id": None,
        "checklist_id": None,
        "destination_fingerprint": disable["target"]["fingerprint"],
        "endpoint_environment_variable": disable["target"][
            "endpoint_environment_variable"
        ],
        "requested_event_ids": [],
        "requested_event_types": [],
        "request_count": 0,
        "receiver_summary": None,
        "target_authority_required": False,
    }

    rollback_stage = _run_receiver_stage(
        operation="rollback",
        plan=rollback,
        authority=rollback_authority,
        checklist=rollback_checklist,
        allowed_hosts=rollback_allowed_hosts,
        requests=rollback_requests,
        response_status=response_status,
        clock=selected_clock,
    )
    rollback_first = _aware_utc(
        rollback_stage["first_received_at"],
        "rollback received_at",
    )
    rollback_last = _aware_utc(
        rollback_stage["last_received_at"],
        "rollback received_at",
    )
    if rollback_first < rotate_last:
        raise ValidationError("rollback receipts precede rotation completion")

    identity = {
        "baseline_authority_id": baseline["authority_id"],
        "destination_id": rotate["destination_id"],
        "disable_plan_id": disable["plan_id"],
        "finished_at": rollback_last.isoformat(),
        "model_version": MODEL_VERSION,
        "rollback_plan_id": rollback["plan_id"],
        "rotate_plan_id": rotate["plan_id"],
        "stages": [rotate_stage, disable_stage, rollback_stage],
        "stale_authority_rejections": {
            "baseline_for_rotation_target": True,
            "rotated_for_rollback_target": True,
        },
        "started_at": started.isoformat(),
    }
    return {
        "rehearsal_id": _rehearsal_id(identity),
        **identity,
        "acknowledgement_mutated": False,
        "delivery_attempt_written": False,
        "dns_lookup_performed": False,
        "endpoint_paths_recorded": False,
        "endpoint_values_recorded": False,
        "external_request_performed": False,
        "infrastructure_deployed": False,
        "outbox_mutated": False,
        "payload_bodies_recorded": False,
        "response_bodies_recorded": False,
        "socket_opened": False,
    }
