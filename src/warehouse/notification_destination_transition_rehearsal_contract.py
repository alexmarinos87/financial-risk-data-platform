from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.controlled_notification_receiver import (
    MODEL_VERSION as RECEIVER_MODEL_VERSION,
)
from src.orchestration.notification_destination_transition_rehearsal import (
    MODEL_VERSION as REHEARSAL_MODEL_VERSION,
)

MODEL_VERSION = "portfolio-risk-notification-destination-transition-record-v1"
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
SAFE_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
ENVIRONMENT_NAME = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
MAX_STAGE_REQUESTS = 50
MAX_TOTAL_REQUESTS = 100
REHEARSAL_SIDE_EFFECT_FLAGS = (
    "acknowledgement_mutated",
    "delivery_attempt_written",
    "dns_lookup_performed",
    "endpoint_paths_recorded",
    "endpoint_values_recorded",
    "external_request_performed",
    "infrastructure_deployed",
    "outbox_mutated",
    "payload_bodies_recorded",
    "response_bodies_recorded",
    "socket_opened",
)
RECEIVER_SIDE_EFFECT_FLAGS = (
    "acknowledgement_mutated",
    "delivery_attempt_written",
    "dns_lookup_performed",
    "endpoint_paths_recorded",
    "external_request_performed",
    "infrastructure_deployed",
    "outbox_mutated",
    "payload_bodies_recorded",
    "response_bodies_recorded",
    "socket_opened",
)


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


def _bounded_integer(value: Any, label: str, *, maximum: int) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between 0 and {maximum}"
        )
    return value


def _safe_text_list(
    value: Any,
    label: str,
    *,
    allow_duplicates: bool,
    event_types: bool = False,
) -> list[str]:
    if not isinstance(value, list):
        raise ValidationError(f"{label} must be an array")
    result: list[str] = []
    for item in value:
        if event_types:
            if not isinstance(item, str) or not SAFE_EVENT_TYPE.fullmatch(item):
                raise ValidationError(f"{label} contains an invalid event type")
            parsed = item
        else:
            parsed_value = _safe_text(item, f"{label} item")
            assert parsed_value is not None
            parsed = parsed_value
        result.append(parsed)
    if not allow_duplicates and len(result) != len(set(result)):
        raise ValidationError(f"{label} must contain no duplicates")
    return result


def _canonical_bytes(value: Mapping[str, Any], label: str) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError(f"{label} is not canonical JSON") from None


def canonical_transition_rehearsal_record_bytes(
    record: Mapping[str, Any],
) -> bytes:
    return _canonical_bytes(record, "destination transition rehearsal record")


def _digest_id(prefix: str, identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(_canonical_bytes(identity, prefix)).hexdigest()[:24]
    return f"{prefix}-{digest}"


def _false_flags(
    evidence: Mapping[str, Any],
    flags: Sequence[str],
    label: str,
) -> None:
    for flag in flags:
        if evidence[flag] is not False:
            raise ValidationError(f"{label} side-effect evidence is invalid")


def _receipt(
    value: Any,
    *,
    index: int,
    allowed_hosts: set[str],
    response_status: int,
) -> dict[str, Any]:
    receipt = _exact_mapping(
        value,
        f"receiver receipt {index}",
        {
            "endpoint_host",
            "event_id",
            "event_type",
            "http_status",
            "idempotency_key",
            "payload_sha256",
            "received_at",
            "request_ordinal",
            "same_content_duplicate",
        },
    )
    host = receipt["endpoint_host"]
    if not isinstance(host, str) or host not in allowed_hosts:
        raise ValidationError("receiver receipt host is not in the approved host set")
    event_id = _safe_text(receipt["event_id"], "receiver receipt event_id")
    idempotency_key = _safe_text(
        receipt["idempotency_key"],
        "receiver receipt idempotency_key",
    )
    assert event_id is not None and idempotency_key is not None
    if event_id != idempotency_key:
        raise ValidationError("receiver receipt idempotency key differs from event_id")
    event_type = receipt["event_type"]
    if not isinstance(event_type, str) or not SAFE_EVENT_TYPE.fullmatch(event_type):
        raise ValidationError("receiver receipt event_type is invalid")
    if receipt["http_status"] != response_status:
        raise ValidationError("receiver receipt HTTP status differs from receiver status")
    payload_sha256 = receipt["payload_sha256"]
    if not isinstance(payload_sha256, str) or not SHA256_PATTERN.fullmatch(
        payload_sha256
    ):
        raise ValidationError("receiver receipt payload SHA-256 is invalid")
    ordinal = receipt["request_ordinal"]
    if ordinal != index:
        raise ValidationError("receiver receipt ordinals are not contiguous")
    duplicate = receipt["same_content_duplicate"]
    if type(duplicate) is not bool:
        raise ValidationError("receiver duplicate evidence must be boolean")
    received_at = _aware_utc(receipt["received_at"], "receiver receipt received_at")
    return {
        "endpoint_host": host,
        "event_id": event_id,
        "event_type": event_type,
        "http_status": response_status,
        "idempotency_key": idempotency_key,
        "payload_sha256": payload_sha256,
        "received_at": received_at.isoformat(),
        "request_ordinal": ordinal,
        "same_content_duplicate": duplicate,
    }


def _receiver_summary(value: Any, label: str) -> dict[str, Any]:
    summary = _exact_mapping(
        value,
        label,
        {
            "acknowledgement_mutated",
            "activation_checklist_id",
            "allowed_event_types",
            "allowed_hosts",
            "authority_id",
            "delivery_attempt_written",
            "destination_fingerprint",
            "destination_id",
            "dns_lookup_performed",
            "endpoint_paths_recorded",
            "external_request_performed",
            "infrastructure_deployed",
            "model_version",
            "outbox_mutated",
            "payload_bodies_recorded",
            "rehearsal_id",
            "receipts",
            "request_count",
            "response_bodies_recorded",
            "response_status",
            "same_content_duplicate_count",
            "socket_opened",
            "unique_idempotency_keys",
        },
    )
    if summary["model_version"] != RECEIVER_MODEL_VERSION:
        raise ValidationError(f"{label} model_version is unsupported")
    checklist_id = _safe_text(
        summary["activation_checklist_id"],
        f"{label} checklist_id",
    )
    authority_id = _safe_text(summary["authority_id"], f"{label} authority_id")
    destination_id = _safe_text(
        summary["destination_id"],
        f"{label} destination_id",
    )
    destination_fingerprint = _safe_text(
        summary["destination_fingerprint"],
        f"{label} destination_fingerprint",
    )
    assert checklist_id is not None
    assert authority_id is not None
    assert destination_id is not None
    assert destination_fingerprint is not None

    hosts = _safe_text_list(
        summary["allowed_hosts"],
        f"{label} allowed_hosts",
        allow_duplicates=False,
    )
    if hosts != sorted(hosts):
        raise ValidationError(f"{label} allowed_hosts must be sorted")
    event_types = _safe_text_list(
        summary["allowed_event_types"],
        f"{label} allowed_event_types",
        allow_duplicates=False,
        event_types=True,
    )
    if event_types != sorted(event_types):
        raise ValidationError(f"{label} allowed_event_types must be sorted")

    response_status = summary["response_status"]
    if type(response_status) is not int or not 200 <= response_status <= 299:
        raise ValidationError(f"{label} response_status is invalid")
    request_count = _bounded_integer(
        summary["request_count"],
        f"{label} request_count",
        maximum=MAX_STAGE_REQUESTS,
    )
    if request_count < 1:
        raise ValidationError(f"{label} must contain at least one request")
    unique_keys = _bounded_integer(
        summary["unique_idempotency_keys"],
        f"{label} unique_idempotency_keys",
        maximum=MAX_STAGE_REQUESTS,
    )
    duplicate_count = _bounded_integer(
        summary["same_content_duplicate_count"],
        f"{label} same_content_duplicate_count",
        maximum=MAX_STAGE_REQUESTS,
    )
    if unique_keys + duplicate_count != request_count:
        raise ValidationError(f"{label} idempotency counts do not reconcile")

    receipts_value = summary["receipts"]
    if not isinstance(receipts_value, list) or len(receipts_value) != request_count:
        raise ValidationError(f"{label} receipt count does not match request_count")
    receipts = [
        _receipt(
            receipt,
            index=index,
            allowed_hosts=set(hosts),
            response_status=response_status,
        )
        for index, receipt in enumerate(receipts_value, start=1)
    ]
    received_times = [
        _aware_utc(receipt["received_at"], "receiver receipt received_at")
        for receipt in receipts
    ]
    if received_times != sorted(received_times):
        raise ValidationError(f"{label} receipt timestamps are not ordered")
    if sum(bool(receipt["same_content_duplicate"]) for receipt in receipts) != (
        duplicate_count
    ):
        raise ValidationError(f"{label} duplicate count differs from receipts")
    seen: dict[str, str] = {}
    for receipt in receipts:
        event_id = str(receipt["event_id"])
        payload_sha256 = str(receipt["payload_sha256"])
        previous = seen.get(event_id)
        if receipt["same_content_duplicate"]:
            if previous != payload_sha256:
                raise ValidationError(
                    f"{label} duplicate receipt does not reuse identical content"
                )
        elif previous is not None:
            raise ValidationError(
                f"{label} repeated event evidence is not marked duplicate"
            )
        seen.setdefault(event_id, payload_sha256)
    if len(seen) != unique_keys:
        raise ValidationError(f"{label} unique idempotency count differs from receipts")

    _false_flags(summary, RECEIVER_SIDE_EFFECT_FLAGS, label)
    identity = {
        "activation_checklist_id": checklist_id,
        "allowed_event_types": event_types,
        "allowed_hosts": hosts,
        "authority_id": authority_id,
        "destination_fingerprint": destination_fingerprint,
        "destination_id": destination_id,
        "model_version": RECEIVER_MODEL_VERSION,
        "receipts": receipts,
        "response_status": response_status,
    }
    expected_rehearsal_id = _digest_id(
        f"{RECEIVER_MODEL_VERSION}-rehearsal",
        identity,
    )
    if summary["rehearsal_id"] != expected_rehearsal_id:
        raise ValidationError(f"{label} rehearsal identity is invalid")
    return {
        "rehearsal_id": expected_rehearsal_id,
        **identity,
        "request_count": request_count,
        "same_content_duplicate_count": duplicate_count,
        "unique_idempotency_keys": unique_keys,
        **{flag: False for flag in RECEIVER_SIDE_EFFECT_FLAGS},
    }


def _active_stage(value: Any, operation: str) -> dict[str, Any]:
    stage = _exact_mapping(
        value,
        f"{operation} stage",
        {
            "authority_id",
            "checklist_id",
            "destination_fingerprint",
            "endpoint_environment_variable",
            "first_received_at",
            "last_received_at",
            "operation",
            "plan_id",
            "receiver_summary",
            "request_count",
            "requested_event_ids",
            "requested_event_types",
        },
    )
    if stage["operation"] != operation:
        raise ValidationError(f"{operation} stage operation is invalid")
    plan_id = _safe_text(stage["plan_id"], f"{operation} plan_id")
    authority_id = _safe_text(stage["authority_id"], f"{operation} authority_id")
    checklist_id = _safe_text(stage["checklist_id"], f"{operation} checklist_id")
    fingerprint = _safe_text(
        stage["destination_fingerprint"],
        f"{operation} destination_fingerprint",
    )
    endpoint_environment = stage["endpoint_environment_variable"]
    if not isinstance(endpoint_environment, str) or not ENVIRONMENT_NAME.fullmatch(
        endpoint_environment
    ):
        raise ValidationError(f"{operation} endpoint environment is invalid")
    assert plan_id is not None
    assert authority_id is not None
    assert checklist_id is not None
    assert fingerprint is not None

    request_count = _bounded_integer(
        stage["request_count"],
        f"{operation} request_count",
        maximum=MAX_STAGE_REQUESTS,
    )
    if request_count < 1:
        raise ValidationError(f"{operation} stage requires at least one request")
    event_ids = _safe_text_list(
        stage["requested_event_ids"],
        f"{operation} requested_event_ids",
        allow_duplicates=True,
    )
    event_types = _safe_text_list(
        stage["requested_event_types"],
        f"{operation} requested_event_types",
        allow_duplicates=True,
        event_types=True,
    )
    if len(event_ids) != request_count or len(event_types) != request_count:
        raise ValidationError(f"{operation} stage request evidence count differs")

    receiver = _receiver_summary(
        stage["receiver_summary"],
        f"{operation} receiver summary",
    )
    comparisons = {
        "authority_id": authority_id,
        "activation_checklist_id": checklist_id,
        "destination_fingerprint": fingerprint,
        "request_count": request_count,
    }
    for field, expected in comparisons.items():
        if receiver[field] != expected:
            raise ValidationError(
                f"{operation} receiver {field} differs from stage evidence"
            )
    receipt_event_ids = [str(receipt["event_id"]) for receipt in receiver["receipts"]]
    receipt_event_types = [
        str(receipt["event_type"]) for receipt in receiver["receipts"]
    ]
    if receipt_event_ids != event_ids or receipt_event_types != event_types:
        raise ValidationError(f"{operation} receiver receipts differ from selected events")
    first_received_at = _aware_utc(
        stage["first_received_at"],
        f"{operation} first_received_at",
    )
    last_received_at = _aware_utc(
        stage["last_received_at"],
        f"{operation} last_received_at",
    )
    if first_received_at > last_received_at:
        raise ValidationError(f"{operation} stage receipt times are reversed")
    if first_received_at.isoformat() != receiver["receipts"][0]["received_at"]:
        raise ValidationError(f"{operation} first receipt timestamp differs")
    if last_received_at.isoformat() != receiver["receipts"][-1]["received_at"]:
        raise ValidationError(f"{operation} last receipt timestamp differs")
    return {
        "operation": operation,
        "plan_id": plan_id,
        "authority_id": authority_id,
        "checklist_id": checklist_id,
        "destination_fingerprint": fingerprint,
        "endpoint_environment_variable": endpoint_environment,
        "requested_event_ids": event_ids,
        "requested_event_types": event_types,
        "request_count": request_count,
        "first_received_at": first_received_at.isoformat(),
        "last_received_at": last_received_at.isoformat(),
        "receiver_summary": receiver,
    }


def _disable_stage(value: Any) -> dict[str, Any]:
    stage = _exact_mapping(
        value,
        "disable stage",
        {
            "authority_id",
            "checklist_id",
            "destination_fingerprint",
            "endpoint_environment_variable",
            "operation",
            "plan_id",
            "receiver_summary",
            "request_count",
            "requested_event_ids",
            "requested_event_types",
            "target_authority_required",
        },
    )
    if stage["operation"] != "disable":
        raise ValidationError("disable stage operation is invalid")
    plan_id = _safe_text(stage["plan_id"], "disable plan_id")
    fingerprint = _safe_text(
        stage["destination_fingerprint"],
        "disable destination_fingerprint",
    )
    assert plan_id is not None and fingerprint is not None
    endpoint_environment = stage["endpoint_environment_variable"]
    if not isinstance(endpoint_environment, str) or not ENVIRONMENT_NAME.fullmatch(
        endpoint_environment
    ):
        raise ValidationError("disable endpoint environment is invalid")
    if (
        stage["authority_id"] is not None
        or stage["checklist_id"] is not None
        or stage["receiver_summary"] is not None
        or stage["request_count"] != 0
        or stage["requested_event_ids"] != []
        or stage["requested_event_types"] != []
        or stage["target_authority_required"] is not False
    ):
        raise ValidationError("disable stage must be authority-free and request-free")
    return {
        "operation": "disable",
        "plan_id": plan_id,
        "authority_id": None,
        "checklist_id": None,
        "destination_fingerprint": fingerprint,
        "endpoint_environment_variable": endpoint_environment,
        "requested_event_ids": [],
        "requested_event_types": [],
        "request_count": 0,
        "receiver_summary": None,
        "target_authority_required": False,
    }


def validate_notification_destination_transition_rehearsal(
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    exact = _exact_mapping(
        summary,
        "destination transition rehearsal",
        {
            "acknowledgement_mutated",
            "baseline_authority_id",
            "delivery_attempt_written",
            "destination_id",
            "disable_plan_id",
            "dns_lookup_performed",
            "endpoint_paths_recorded",
            "endpoint_values_recorded",
            "external_request_performed",
            "finished_at",
            "infrastructure_deployed",
            "model_version",
            "outbox_mutated",
            "payload_bodies_recorded",
            "rehearsal_id",
            "response_bodies_recorded",
            "rollback_plan_id",
            "rotate_plan_id",
            "socket_opened",
            "stages",
            "stale_authority_rejections",
            "started_at",
        },
    )
    if exact["model_version"] != REHEARSAL_MODEL_VERSION:
        raise ValidationError("destination transition rehearsal model_version is unsupported")
    destination_id = _safe_text(exact["destination_id"], "destination_id")
    baseline_authority_id = _safe_text(
        exact["baseline_authority_id"],
        "baseline_authority_id",
    )
    rotate_plan_id = _safe_text(exact["rotate_plan_id"], "rotate_plan_id")
    disable_plan_id = _safe_text(exact["disable_plan_id"], "disable_plan_id")
    rollback_plan_id = _safe_text(exact["rollback_plan_id"], "rollback_plan_id")
    assert destination_id is not None
    assert baseline_authority_id is not None
    assert rotate_plan_id is not None
    assert disable_plan_id is not None
    assert rollback_plan_id is not None

    stages_value = exact["stages"]
    if not isinstance(stages_value, list) or len(stages_value) != 3:
        raise ValidationError("destination transition rehearsal must have three stages")
    rotate_stage = _active_stage(stages_value[0], "rotate")
    disable_stage = _disable_stage(stages_value[1])
    rollback_stage = _active_stage(stages_value[2], "rollback")
    if rotate_stage["plan_id"] != rotate_plan_id:
        raise ValidationError("rotate stage plan identity differs")
    if disable_stage["plan_id"] != disable_plan_id:
        raise ValidationError("disable stage plan identity differs")
    if rollback_stage["plan_id"] != rollback_plan_id:
        raise ValidationError("rollback stage plan identity differs")
    for stage in (rotate_stage, rollback_stage):
        if stage["receiver_summary"]["destination_id"] != destination_id:
            raise ValidationError("receiver stage destination identity differs")

    stale = _exact_mapping(
        exact["stale_authority_rejections"],
        "stale_authority_rejections",
        {
            "baseline_for_rotation_target",
            "rotated_for_rollback_target",
        },
    )
    if any(stale[key] is not True for key in stale):
        raise ValidationError("stale authority rejection evidence is incomplete")

    started_at = _aware_utc(exact["started_at"], "started_at")
    finished_at = _aware_utc(exact["finished_at"], "finished_at")
    rotate_first = _aware_utc(
        rotate_stage["first_received_at"],
        "rotate first_received_at",
    )
    rotate_last = _aware_utc(
        rotate_stage["last_received_at"],
        "rotate last_received_at",
    )
    rollback_first = _aware_utc(
        rollback_stage["first_received_at"],
        "rollback first_received_at",
    )
    rollback_last = _aware_utc(
        rollback_stage["last_received_at"],
        "rollback last_received_at",
    )
    if not started_at <= rotate_first <= rotate_last <= rollback_first <= rollback_last:
        raise ValidationError("destination transition rehearsal timestamps are not ordered")
    if finished_at != rollback_last:
        raise ValidationError("destination transition rehearsal finished_at differs")
    total_requests = rotate_stage["request_count"] + rollback_stage["request_count"]
    if total_requests > MAX_TOTAL_REQUESTS:
        raise ValidationError("destination transition rehearsal request bound exceeded")

    _false_flags(exact, REHEARSAL_SIDE_EFFECT_FLAGS, "transition rehearsal")
    canonical_stages = [rotate_stage, disable_stage, rollback_stage]
    identity = {
        "baseline_authority_id": baseline_authority_id,
        "destination_id": destination_id,
        "disable_plan_id": disable_plan_id,
        "finished_at": finished_at.isoformat(),
        "model_version": REHEARSAL_MODEL_VERSION,
        "rollback_plan_id": rollback_plan_id,
        "rotate_plan_id": rotate_plan_id,
        "stages": canonical_stages,
        "stale_authority_rejections": {
            "baseline_for_rotation_target": True,
            "rotated_for_rollback_target": True,
        },
        "started_at": started_at.isoformat(),
    }
    expected_rehearsal_id = _digest_id(
        f"{REHEARSAL_MODEL_VERSION}-rehearsal",
        identity,
    )
    if exact["rehearsal_id"] != expected_rehearsal_id:
        raise ValidationError("destination transition rehearsal identity is invalid")
    return {
        "rehearsal_id": expected_rehearsal_id,
        **identity,
        **{flag: False for flag in REHEARSAL_SIDE_EFFECT_FLAGS},
    }


def _record_identity(
    *,
    request_id: str,
    recorded_at: datetime,
    rehearsal: Mapping[str, Any],
) -> dict[str, Any]:
    stages = rehearsal["stages"]
    rotate = stages[0]
    disable = stages[1]
    rollback = stages[2]
    duplicate_count = (
        rotate["receiver_summary"]["same_content_duplicate_count"]
        + rollback["receiver_summary"]["same_content_duplicate_count"]
    )
    return {
        "baseline_authority_id": rehearsal["baseline_authority_id"],
        "destination_id": rehearsal["destination_id"],
        "disable_destination_fingerprint": disable["destination_fingerprint"],
        "disable_endpoint_environment_variable": disable[
            "endpoint_environment_variable"
        ],
        "disable_plan_id": rehearsal["disable_plan_id"],
        "finished_at": rehearsal["finished_at"],
        "model_version": MODEL_VERSION,
        "recorded_at": recorded_at.isoformat(),
        "rehearsal_id": rehearsal["rehearsal_id"],
        "rehearsal_summary": dict(rehearsal),
        "request_id": request_id,
        "rollback_authority_id": rollback["authority_id"],
        "rollback_checklist_id": rollback["checklist_id"],
        "rollback_destination_fingerprint": rollback["destination_fingerprint"],
        "rollback_endpoint_environment_variable": rollback[
            "endpoint_environment_variable"
        ],
        "rollback_plan_id": rehearsal["rollback_plan_id"],
        "rollback_request_count": rollback["request_count"],
        "rotate_authority_id": rotate["authority_id"],
        "rotate_checklist_id": rotate["checklist_id"],
        "rotate_destination_fingerprint": rotate["destination_fingerprint"],
        "rotate_endpoint_environment_variable": rotate[
            "endpoint_environment_variable"
        ],
        "rotate_plan_id": rehearsal["rotate_plan_id"],
        "rotate_request_count": rotate["request_count"],
        "same_content_duplicate_count": duplicate_count,
        "started_at": rehearsal["started_at"],
    }


def build_notification_destination_transition_rehearsal_record(
    *,
    request_id: str,
    recorded_at: datetime | str,
    rehearsal: Mapping[str, Any],
) -> dict[str, Any]:
    selected_request_id = _safe_text(request_id, "request_id")
    assert selected_request_id is not None
    recorded = _aware_utc(recorded_at, "recorded_at")
    validated_rehearsal = validate_notification_destination_transition_rehearsal(
        rehearsal
    )
    finished = _aware_utc(validated_rehearsal["finished_at"], "finished_at")
    if recorded < finished:
        raise ValidationError("recorded_at must not precede rehearsal completion")
    identity = _record_identity(
        request_id=selected_request_id,
        recorded_at=recorded,
        rehearsal=validated_rehearsal,
    )
    return {
        "record_id": _digest_id(f"{MODEL_VERSION}-record", identity),
        **identity,
    }


def validate_notification_destination_transition_rehearsal_record(
    record: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "baseline_authority_id",
        "destination_id",
        "disable_destination_fingerprint",
        "disable_endpoint_environment_variable",
        "disable_plan_id",
        "finished_at",
        "model_version",
        "record_id",
        "recorded_at",
        "rehearsal_id",
        "rehearsal_summary",
        "request_id",
        "rollback_authority_id",
        "rollback_checklist_id",
        "rollback_destination_fingerprint",
        "rollback_endpoint_environment_variable",
        "rollback_plan_id",
        "rollback_request_count",
        "rotate_authority_id",
        "rotate_checklist_id",
        "rotate_destination_fingerprint",
        "rotate_endpoint_environment_variable",
        "rotate_plan_id",
        "rotate_request_count",
        "same_content_duplicate_count",
        "started_at",
    }
    exact = _exact_mapping(record, "transition rehearsal record", expected)
    if exact["model_version"] != MODEL_VERSION:
        raise ValidationError("transition rehearsal record model_version is unsupported")
    rebuilt = build_notification_destination_transition_rehearsal_record(
        request_id=exact["request_id"],
        recorded_at=exact["recorded_at"],
        rehearsal=exact["rehearsal_summary"],
    )
    if dict(exact) != rebuilt:
        raise ValidationError("transition rehearsal record is not canonical")
    return rebuilt
