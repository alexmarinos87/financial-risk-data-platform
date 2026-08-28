from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_activation_checklist import (
    canonical_activation_checklist_bytes,
    validate_notification_activation_checklist,
)

MODEL_VERSION = "portfolio-risk-controlled-receiver-rehearsal-record-v1"
RECEIVER_MODEL_VERSION = "portfolio-risk-controlled-notification-receiver-v1"
TERMINAL_STATUSES = frozenset(
    {"completed", "rejected_before_request", "failed_during_rehearsal"}
)
FAILURE_CODES = frozenset({"storage_error", "unexpected_error", "validation_error"})
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
SAFE_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
SAFE_HOST = re.compile(r"^[a-z0-9][a-z0-9.-]{0,252}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
MAX_EVENTS = 100
MAX_HOSTS = 16
MAX_EVENT_TYPES = 64
SIDE_EFFECT_FLAGS = (
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


def _bounded_count(value: Any, label: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_EVENTS:
        raise ValidationError(
            f"{label} must be an integer between 0 and {MAX_EVENTS}"
        )
    return value


def _hosts(values: Any) -> list[str]:
    if not isinstance(values, list):
        raise ValidationError("allowed_hosts must be an array")
    parsed: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise ValidationError("allowed_hosts contains an invalid host")
        host = value.casefold()
        if (
            value != host
            or not SAFE_HOST.fullmatch(host)
            or ".." in host
            or host.endswith(".")
        ):
            raise ValidationError("allowed_hosts contains an invalid host")
        parsed.append(host)
    if not parsed or len(parsed) > MAX_HOSTS:
        raise ValidationError(
            f"allowed_hosts must contain between 1 and {MAX_HOSTS} hosts"
        )
    if parsed != sorted(set(parsed)):
        raise ValidationError("allowed_hosts must be sorted with no duplicates")
    return parsed


def _event_types(values: Any) -> list[str]:
    if not isinstance(values, list):
        raise ValidationError("allowed_event_types must be an array")
    parsed: list[str] = []
    for value in values:
        if not isinstance(value, str) or not SAFE_EVENT_TYPE.fullmatch(value):
            raise ValidationError("allowed_event_types contains an invalid event type")
        parsed.append(value)
    if not parsed or len(parsed) > MAX_EVENT_TYPES:
        raise ValidationError(
            "allowed_event_types must contain between 1 and "
            f"{MAX_EVENT_TYPES} event types"
        )
    if parsed != sorted(set(parsed)):
        raise ValidationError(
            "allowed_event_types must be sorted with no duplicates"
        )
    return parsed


def _response_status(value: Any) -> int:
    if type(value) is not int or not 200 <= value <= 299:
        raise ValidationError("response_status must be a successful HTTP status")
    return value


def canonical_controlled_receiver_rehearsal_bytes(
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
        raise ValidationError("controlled receiver record is not canonical JSON") from None


def _record_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        canonical_controlled_receiver_rehearsal_bytes(identity)
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-record-{digest}"


def _rehearsal_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{RECEIVER_MODEL_VERSION}-rehearsal-{digest}"


def _canonical_receipts(
    values: Any,
    *,
    allowed_hosts: Sequence[str],
    allowed_event_types: Sequence[str],
    response_status: int,
    started_at: datetime,
    finished_at: datetime,
) -> list[dict[str, Any]]:
    if not isinstance(values, list) or len(values) > MAX_EVENTS:
        raise ValidationError("receiver receipts must be a bounded array")
    receipts: list[dict[str, Any]] = []
    seen_payloads: dict[str, str] = {}
    for index, raw in enumerate(values, start=1):
        receipt = _exact_mapping(
            raw,
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
        endpoint_host = receipt["endpoint_host"]
        if endpoint_host not in allowed_hosts:
            raise ValidationError("receiver receipt host is outside the allow-list")
        event_id = _safe_text(receipt["event_id"], "receipt.event_id")
        idempotency_key = _safe_text(
            receipt["idempotency_key"],
            "receipt.idempotency_key",
        )
        assert event_id is not None and idempotency_key is not None
        if event_id != idempotency_key:
            raise ValidationError("receiver receipt idempotency identity changed")
        event_type = receipt["event_type"]
        if event_type not in allowed_event_types:
            raise ValidationError("receiver receipt event type is not approved")
        if receipt["http_status"] != response_status:
            raise ValidationError("receiver receipt HTTP status changed")
        payload_sha256 = receipt["payload_sha256"]
        if not isinstance(payload_sha256, str) or not SHA256_PATTERN.fullmatch(
            payload_sha256
        ):
            raise ValidationError("receiver receipt payload SHA-256 is invalid")
        received_at = _aware_utc(receipt["received_at"], "receipt.received_at")
        if not started_at <= received_at <= finished_at:
            raise ValidationError("receiver receipt timestamp is outside the run")
        if receipt["request_ordinal"] != index:
            raise ValidationError("receiver receipt ordinals must be contiguous")
        duplicate = receipt["same_content_duplicate"]
        if type(duplicate) is not bool:
            raise ValidationError("receiver duplicate evidence must be boolean")
        previous = seen_payloads.get(idempotency_key)
        if duplicate != (previous is not None):
            raise ValidationError("receiver duplicate evidence is inconsistent")
        if previous is not None and previous != payload_sha256:
            raise ValidationError("receiver idempotency evidence conflicts")
        seen_payloads.setdefault(idempotency_key, payload_sha256)
        receipts.append(
            {
                **dict(receipt),
                "received_at": received_at.isoformat(),
            }
        )
    return receipts


def _canonical_receiver_summary(
    value: Any,
    *,
    checklist: Mapping[str, Any],
    allowed_hosts: Sequence[str],
    allowed_event_types: Sequence[str],
    response_status: int,
    started_at: datetime,
    finished_at: datetime,
) -> dict[str, Any]:
    summary = _exact_mapping(
        value,
        "controlled receiver summary",
        {
            "rehearsal_id",
            "activation_checklist_id",
            "allowed_event_types",
            "allowed_hosts",
            "authority_id",
            "destination_fingerprint",
            "destination_id",
            "model_version",
            "receipts",
            "response_status",
            "request_count",
            "same_content_duplicate_count",
            "unique_idempotency_keys",
            *SIDE_EFFECT_FLAGS,
        },
    )
    if summary["model_version"] != RECEIVER_MODEL_VERSION:
        raise ValidationError("controlled receiver model_version is unsupported")
    if summary["activation_checklist_id"] != checklist["checklist_id"]:
        raise ValidationError("controlled receiver checklist identity changed")
    if summary["authority_id"] != checklist["authority_id"]:
        raise ValidationError("controlled receiver authority identity changed")
    if summary["destination_id"] != checklist["destination_id"]:
        raise ValidationError("controlled receiver destination identity changed")
    if summary["destination_fingerprint"] != checklist["destination_fingerprint"]:
        raise ValidationError("controlled receiver destination fingerprint changed")
    if summary["allowed_hosts"] != list(allowed_hosts):
        raise ValidationError("controlled receiver host allow-list changed")
    if summary["allowed_event_types"] != list(allowed_event_types):
        raise ValidationError("controlled receiver event allow-list changed")
    if summary["response_status"] != response_status:
        raise ValidationError("controlled receiver response status changed")
    for flag in SIDE_EFFECT_FLAGS:
        if summary[flag] is not False:
            raise ValidationError("controlled receiver side-effect evidence is invalid")

    receipts = _canonical_receipts(
        summary["receipts"],
        allowed_hosts=allowed_hosts,
        allowed_event_types=allowed_event_types,
        response_status=response_status,
        started_at=started_at,
        finished_at=finished_at,
    )
    request_count = _bounded_count(summary["request_count"], "request_count")
    duplicate_count = _bounded_count(
        summary["same_content_duplicate_count"],
        "same_content_duplicate_count",
    )
    unique_keys = _bounded_count(
        summary["unique_idempotency_keys"],
        "unique_idempotency_keys",
    )
    if request_count != len(receipts):
        raise ValidationError("controlled receiver request count disagrees")
    if duplicate_count != sum(
        receipt["same_content_duplicate"] for receipt in receipts
    ):
        raise ValidationError("controlled receiver duplicate count disagrees")
    if unique_keys != len({receipt["idempotency_key"] for receipt in receipts}):
        raise ValidationError("controlled receiver unique key count disagrees")

    rehearsal_identity = {
        "activation_checklist_id": checklist["checklist_id"],
        "allowed_event_types": list(allowed_event_types),
        "allowed_hosts": list(allowed_hosts),
        "authority_id": checklist["authority_id"],
        "destination_fingerprint": checklist["destination_fingerprint"],
        "destination_id": checklist["destination_id"],
        "model_version": RECEIVER_MODEL_VERSION,
        "receipts": receipts,
        "response_status": response_status,
    }
    if summary["rehearsal_id"] != _rehearsal_id(rehearsal_identity):
        raise ValidationError("controlled receiver rehearsal_id does not match content")
    return {
        **dict(summary),
        "receipts": receipts,
    }


def build_controlled_receiver_rehearsal_record(
    *,
    request_id: str,
    terminal_status: str,
    failure_code: str | None,
    activation_checklist: Mapping[str, Any],
    allowed_hosts: Sequence[str],
    allowed_event_types: Sequence[str],
    response_status: int,
    started_at: datetime | str,
    finished_at: datetime | str,
    recorded_at: datetime | str,
    attempted_request_count: int,
    receiver_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    selected_request_id = _safe_text(request_id, "request_id")
    assert selected_request_id is not None
    if terminal_status not in TERMINAL_STATUSES:
        raise ValidationError("terminal_status is invalid")
    if failure_code is not None and failure_code not in FAILURE_CODES:
        raise ValidationError("failure_code is invalid")
    checklist = validate_notification_activation_checklist(activation_checklist)
    if checklist["activation_ready"] is not True:
        raise ValidationError("rehearsal history requires an activation-ready checklist")
    started = _aware_utc(started_at, "started_at")
    finished = _aware_utc(finished_at, "finished_at")
    recorded = _aware_utc(recorded_at, "recorded_at")
    if finished < started or recorded < finished:
        raise ValidationError("rehearsal timestamps are not ordered")
    reviewed_at = _aware_utc(checklist["reviewed_at"], "checklist.reviewed_at")
    expires_at = _aware_utc(
        checklist["review_expires_at"],
        "checklist.review_expires_at",
    )
    if not reviewed_at <= started < expires_at:
        raise ValidationError("rehearsal start is outside the checklist review window")
    hosts = _hosts(list(allowed_hosts))
    event_types = _event_types(list(allowed_event_types))
    status = _response_status(response_status)
    attempted = _bounded_count(
        attempted_request_count,
        "attempted_request_count",
    )
    summary: dict[str, Any] | None = None
    if receiver_summary is not None:
        summary = _canonical_receiver_summary(
            receiver_summary,
            checklist=checklist,
            allowed_hosts=hosts,
            allowed_event_types=event_types,
            response_status=status,
            started_at=started,
            finished_at=finished,
        )
    request_count = 0 if summary is None else summary["request_count"]
    unique_keys = 0 if summary is None else summary["unique_idempotency_keys"]
    duplicate_count = (
        0 if summary is None else summary["same_content_duplicate_count"]
    )
    receipts = [] if summary is None else summary["receipts"]
    rehearsal_id = None if summary is None else summary["rehearsal_id"]

    if terminal_status == "completed":
        if failure_code is not None or summary is None:
            raise ValidationError("completed rehearsal requires summary and no failure")
        if request_count == 0 or attempted != request_count:
            raise ValidationError("completed rehearsal request counts disagree")
    elif terminal_status == "rejected_before_request":
        if failure_code is None or summary is not None or attempted != 0:
            raise ValidationError(
                "rejected_before_request must have bounded failure and no requests"
            )
    else:
        if failure_code is None or summary is None or attempted <= request_count:
            raise ValidationError(
                "failed_during_rehearsal requires one rejected request"
            )

    identity = {
        "activation_checklist": checklist,
        "allowed_event_types": event_types,
        "allowed_hosts": hosts,
        "attempted_request_count": attempted,
        "failure_code": failure_code,
        "finished_at": finished.isoformat(),
        "model_version": MODEL_VERSION,
        "receiver_model_version": RECEIVER_MODEL_VERSION,
        "receiver_summary": summary,
        "recorded_at": recorded.isoformat(),
        "rehearsal_id": rehearsal_id,
        "request_count": request_count,
        "request_id": selected_request_id,
        "response_status": status,
        "same_content_duplicate_count": duplicate_count,
        "started_at": started.isoformat(),
        "terminal_status": terminal_status,
        "unique_idempotency_keys": unique_keys,
        "receipts": receipts,
        "credential_recorded": False,
        "endpoint_value_recorded": False,
        "payload_bodies_recorded": False,
        "response_bodies_recorded": False,
        "external_request_performed": False,
        "socket_opened": False,
        "dns_lookup_performed": False,
        "delivery_attempt_written": False,
        "outbox_mutated": False,
        "acknowledgement_mutated": False,
        "infrastructure_deployed": False,
    }
    return {"record_id": _record_id(identity), **identity}


def validate_controlled_receiver_rehearsal_record(
    record: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "record_id",
        "activation_checklist",
        "allowed_event_types",
        "allowed_hosts",
        "attempted_request_count",
        "failure_code",
        "finished_at",
        "model_version",
        "receiver_model_version",
        "receiver_summary",
        "recorded_at",
        "rehearsal_id",
        "request_count",
        "request_id",
        "response_status",
        "same_content_duplicate_count",
        "started_at",
        "terminal_status",
        "unique_idempotency_keys",
        "receipts",
        "credential_recorded",
        "endpoint_value_recorded",
        "payload_bodies_recorded",
        "response_bodies_recorded",
        "external_request_performed",
        "socket_opened",
        "dns_lookup_performed",
        "delivery_attempt_written",
        "outbox_mutated",
        "acknowledgement_mutated",
        "infrastructure_deployed",
    }
    exact = _exact_mapping(record, "controlled receiver record", expected)
    if exact["model_version"] != MODEL_VERSION:
        raise ValidationError("controlled receiver record model_version is unsupported")
    rebuilt = build_controlled_receiver_rehearsal_record(
        request_id=exact["request_id"],
        terminal_status=exact["terminal_status"],
        failure_code=exact["failure_code"],
        activation_checklist=exact["activation_checklist"],
        allowed_hosts=exact["allowed_hosts"],
        allowed_event_types=exact["allowed_event_types"],
        response_status=exact["response_status"],
        started_at=exact["started_at"],
        finished_at=exact["finished_at"],
        recorded_at=exact["recorded_at"],
        attempted_request_count=exact["attempted_request_count"],
        receiver_summary=exact["receiver_summary"],
    )
    if dict(exact) != rebuilt:
        raise ValidationError("controlled receiver record is not canonical")
    return rebuilt


def activation_checklist_sha256(checklist: Mapping[str, Any]) -> str:
    validated = validate_notification_activation_checklist(checklist)
    return hashlib.sha256(canonical_activation_checklist_bytes(validated)).hexdigest()
