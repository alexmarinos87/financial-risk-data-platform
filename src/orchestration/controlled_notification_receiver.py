from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from src.common.exceptions import ValidationError
from src.orchestration.notification_activation_checklist import (
    validate_notification_activation_checklist,
)

MODEL_VERSION = "portfolio-risk-controlled-notification-receiver-v1"
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
SAFE_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
SAFE_HOST = re.compile(r"^[a-z0-9][a-z0-9.-]{0,252}$")
MAX_HOSTS = 16
MAX_EVENT_TYPES = 64
MAX_PAYLOAD_BYTES = 65_536
MAX_REQUESTS = 100
MAX_TIMEOUT_SECONDS = 30.0
Clock = Callable[[], datetime]


def _safe_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_TEXT.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _aware_utc(value: Any, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValidationError(f"{label} must be timezone-aware")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValidationError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _hosts(values: Sequence[str]) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise ValidationError("allowed_hosts must be an array")
    parsed: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise ValidationError("allowed_hosts contains an invalid host")
        host = value.strip().casefold()
        if (
            not SAFE_HOST.fullmatch(host)
            or ".." in host
            or host.endswith(".")
        ):
            raise ValidationError("allowed_hosts contains an invalid host")
        parsed.append(host)
    if not parsed or len(parsed) > MAX_HOSTS:
        raise ValidationError(
            f"allowed_hosts must contain between 1 and {MAX_HOSTS} hosts"
        )
    if len(parsed) != len(set(parsed)):
        raise ValidationError("allowed_hosts must contain no duplicates")
    return sorted(parsed)


def _event_types(values: Sequence[str]) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise ValidationError("allowed_event_types must be an array")
    parsed: list[str] = []
    for value in values:
        if not isinstance(value, str) or not SAFE_EVENT_TYPE.fullmatch(value):
            raise ValidationError(
                "allowed_event_types contains an invalid event type"
            )
        parsed.append(value)
    if not parsed or len(parsed) > MAX_EVENT_TYPES:
        raise ValidationError(
            "allowed_event_types must contain between 1 and "
            f"{MAX_EVENT_TYPES} event types"
        )
    if len(parsed) != len(set(parsed)):
        raise ValidationError("allowed_event_types must contain no duplicates")
    return sorted(parsed)


def _positive_integer(value: Any, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between 1 and {maximum}"
        )
    return value


def _response_status(value: Any) -> int:
    if type(value) is not int or not 200 <= value <= 299:
        raise ValidationError("response_status must be a successful HTTP status")
    return value


def _endpoint_host(endpoint: Any, allowed_hosts: Sequence[str]) -> str:
    if not isinstance(endpoint, str) or not endpoint:
        raise ValidationError("controlled receiver endpoint must be non-empty text")
    parsed = urlparse(endpoint)
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValidationError(
            "controlled receiver endpoint must be HTTPS without credentials "
            "or fragment"
        )
    host = parsed.hostname.casefold()
    if host not in allowed_hosts:
        raise ValidationError("controlled receiver endpoint host is not approved")
    return host


def _headers(values: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(values, Mapping):
        raise ValidationError("controlled receiver headers must be a mapping")
    parsed: dict[str, str] = {}
    for key, value in values.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValidationError("controlled receiver headers are invalid")
        normalized = key.strip().casefold()
        if normalized in parsed:
            raise ValidationError("controlled receiver headers contain duplicates")
        parsed[normalized] = value.strip()
    expected = {"content-type", "idempotency-key", "user-agent"}
    if set(parsed) != expected:
        raise ValidationError(
            "controlled receiver header fields are invalid; "
            f"missing={sorted(expected - set(parsed))}, "
            f"unknown={sorted(set(parsed) - expected)}"
        )
    if parsed["content-type"].casefold() != "application/json":
        raise ValidationError("controlled receiver Content-Type is unsupported")
    _safe_text(parsed["idempotency-key"], "Idempotency-Key")
    if not parsed["user-agent"]:
        raise ValidationError("controlled receiver User-Agent must be non-empty")
    return parsed


def _payload(
    value: Any,
    *,
    max_payload_bytes: int,
) -> tuple[dict[str, Any], str]:
    if not isinstance(value, bytes) or not value:
        raise ValidationError("controlled receiver payload must be non-empty bytes")
    if len(value) > max_payload_bytes:
        raise ValidationError("controlled receiver payload exceeds the byte limit")
    try:
        decoded = value.decode("utf-8")
        document = json.loads(decoded)
    except (UnicodeError, ValueError):
        raise ValidationError("controlled receiver payload is invalid JSON") from None
    if not isinstance(document, Mapping):
        raise ValidationError("controlled receiver payload must be a JSON object")
    try:
        canonical = json.dumps(
            dict(document),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError(
            "controlled receiver payload is not canonical JSON"
        ) from None
    if canonical != value:
        raise ValidationError("controlled receiver payload must be canonical JSON")
    return dict(document), hashlib.sha256(value).hexdigest()


def _rehearsal_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    return f"{MODEL_VERSION}-rehearsal-{digest[:24]}"


class ControlledNotificationReceiver:
    """In-memory webhook transport with receiver-side idempotency evidence."""

    def __init__(
        self,
        *,
        activation_checklist: Mapping[str, Any],
        allowed_hosts: Sequence[str],
        allowed_event_types: Sequence[str],
        response_status: int = 204,
        max_requests: int = MAX_REQUESTS,
        max_payload_bytes: int = MAX_PAYLOAD_BYTES,
        clock: Clock | None = None,
    ) -> None:
        checklist = validate_notification_activation_checklist(
            activation_checklist
        )
        if checklist["activation_ready"] is not True:
            raise ValidationError(
                "controlled receiver requires an activation-ready checklist"
            )
        self._checklist = checklist
        self._allowed_hosts = _hosts(allowed_hosts)
        self._allowed_event_types = _event_types(allowed_event_types)
        self._response_status = _response_status(response_status)
        self._max_requests = _positive_integer(
            max_requests,
            "max_requests",
            MAX_REQUESTS,
        )
        self._max_payload_bytes = _positive_integer(
            max_payload_bytes,
            "max_payload_bytes",
            MAX_PAYLOAD_BYTES,
        )
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._seen_payloads: dict[str, str] = {}
        self._receipts: list[dict[str, Any]] = []

    def __call__(
        self,
        endpoint: str,
        payload: bytes,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> int:
        if len(self._receipts) >= self._max_requests:
            raise ValidationError(
                "controlled receiver request limit has been reached"
            )
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 0 < float(timeout_seconds) <= MAX_TIMEOUT_SECONDS
        ):
            raise ValidationError("controlled receiver timeout is invalid")

        host = _endpoint_host(endpoint, self._allowed_hosts)
        parsed_headers = _headers(headers)
        document, payload_sha256 = _payload(
            payload,
            max_payload_bytes=self._max_payload_bytes,
        )
        event_id = _safe_text(document.get("event_id"), "payload event_id")
        event_type_value = document.get("event_type")
        if (
            not isinstance(event_type_value, str)
            or not SAFE_EVENT_TYPE.fullmatch(event_type_value)
        ):
            raise ValidationError("payload event_type is invalid")
        if event_type_value not in self._allowed_event_types:
            raise ValidationError("payload event_type is not approved")
        idempotency_key = parsed_headers["idempotency-key"]
        if idempotency_key != event_id:
            raise ValidationError(
                "Idempotency-Key must equal the payload event_id"
            )

        previous_sha256 = self._seen_payloads.get(idempotency_key)
        duplicate = previous_sha256 is not None
        if duplicate and previous_sha256 != payload_sha256:
            raise ValidationError(
                "Idempotency-Key was reused with a different payload"
            )
        self._seen_payloads.setdefault(idempotency_key, payload_sha256)
        received_at = _aware_utc(self._clock(), "received_at")
        self._receipts.append(
            {
                "endpoint_host": host,
                "event_id": event_id,
                "event_type": event_type_value,
                "http_status": self._response_status,
                "idempotency_key": idempotency_key,
                "payload_sha256": payload_sha256,
                "received_at": received_at.isoformat(),
                "request_ordinal": len(self._receipts) + 1,
                "same_content_duplicate": duplicate,
            }
        )
        return self._response_status

    def summary(self) -> dict[str, Any]:
        receipts = [dict(receipt) for receipt in self._receipts]
        identity = {
            "activation_checklist_id": self._checklist["checklist_id"],
            "allowed_event_types": list(self._allowed_event_types),
            "allowed_hosts": list(self._allowed_hosts),
            "authority_id": self._checklist["authority_id"],
            "destination_fingerprint": self._checklist[
                "destination_fingerprint"
            ],
            "destination_id": self._checklist["destination_id"],
            "model_version": MODEL_VERSION,
            "receipts": receipts,
            "response_status": self._response_status,
        }
        return {
            "rehearsal_id": _rehearsal_id(identity),
            **identity,
            "request_count": len(receipts),
            "same_content_duplicate_count": sum(
                receipt["same_content_duplicate"] for receipt in receipts
            ),
            "unique_idempotency_keys": len(self._seen_payloads),
            "acknowledgement_mutated": False,
            "delivery_attempt_written": False,
            "dns_lookup_performed": False,
            "endpoint_paths_recorded": False,
            "external_request_performed": False,
            "infrastructure_deployed": False,
            "outbox_mutated": False,
            "payload_bodies_recorded": False,
            "response_bodies_recorded": False,
            "socket_opened": False,
        }
