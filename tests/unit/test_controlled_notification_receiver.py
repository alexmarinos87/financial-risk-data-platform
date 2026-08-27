from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.controlled_notification_receiver import (
    MODEL_VERSION,
    ControlledNotificationReceiver,
)
from src.orchestration.notification_activation_checklist import (
    CONTROL_NAMES,
    build_notification_activation_checklist,
)

REVIEWED_AT = datetime(2026, 8, 27, 22, tzinfo=timezone.utc)
EXPIRES_AT = REVIEWED_AT + timedelta(days=30)
RECEIVED_AT = REVIEWED_AT + timedelta(minutes=5)
ENDPOINT = "https://receiver.test/risk?mode=controlled"


def _controls(**overrides: bool) -> dict[str, bool]:
    controls = {name: True for name in CONTROL_NAMES}
    controls.update(overrides)
    return controls


def _checklist(**overrides: Any) -> dict[str, Any]:
    parameters: dict[str, Any] = {
        "destination_id": "risk-operations-webhook",
        "destination_fingerprint": "destination-fingerprint-v1",
        "authority_id": "destination-authority-v1",
        "reviewed_by": ["risk-reviewer", "receiver-owner"],
        "reviewed_at": REVIEWED_AT,
        "review_expires_at": EXPIRES_AT,
        "controls": _controls(),
    }
    parameters.update(overrides)
    return build_notification_activation_checklist(**parameters)


def _payload(
    event_id: str = "event-1",
    *,
    event_type: str = "breach_opened",
    severity: str = "critical",
) -> bytes:
    document = {
        "event_id": event_id,
        "event_type": event_type,
        "metric_name": "portfolio_volatility_annualized",
        "payload": {"severity": severity},
        "policy_id": "us-tech-standard",
        "portfolio_id": "us-tech-equal",
        "status": severity,
        "subject_key": "us-tech-equal",
        "ts_event": "2026-08-27T22:00:00+00:00",
    }
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _headers(event_id: str = "event-1") -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Idempotency-Key": event_id,
        "User-Agent": "financial-risk-data-platform/1",
    }


def _clock(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


def _receiver(**overrides: Any) -> ControlledNotificationReceiver:
    parameters: dict[str, Any] = {
        "activation_checklist": _checklist(),
        "allowed_hosts": ["receiver.test"],
        "allowed_event_types": ["breach_opened", "breach_resolved"],
        "clock": _clock(RECEIVED_AT, RECEIVED_AT + timedelta(seconds=1)),
    }
    parameters.update(overrides)
    return ControlledNotificationReceiver(**parameters)


def test_receiver_accepts_same_content_duplicate_without_network() -> None:
    receiver = _receiver()
    payload = _payload()

    assert receiver(ENDPOINT, payload, _headers(), 5.0) == 204
    assert receiver(ENDPOINT, payload, _headers(), 5.0) == 204

    summary = receiver.summary()
    assert summary["model_version"] == MODEL_VERSION
    assert summary["request_count"] == 2
    assert summary["unique_idempotency_keys"] == 1
    assert summary["same_content_duplicate_count"] == 1
    assert summary["receipts"][0]["same_content_duplicate"] is False
    assert summary["receipts"][1]["same_content_duplicate"] is True
    assert summary["external_request_performed"] is False
    assert summary["socket_opened"] is False
    assert summary["dns_lookup_performed"] is False
    assert summary["delivery_attempt_written"] is False

    rendered = json.dumps(summary, sort_keys=True)
    assert "https://" not in rendered
    assert "/risk" not in rendered
    assert '"severity"' not in rendered
    assert "postgresql://" not in rendered


def test_receiver_summary_is_deterministic_for_exact_evidence() -> None:
    first = _receiver()
    second = _receiver()
    for receiver in (first, second):
        receiver(ENDPOINT, _payload(), _headers(), 5.0)
        receiver(ENDPOINT, _payload(), _headers(), 5.0)

    assert first.summary() == second.summary()


def test_idempotency_key_reuse_with_changed_payload_fails_closed() -> None:
    receiver = _receiver()
    receiver(ENDPOINT, _payload(), _headers(), 5.0)

    with pytest.raises(ValidationError, match="different payload"):
        receiver(
            ENDPOINT,
            _payload(severity="warning"),
            _headers(),
            5.0,
        )

    assert receiver.summary()["request_count"] == 1


def test_checklist_endpoint_and_header_controls_fail_closed() -> None:
    incomplete = _checklist(
        controls=_controls(receiver_idempotency_confirmed=False),
    )
    with pytest.raises(ValidationError, match="activation-ready"):
        _receiver(activation_checklist=incomplete)

    receiver = _receiver()
    with pytest.raises(ValidationError, match="must be HTTPS"):
        receiver("http://receiver.test/risk", _payload(), _headers(), 5.0)
    with pytest.raises(ValidationError, match="not approved"):
        receiver("https://other.test/risk", _payload(), _headers(), 5.0)
    with pytest.raises(ValidationError, match="credentials"):
        receiver(
            "https://user:password@receiver.test/risk",
            _payload(),
            _headers(),
            5.0,
        )

    secret_headers = _headers()
    secret_headers["Authorization"] = "Bearer must-not-be-accepted"
    with pytest.raises(ValidationError, match="unknown"):
        receiver(ENDPOINT, _payload(), secret_headers, 5.0)


def test_payload_event_and_bound_controls_fail_closed() -> None:
    receiver = _receiver()
    noncanonical = json.dumps(json.loads(_payload()), indent=2).encode("utf-8")
    with pytest.raises(ValidationError, match="canonical JSON"):
        receiver(ENDPOINT, noncanonical, _headers(), 5.0)

    with pytest.raises(ValidationError, match="not approved"):
        receiver(
            ENDPOINT,
            _payload(event_type="unreviewed_event"),
            _headers(),
            5.0,
        )

    with pytest.raises(ValidationError, match="must equal"):
        receiver(ENDPOINT, _payload(), _headers("another-event"), 5.0)

    one_request = _receiver(
        max_requests=1,
        clock=_clock(RECEIVED_AT),
    )
    one_request(ENDPOINT, _payload(), _headers(), 5.0)
    with pytest.raises(ValidationError, match="request limit"):
        one_request(ENDPOINT, _payload(), _headers(), 5.0)

    small_payload = _receiver(max_payload_bytes=8)
    with pytest.raises(ValidationError, match="byte limit"):
        small_payload(ENDPOINT, _payload(), _headers(), 5.0)

    with pytest.raises(ValidationError, match="timeout"):
        receiver(ENDPOINT, _payload(), _headers(), 31.0)
