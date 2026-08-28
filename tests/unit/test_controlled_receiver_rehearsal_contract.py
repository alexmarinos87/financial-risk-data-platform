from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.notification_activation_checklist import (
    CONTROL_NAMES,
    build_notification_activation_checklist,
)
from src.warehouse.controlled_receiver_rehearsal_contract import (
    RECEIVER_MODEL_VERSION,
    build_controlled_receiver_rehearsal_record,
    validate_controlled_receiver_rehearsal_record,
)

STARTED = datetime(2026, 8, 28, 10, tzinfo=timezone.utc)
FINISHED = STARTED + timedelta(seconds=3)
RECORDED = FINISHED + timedelta(seconds=1)


def checklist() -> dict[str, Any]:
    return build_notification_activation_checklist(
        destination_id="risk-operations-webhook",
        destination_fingerprint="destination-fingerprint-v1",
        authority_id="destination-authority-v1",
        reviewed_by=["reviewer-a", "reviewer-b"],
        reviewed_at=STARTED - timedelta(days=1),
        review_expires_at=STARTED + timedelta(days=1),
        controls={name: True for name in CONTROL_NAMES},
    )


def summary() -> dict[str, Any]:
    receipts = [
        {
            "endpoint_host": "receiver.test",
            "event_id": "event-1",
            "event_type": "breach_opened",
            "http_status": 204,
            "idempotency_key": "event-1",
            "payload_sha256": "a" * 64,
            "received_at": (STARTED + timedelta(seconds=1)).isoformat(),
            "request_ordinal": 1,
            "same_content_duplicate": False,
        },
        {
            "endpoint_host": "receiver.test",
            "event_id": "event-1",
            "event_type": "breach_opened",
            "http_status": 204,
            "idempotency_key": "event-1",
            "payload_sha256": "a" * 64,
            "received_at": (STARTED + timedelta(seconds=2)).isoformat(),
            "request_ordinal": 2,
            "same_content_duplicate": True,
        },
    ]
    identity = {
        "activation_checklist_id": checklist()["checklist_id"],
        "allowed_event_types": ["breach_opened"],
        "allowed_hosts": ["receiver.test"],
        "authority_id": "destination-authority-v1",
        "destination_fingerprint": "destination-fingerprint-v1",
        "destination_id": "risk-operations-webhook",
        "model_version": RECEIVER_MODEL_VERSION,
        "receipts": receipts,
        "response_status": 204,
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:24]
    return {
        "rehearsal_id": f"{RECEIVER_MODEL_VERSION}-rehearsal-{digest}",
        **identity,
        "request_count": 2,
        "same_content_duplicate_count": 1,
        "unique_idempotency_keys": 1,
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


def build(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "request_id": "REHEARSAL-001",
        "terminal_status": "completed",
        "failure_code": None,
        "activation_checklist": checklist(),
        "allowed_hosts": ["receiver.test"],
        "allowed_event_types": ["breach_opened"],
        "response_status": 204,
        "started_at": STARTED,
        "finished_at": FINISHED,
        "recorded_at": RECORDED,
        "attempted_request_count": 2,
        "receiver_summary": summary(),
    }
    values.update(overrides)
    return build_controlled_receiver_rehearsal_record(**values)


def test_completed_record_is_deterministic_canonical_and_secret_free() -> None:
    first = build()
    second = build()
    assert first == second
    assert validate_controlled_receiver_rehearsal_record(first) == first
    assert first["request_count"] == 2
    rendered = json.dumps(first)
    assert "https://" not in rendered
    assert "postgresql://" not in rendered
    assert '"payload"' not in rendered


def test_rejected_and_failed_statuses_are_distinct() -> None:
    rejected = build(
        request_id="REHEARSAL-REJECTED",
        terminal_status="rejected_before_request",
        failure_code="validation_error",
        attempted_request_count=0,
        receiver_summary=None,
    )
    assert rejected["request_count"] == 0
    assert rejected["rehearsal_id"] is None

    partial = summary()
    partial["receipts"] = partial["receipts"][:1]
    partial["request_count"] = 1
    partial["same_content_duplicate_count"] = 0
    partial["unique_idempotency_keys"] = 1
    identity = {
        key: partial[key]
        for key in (
            "activation_checklist_id",
            "allowed_event_types",
            "allowed_hosts",
            "authority_id",
            "destination_fingerprint",
            "destination_id",
            "model_version",
            "receipts",
            "response_status",
        )
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:24]
    partial["rehearsal_id"] = f"{RECEIVER_MODEL_VERSION}-rehearsal-{digest}"
    failed = build(
        request_id="REHEARSAL-FAILED",
        terminal_status="failed_during_rehearsal",
        failure_code="validation_error",
        attempted_request_count=2,
        receiver_summary=partial,
    )
    assert failed["request_count"] == 1
    assert failed["attempted_request_count"] == 2


def test_tampered_counts_and_side_effects_fail_closed() -> None:
    record = build()
    record["request_count"] = 1
    with pytest.raises(ValidationError, match="not canonical"):
        validate_controlled_receiver_rehearsal_record(record)

    changed = summary()
    changed["external_request_performed"] = True
    with pytest.raises(ValidationError, match="side-effect"):
        build(receiver_summary=changed)


def test_review_window_and_status_rules_fail_closed() -> None:
    with pytest.raises(ValidationError, match="review window"):
        build(
            started_at=STARTED + timedelta(days=2),
            finished_at=FINISHED + timedelta(days=2),
            recorded_at=RECORDED + timedelta(days=2),
        )

    with pytest.raises(ValidationError, match="completed rehearsal"):
        build(attempted_request_count=3)

    with pytest.raises(ValidationError, match="one rejected request"):
        build(
            terminal_status="failed_during_rehearsal",
            failure_code="validation_error",
            attempted_request_count=2,
        )
