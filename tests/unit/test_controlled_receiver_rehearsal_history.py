from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.warehouse.controlled_receiver_rehearsal_contract import (
    MODEL_VERSION,
    RECEIVER_MODEL_VERSION,
    build_controlled_receiver_rehearsal_record,
    validate_controlled_receiver_rehearsal_record,
)
from src.warehouse.controlled_receiver_rehearsal_recorder import _read_record

STARTED_AT = datetime(2026, 8, 28, 9, tzinfo=timezone.utc)
FINISHED_AT = STARTED_AT + timedelta(seconds=10)
RECORDED_AT = FINISHED_AT + timedelta(seconds=1)


def _receipt(
    ordinal: int,
    *,
    event_id: str = "event-1",
    duplicate: bool = False,
) -> dict[str, Any]:
    return {
        "endpoint_host": "receiver.example.test",
        "event_id": event_id,
        "event_type": "breach_opened",
        "idempotency_key": event_id,
        "payload_sha256": "a" * 64,
        "received_at": (STARTED_AT + timedelta(seconds=ordinal)).isoformat(),
        "request_ordinal": ordinal,
        "response_status": 204,
        "same_content_duplicate": duplicate,
    }


def _build(**overrides: Any) -> dict[str, Any]:
    parameters: dict[str, Any] = {
        "request_id": "REHEARSAL-2026-001",
        "checklist_id": "activation-checklist-1",
        "destination_id": "risk-operations-webhook",
        "destination_fingerprint": "destination-fingerprint-1",
        "authority_id": "destination-authority-1",
        "request_package_sha256": "b" * 64,
        "allowed_hosts": ["receiver.example.test"],
        "allowed_event_types": ["breach_opened"],
        "response_status": 204,
        "request_count": 2,
        "receipts": [
            _receipt(1),
            _receipt(2, duplicate=True),
        ],
        "started_at": STARTED_AT,
        "finished_at": FINISHED_AT,
        "recorded_at": RECORDED_AT,
        "terminal_status": "completed",
        "failure_code": None,
        "receiver_rehearsal_id": "controlled-receiver-rehearsal-1",
    }
    parameters.update(overrides)
    return build_controlled_receiver_rehearsal_record(**parameters)


def test_completed_record_is_deterministic_canonical_and_secret_free() -> None:
    first = _build()
    second = _build()

    assert first == second
    assert first["model_version"] == MODEL_VERSION
    assert first["receiver_model_version"] == RECEIVER_MODEL_VERSION
    assert first["receipt_count"] == 2
    assert first["duplicate_count"] == 1
    assert validate_controlled_receiver_rehearsal_record(first) == first
    assert first["external_request_performed"] is False
    assert first["payload_bodies_recorded"] is False
    assert first["response_bodies_recorded"] is False
    assert first["socket_opened"] is False

    rendered = json.dumps(first, sort_keys=True)
    assert "https://" not in rendered
    assert "postgresql://" not in rendered
    assert "secret-value" not in rendered


def test_terminal_states_are_distinct_and_deterministic() -> None:
    rejected = _build(
        request_id="REHEARSAL-REJECTED",
        request_count=1,
        receipts=[],
        terminal_status="rejected_before_request",
        failure_code="validation_error",
        receiver_rehearsal_id=None,
    )
    failed = _build(
        request_id="REHEARSAL-FAILED",
        receipts=[_receipt(1)],
        terminal_status="failed_during_rehearsal",
        failure_code="validation_error",
        receiver_rehearsal_id=None,
    )

    assert rejected["receipt_count"] == 0
    assert failed["receipt_count"] == 1
    assert rejected["record_id"] != failed["record_id"]
    assert validate_controlled_receiver_rehearsal_record(rejected) == rejected
    assert validate_controlled_receiver_rehearsal_record(failed) == failed


def test_terminal_state_rules_fail_closed() -> None:
    with pytest.raises(ValidationError, match="every request"):
        _build(receipts=[_receipt(1)])

    with pytest.raises(ValidationError, match="zero receipts"):
        _build(
            terminal_status="rejected_before_request",
            failure_code="validation_error",
            receiver_rehearsal_id=None,
        )

    with pytest.raises(ValidationError, match="partial receipts"):
        _build(
            receipts=[],
            terminal_status="failed_during_rehearsal",
            failure_code="validation_error",
            receiver_rehearsal_id=None,
        )

    with pytest.raises(ValidationError, match="requires receiver_rehearsal_id"):
        _build(receiver_rehearsal_id=None)


def test_receipt_contract_rejects_changed_order_scope_and_identity() -> None:
    changed_order = [_receipt(2), _receipt(1)]
    with pytest.raises(ValidationError, match="request_ordinal"):
        _build(receipts=changed_order)

    wrong_host = _receipt(1)
    wrong_host["endpoint_host"] = "other.example.test"
    with pytest.raises(ValidationError, match="endpoint_host"):
        _build(receipts=[wrong_host], request_count=1)

    wrong_type = _receipt(1)
    wrong_type["event_type"] = "breach_resolved"
    with pytest.raises(ValidationError, match="event_type"):
        _build(receipts=[wrong_type], request_count=1)

    wrong_key = _receipt(1)
    wrong_key["idempotency_key"] = "another-event"
    with pytest.raises(ValidationError, match="idempotency key"):
        _build(receipts=[wrong_key], request_count=1)


def test_tampered_derived_fields_and_unknown_fields_are_rejected() -> None:
    record = _build()

    tampered_count = dict(record)
    tampered_count["duplicate_count"] = 0
    with pytest.raises(ValidationError, match="duplicate_count"):
        validate_controlled_receiver_rehearsal_record(tampered_count)

    tampered_id = dict(record)
    tampered_id["record_id"] = "different-record"
    with pytest.raises(ValidationError, match="record_id"):
        validate_controlled_receiver_rehearsal_record(tampered_id)

    unknown = dict(record)
    unknown["endpoint_url"] = "https://receiver.example.test/risk"
    with pytest.raises(ValidationError, match="unknown"):
        validate_controlled_receiver_rehearsal_record(unknown)


def test_record_reader_is_bounded_and_symlink_safe(tmp_path: Path) -> None:
    record = _build()
    path = tmp_path / "record.json"
    path.write_text(json.dumps(record), encoding="utf-8")
    assert _read_record(path) == record

    link = tmp_path / "record-link.json"
    link.symlink_to(path)
    with pytest.raises(ValidationError, match="symbolic link"):
        _read_record(link)

    oversized = tmp_path / "oversized.json"
    oversized.write_text("x" * 1_048_577, encoding="utf-8")
    with pytest.raises(ValidationError, match="1 MB"):
        _read_record(oversized)
