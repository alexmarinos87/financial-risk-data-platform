from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.build_notification_retry_readiness_binding import (
    build_retry_readiness_binding_from_files,
)
from src.warehouse.notification_execution_readiness_enforcement import (
    _enforcement_evidence,
)
from src.warehouse.notification_retry_execution_contract import (
    build_retry_execution_record,
)
from src.warehouse.notification_retry_readiness_binding_contract import (
    build_notification_retry_readiness_binding,
    validate_notification_retry_readiness_binding,
)

BASE_TIME = datetime(2026, 9, 2, 18, 0, tzinfo=timezone.utc)
LOCK_MODEL = "portfolio-risk-notification-delivery-lock-v1"
LOCK_SCOPE = "portfolio-risk-notification-delivery"
LOCK_KEY = "notification-lock-key-1"


def _enforcement(
    *,
    execution_kind: str = "retry",
    enforced_at: datetime | None = None,
    lock_key: str = LOCK_KEY,
) -> dict[str, Any]:
    selected_time = enforced_at or BASE_TIME + timedelta(minutes=2)
    return _enforcement_evidence(
        destination_id="risk-operations-webhook",
        execution_kind=execution_kind,
        enforced_at=selected_time,
        record={
            "record_id": "readiness-record-1",
            "request_id": "readiness-request-1",
            "decision": {
                "decision_id": "retained-decision-1",
                "evaluated_at": BASE_TIME.isoformat(),
            },
        },
        refreshed_decision={
            "decision_id": "refreshed-decision-1",
            "evaluated_at": selected_time.isoformat(),
        },
        lock={
            "key_fingerprint": lock_key,
            "model_version": LOCK_MODEL,
            "scope": LOCK_SCOPE,
        },
    )


def _terminal(
    *,
    lock_key: str | None = LOCK_KEY,
) -> dict[str, Any]:
    return build_retry_execution_record(
        request_id="retry-request-1",
        plan_id="retry-plan-1",
        started_at=BASE_TIME + timedelta(minutes=1),
        finished_at=BASE_TIME + timedelta(minutes=3),
        recorded_at=BASE_TIME + timedelta(minutes=4),
        terminal_status="failed_after_request",
        failure_code="validation_error",
        request_count=1,
        attempts_persisted=1,
        succeeded_count=0,
        failed_count=1,
        attempt_ids=["attempt-1"],
        requested_event_ids=["event-1"],
        persisted_event_ids=["event-1"],
        execution_summary=None,
        endpoint_host="alerts.example.test",
        delivery_fingerprint="delivery-fingerprint-1",
        retry_policy_fingerprint="retry-policy-fingerprint-1",
        retry_execution_policy_fingerprint="retry-execution-policy-1",
        lock_model_version=LOCK_MODEL if lock_key is not None else None,
        lock_key_fingerprint=lock_key,
        lock_acquired=True if lock_key is not None else None,
        lock_released=True if lock_key is not None else None,
    )


def _binding() -> dict[str, Any]:
    return build_notification_retry_readiness_binding(
        terminal_record=_terminal(),
        readiness_enforcement=_enforcement(),
        recorded_at=BASE_TIME + timedelta(minutes=5),
    )


def test_binding_is_deterministic_canonical_and_secret_free() -> None:
    first = _binding()
    second = _binding()

    assert first == second
    assert validate_notification_retry_readiness_binding(first) == first
    assert first["readiness_enforcement"]["execution_kind"] == "retry"
    assert first["terminal_execution"]["terminal_status"] == "failed_after_request"
    rendered = json.dumps(first, sort_keys=True)
    for secret in (
        "https://alerts.example.test/path",
        "postgresql://",
        "Authorization",
        "payload-body",
        "response-body",
    ):
        assert secret not in rendered


def test_binding_identity_changes_with_recording_time() -> None:
    first = _binding()
    later = build_notification_retry_readiness_binding(
        terminal_record=_terminal(),
        readiness_enforcement=_enforcement(),
        recorded_at=BASE_TIME + timedelta(minutes=6),
    )

    assert later["binding_id"] != first["binding_id"]


def test_initial_authority_cannot_bind_retry_terminal_history() -> None:
    with pytest.raises(ValidationError, match="retry readiness authority"):
        build_notification_retry_readiness_binding(
            terminal_record=_terminal(),
            readiness_enforcement=_enforcement(execution_kind="initial"),
            recorded_at=BASE_TIME + timedelta(minutes=5),
        )


def test_enforcement_must_occur_inside_terminal_execution_window() -> None:
    with pytest.raises(ValidationError, match="during the terminal execution window"):
        build_notification_retry_readiness_binding(
            terminal_record=_terminal(),
            readiness_enforcement=_enforcement(
                enforced_at=BASE_TIME + timedelta(seconds=30)
            ),
            recorded_at=BASE_TIME + timedelta(minutes=5),
        )


def test_requestful_terminal_history_requires_matching_lock_identity() -> None:
    with pytest.raises(ValidationError, match="retain delivery lock identity"):
        build_notification_retry_readiness_binding(
            terminal_record=_terminal(lock_key=None),
            readiness_enforcement=_enforcement(),
            recorded_at=BASE_TIME + timedelta(minutes=5),
        )

    with pytest.raises(ValidationError, match="lock fingerprints differ"):
        build_notification_retry_readiness_binding(
            terminal_record=_terminal(lock_key="another-lock-key"),
            readiness_enforcement=_enforcement(),
            recorded_at=BASE_TIME + timedelta(minutes=5),
        )


def test_binding_recording_must_follow_terminal_persistence() -> None:
    with pytest.raises(ValidationError, match="precedes terminal record persistence"):
        build_notification_retry_readiness_binding(
            terminal_record=_terminal(),
            readiness_enforcement=_enforcement(),
            recorded_at=BASE_TIME + timedelta(minutes=3, seconds=30),
        )


def test_binding_validation_rejects_digest_and_identity_tampering() -> None:
    for key in (
        "binding_id",
        "readiness_enforcement_sha256",
    ):
        tampered = copy.deepcopy(_binding())
        tampered[key] = "0" * 64 if key.endswith("sha256") else "tampered-binding"
        with pytest.raises(ValidationError):
            validate_notification_retry_readiness_binding(tampered)

    terminal_tampered = copy.deepcopy(_binding())
    terminal_tampered["terminal_execution"]["document_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="binding_id"):
        validate_notification_retry_readiness_binding(terminal_tampered)


def test_file_builder_reads_nested_enforcement_and_rejects_symlinks(
    tmp_path: Path,
) -> None:
    terminal_path = tmp_path / "terminal.json"
    execution_path = tmp_path / "execution.json"
    terminal_path.write_text(json.dumps(_terminal()), encoding="utf-8")
    execution_path.write_text(
        json.dumps({"execution_readiness": _enforcement()}),
        encoding="utf-8",
    )

    binding = build_retry_readiness_binding_from_files(
        terminal_record_path=terminal_path,
        execution_summary_path=execution_path,
        recorded_at=BASE_TIME + timedelta(minutes=5),
    )
    assert binding == _binding()

    link = tmp_path / "terminal-link.json"
    link.symlink_to(terminal_path)
    with pytest.raises(ValidationError, match="symbolic link"):
        build_retry_readiness_binding_from_files(
            terminal_record_path=link,
            execution_summary_path=execution_path,
            recorded_at=BASE_TIME + timedelta(minutes=5),
        )
