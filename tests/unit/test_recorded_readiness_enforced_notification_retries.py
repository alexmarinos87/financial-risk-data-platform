from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.run_recorded_readiness_enforced_portfolio_risk_notification_retries import (
    RecordedReadinessRetryExecutionError,
    execute_and_record_readiness_enforced_portfolio_risk_notification_retries,
)
from src.warehouse.notification_execution_readiness_enforcement import (
    _enforcement_evidence,
)
from src.warehouse.notification_retry_execution_contract import (
    build_retry_execution_record,
)
from src.warehouse.notification_retry_governance_bundle_recorder import (
    validate_notification_retry_governance_bundle,
)
from src.warehouse.notification_retry_readiness_binding_contract import (
    build_notification_retry_readiness_binding,
)

BASE_TIME = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
CONFIG_PATH = Path("config/notification_delivery.yaml")
DESTINATION_PATH = Path("config/notification_destinations.yaml")
DESTINATION_ID = "risk-operations-webhook"
PLAN_ID = "recorded-readiness-plan-1"
REQUEST_ID = "RECORDED-READINESS-REQUEST-1"
LOCK_MODEL = "portfolio-risk-notification-delivery-lock-v1"
LOCK_SCOPE = "portfolio-risk-notification-delivery"
LOCK_KEY = "recorded-readiness-lock-key"
PAYLOAD_SHA256 = "0" * 64


def _clock(*values: datetime):
    remaining = iter(values)
    return lambda: next(remaining)


def _plan_loader(_: Path) -> dict[str, Any]:
    return {"plan_id": PLAN_ID}


def _readiness(*, enforced_at: datetime = BASE_TIME) -> dict[str, Any]:
    return _enforcement_evidence(
        destination_id=DESTINATION_ID,
        execution_kind="retry",
        enforced_at=enforced_at,
        record={
            "record_id": "recorded-readiness-record-1",
            "request_id": "recorded-readiness-decision-request-1",
            "decision": {
                "decision_id": "recorded-retained-decision-1",
                "evaluated_at": (enforced_at - timedelta(minutes=1)).isoformat(),
            },
        },
        refreshed_decision={
            "decision_id": "recorded-refreshed-decision-1",
            "evaluated_at": enforced_at.isoformat(),
        },
        lock={
            "key_fingerprint": LOCK_KEY,
            "model_version": LOCK_MODEL,
            "scope": LOCK_SCOPE,
        },
    )


def _authority(*, evaluated_at: datetime = BASE_TIME) -> dict[str, Any]:
    return {
        "authority_id": "recorded-destination-authority-1",
        "destination_fingerprint": "recorded-destination-fingerprint-1",
        "destination_id": DESTINATION_ID,
        "endpoint_environment_variable": "RISK_NOTIFICATION_WEBHOOK_URL",
        "evaluated_at": evaluated_at.isoformat(),
        "evaluated_event_types": ["breach_opened"],
        "model_version": "portfolio-risk-notification-destination-authority-v1",
        "channel": "webhook",
        "activation": {
            "enabled": True,
            "status": "active",
            "change_request_id": "CHG-RECORDED-READINESS",
            "reviewed_at": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
            "review_expires_at": datetime(2027, 1, 1, tzinfo=timezone.utc).isoformat(),
        },
        "allowed_event_types": [
            "breach_escalated",
            "breach_opened",
            "breach_resolved",
        ],
        "active": True,
        "endpoint_value_recorded": False,
        "external_request_performed": False,
        "delivery_attempt_written": False,
        "outbox_mutated": False,
        "acknowledgement_mutated": False,
    }


def _base_summary(
    *,
    executed_at: datetime,
    request_id: str = REQUEST_ID,
    plan_id: str = PLAN_ID,
) -> dict[str, Any]:
    outcome = {
        "attempt_id": f"{request_id}-attempt-1",
        "attempt_number": 1,
        "attempted_at": executed_at.isoformat(),
        "error_code": None,
        "event_id": f"{request_id}-event-1",
        "http_status": 200,
        "outcome": "succeeded",
        "payload_sha256": PAYLOAD_SHA256,
    }
    return {
        "execution_id": f"{request_id}-execution",
        "model_version": "portfolio-risk-manual-retry-execution-v1",
        "request_id": request_id,
        "plan_id": plan_id,
        "executed_at": executed_at.isoformat(),
        "channel": "webhook",
        "endpoint": {
            "host": "alerts.example.test",
            "full_url_recorded": False,
        },
        "configuration": {
            "delivery_fingerprint": "recorded-delivery-fingerprint-1",
            "retry_execution_policy_fingerprint": (
                "recorded-retry-execution-fingerprint-1"
            ),
            "retry_policy_fingerprint": "recorded-retry-policy-fingerprint-1",
        },
        "revalidation": {
            "performed": True,
            "current_plan_id": plan_id,
            "events_checked": 1,
            "exact_event_evidence_unchanged": True,
        },
        "selection": {
            "planned_retryable_events": 1,
            "executed_events": 1,
            "max_events": 25,
        },
        "outcomes": [outcome],
        "outcome_counts": {"succeeded": 1, "failed": 0},
        "execution": {
            "requested": True,
            "performed": True,
            "external_requests_performed": 1,
            "delivery_attempts_written": 1,
        },
        "concurrency_control": {
            "performed": True,
            "acquired": True,
            "released": True,
            "held_through_revalidation": True,
            "held_through_attempt_persistence": True,
            "model_version": LOCK_MODEL,
            "scope": LOCK_SCOPE,
            "key_fingerprint": LOCK_KEY,
        },
        "response_bodies_recorded": False,
        "plan_mutated": False,
        "acknowledgement_mutated": False,
        "dead_letter_mutated": False,
    }


def _successful_executor(**kwargs: Any) -> dict[str, Any]:
    enforced_at = kwargs["clock"]()
    readiness = kwargs["readiness_enforcer"](evaluated_at=enforced_at)
    executed_at = kwargs["clock"]()
    authority = _authority(evaluated_at=executed_at)
    kwargs["destination_authority_observer"](authority)
    event_id = f"{kwargs['request_id']}-event-1"
    status = kwargs["transport"](
        "https://alerts.example.test/hook",
        b"{}",
        {"Idempotency-Key": event_id},
        5.0,
    )
    attempt = {
        "attempt_id": f"{kwargs['request_id']}-attempt-1",
        "event_id": event_id,
        "outcome": "succeeded",
    }
    kwargs["attempt_writer"](attempt)
    summary = _base_summary(
        executed_at=executed_at,
        request_id=kwargs["request_id"],
        plan_id=kwargs["confirm_plan_id"],
    )
    summary["outcomes"][0]["http_status"] = status
    summary["destination_authority"] = authority
    summary["execution_readiness"] = readiness
    summary["governed_execution"] = {
        "model_version": "portfolio-risk-notification-retry-readiness-enforced-v1",
        "plan_id": kwargs["confirm_plan_id"],
        "destination_id": kwargs["destination_id"],
        "readiness_enforcement_id": readiness["enforcement_id"],
        "single_physical_lock_acquisition": True,
        "nested_lock_reacquisition_performed": False,
        "lock_reused_by_retry_executor": True,
        "outer_lock_released": True,
    }
    return summary


def _bundle_recorder(calls: list[tuple[dict[str, Any], dict[str, Any]]]):
    def record(**kwargs: Any) -> dict[str, Any]:
        terminal, binding = validate_notification_retry_governance_bundle(
            terminal_record=kwargs["terminal_record"],
            readiness_binding=kwargs["readiness_binding"],
        )
        calls.append((terminal, binding))
        return {
            "terminal_history": {
                "record_id": terminal["record_id"],
                "terminal_status": terminal["terminal_status"],
                "created": True,
            },
            "readiness_history": {
                "binding_id": binding["binding_id"],
                "terminal_record_id": terminal["record_id"],
                "created": True,
            },
            "created": True,
            "atomic_commit": True,
        }

    return record


def _execute(**overrides: Any) -> dict[str, Any]:
    arguments: dict[str, Any] = {
        "plan_path": Path("retry-plan.json"),
        "confirm_plan_id": PLAN_ID,
        "request_id": REQUEST_ID,
        "config_path": CONFIG_PATH,
        "destination_config_path": DESTINATION_PATH,
        "destination_id": DESTINATION_ID,
        "dsn": "postgresql://unit-test",
        "execute": True,
        "environment": {
            "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.example.test/hook"
        },
        "plan_loader": _plan_loader,
        "history_reader": lambda **_: None,
        "readiness_binding_reader": lambda **_: None,
        "destination_binding_reader": lambda **_: None,
    }
    arguments.update(overrides)
    return execute_and_record_readiness_enforced_portfolio_risk_notification_retries(
        **arguments
    )


def test_successful_execution_atomically_records_terminal_and_readiness() -> None:
    bundle_calls: list[tuple[dict[str, Any], dict[str, Any]]] = []
    destination_bindings: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []

    result = _execute(
        executor=_successful_executor,
        readiness_enforcer=lambda **_: _readiness(),
        transport=lambda *_: 200,
        attempt_writer=lambda attempt: attempts.append(dict(attempt)),
        bundle_recorder=_bundle_recorder(bundle_calls),
        destination_binding_recorder=lambda **kwargs: (
            destination_bindings.append(dict(kwargs["binding"]))
            or {"binding_id": kwargs["binding"]["binding_id"]}
        ),
        clock=_clock(
            BASE_TIME,
            BASE_TIME + timedelta(minutes=3),
            BASE_TIME + timedelta(minutes=4),
            BASE_TIME + timedelta(minutes=5),
        ),
    )

    assert result["atomic_commit"] is True
    assert result["replayed"] is False
    assert len(attempts) == 1
    assert len(bundle_calls) == 1
    terminal, binding = bundle_calls[0]
    assert terminal["terminal_status"] == "completed"
    assert terminal["request_count"] == 1
    assert terminal["attempts_persisted"] == 1
    assert binding["terminal_execution"]["record_id"] == terminal["record_id"]
    assert binding["readiness_enforcement"]["execution_kind"] == "retry"
    assert len(destination_bindings) == 1


def test_readiness_rejection_occurs_before_transport_and_history() -> None:
    transports: list[bool] = []
    bundles: list[tuple[dict[str, Any], dict[str, Any]]] = []

    def reject_readiness(**_: Any) -> dict[str, Any]:
        raise ValidationError("readiness decision is blocked")

    with pytest.raises(ValidationError, match="blocked"):
        _execute(
            executor=_successful_executor,
            readiness_enforcer=reject_readiness,
            transport=lambda *_: transports.append(True) or 200,
            attempt_writer=lambda _: None,
            bundle_recorder=_bundle_recorder(bundles),
            destination_binding_recorder=lambda **_: {},
            clock=_clock(BASE_TIME),
        )

    assert transports == []
    assert bundles == []


def test_request_without_attempt_evidence_records_persistence_uncertainty() -> None:
    bundle_calls: list[tuple[dict[str, Any], dict[str, Any]]] = []

    def failing_executor(**kwargs: Any) -> dict[str, Any]:
        enforced_at = kwargs["clock"]()
        kwargs["readiness_enforcer"](evaluated_at=enforced_at)
        kwargs["clock"]()
        kwargs["transport"](
            "https://alerts.example.test/hook",
            b"{}",
            {"Idempotency-Key": f"{kwargs['request_id']}-event-1"},
            5.0,
        )
        raise StorageError("attempt persistence unavailable")

    with pytest.raises(RecordedReadinessRetryExecutionError) as captured:
        _execute(
            executor=failing_executor,
            readiness_enforcer=lambda **_: _readiness(),
            transport=lambda *_: 200,
            attempt_writer=lambda _: None,
            bundle_recorder=_bundle_recorder(bundle_calls),
            destination_binding_recorder=lambda **_: {},
            clock=_clock(
                BASE_TIME,
                BASE_TIME + timedelta(minutes=3),
                BASE_TIME + timedelta(minutes=4),
                BASE_TIME + timedelta(minutes=5),
            ),
        )

    assert captured.value.failure_code == "storage_error"
    assert len(bundle_calls) == 1
    terminal, binding = bundle_calls[0]
    assert terminal["terminal_status"] == "persistence_uncertain"
    assert terminal["request_count"] == 1
    assert terminal["attempts_persisted"] == 0
    assert binding["terminal_execution"]["record_id"] == terminal["record_id"]


def _retained_completed_bundle() -> tuple[dict[str, Any], dict[str, Any]]:
    readiness = _readiness(enforced_at=BASE_TIME + timedelta(minutes=1))
    summary = _base_summary(executed_at=BASE_TIME)
    terminal = build_retry_execution_record(
        request_id=REQUEST_ID,
        plan_id=PLAN_ID,
        started_at=BASE_TIME,
        finished_at=BASE_TIME + timedelta(minutes=3),
        recorded_at=BASE_TIME + timedelta(minutes=4),
        terminal_status="completed",
        failure_code=None,
        request_count=1,
        attempts_persisted=1,
        succeeded_count=1,
        failed_count=0,
        attempt_ids=[f"{REQUEST_ID}-attempt-1"],
        requested_event_ids=[f"{REQUEST_ID}-event-1"],
        persisted_event_ids=[f"{REQUEST_ID}-event-1"],
        execution_summary=summary,
    )
    binding = build_notification_retry_readiness_binding(
        terminal_record=terminal,
        readiness_enforcement=readiness,
        recorded_at=BASE_TIME + timedelta(minutes=5),
    )
    return terminal, binding


def test_exact_replay_returns_retained_evidence_without_another_request() -> None:
    terminal, binding = _retained_completed_bundle()
    executions: list[bool] = []

    result = _execute(
        history_reader=lambda **_: {
            "record": terminal,
            "history": {"record_id": terminal["record_id"]},
        },
        readiness_binding_reader=lambda **_: {
            "binding": binding,
            "history": {"binding_id": binding["binding_id"]},
        },
        destination_binding_reader=lambda **_: None,
        executor=lambda **_: executions.append(True) or {},
    )

    assert executions == []
    assert result["replayed"] is True
    assert result["external_request_replayed"] is False
    assert result["terminal_history"]["record_id"] == terminal["record_id"]
    assert result["readiness_history"]["binding_id"] == binding["binding_id"]
    assert result["execution_summary"]["execution_readiness"] == (
        binding["readiness_enforcement"]
    )


def test_replay_rejects_terminal_history_without_readiness_binding() -> None:
    terminal, _ = _retained_completed_bundle()
    executions: list[bool] = []

    with pytest.raises(ValidationError, match="replay is blocked"):
        _execute(
            history_reader=lambda **_: {
                "record": terminal,
                "history": {"record_id": terminal["record_id"]},
            },
            readiness_binding_reader=lambda **_: None,
            executor=lambda **_: executions.append(True) or {},
        )

    assert executions == []
