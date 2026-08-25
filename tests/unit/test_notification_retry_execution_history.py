from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration import run_recorded_portfolio_risk_notification_retries as recorded
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    LOCK_KEY_FINGERPRINT,
    LOCK_MODEL_VERSION,
    LOCK_SCOPE,
)
from src.warehouse.notification_retry_execution_contract import (
    build_notification_retry_execution_record,
    notification_retry_execution_document_sha256,
    validate_notification_retry_execution_record,
)

STARTED_AT = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
FINISHED_AT = STARTED_AT + timedelta(seconds=2)
CONFIGURATION = {
    "delivery_fingerprint": "delivery-fingerprint",
    "retry_policy_fingerprint": "retry-policy-fingerprint",
    "retry_execution_policy_fingerprint": "execution-policy-fingerprint",
}


def _execution_summary() -> dict[str, Any]:
    return {
        "execution_id": "execution-1",
        "model_version": "portfolio-risk-manual-retry-execution-v1",
        "request_id": "RETRY-1",
        "plan_id": "plan-1",
        "executed_at": STARTED_AT.isoformat(),
        "channel": "webhook",
        "endpoint": {
            "host": "alerts.example.test",
            "full_url_recorded": False,
        },
        "configuration": CONFIGURATION,
        "revalidation": {
            "performed": True,
            "current_plan_id": "plan-1",
            "events_checked": 1,
            "exact_event_evidence_unchanged": True,
        },
        "selection": {
            "planned_retryable_events": 1,
            "executed_events": 1,
            "max_events": 25,
        },
        "outcomes": [
            {
                "event_id": "event-1",
                "attempt_id": "attempt-1",
                "attempt_number": 2,
                "attempted_at": STARTED_AT.isoformat(),
                "error_code": None,
                "http_status": 204,
                "outcome": "succeeded",
                "payload_sha256": "a" * 64,
            }
        ],
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
            "model_version": LOCK_MODEL_VERSION,
            "scope": LOCK_SCOPE,
            "key_fingerprint": LOCK_KEY_FINGERPRINT,
        },
        "response_bodies_recorded": False,
        "plan_mutated": False,
        "acknowledgement_mutated": False,
        "dead_letter_mutated": False,
    }


def _completed_record() -> dict[str, Any]:
    return build_notification_retry_execution_record(
        request_id="RETRY-1",
        plan_id="plan-1",
        terminal_status="completed",
        started_at=STARTED_AT,
        finished_at=FINISHED_AT,
        execution_id="execution-1",
        delivery_fingerprint=CONFIGURATION["delivery_fingerprint"],
        retry_policy_fingerprint=CONFIGURATION["retry_policy_fingerprint"],
        retry_execution_policy_fingerprint=(
            CONFIGURATION["retry_execution_policy_fingerprint"]
        ),
        delivery_lock_model_version=LOCK_MODEL_VERSION,
        delivery_lock_key_fingerprint=LOCK_KEY_FINGERPRINT,
        requested_event_ids=["event-1"],
        persisted_event_ids=["event-1"],
        persisted_attempt_ids=["attempt-1"],
        execution=_execution_summary(),
    )


def test_completed_record_is_deterministic_and_secret_safe() -> None:
    first = _completed_record()
    second = _completed_record()

    assert first == second
    assert first["record_id"].startswith(
        "portfolio-risk-notification-retry-execution-record-v1-record-"
    )
    assert len(notification_retry_execution_document_sha256(first)) == 64
    assert validate_notification_retry_execution_record(first) == first
    assert "postgresql://" not in str(first)


def test_terminal_status_contracts_fail_closed() -> None:
    with pytest.raises(ValidationError, match="unpersisted requested event"):
        build_notification_retry_execution_record(
            request_id="RETRY-2",
            plan_id="plan-2",
            terminal_status="persistence_uncertain",
            started_at=STARTED_AT,
            finished_at=FINISHED_AT,
            failure_stage="attempt_persistence",
            failure_code="attempt_persistence_uncertain",
            delivery_fingerprint=CONFIGURATION["delivery_fingerprint"],
            retry_policy_fingerprint=CONFIGURATION["retry_policy_fingerprint"],
            retry_execution_policy_fingerprint=(
                CONFIGURATION["retry_execution_policy_fingerprint"]
            ),
            delivery_lock_model_version=LOCK_MODEL_VERSION,
            delivery_lock_key_fingerprint=LOCK_KEY_FINGERPRINT,
            requested_event_ids=["event-1"],
            persisted_event_ids=["event-1"],
            persisted_attempt_ids=["attempt-1"],
        )

    tampered = _completed_record()
    tampered["requested_event_ids"] = ["event-other"]
    with pytest.raises(ValidationError):
        validate_notification_retry_execution_record(tampered)


def _patch_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(recorded, "load_retry_plan", lambda _: {"plan_id": "plan-1"})
    monkeypatch.setattr(
        recorded,
        "load_retry_execution_contract",
        lambda _: (
            SimpleNamespace(fingerprint=CONFIGURATION["delivery_fingerprint"]),
            SimpleNamespace(fingerprint=CONFIGURATION["retry_policy_fingerprint"]),
            SimpleNamespace(
                fingerprint=CONFIGURATION["retry_execution_policy_fingerprint"]
            ),
        ),
    )


def _clock() -> Any:
    values = iter([STARTED_AT, FINISHED_AT])
    return lambda: next(values)


def _persistence(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "record_id": record["record_id"],
        "request_id": record["request_id"],
        "plan_id": record["plan_id"],
        "execution_id": record["execution_id"],
        "terminal_status": record["terminal_status"],
        "requested_event_count": len(record["requested_event_ids"]),
        "persisted_event_count": len(record["persisted_event_ids"]),
        "document_sha256": notification_retry_execution_document_sha256(record),
        "created": True,
    }


def test_recorded_wrapper_retains_completed_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_contracts(monkeypatch)
    records: list[dict[str, Any]] = []

    def executor(**kwargs: Any) -> dict[str, Any]:
        kwargs["transport"](
            "https://alerts.example.test/risk",
            b"{}",
            {"Idempotency-Key": "event-1"},
            5.0,
        )
        kwargs["attempt_writer"](
            {"event_id": "event-1", "attempt_id": "attempt-1"}
        )
        return _execution_summary()

    result = recorded.run_recorded_portfolio_risk_notification_retries(
        plan_path=Path("plan.json"),
        confirm_plan_id="plan-1",
        request_id="RETRY-1",
        config_path=Path("config.yaml"),
        dsn="dsn-not-retained",
        execute=True,
        attempt_writer=lambda _: None,
        transport=lambda *_: 204,
        clock=_clock(),
        executor=executor,
        existing_reader=lambda **_: None,
        recorder=lambda **kwargs: (
            records.append(kwargs["record"]) or _persistence(kwargs["record"])
        ),
    )

    assert result["record"]["terminal_status"] == "completed"
    assert result["record"]["requested_event_ids"] == ["event-1"]
    assert result["record"]["persisted_attempt_ids"] == ["attempt-1"]
    assert records == [result["record"]]
    assert "dsn-not-retained" not in str(result)


def test_recorded_wrapper_classifies_failures_without_exception_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_contracts(monkeypatch)

    def before_request(**_: Any) -> dict[str, Any]:
        raise ValidationError("arbitrary source detail")

    first = recorded.run_recorded_portfolio_risk_notification_retries(
        plan_path=Path("plan.json"),
        confirm_plan_id="plan-1",
        request_id="RETRY-1",
        config_path=Path("config.yaml"),
        dsn="dsn",
        execute=True,
        clock=_clock(),
        executor=before_request,
        existing_reader=lambda **_: None,
        recorder=lambda **kwargs: _persistence(kwargs["record"]),
    )
    assert first["record"]["terminal_status"] == "failed_before_request"
    assert first["record"]["failure_code"] == "validation_failed"
    assert "arbitrary source detail" not in str(first)

    def persistence_failure(_: Any) -> None:
        raise StorageError("write failed")

    def uncertain(**kwargs: Any) -> dict[str, Any]:
        kwargs["transport"](
            "https://alerts.example.test/risk",
            b"{}",
            {"Idempotency-Key": "event-1"},
            5.0,
        )
        kwargs["attempt_writer"](
            {"event_id": "event-1", "attempt_id": "attempt-1"}
        )
        raise AssertionError("unreachable")

    second = recorded.run_recorded_portfolio_risk_notification_retries(
        plan_path=Path("plan.json"),
        confirm_plan_id="plan-1",
        request_id="RETRY-2",
        config_path=Path("config.yaml"),
        dsn="dsn",
        execute=True,
        attempt_writer=persistence_failure,
        transport=lambda *_: 204,
        clock=_clock(),
        executor=uncertain,
        existing_reader=lambda **_: None,
        recorder=lambda **kwargs: _persistence(kwargs["record"]),
    )
    assert second["record"]["terminal_status"] == "persistence_uncertain"
    assert second["record"]["requested_event_ids"] == ["event-1"]
    assert second["record"]["persisted_event_ids"] == []
    assert second["record"]["failure_code"] == "attempt_persistence_uncertain"


def test_existing_request_replays_without_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_contracts(monkeypatch)
    existing = _completed_record()
    calls: list[bool] = []

    result = recorded.run_recorded_portfolio_risk_notification_retries(
        plan_path=Path("plan.json"),
        confirm_plan_id="plan-1",
        request_id="RETRY-1",
        config_path=Path("config.yaml"),
        dsn="dsn",
        execute=True,
        executor=lambda **_: calls.append(True) or _execution_summary(),
        existing_reader=lambda **_: existing,
    )

    assert calls == []
    assert result["replayed"] is True
    assert result["external_request_performed_this_invocation"] is False
    assert result["record"] == existing


def test_request_id_cannot_be_reused_for_another_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(recorded, "load_retry_plan", lambda _: {"plan_id": "plan-2"})
    with pytest.raises(ValidationError, match="different notification retry plan"):
        recorded.run_recorded_portfolio_risk_notification_retries(
            plan_path=Path("plan.json"),
            confirm_plan_id="plan-2",
            request_id="RETRY-1",
            config_path=Path("config.yaml"),
            dsn="dsn",
            execute=True,
            existing_reader=lambda **_: _completed_record(),
        )


def test_recorded_wrapper_requires_explicit_execute_before_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_contracts(monkeypatch)
    reads: list[bool] = []
    with pytest.raises(ValidationError, match="explicit --execute"):
        recorded.run_recorded_portfolio_risk_notification_retries(
            plan_path=Path("plan.json"),
            confirm_plan_id="plan-1",
            request_id="RETRY-1",
            config_path=Path("config.yaml"),
            dsn="dsn",
            execute=False,
            existing_reader=lambda **_: reads.append(True) or _completed_record(),
        )
    assert reads == []
