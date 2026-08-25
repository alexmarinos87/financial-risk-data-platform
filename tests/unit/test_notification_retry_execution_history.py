from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.plan_portfolio_risk_notification_retries import (
    plan_portfolio_risk_notification_retries,
)
from src.orchestration.run_recorded_portfolio_risk_notification_retries import (
    RecordedRetryExecutionError,
    _write_summary,
    execute_and_record_portfolio_risk_notification_retries,
)
from src.warehouse.notification_retry_execution_contract import (
    MODEL_VERSION,
    build_retry_execution_record,
    validate_retry_execution_record,
)

PLANNED_AT = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
STARTED_AT = PLANNED_AT + timedelta(minutes=1)


def _config_payload() -> dict[str, Any]:
    return {
        "delivery": {
            "webhook": {
                "enabled": True,
                "endpoint_env": "RISK_NOTIFICATION_WEBHOOK_URL",
                "timeout_seconds": 5,
                "max_batch_events": 25,
                "max_attempts_per_event": 3,
                "initial_backoff_seconds": 1,
            },
            "retry_planning": {
                "max_candidate_rows": 500,
                "max_plan_events": 25,
                "max_event_age_seconds": 7 * 24 * 60 * 60,
                "max_backoff_seconds": 3600,
                "retryable_http_statuses": [408, 425, 429, 500, 502, 503, 504],
                "retryable_error_codes": ["network_error"],
            },
            "retry_execution": {
                "enabled": True,
                "max_plan_age_seconds": 3600,
                "max_events": 25,
            },
        }
    }


def _write_config(tmp_path: Path) -> Path:
    path = tmp_path / "notification-delivery.yaml"
    path.write_text(
        yaml.safe_dump(_config_payload(), sort_keys=False),
        encoding="utf-8",
    )
    return path


def _candidate() -> dict[str, Any]:
    event_time = PLANNED_AT - timedelta(hours=2)
    return {
        "event_id": "event-1",
        "outbox_model_version": "portfolio-risk-notification-outbox-v1",
        "event_type": "breach_opened",
        "transition_type": "opened",
        "delivery_disposition": "pending",
        "source_evaluation_calculation_id": "evaluation-event-1",
        "policy_id": "us-tech-standard",
        "policy_fingerprint": "policy-fingerprint",
        "portfolio_id": "us-tech-equal",
        "definition_fingerprint": "definition-fingerprint",
        "metric_name": "portfolio_volatility_annualized",
        "subject_type": "portfolio",
        "subject_key": "us-tech-equal",
        "current_status": "critical",
        "ts_event": event_time,
        "ts_ingest": event_time + timedelta(seconds=1),
        "payload_json": {"event_id": "event-1", "severity": "critical"},
        "attempt_count": 1,
        "last_attempt_id": "attempt-event-1-1",
        "last_attempt_number": 1,
        "last_attempted_at": PLANNED_AT - timedelta(minutes=10),
        "last_attempt_outcome": "failed",
        "last_http_status": 503,
        "last_error_code": "http_503",
        "acknowledgement_id": None,
        "acknowledged_at": None,
        "acknowledgement_disposition": None,
    }


def _write_plan(tmp_path: Path, config_path: Path) -> tuple[Path, dict[str, Any]]:
    plan = plan_portfolio_risk_notification_retries(
        config_path=config_path,
        dsn="not-used",
        planned_at=PLANNED_AT,
        policy_id="us-tech-standard",
        portfolio_id="us-tech-equal",
        reader=lambda **_: [_candidate()],
    )
    path = tmp_path / "retry-plan.json"
    path.write_text(json.dumps(plan, sort_keys=True), encoding="utf-8")
    return path, plan


def _clock(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


def _completed_summary(
    *,
    plan_id: str,
    request_id: str,
    executed_at: datetime,
) -> dict[str, Any]:
    return {
        "execution_id": "portfolio-risk-manual-retry-execution-v1-execution-abc",
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
            "delivery_fingerprint": "delivery-fingerprint",
            "retry_execution_policy_fingerprint": "execution-fingerprint",
            "retry_policy_fingerprint": "retry-fingerprint",
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
        "outcomes": [
            {
                "attempt_id": "attempt-event-1-2",
                "attempt_number": 2,
                "attempted_at": executed_at.isoformat(),
                "error_code": None,
                "event_id": "event-1",
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
            "model_version": "portfolio-risk-notification-delivery-lock-v1",
            "scope": "portfolio-risk-notification-delivery",
            "key_fingerprint": "lock-fingerprint",
        },
        "response_bodies_recorded": False,
        "plan_mutated": False,
        "acknowledgement_mutated": False,
        "dead_letter_mutated": False,
    }


def test_completed_record_is_deterministic_and_canonical() -> None:
    summary = _completed_summary(
        plan_id="plan-1",
        request_id="request-1",
        executed_at=STARTED_AT,
    )
    kwargs = {
        "request_id": "request-1",
        "plan_id": "plan-1",
        "started_at": STARTED_AT,
        "finished_at": STARTED_AT + timedelta(seconds=1),
        "recorded_at": STARTED_AT + timedelta(seconds=2),
        "terminal_status": "completed",
        "failure_code": None,
        "request_count": 1,
        "attempts_persisted": 1,
        "succeeded_count": 1,
        "failed_count": 0,
        "attempt_ids": ["attempt-event-1-2"],
        "requested_event_ids": ["event-1"],
        "persisted_event_ids": ["event-1"],
        "execution_summary": summary,
    }
    first = build_retry_execution_record(**kwargs)
    second = build_retry_execution_record(**kwargs)

    assert first == second
    assert first["model_version"] == MODEL_VERSION
    assert validate_retry_execution_record(first) == first
    assert first["terminal_status"] == "completed"


def test_failure_statuses_preserve_ambiguous_requested_event_identity() -> None:
    before = build_retry_execution_record(
        request_id="request-before",
        plan_id="plan-1",
        started_at=STARTED_AT,
        finished_at=STARTED_AT,
        recorded_at=STARTED_AT,
        terminal_status="failed_before_request",
        failure_code="validation_error",
        request_count=0,
        attempts_persisted=0,
        succeeded_count=0,
        failed_count=0,
        attempt_ids=[],
        requested_event_ids=[],
        persisted_event_ids=[],
        execution_summary=None,
    )
    assert before["terminal_status"] == "failed_before_request"

    uncertain = build_retry_execution_record(
        request_id="request-uncertain",
        plan_id="plan-1",
        started_at=STARTED_AT,
        finished_at=STARTED_AT,
        recorded_at=STARTED_AT,
        terminal_status="persistence_uncertain",
        failure_code="storage_error",
        request_count=1,
        attempts_persisted=0,
        succeeded_count=0,
        failed_count=0,
        attempt_ids=[],
        requested_event_ids=["event-1"],
        persisted_event_ids=[],
        execution_summary=None,
        endpoint_host="alerts.example.test",
        delivery_fingerprint="delivery-fingerprint",
        retry_policy_fingerprint="retry-fingerprint",
        retry_execution_policy_fingerprint="execution-fingerprint",
        lock_model_version="portfolio-risk-notification-delivery-lock-v1",
        lock_key_fingerprint="lock-fingerprint",
        lock_acquired=True,
        lock_released=None,
    )
    assert uncertain["requested_event_ids"] == ["event-1"]
    assert uncertain["persisted_event_ids"] == []

    with pytest.raises(ValidationError, match="request without attempt"):
        build_retry_execution_record(
            request_id="request-invalid",
            plan_id="plan-1",
            started_at=STARTED_AT,
            finished_at=STARTED_AT,
            recorded_at=STARTED_AT,
            terminal_status="persistence_uncertain",
            failure_code="storage_error",
            request_count=1,
            attempts_persisted=1,
            succeeded_count=1,
            failed_count=0,
            attempt_ids=["attempt-1"],
            requested_event_ids=["event-1"],
            persisted_event_ids=["event-1"],
            execution_summary=None,
        )


def test_completed_summary_rejects_unknown_fields() -> None:
    summary = _completed_summary(
        plan_id="plan-1",
        request_id="request-1",
        executed_at=STARTED_AT,
    )
    summary["unknown"] = True
    with pytest.raises(ValidationError, match="unknown"):
        build_retry_execution_record(
            request_id="request-1",
            plan_id="plan-1",
            started_at=STARTED_AT,
            finished_at=STARTED_AT,
            recorded_at=STARTED_AT,
            terminal_status="completed",
            failure_code=None,
            request_count=1,
            attempts_persisted=1,
            succeeded_count=1,
            failed_count=0,
            attempt_ids=["attempt-event-1-2"],
            requested_event_ids=["event-1"],
            persisted_event_ids=["event-1"],
            execution_summary=summary,
        )


def test_recorded_wrapper_persists_completed_execution(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    plan_path, plan = _write_plan(tmp_path, config_path)
    records: list[dict[str, Any]] = []

    def executor(**kwargs: Any) -> dict[str, Any]:
        started = kwargs["clock"]()
        kwargs["transport"](
            "https://alerts.example.test/risk",
            b"{}",
            {"Idempotency-Key": "event-1"},
            5.0,
        )
        kwargs["attempt_writer"](
            {
                "attempt_id": "attempt-event-1-2",
                "event_id": "event-1",
                "outcome": "succeeded",
            }
        )
        return _completed_summary(
            plan_id=plan["plan_id"],
            request_id="RETRY-2026-001",
            executed_at=started,
        )

    result = execute_and_record_portfolio_risk_notification_retries(
        plan_path=plan_path,
        confirm_plan_id=plan["plan_id"],
        request_id="RETRY-2026-001",
        config_path=config_path,
        dsn="postgresql://secret-value",
        execute=True,
        environment={"RISK_NOTIFICATION_WEBHOOK_URL": "https://example.test"},
        executor=executor,
        recorder=lambda **kwargs: (
            records.append(validate_retry_execution_record(kwargs["record"]))
            or {
                "record_id": kwargs["record"]["record_id"],
                "created": True,
            }
        ),
        transport=lambda *_: 204,
        attempt_writer=lambda _: None,
        clock=_clock(
            STARTED_AT,
            STARTED_AT + timedelta(seconds=1),
            STARTED_AT + timedelta(seconds=2),
        ),
    )

    assert records[0]["terminal_status"] == "completed"
    assert records[0]["attempt_ids"] == ["attempt-event-1-2"]
    assert records[0]["requested_event_ids"] == ["event-1"]
    assert result["history"]["created"] is True
    assert "postgresql://secret-value" not in json.dumps(result)


def test_recorded_wrapper_classifies_failure_boundaries(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    plan_path, plan = _write_plan(tmp_path, config_path)

    def run(executor: Any, request_id: str) -> dict[str, Any]:
        records: list[dict[str, Any]] = []
        with pytest.raises(RecordedRetryExecutionError) as raised:
            execute_and_record_portfolio_risk_notification_retries(
                plan_path=plan_path,
                confirm_plan_id=plan["plan_id"],
                request_id=request_id,
                config_path=config_path,
                dsn="dsn",
                execute=True,
                executor=executor,
                recorder=lambda **kwargs: (
                    records.append(validate_retry_execution_record(kwargs["record"]))
                    or {
                        "record_id": kwargs["record"]["record_id"],
                        "created": True,
                    }
                ),
                transport=lambda *_: 204,
                attempt_writer=lambda _: None,
                clock=_clock(STARTED_AT, STARTED_AT, STARTED_AT),
            )
        assert raised.value.history["created"] is True
        return records[0]

    before = run(
        lambda **_: (_ for _ in ()).throw(ValidationError("bad")),
        "REQUEST-BEFORE",
    )
    assert before["terminal_status"] == "failed_before_request"

    def uncertain_executor(**kwargs: Any) -> dict[str, Any]:
        kwargs["transport"](
            "https://alerts.example.test/risk",
            b"{}",
            {"Idempotency-Key": "event-1"},
            5.0,
        )
        raise StorageError("write uncertain")

    uncertain = run(uncertain_executor, "REQUEST-UNCERTAIN")
    assert uncertain["terminal_status"] == "persistence_uncertain"
    assert uncertain["requested_event_ids"] == ["event-1"]
    assert uncertain["persisted_event_ids"] == []
    assert uncertain["endpoint_host"] is None

    def after_executor(**kwargs: Any) -> dict[str, Any]:
        kwargs["transport"](
            "https://alerts.example.test/risk",
            b"{}",
            {"Idempotency-Key": "event-1"},
            5.0,
        )
        kwargs["attempt_writer"](
            {
                "attempt_id": "attempt-event-1-2",
                "event_id": "event-1",
                "outcome": "failed",
            }
        )
        raise ValidationError("later failure")

    after = run(after_executor, "REQUEST-AFTER")
    assert after["terminal_status"] == "failed_after_request"
    assert after["failed_count"] == 1
    assert after["requested_event_ids"] == after["persisted_event_ids"]


def test_recorded_summary_writer_rejects_symbolic_links(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "summary.json"
    link.symlink_to(target)
    with pytest.raises(StorageError, match="symbolic link"):
        _write_summary(link, {"safe": True})
