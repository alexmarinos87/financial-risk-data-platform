from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration.plan_portfolio_risk_notification_retries import (
    plan_portfolio_risk_notification_retries,
)
from src.orchestration.run_recorded_portfolio_risk_notification_retries import (
    RecordedRetryExecutionError,
    execute_and_record_portfolio_risk_notification_retries,
)
from src.warehouse.notification_retry_execution_contract import (
    build_retry_execution_record,
)

PLANNED_AT = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
EXECUTED_AT = PLANNED_AT + timedelta(minutes=1)


def _write_config(tmp_path: Path) -> Path:
    path = tmp_path / "notification-delivery.yaml"
    path.write_text(
        yaml.safe_dump(
            {
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
                        "max_event_age_seconds": 604800,
                        "max_backoff_seconds": 3600,
                        "retryable_http_statuses": [
                            408,
                            425,
                            429,
                            500,
                            502,
                            503,
                            504,
                        ],
                        "retryable_error_codes": ["network_error"],
                    },
                    "retry_execution": {
                        "enabled": True,
                        "max_plan_age_seconds": 3600,
                        "max_events": 25,
                    },
                }
            },
            sort_keys=False,
        ),
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


def _completed_summary(plan_id: str, request_id: str) -> dict[str, Any]:
    return {
        "execution_id": "portfolio-risk-manual-retry-execution-v1-execution-abc",
        "model_version": "portfolio-risk-manual-retry-execution-v1",
        "request_id": request_id,
        "plan_id": plan_id,
        "executed_at": EXECUTED_AT.isoformat(),
        "channel": "webhook",
        "endpoint": {"host": "alerts.example.test", "full_url_recorded": False},
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
                "attempted_at": EXECUTED_AT.isoformat(),
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


def _completed_record(plan_id: str, request_id: str) -> dict[str, Any]:
    summary = _completed_summary(plan_id, request_id)
    return build_retry_execution_record(
        request_id=request_id,
        plan_id=plan_id,
        started_at=EXECUTED_AT,
        finished_at=EXECUTED_AT,
        recorded_at=EXECUTED_AT,
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


def test_completed_request_replay_returns_history_before_side_effects(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    plan_path, plan = _write_plan(tmp_path, config_path)
    record = _completed_record(plan["plan_id"], "RETRY-2026-001")
    calls: list[str] = []

    result = execute_and_record_portfolio_risk_notification_retries(
        plan_path=plan_path,
        confirm_plan_id=plan["plan_id"],
        request_id="RETRY-2026-001",
        config_path=config_path,
        dsn="not-used",
        execute=True,
        history_reader=lambda **_: {
            "record": record,
            "history": {"record_id": record["record_id"], "created": False},
        },
        executor=lambda **_: calls.append("executor") or {},
        recorder=lambda **_: calls.append("recorder") or {},
        transport=lambda *_: calls.append("transport") or 204,
        attempt_writer=lambda _: calls.append("writer"),
    )

    assert calls == []
    assert result["history"] == {"record_id": record["record_id"], "created": False}
    assert result["execution_summary"] == record["execution_summary"]


def test_failed_request_replay_returns_same_terminal_failure_without_execution(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    plan_path, plan = _write_plan(tmp_path, config_path)
    record = build_retry_execution_record(
        request_id="RETRY-2026-002",
        plan_id=plan["plan_id"],
        started_at=EXECUTED_AT,
        finished_at=EXECUTED_AT,
        recorded_at=EXECUTED_AT,
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
    calls: list[str] = []

    with pytest.raises(RecordedRetryExecutionError) as raised:
        execute_and_record_portfolio_risk_notification_retries(
            plan_path=plan_path,
            confirm_plan_id=plan["plan_id"],
            request_id="RETRY-2026-002",
            config_path=config_path,
            dsn="not-used",
            execute=True,
            history_reader=lambda **_: {
                "record": record,
                "history": {"record_id": record["record_id"], "created": False},
            },
            executor=lambda **_: calls.append("executor") or {},
        )

    assert calls == []
    assert raised.value.failure_code == "validation_error"
    assert raised.value.history["record_id"] == record["record_id"]


def test_request_id_plan_conflict_and_missing_authority_fail_before_lookup(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    plan_path, plan = _write_plan(tmp_path, config_path)
    reads: list[str] = []

    with pytest.raises(ValidationError, match="explicit --execute"):
        execute_and_record_portfolio_risk_notification_retries(
            plan_path=plan_path,
            confirm_plan_id=plan["plan_id"],
            request_id="RETRY-2026-003",
            config_path=config_path,
            dsn="not-used",
            execute=False,
            history_reader=lambda **_: reads.append("read") or None,
        )
    with pytest.raises(ValidationError, match="confirm_plan_id"):
        execute_and_record_portfolio_risk_notification_retries(
            plan_path=plan_path,
            confirm_plan_id="wrong-plan",
            request_id="RETRY-2026-003",
            config_path=config_path,
            dsn="not-used",
            execute=True,
            history_reader=lambda **_: reads.append("read") or None,
        )
    assert reads == []

    conflicting = build_retry_execution_record(
        request_id="RETRY-2026-003",
        plan_id="different-plan",
        started_at=EXECUTED_AT,
        finished_at=EXECUTED_AT,
        recorded_at=EXECUTED_AT,
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
    with pytest.raises(ValidationError, match="different notification retry plan"):
        execute_and_record_portfolio_risk_notification_retries(
            plan_path=plan_path,
            confirm_plan_id=plan["plan_id"],
            request_id="RETRY-2026-003",
            config_path=config_path,
            dsn="not-used",
            execute=True,
            history_reader=lambda **_: {
                "record": conflicting,
                "history": {"record_id": conflicting["record_id"], "created": False},
            },
            executor=lambda **_: (_ for _ in ()).throw(
                AssertionError("executor must not run")
            ),
        )
