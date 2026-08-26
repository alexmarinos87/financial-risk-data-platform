from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from src.orchestration.plan_portfolio_risk_notification_retries import (
    plan_portfolio_risk_notification_retries,
)
from src.orchestration.run_recorded_portfolio_risk_notification_retries import (
    execute_and_record_portfolio_risk_notification_retries,
)
from src.warehouse.notification_retry_destination_binding_contract import (
    validate_retry_destination_binding,
)
from src.warehouse.notification_retry_execution_contract import (
    validate_retry_execution_record,
)

PLANNED_AT = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
STARTED_AT = PLANNED_AT + timedelta(minutes=1)


def _config(tmp_path: Path) -> Path:
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
                        "retryable_http_statuses": [503],
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


def _authority() -> dict[str, Any]:
    return {
        "authority_id": "portfolio-risk-notification-destination-authority-v1-authority-test",
        "destination_fingerprint": "portfolio-risk-notification-destination-v1-test",
        "destination_id": "risk-operations-webhook",
        "endpoint_environment_variable": "RISK_NOTIFICATION_WEBHOOK_URL",
        "evaluated_at": STARTED_AT.isoformat(),
        "evaluated_event_types": ["breach_opened"],
        "model_version": "portfolio-risk-notification-destination-authority-v1",
        "channel": "webhook",
        "activation": {
            "enabled": True,
            "status": "active",
            "change_request_id": "CHANGE-DESTINATION-001",
            "reviewed_at": "2026-01-01T00:00:00+00:00",
            "review_expires_at": "2026-12-31T00:00:00+00:00",
        },
        "allowed_event_types": ["breach_opened"],
        "active": True,
        "endpoint_value_recorded": False,
        "external_request_performed": False,
        "delivery_attempt_written": False,
        "outbox_mutated": False,
        "acknowledgement_mutated": False,
    }


def test_recorded_wrapper_retains_terminal_and_destination_evidence(
    tmp_path: Path,
) -> None:
    config_path = _config(tmp_path)
    candidate = _candidate()
    plan = plan_portfolio_risk_notification_retries(
        config_path=config_path,
        dsn="not-used",
        planned_at=PLANNED_AT,
        policy_id="us-tech-standard",
        portfolio_id="us-tech-equal",
        reader=lambda **_: [candidate],
    )
    plan_path = tmp_path / "retry-plan.json"
    plan_path.write_text(json.dumps(plan, sort_keys=True), encoding="utf-8")
    terminal_records: list[dict[str, Any]] = []
    bindings: list[dict[str, Any]] = []
    times = iter(
        [
            STARTED_AT,
            STARTED_AT + timedelta(seconds=1),
            STARTED_AT + timedelta(seconds=2),
        ]
    )

    def executor(**kwargs: Any) -> dict[str, Any]:
        executed_at = kwargs["clock"]()
        authority = _authority()
        kwargs["destination_authority_observer"](authority)
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
        return {
            "execution_id": "portfolio-risk-manual-retry-execution-v1-execution-test",
            "model_version": "portfolio-risk-manual-retry-execution-v1",
            "request_id": "RETRY-DESTINATION-001",
            "plan_id": plan["plan_id"],
            "executed_at": executed_at.isoformat(),
            "channel": "webhook",
            "destination_authority": authority,
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
                "current_plan_id": plan["plan_id"],
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

    result = execute_and_record_portfolio_risk_notification_retries(
        plan_path=plan_path,
        confirm_plan_id=plan["plan_id"],
        request_id="RETRY-DESTINATION-001",
        config_path=config_path,
        dsn="postgresql://secret-value",
        execute=True,
        environment={"RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.test/risk"},
        executor=executor,
        recorder=lambda **kwargs: (
            terminal_records.append(
                validate_retry_execution_record(kwargs["record"])
            )
            or {
                "record_id": kwargs["record"]["record_id"],
                "created": True,
            }
        ),
        destination_binding_recorder=lambda **kwargs: (
            bindings.append(validate_retry_destination_binding(kwargs["binding"]))
            or {
                "binding_id": kwargs["binding"]["binding_id"],
                "record_id": kwargs["binding"]["record_id"],
                "created": True,
            }
        ),
        transport=lambda *_: 204,
        attempt_writer=lambda _: None,
        clock=lambda: next(times),
    )

    assert terminal_records[0]["execution_summary"].get(
        "destination_authority"
    ) is None
    assert bindings[0]["record_id"] == terminal_records[0]["record_id"]
    assert bindings[0]["destination_authority"] == _authority()
    assert result["destination_history"]["created"] is True
    assert "postgresql://secret-value" not in json.dumps(result)
