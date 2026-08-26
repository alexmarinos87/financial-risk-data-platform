from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration.execute_portfolio_risk_notification_retries import (
    execute_portfolio_risk_notification_retries,
)
from src.orchestration.plan_portfolio_risk_notification_retries import (
    plan_portfolio_risk_notification_retries,
)
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    LOCK_KEY_FINGERPRINT,
    LOCK_MODEL_VERSION,
    LOCK_SCOPE,
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


def _write_plan(
    tmp_path: Path,
    config_path: Path,
    candidate: dict[str, Any],
) -> tuple[Path, dict[str, Any]]:
    plan = plan_portfolio_risk_notification_retries(
        config_path=config_path,
        dsn="not-used",
        planned_at=PLANNED_AT,
        policy_id="us-tech-standard",
        portfolio_id="us-tech-equal",
        reader=lambda **_: [candidate],
    )
    path = tmp_path / "retry-plan.json"
    path.write_text(json.dumps(plan, sort_keys=True), encoding="utf-8")
    return path, plan


def _authority(**kwargs: Any) -> dict[str, Any]:
    return {
        "authority_id": "portfolio-risk-notification-destination-authority-v1-authority-test",
        "destination_fingerprint": "portfolio-risk-notification-destination-v1-test",
        "destination_id": kwargs["destination_id"],
        "endpoint_environment_variable": kwargs["delivery_endpoint_env"],
        "evaluated_at": EXECUTED_AT.isoformat(),
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


def test_authority_is_refreshed_under_lock_before_transport(tmp_path: Path) -> None:
    config = _write_config(tmp_path)
    candidate = _candidate()
    plan_path, plan = _write_plan(tmp_path, config, candidate)
    lock_active = False
    order: list[str] = []
    observed: list[dict[str, Any]] = []

    @contextmanager
    def lock(**_: Any) -> Iterator[Mapping[str, Any]]:
        nonlocal lock_active
        lock_active = True
        order.append("lock")
        try:
            yield {
                "model_version": LOCK_MODEL_VERSION,
                "scope": LOCK_SCOPE,
                "key_fingerprint": LOCK_KEY_FINGERPRINT,
            }
        finally:
            lock_active = False

    def resolver(**kwargs: Any) -> dict[str, Any]:
        assert lock_active is True
        assert kwargs["event_types"] == ["breach_opened"]
        order.append("authority")
        return _authority(**kwargs)

    summary = execute_portfolio_risk_notification_retries(
        plan_path=plan_path,
        confirm_plan_id=plan["plan_id"],
        request_id="RETRY-AUTHORITY-001",
        config_path=config,
        dsn="postgresql://secret-value",
        execute=True,
        environment={"RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.test/risk"},
        reader=lambda **_: [candidate],
        attempt_writer=lambda _: None,
        transport=lambda *_: order.append("transport") or 204,
        clock=lambda: EXECUTED_AT,
        lock_factory=lock,
        destination_authority_resolver=resolver,
        destination_authority_observer=lambda value: observed.append(dict(value)),
    )

    assert order == ["lock", "authority", "transport"]
    assert observed == [summary["destination_authority"]]
    assert summary["destination_authority"]["active"] is True
    assert "postgresql://secret-value" not in json.dumps(summary)


def test_authority_rejection_prevents_first_transport(tmp_path: Path) -> None:
    config = _write_config(tmp_path)
    candidate = _candidate()
    plan_path, plan = _write_plan(tmp_path, config, candidate)
    sends: list[bool] = []

    @contextmanager
    def lock(**_: Any) -> Iterator[Mapping[str, Any]]:
        yield {
            "model_version": LOCK_MODEL_VERSION,
            "scope": LOCK_SCOPE,
            "key_fingerprint": LOCK_KEY_FINGERPRINT,
        }

    with pytest.raises(ValidationError, match="expired"):
        execute_portfolio_risk_notification_retries(
            plan_path=plan_path,
            confirm_plan_id=plan["plan_id"],
            request_id="RETRY-AUTHORITY-002",
            config_path=config,
            dsn="dsn",
            execute=True,
            environment={
                "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.test/risk"
            },
            reader=lambda **_: [candidate],
            attempt_writer=lambda _: None,
            transport=lambda *_: sends.append(True) or 204,
            clock=lambda: EXECUTED_AT,
            lock_factory=lock,
            destination_authority_resolver=lambda **_: (
                (_ for _ in ()).throw(ValidationError("destination review expired"))
            ),
        )
    assert sends == []
