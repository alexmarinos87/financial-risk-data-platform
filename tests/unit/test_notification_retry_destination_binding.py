from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.common.exceptions import ValidationError
from src.warehouse.notification_retry_destination_binding_contract import (
    MODEL_VERSION,
    build_retry_destination_binding,
    validate_retry_destination_binding,
)

EVALUATED_AT = datetime(2026, 1, 10, 12, 1, tzinfo=timezone.utc)


def _authority(*, destination_id: str = "risk-operations-webhook") -> dict[str, object]:
    return {
        "authority_id": "portfolio-risk-notification-destination-authority-v1-authority-abc",
        "destination_fingerprint": "portfolio-risk-notification-destination-v1-destination-abc",
        "destination_id": destination_id,
        "endpoint_environment_variable": "RISK_NOTIFICATION_WEBHOOK_URL",
        "evaluated_at": EVALUATED_AT.isoformat(),
        "evaluated_event_types": ["breach_opened"],
        "model_version": "portfolio-risk-notification-destination-authority-v1",
        "channel": "webhook",
        "activation": {
            "enabled": True,
            "status": "active",
            "change_request_id": "CHANGE-DESTINATION-001",
            "reviewed_at": (EVALUATED_AT - timedelta(days=1)).isoformat(),
            "review_expires_at": (EVALUATED_AT + timedelta(days=1)).isoformat(),
        },
        "allowed_event_types": ["breach_opened", "breach_resolved"],
        "active": True,
        "endpoint_value_recorded": False,
        "external_request_performed": False,
        "delivery_attempt_written": False,
        "outbox_mutated": False,
        "acknowledgement_mutated": False,
    }


def test_binding_is_deterministic_and_canonical() -> None:
    kwargs = {
        "record_id": "retry-record-1",
        "request_id": "RETRY-001",
        "plan_id": "retry-plan-1",
        "execution_id": "retry-execution-1",
        "destination_authority": _authority(),
        "recorded_at": EVALUATED_AT + timedelta(seconds=1),
    }
    first = build_retry_destination_binding(**kwargs)
    second = build_retry_destination_binding(**kwargs)

    assert first == second
    assert first["model_version"] == MODEL_VERSION
    assert validate_retry_destination_binding(first) == first


def test_binding_changes_with_destination_identity() -> None:
    first = build_retry_destination_binding(
        record_id="retry-record-1",
        request_id="RETRY-001",
        plan_id="retry-plan-1",
        execution_id=None,
        destination_authority=_authority(),
        recorded_at=EVALUATED_AT + timedelta(seconds=1),
    )
    second = build_retry_destination_binding(
        record_id="retry-record-1",
        request_id="RETRY-001",
        plan_id="retry-plan-1",
        execution_id=None,
        destination_authority=_authority(destination_id="secondary-webhook"),
        recorded_at=EVALUATED_AT + timedelta(seconds=1),
    )
    assert first["binding_id"] != second["binding_id"]


def test_inactive_or_side_effect_authority_fails_closed() -> None:
    inactive = _authority()
    inactive["active"] = False
    with pytest.raises(ValidationError, match="active"):
        build_retry_destination_binding(
            record_id="retry-record-1",
            request_id="RETRY-001",
            plan_id="retry-plan-1",
            execution_id=None,
            destination_authority=inactive,
            recorded_at=EVALUATED_AT + timedelta(seconds=1),
        )

    side_effect = _authority()
    side_effect["external_request_performed"] = True
    with pytest.raises(ValidationError, match="side-effect"):
        build_retry_destination_binding(
            record_id="retry-record-1",
            request_id="RETRY-001",
            plan_id="retry-plan-1",
            execution_id=None,
            destination_authority=side_effect,
            recorded_at=EVALUATED_AT + timedelta(seconds=1),
        )


def test_sql_contract_is_append_only_and_links_terminal_history() -> None:
    sql = Path(
        "sql/portfolio_risk_notification_retry_destination_binding_schema.sql"
    ).read_text(encoding="utf-8")
    assert "portfolio_risk_notification_retry_destination_bindings" in sql
    assert "REFERENCES" in sql
    assert "portfolio_risk_notification_retry_executions (record_id)" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "destination_fingerprint" in sql
    assert "endpoint_environment_variable" in sql
