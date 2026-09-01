from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    WebhookDeliveryConfig,
)
from src.orchestration.portfolio_risk_notification_destination_contract import (
    DestinationActivation,
    DestinationOwner,
    NotificationDestination,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    RetryExecutionPolicy,
)
from src.warehouse.notification_execution_readiness_enforcement import (
    MAX_READINESS_AGE,
    enforce_notification_execution_readiness,
    validate_notification_execution_readiness_enforcement,
)
from src.warehouse.notification_execution_readiness_gate import (
    evaluate_notification_execution_readiness,
)
from src.warehouse.notification_execution_readiness_history_contract import (
    build_notification_execution_readiness_record,
)

DESTINATION_ID = "risk-operations-webhook"
BASE_TIME = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def _delivery_config(*, enabled: bool = True, timeout_seconds: int = 5):
    return WebhookDeliveryConfig(
        enabled=enabled,
        endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL",
        timeout_seconds=timeout_seconds,
        max_batch_events=25,
        max_attempts_per_event=3,
        initial_backoff_seconds=1,
    )


def _destination() -> NotificationDestination:
    return NotificationDestination(
        destination_id=DESTINATION_ID,
        channel="webhook",
        endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL",
        owner=DestinationOwner(
            team="risk-operations",
            contact="risk-operations-oncall",
        ),
        purpose="portfolio-risk-breach-lifecycle",
        recipient_scope="risk-operations",
        data_classification="internal",
        allowed_event_types=(
            "breach_escalated",
            "breach_opened",
            "breach_resolved",
        ),
        activation=DestinationActivation(
            enabled=True,
            change_request_id="CHG-2026-READINESS",
            reviewed_by=("risk-control-reviewer",),
            reviewed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            review_expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
        ),
    )


def _decision(
    *,
    evaluated_at: datetime,
    execution_kind: str = "initial",
    delivery_config: WebhookDeliveryConfig | None = None,
) -> dict[str, Any]:
    destination = _destination()
    return evaluate_notification_execution_readiness(
        execution_kind=execution_kind,
        evaluated_at=evaluated_at,
        delivery_config=delivery_config or _delivery_config(),
        retry_policy_fingerprint="retry-policy-fingerprint-1",
        retry_execution_policy=RetryExecutionPolicy(
            enabled=True,
            max_plan_age_seconds=3600,
            max_events=25,
        ),
        destination=destination,
        activation_review={
            "authority_id": "authority-1",
            "checklist_id": "checklist-1",
            "destination_fingerprint": destination.fingerprint,
            "destination_id": DESTINATION_ID,
            "operational_activation_ready": True,
            "review_status": "ready",
        },
        transition_review={
            "activation_review_status": "ready",
            "current_authority_id": "authority-1",
            "current_checklist_id": "checklist-1",
            "current_destination_fingerprint": destination.fingerprint,
            "destination_id": DESTINATION_ID,
            "operational_activation_ready": True,
            "rollback_authority_id": None,
            "rollback_checklist_id": None,
            "rollback_destination_fingerprint": None,
            "rollback_endpoint_environment_variable": None,
            "rollback_plan_id": None,
            "transition_matches_current_activation": True,
            "transition_ready": True,
            "transition_record_id": "transition-record-1",
            "transition_rehearsal_id": "transition-rehearsal-1",
            "transition_review_status": "ready",
        },
        ambiguities=[],
    )


def _record(
    *,
    evaluated_at: datetime = BASE_TIME,
    execution_kind: str = "initial",
) -> dict[str, Any]:
    return build_notification_execution_readiness_record(
        request_id=f"readiness-request-{execution_kind}",
        recorded_at=evaluated_at + timedelta(seconds=1),
        decision=_decision(
            evaluated_at=evaluated_at,
            execution_kind=execution_kind,
        ),
    )


def _review(
    record: dict[str, Any],
    *,
    status: str = "allowed",
    execution_ready: bool = True,
) -> dict[str, Any]:
    decision = record["decision"]
    return {
        "destination_id": decision["destination"]["destination_id"],
        "execution_kind": decision["execution_kind"],
        "readiness_record_id": record["record_id"],
        "readiness_request_id": record["request_id"],
        "decision_id": decision["decision_id"],
        "decision_evaluated_at": decision["evaluated_at"],
        "decision_recorded_at": record["recorded_at"],
        "decision": decision["decision"],
        "blocking_reasons_json": decision["blocking_reasons"],
        "readiness_review_status": status,
        "execution_ready": execution_ready,
        "record_json": record,
    }


def _lock() -> dict[str, Any]:
    return {
        "model_version": "portfolio-risk-notification-delivery-lock-v1",
        "scope": "portfolio-risk-notification-delivery",
        "key_fingerprint": "lock-key-fingerprint-1",
        "acquired": True,
    }


def _run(
    *,
    review: dict[str, Any],
    as_of: datetime,
    refreshed: dict[str, Any],
) -> dict[str, Any]:
    return enforce_notification_execution_readiness(
        dsn="dsn",
        destination_id=DESTINATION_ID,
        execution_kind="initial",
        evaluated_at=as_of,
        delivery_config_path=Path("config/notification_delivery.yaml"),
        destination_config_path=Path("config/notification_destinations.yaml"),
        lock_evidence=_lock(),
        review_reader=lambda **_: review,
        gate_runner=lambda **_: refreshed,
    )


def test_matching_current_allow_is_refreshed_and_bound_to_lock() -> None:
    record = _record()
    as_of = BASE_TIME + timedelta(minutes=2)
    refreshed = _decision(evaluated_at=as_of)

    evidence = _run(review=_review(record), as_of=as_of, refreshed=refreshed)

    assert evidence["execution_ready"] is True
    assert evidence["readiness_review_status"] == "allowed"
    assert evidence["readiness_record_id"] == record["record_id"]
    assert evidence["retained_decision_id"] == record["decision"]["decision_id"]
    assert evidence["refreshed_decision_id"] == refreshed["decision_id"]
    assert evidence["substantive_evidence_match"] is True
    assert evidence["lock"] == {
        "key_fingerprint": "lock-key-fingerprint-1",
        "model_version": "portfolio-risk-notification-delivery-lock-v1",
        "scope": "portfolio-risk-notification-delivery",
    }
    assert validate_notification_execution_readiness_enforcement(evidence) == evidence


def test_non_allowed_current_state_rejects_before_refresh() -> None:
    record = _record()
    refreshes: list[bool] = []

    with pytest.raises(ValidationError, match="decision_stale"):
        enforce_notification_execution_readiness(
            dsn="dsn",
            destination_id=DESTINATION_ID,
            execution_kind="initial",
            evaluated_at=BASE_TIME + timedelta(minutes=1),
            delivery_config_path=Path("delivery.yaml"),
            destination_config_path=Path("destinations.yaml"),
            lock_evidence=_lock(),
            review_reader=lambda **_: _review(
                record,
                status="decision_stale",
                execution_ready=False,
            ),
            gate_runner=lambda **_: refreshes.append(True) or {},
        )

    assert refreshes == []


def test_retained_allow_expires_after_reviewed_five_minute_window() -> None:
    record = _record()
    as_of = BASE_TIME + MAX_READINESS_AGE + timedelta(microseconds=1)

    with pytest.raises(ValidationError, match="stale"):
        _run(
            review=_review(record),
            as_of=as_of,
            refreshed=_decision(evaluated_at=as_of),
        )


def test_refreshed_block_rejects_before_execution_authority_is_returned() -> None:
    record = _record()
    as_of = BASE_TIME + timedelta(minutes=1)
    blocked = _decision(
        evaluated_at=as_of,
        delivery_config=_delivery_config(enabled=False),
    )

    with pytest.raises(ValidationError, match="delivery_disabled"):
        _run(review=_review(record), as_of=as_of, refreshed=blocked)


def test_changed_live_configuration_supersedes_retained_allow() -> None:
    record = _record()
    as_of = BASE_TIME + timedelta(minutes=1)
    changed = _decision(
        evaluated_at=as_of,
        delivery_config=_delivery_config(timeout_seconds=6),
    )
    assert changed["decision"] == "allow"

    with pytest.raises(ValidationError, match="superseded during preflight"):
        _run(review=_review(record), as_of=as_of, refreshed=changed)


def test_serving_row_must_match_retained_canonical_record() -> None:
    record = _record()
    review = _review(record)
    review["decision_id"] = "another-decision-id"
    as_of = BASE_TIME + timedelta(minutes=1)

    with pytest.raises(ValidationError, match="serving row"):
        _run(
            review=review,
            as_of=as_of,
            refreshed=_decision(evaluated_at=as_of),
        )


def test_enforcement_evidence_rejects_identity_tampering() -> None:
    record = _record()
    as_of = BASE_TIME + timedelta(minutes=1)
    evidence = _run(
        review=_review(record),
        as_of=as_of,
        refreshed=_decision(evaluated_at=as_of),
    )
    evidence["enforcement_id"] = "tampered-enforcement-id"

    with pytest.raises(ValidationError, match="enforcement_id"):
        validate_notification_execution_readiness_enforcement(evidence)
