from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

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
from src.warehouse.notification_execution_readiness_gate import (
    evaluate_notification_execution_readiness,
)
from src.warehouse.notification_execution_readiness_history_contract import (
    MODEL_VERSION,
    build_notification_execution_readiness_record,
    canonical_notification_execution_readiness_record_bytes,
    validate_notification_execution_readiness_record,
)

NOW = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)
DESTINATION_ID = "risk-operations-webhook"
ENDPOINT_ENV = "RISK_NOTIFICATION_WEBHOOK_URL"


def _decision(*, allowed: bool = True) -> dict[str, object]:
    destination = NotificationDestination(
        destination_id=DESTINATION_ID,
        channel="webhook",
        endpoint_env=ENDPOINT_ENV,
        owner=DestinationOwner(
            team="risk-operations",
            contact="risk-operations-oncall",
        ),
        purpose="portfolio-risk-breach-lifecycle",
        recipient_scope="risk-operations",
        data_classification="internal",
        allowed_event_types=("breach_opened",),
        activation=DestinationActivation(
            enabled=True,
            change_request_id="CHG-READY-001",
            reviewed_by=("platform-reviewer",),
            reviewed_at=NOW - timedelta(days=1),
            review_expires_at=NOW + timedelta(days=30),
        ),
    )
    activation = {
        "destination_id": DESTINATION_ID,
        "destination_fingerprint": destination.fingerprint,
        "authority_id": "destination-authority-1",
        "checklist_id": "activation-checklist-1",
        "review_status": "ready",
        "operational_activation_ready": True,
    }
    transition = {
        "destination_id": DESTINATION_ID,
        "current_destination_fingerprint": destination.fingerprint,
        "current_authority_id": activation["authority_id"],
        "current_checklist_id": activation["checklist_id"],
        "activation_review_status": activation["review_status"],
        "operational_activation_ready": True,
        "transition_record_id": "transition-record-1",
        "transition_rehearsal_id": "transition-rehearsal-1",
        "rollback_plan_id": "rollback-plan-1",
        "rollback_authority_id": activation["authority_id"],
        "rollback_checklist_id": activation["checklist_id"],
        "rollback_destination_fingerprint": destination.fingerprint,
        "rollback_endpoint_environment_variable": ENDPOINT_ENV,
        "transition_matches_current_activation": True,
        "transition_review_status": "ready",
        "transition_ready": True,
    }
    return evaluate_notification_execution_readiness(
        execution_kind="initial",
        evaluated_at=NOW,
        delivery_config=WebhookDeliveryConfig(
            enabled=allowed,
            endpoint_env=ENDPOINT_ENV,
            timeout_seconds=5,
            max_batch_events=25,
            max_attempts_per_event=3,
            initial_backoff_seconds=1,
        ),
        retry_policy_fingerprint="retry-policy-1",
        retry_execution_policy=RetryExecutionPolicy(
            enabled=True,
            max_plan_age_seconds=3600,
            max_events=25,
        ),
        destination=destination,
        activation_review=activation,
        transition_review=transition,
        ambiguities=[],
    )


def test_readiness_record_is_deterministic_and_canonical() -> None:
    decision = _decision()
    first = build_notification_execution_readiness_record(
        request_id="READINESS-DECISION-001",
        recorded_at=NOW + timedelta(seconds=1),
        decision=decision,
    )
    second = build_notification_execution_readiness_record(
        request_id="READINESS-DECISION-001",
        recorded_at=NOW + timedelta(seconds=1),
        decision=decision,
    )

    assert first == second
    assert first["model_version"] == MODEL_VERSION
    assert validate_notification_execution_readiness_record(first) == first
    assert canonical_notification_execution_readiness_record_bytes(first)


def test_changed_request_time_or_decision_changes_record_identity() -> None:
    base = build_notification_execution_readiness_record(
        request_id="READINESS-DECISION-001",
        recorded_at=NOW + timedelta(seconds=1),
        decision=_decision(),
    )
    later = build_notification_execution_readiness_record(
        request_id="READINESS-DECISION-001",
        recorded_at=NOW + timedelta(seconds=2),
        decision=_decision(),
    )
    blocked = build_notification_execution_readiness_record(
        request_id="READINESS-DECISION-001",
        recorded_at=NOW + timedelta(seconds=1),
        decision=_decision(allowed=False),
    )

    assert base["record_id"] != later["record_id"]
    assert base["record_id"] != blocked["record_id"]


def test_record_rejects_tampering_and_invalid_time_order() -> None:
    record = build_notification_execution_readiness_record(
        request_id="READINESS-DECISION-001",
        recorded_at=NOW + timedelta(seconds=1),
        decision=_decision(),
    )
    changed = deepcopy(record)
    changed["request_id"] = "READINESS-DECISION-002"
    with pytest.raises(ValidationError, match="not canonical"):
        validate_notification_execution_readiness_record(changed)

    unknown = deepcopy(record)
    unknown["execute"] = True
    with pytest.raises(ValidationError, match="fields are invalid"):
        validate_notification_execution_readiness_record(unknown)

    with pytest.raises(ValidationError, match="must not precede"):
        build_notification_execution_readiness_record(
            request_id="READINESS-DECISION-003",
            recorded_at=NOW - timedelta(seconds=1),
            decision=_decision(),
        )
