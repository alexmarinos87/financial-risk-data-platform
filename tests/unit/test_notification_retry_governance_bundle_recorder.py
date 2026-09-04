from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.common.exceptions import ValidationError
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

BASE_TIME = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)
LOCK_MODEL = "portfolio-risk-notification-delivery-lock-v1"
LOCK_SCOPE = "portfolio-risk-notification-delivery"
LOCK_KEY = "atomic-governance-lock-key"


def _terminal(*, request_id: str = "atomic-request-1") -> dict[str, Any]:
    return build_retry_execution_record(
        request_id=request_id,
        plan_id="atomic-plan-1",
        started_at=BASE_TIME + timedelta(minutes=1),
        finished_at=BASE_TIME + timedelta(minutes=3),
        recorded_at=BASE_TIME + timedelta(minutes=4),
        terminal_status="failed_after_request",
        failure_code="validation_error",
        request_count=1,
        attempts_persisted=1,
        succeeded_count=0,
        failed_count=1,
        attempt_ids=[f"{request_id}-attempt"],
        requested_event_ids=[f"{request_id}-event"],
        persisted_event_ids=[f"{request_id}-event"],
        execution_summary=None,
        endpoint_host="alerts.example.test",
        delivery_fingerprint="atomic-delivery-fingerprint",
        retry_policy_fingerprint="atomic-retry-policy",
        retry_execution_policy_fingerprint="atomic-execution-policy",
        lock_model_version=LOCK_MODEL,
        lock_key_fingerprint=LOCK_KEY,
        lock_acquired=True,
        lock_released=True,
    )


def _enforcement(*, suffix: str = "1") -> dict[str, Any]:
    enforced_at = BASE_TIME + timedelta(minutes=2)
    return _enforcement_evidence(
        destination_id="atomic-governance-webhook",
        execution_kind="retry",
        enforced_at=enforced_at,
        record={
            "record_id": "atomic-readiness-record-1",
            "request_id": "atomic-readiness-request-1",
            "decision": {
                "decision_id": "atomic-retained-decision-1",
                "evaluated_at": BASE_TIME.isoformat(),
            },
        },
        refreshed_decision={
            "decision_id": f"atomic-refreshed-decision-{suffix}",
            "evaluated_at": enforced_at.isoformat(),
        },
        lock={
            "key_fingerprint": LOCK_KEY,
            "model_version": LOCK_MODEL,
            "scope": LOCK_SCOPE,
        },
    )


def _binding(terminal: dict[str, Any]) -> dict[str, Any]:
    return build_notification_retry_readiness_binding(
        terminal_record=terminal,
        readiness_enforcement=_enforcement(),
        recorded_at=BASE_TIME + timedelta(minutes=5),
    )


def test_bundle_validation_preserves_exact_terminal_and_binding() -> None:
    terminal = _terminal()
    binding = _binding(terminal)

    validated_terminal, validated_binding = (
        validate_notification_retry_governance_bundle(
            terminal_record=terminal,
            readiness_binding=binding,
        )
    )

    assert validated_terminal == terminal
    assert validated_binding == binding


def test_bundle_rejects_binding_built_for_another_terminal_record() -> None:
    terminal = _terminal()
    binding = _binding(terminal)
    changed = _terminal(request_id="atomic-request-2")

    with pytest.raises(ValidationError, match="supplied terminal record"):
        validate_notification_retry_governance_bundle(
            terminal_record=changed,
            readiness_binding=binding,
        )
