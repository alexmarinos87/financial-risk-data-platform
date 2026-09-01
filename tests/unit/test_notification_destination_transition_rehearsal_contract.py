from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration.notification_activation_checklist import (
    CONTROL_NAMES,
    build_notification_activation_checklist,
)
from src.orchestration.notification_destination_transition_plan import (
    build_notification_destination_transition_plan,
)
from src.orchestration.notification_destination_transition_rehearsal import (
    rehearse_notification_destination_transition,
)
from src.orchestration.portfolio_risk_notification_destination_authority import (
    resolve_notification_destination_authority,
)
from src.warehouse.notification_destination_transition_rehearsal_contract import (
    MODEL_VERSION,
    build_notification_destination_transition_rehearsal_record,
    validate_notification_destination_transition_rehearsal,
    validate_notification_destination_transition_rehearsal_record,
)

DESTINATION_ID = "risk-operations-webhook"
OLD_ENDPOINT_ENV = "RISK_NOTIFICATION_WEBHOOK_URL"
NEW_ENDPOINT_ENV = "RISK_NOTIFICATION_WEBHOOK_URL_V2"
PLANNED_AT = datetime(2026, 6, 15, 12, tzinfo=timezone.utc)
STARTED_AT = PLANNED_AT + timedelta(minutes=1)


def _activation(
    *,
    enabled: bool,
    change_request_id: str,
    reviewed_at: str,
) -> dict[str, Any]:
    if not enabled:
        return {
            "enabled": False,
            "change_request_id": None,
            "reviewed_by": [],
            "reviewed_at": None,
            "review_expires_at": None,
        }
    return {
        "enabled": True,
        "change_request_id": change_request_id,
        "reviewed_by": ["risk-control-reviewer"],
        "reviewed_at": reviewed_at,
        "review_expires_at": "2026-12-31T00:00:00Z",
    }


def _config(
    *,
    endpoint_env: str,
    enabled: bool,
    change_request_id: str,
    reviewed_at: str,
) -> dict[str, Any]:
    return {
        "model_version": "portfolio-risk-notification-destination-v1",
        "destinations": {
            DESTINATION_ID: {
                "channel": "webhook",
                "endpoint_env": endpoint_env,
                "owner": {
                    "team": "risk-operations",
                    "contact": "risk-operations-oncall",
                },
                "purpose": "portfolio-risk-breach-lifecycle",
                "recipient_scope": "risk-operations",
                "data_classification": "internal",
                "allowed_event_types": [
                    "breach_escalated",
                    "breach_opened",
                    "breach_resolved",
                ],
                "activation": _activation(
                    enabled=enabled,
                    change_request_id=change_request_id,
                    reviewed_at=reviewed_at,
                ),
            }
        },
    }


def _write(tmp_path: Path, name: str, value: dict[str, Any]) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / name
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")
    return path


def _clock(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


def _request(endpoint: str, event_id: str) -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "payload": {
            "event_id": event_id,
            "event_type": "breach_opened",
            "payload": {"severity": "critical"},
            "policy_id": "us-tech-standard",
            "portfolio_id": "us-tech-equal",
        },
    }


def _rehearsal(tmp_path: Path) -> dict[str, Any]:
    baseline = _write(
        tmp_path,
        "baseline.yaml",
        _config(
            endpoint_env=OLD_ENDPOINT_ENV,
            enabled=True,
            change_request_id="CHG-BASELINE",
            reviewed_at="2026-01-01T00:00:00Z",
        ),
    )
    rotated = _write(
        tmp_path,
        "rotated.yaml",
        _config(
            endpoint_env=NEW_ENDPOINT_ENV,
            enabled=True,
            change_request_id="CHG-ROTATE",
            reviewed_at="2026-05-01T00:00:00Z",
        ),
    )
    disabled = _write(
        tmp_path,
        "disabled.yaml",
        _config(
            endpoint_env=NEW_ENDPOINT_ENV,
            enabled=False,
            change_request_id="unused",
            reviewed_at="2026-05-01T00:00:00Z",
        ),
    )
    rollback = _write(
        tmp_path,
        "rollback.yaml",
        _config(
            endpoint_env=OLD_ENDPOINT_ENV,
            enabled=True,
            change_request_id="CHG-ROLLBACK",
            reviewed_at="2026-06-01T00:00:00Z",
        ),
    )
    rotate_plan = build_notification_destination_transition_plan(
        operation="rotate",
        current_config_path=baseline,
        target_config_path=rotated,
        destination_id=DESTINATION_ID,
        planned_at=PLANNED_AT,
    )
    disable_plan = build_notification_destination_transition_plan(
        operation="disable",
        current_config_path=rotated,
        target_config_path=disabled,
        destination_id=DESTINATION_ID,
        planned_at=PLANNED_AT,
    )
    rollback_plan = build_notification_destination_transition_plan(
        operation="rollback",
        current_config_path=disabled,
        target_config_path=rollback,
        destination_id=DESTINATION_ID,
        planned_at=PLANNED_AT,
        prior_plan_id=disable_plan["plan_id"],
    )
    baseline_authority = resolve_notification_destination_authority(
        destination_config_path=baseline,
        destination_id=DESTINATION_ID,
        delivery_endpoint_env=OLD_ENDPOINT_ENV,
        evaluated_at=PLANNED_AT,
        event_types=["breach_opened"],
    )
    rotate_authority = resolve_notification_destination_authority(
        destination_config_path=rotated,
        destination_id=DESTINATION_ID,
        delivery_endpoint_env=NEW_ENDPOINT_ENV,
        evaluated_at=PLANNED_AT,
        event_types=["breach_opened"],
    )
    rollback_authority = resolve_notification_destination_authority(
        destination_config_path=rollback,
        destination_id=DESTINATION_ID,
        delivery_endpoint_env=OLD_ENDPOINT_ENV,
        evaluated_at=PLANNED_AT,
        event_types=["breach_opened"],
    )
    controls = {name: True for name in CONTROL_NAMES}
    rotate_checklist = build_notification_activation_checklist(
        destination_id=DESTINATION_ID,
        destination_fingerprint=rotate_authority["destination_fingerprint"],
        authority_id=rotate_authority["authority_id"],
        reviewed_by=["receiver-owner", "risk-control-reviewer"],
        reviewed_at=PLANNED_AT - timedelta(days=1),
        review_expires_at=PLANNED_AT + timedelta(days=30),
        controls=controls,
    )
    rollback_checklist = build_notification_activation_checklist(
        destination_id=DESTINATION_ID,
        destination_fingerprint=rollback_authority["destination_fingerprint"],
        authority_id=rollback_authority["authority_id"],
        reviewed_by=["receiver-owner", "risk-control-reviewer"],
        reviewed_at=PLANNED_AT - timedelta(hours=1),
        review_expires_at=PLANNED_AT + timedelta(days=30),
        controls=controls,
    )
    rotate_request = _request(
        "https://receiver-v2.test/risk",
        "rotation-event-1",
    )
    rollback_request = _request(
        "https://receiver-v1.test/risk",
        "rollback-event-1",
    )
    return rehearse_notification_destination_transition(
        rotate_plan=rotate_plan,
        disable_plan=disable_plan,
        rollback_plan=rollback_plan,
        baseline_authority=baseline_authority,
        rotate_authority=rotate_authority,
        rollback_authority=rollback_authority,
        rotate_checklist=rotate_checklist,
        rollback_checklist=rollback_checklist,
        rotate_allowed_hosts=["receiver-v2.test"],
        rollback_allowed_hosts=["receiver-v1.test"],
        rotate_requests=[rotate_request, rotate_request],
        rollback_requests=[rollback_request],
        started_at=STARTED_AT,
        clock=_clock(
            STARTED_AT + timedelta(seconds=1),
            STARTED_AT + timedelta(seconds=2),
            STARTED_AT + timedelta(seconds=3),
        ),
    )


def test_record_is_deterministic_canonical_and_count_reconciled(
    tmp_path: Path,
) -> None:
    rehearsal = _rehearsal(tmp_path)
    recorded_at = STARTED_AT + timedelta(seconds=4)

    first = build_notification_destination_transition_rehearsal_record(
        request_id="TRANSITION-REHEARSAL-001",
        recorded_at=recorded_at,
        rehearsal=rehearsal,
    )
    second = build_notification_destination_transition_rehearsal_record(
        request_id="TRANSITION-REHEARSAL-001",
        recorded_at=recorded_at,
        rehearsal=rehearsal,
    )

    assert first == second
    assert first["model_version"] == MODEL_VERSION
    assert first["rotate_request_count"] == 2
    assert first["rollback_request_count"] == 1
    assert first["same_content_duplicate_count"] == 1
    assert first["disable_endpoint_environment_variable"] == NEW_ENDPOINT_ENV
    assert first["rollback_endpoint_environment_variable"] == OLD_ENDPOINT_ENV
    assert validate_notification_destination_transition_rehearsal_record(first) == first
    assert validate_notification_destination_transition_rehearsal(rehearsal) == rehearsal


def test_changed_request_or_record_time_changes_record_identity(tmp_path: Path) -> None:
    rehearsal = _rehearsal(tmp_path)
    recorded_at = STARTED_AT + timedelta(seconds=4)
    first = build_notification_destination_transition_rehearsal_record(
        request_id="TRANSITION-REHEARSAL-001",
        recorded_at=recorded_at,
        rehearsal=rehearsal,
    )
    changed_request = build_notification_destination_transition_rehearsal_record(
        request_id="TRANSITION-REHEARSAL-002",
        recorded_at=recorded_at,
        rehearsal=rehearsal,
    )
    changed_time = build_notification_destination_transition_rehearsal_record(
        request_id="TRANSITION-REHEARSAL-001",
        recorded_at=recorded_at + timedelta(seconds=1),
        rehearsal=rehearsal,
    )

    assert len({first["record_id"], changed_request["record_id"], changed_time["record_id"]}) == 3


def test_record_rejects_time_before_rehearsal_completion(tmp_path: Path) -> None:
    rehearsal = _rehearsal(tmp_path)
    with pytest.raises(ValidationError, match="must not precede"):
        build_notification_destination_transition_rehearsal_record(
            request_id="TRANSITION-REHEARSAL-001",
            recorded_at=STARTED_AT,
            rehearsal=rehearsal,
        )


def test_rehearsal_validation_rejects_disable_stage_side_effects(
    tmp_path: Path,
) -> None:
    rehearsal = _rehearsal(tmp_path)
    rehearsal["stages"][1]["request_count"] = 1

    with pytest.raises(ValidationError, match="authority-free and request-free"):
        validate_notification_destination_transition_rehearsal(rehearsal)


def test_rehearsal_validation_rejects_changed_receipt_content(
    tmp_path: Path,
) -> None:
    rehearsal = _rehearsal(tmp_path)
    rehearsal["stages"][0]["receiver_summary"]["receipts"][1][
        "payload_sha256"
    ] = "0" * 64

    with pytest.raises(ValidationError, match="identical content"):
        validate_notification_destination_transition_rehearsal(rehearsal)


def test_rehearsal_validation_rejects_changed_safety_or_identity(
    tmp_path: Path,
) -> None:
    rehearsal = _rehearsal(tmp_path)
    rehearsal["external_request_performed"] = True
    with pytest.raises(ValidationError, match="side-effect"):
        validate_notification_destination_transition_rehearsal(rehearsal)

    rehearsal = _rehearsal(tmp_path / "identity")
    rehearsal["rehearsal_id"] = "portfolio-risk-notification-transition-tampered"
    with pytest.raises(ValidationError, match="identity"):
        validate_notification_destination_transition_rehearsal(rehearsal)


def test_record_validation_rejects_extracted_field_tampering(tmp_path: Path) -> None:
    record = build_notification_destination_transition_rehearsal_record(
        request_id="TRANSITION-REHEARSAL-001",
        recorded_at=STARTED_AT + timedelta(seconds=4),
        rehearsal=_rehearsal(tmp_path),
    )
    record["rollback_destination_fingerprint"] = "tampered-fingerprint"

    with pytest.raises(ValidationError, match="not canonical"):
        validate_notification_destination_transition_rehearsal_record(record)
