from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_destination_transition_plan import (
    MODEL_VERSION,
    _build_parser,
    _write_summary,
    build_notification_destination_transition_plan,
    validate_notification_destination_transition_plan,
    validate_target_destination_authority,
)
from src.orchestration.portfolio_risk_notification_destination_authority import (
    resolve_notification_destination_authority,
)

PLANNED_AT = datetime(2026, 6, 15, 12, tzinfo=timezone.utc)
DESTINATION_ID = "risk-operations-webhook"
OLD_ENDPOINT = "RISK_NOTIFICATION_WEBHOOK_URL"
NEW_ENDPOINT = "RISK_NOTIFICATION_WEBHOOK_URL_V2"


def _activation(
    *,
    enabled: bool,
    change_request_id: str = "CHG-2026-001",
    reviewed_at: str = "2026-01-01T00:00:00Z",
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


def _payload(
    *,
    endpoint_env: str,
    enabled: bool,
    change_request_id: str = "CHG-2026-001",
    reviewed_at: str = "2026-01-01T00:00:00Z",
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


def _write(tmp_path: Path, name: str, payload: dict[str, Any]) -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _rotation_paths(tmp_path: Path) -> tuple[Path, Path]:
    current = _write(
        tmp_path,
        "current.yaml",
        _payload(endpoint_env=OLD_ENDPOINT, enabled=True),
    )
    target = _write(
        tmp_path,
        "rotated.yaml",
        _payload(
            endpoint_env=NEW_ENDPOINT,
            enabled=True,
            change_request_id="CHG-ROTATE-001",
            reviewed_at="2026-05-01T00:00:00Z",
        ),
    )
    return current, target


def test_rotation_plan_is_deterministic_secret_free_and_authority_bound(
    tmp_path: Path,
) -> None:
    current, target = _rotation_paths(tmp_path)
    kwargs = {
        "operation": "rotate",
        "current_config_path": current,
        "target_config_path": target,
        "destination_id": DESTINATION_ID,
        "planned_at": PLANNED_AT,
    }

    first = build_notification_destination_transition_plan(**kwargs)
    second = build_notification_destination_transition_plan(**kwargs)

    assert first == second
    assert first["model_version"] == MODEL_VERSION
    assert first["operation"] == "rotate"
    assert first["endpoint_environment_changed"] is True
    assert first["target_authority_required"] is True
    assert first["current_authority_accepted_by_target"] is False
    assert first["current"]["endpoint_environment_variable"] == OLD_ENDPOINT
    assert first["target"]["endpoint_environment_variable"] == NEW_ENDPOINT
    assert validate_notification_destination_transition_plan(first) == first

    target_authority = resolve_notification_destination_authority(
        destination_config_path=target,
        destination_id=DESTINATION_ID,
        delivery_endpoint_env=NEW_ENDPOINT,
        evaluated_at=PLANNED_AT,
        event_types=["breach_opened"],
    )
    assert validate_target_destination_authority(
        plan=first,
        authority=target_authority,
    ) == target_authority

    rendered = json.dumps(first, sort_keys=True)
    assert "https://" not in rendered
    assert first["external_request_performed"] is False
    assert first["delivery_attempt_written"] is False
    assert first["endpoint_value_recorded"] is False


def test_old_authority_cannot_authorise_rotated_destination(tmp_path: Path) -> None:
    current, target = _rotation_paths(tmp_path)
    plan = build_notification_destination_transition_plan(
        operation="rotate",
        current_config_path=current,
        target_config_path=target,
        destination_id=DESTINATION_ID,
        planned_at=PLANNED_AT,
    )
    old_authority = resolve_notification_destination_authority(
        destination_config_path=current,
        destination_id=DESTINATION_ID,
        delivery_endpoint_env=OLD_ENDPOINT,
        evaluated_at=PLANNED_AT,
        event_types=["breach_opened"],
    )

    with pytest.raises(ValidationError, match="does not match"):
        validate_target_destination_authority(
            plan=plan,
            authority=old_authority,
        )


def test_disable_and_rollback_plans_form_a_bounded_chain(tmp_path: Path) -> None:
    baseline, rotated = _rotation_paths(tmp_path)
    disabled = _write(
        tmp_path,
        "disabled.yaml",
        _payload(endpoint_env=NEW_ENDPOINT, enabled=False),
    )
    rollback = _write(
        tmp_path,
        "rollback.yaml",
        _payload(
            endpoint_env=OLD_ENDPOINT,
            enabled=True,
            change_request_id="CHG-ROLLBACK-001",
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

    assert disable_plan["target_authority_required"] is False
    assert disable_plan["endpoint_environment_changed"] is False
    assert rollback_plan["prior_plan_id"] == disable_plan["plan_id"]
    assert rollback_plan["target_authority_required"] is True
    assert rollback_plan["target"]["endpoint_environment_variable"] == OLD_ENDPOINT
    assert rotate_plan["target"]["fingerprint"] == disable_plan["current"]["fingerprint"]
    assert disable_plan["target"]["fingerprint"] == rollback_plan["current"]["fingerprint"]

    with pytest.raises(ValidationError, match="must not receive target authority"):
        validate_target_destination_authority(
            plan=disable_plan,
            authority={},
        )

    original_authority = resolve_notification_destination_authority(
        destination_config_path=baseline,
        destination_id=DESTINATION_ID,
        delivery_endpoint_env=OLD_ENDPOINT,
        evaluated_at=PLANNED_AT,
    )
    with pytest.raises(ValidationError, match="fingerprint"):
        validate_target_destination_authority(
            plan=rollback_plan,
            authority=original_authority,
        )

    rollback_authority = resolve_notification_destination_authority(
        destination_config_path=rollback,
        destination_id=DESTINATION_ID,
        delivery_endpoint_env=OLD_ENDPOINT,
        evaluated_at=PLANNED_AT,
    )
    validate_target_destination_authority(
        plan=rollback_plan,
        authority=rollback_authority,
    )


def test_transition_rejects_scope_creep_and_invalid_state_changes(
    tmp_path: Path,
) -> None:
    current, target = _rotation_paths(tmp_path)
    changed_owner = _payload(
        endpoint_env=NEW_ENDPOINT,
        enabled=True,
        change_request_id="CHG-ROTATE-001",
        reviewed_at="2026-05-01T00:00:00Z",
    )
    changed_owner["destinations"][DESTINATION_ID]["owner"]["team"] = "another-team"
    changed_owner_path = _write(tmp_path, "changed-owner.yaml", changed_owner)
    with pytest.raises(ValidationError, match="only endpoint identity"):
        build_notification_destination_transition_plan(
            operation="rotate",
            current_config_path=current,
            target_config_path=changed_owner_path,
            destination_id=DESTINATION_ID,
            planned_at=PLANNED_AT,
        )

    same_endpoint = _write(
        tmp_path,
        "same-endpoint.yaml",
        _payload(
            endpoint_env=OLD_ENDPOINT,
            enabled=True,
            change_request_id="CHG-ROTATE-002",
            reviewed_at="2026-05-01T00:00:00Z",
        ),
    )
    with pytest.raises(ValidationError, match="new endpoint"):
        build_notification_destination_transition_plan(
            operation="rotate",
            current_config_path=current,
            target_config_path=same_endpoint,
            destination_id=DESTINATION_ID,
            planned_at=PLANNED_AT,
        )

    disabled_new_endpoint = _write(
        tmp_path,
        "disabled-new-endpoint.yaml",
        _payload(endpoint_env=NEW_ENDPOINT, enabled=False),
    )
    with pytest.raises(ValidationError, match="retain the endpoint"):
        build_notification_destination_transition_plan(
            operation="disable",
            current_config_path=current,
            target_config_path=disabled_new_endpoint,
            destination_id=DESTINATION_ID,
            planned_at=PLANNED_AT,
        )

    with pytest.raises(ValidationError, match="prior_plan_id"):
        build_notification_destination_transition_plan(
            operation="rollback",
            current_config_path=disabled_new_endpoint,
            target_config_path=target,
            destination_id=DESTINATION_ID,
            planned_at=PLANNED_AT,
        )


def test_plan_validation_rejects_tampering(tmp_path: Path) -> None:
    current, target = _rotation_paths(tmp_path)
    plan = build_notification_destination_transition_plan(
        operation="rotate",
        current_config_path=current,
        target_config_path=target,
        destination_id=DESTINATION_ID,
        planned_at=PLANNED_AT,
    )
    plan["target"]["fingerprint"] = "tampered-fingerprint"

    with pytest.raises(ValidationError, match="identity"):
        validate_notification_destination_transition_plan(plan)


def test_cli_has_no_execute_switch_and_summary_rejects_symlink(
    tmp_path: Path,
) -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--operation",
                "rotate",
                "--current-config",
                "current.yaml",
                "--target-config",
                "target.yaml",
                "--destination-id",
                DESTINATION_ID,
                "--planned-at",
                PLANNED_AT.isoformat(),
                "--execute",
            ]
        )

    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "summary.json"
    link.symlink_to(target)
    with pytest.raises(StorageError, match="symbolic link"):
        _write_summary(link, {"safe": True})
