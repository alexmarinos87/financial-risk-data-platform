from __future__ import annotations

import json
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
    MODEL_VERSION,
    rehearse_notification_destination_transition,
)
from src.orchestration.portfolio_risk_notification_destination_authority import (
    resolve_notification_destination_authority,
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


def _payload(
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


def _write(tmp_path: Path, name: str, payload: dict[str, Any]) -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _evidence(tmp_path: Path, *, wrong_rollback_parent: bool = False) -> dict[str, Any]:
    baseline = _write(
        tmp_path,
        "baseline.yaml",
        _payload(
            endpoint_env=OLD_ENDPOINT_ENV,
            enabled=True,
            change_request_id="CHG-BASELINE",
            reviewed_at="2026-01-01T00:00:00Z",
        ),
    )
    rotated = _write(
        tmp_path,
        "rotated.yaml",
        _payload(
            endpoint_env=NEW_ENDPOINT_ENV,
            enabled=True,
            change_request_id="CHG-ROTATE",
            reviewed_at="2026-05-01T00:00:00Z",
        ),
    )
    disabled = _write(
        tmp_path,
        "disabled.yaml",
        _payload(
            endpoint_env=NEW_ENDPOINT_ENV,
            enabled=False,
            change_request_id="unused",
            reviewed_at="2026-05-01T00:00:00Z",
        ),
    )
    rollback = _write(
        tmp_path,
        "rollback.yaml",
        _payload(
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
        prior_plan_id=(
            rotate_plan["plan_id"]
            if wrong_rollback_parent
            else disable_plan["plan_id"]
        ),
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
    return {
        "rotate_plan": rotate_plan,
        "disable_plan": disable_plan,
        "rollback_plan": rollback_plan,
        "baseline_authority": baseline_authority,
        "rotate_authority": rotate_authority,
        "rollback_authority": rollback_authority,
        "rotate_checklist": rotate_checklist,
        "rollback_checklist": rollback_checklist,
    }


def _request(
    *,
    endpoint: str,
    event_id: str,
    severity: str = "critical",
) -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "payload": {
            "event_id": event_id,
            "event_type": "breach_opened",
            "payload": {"severity": severity},
            "policy_id": "us-tech-standard",
            "portfolio_id": "us-tech-equal",
        },
    }


def _clock(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


def _run(tmp_path: Path) -> dict[str, Any]:
    evidence = _evidence(tmp_path)
    rotate_request = _request(
        endpoint="https://receiver-v2.test/risk?mode=controlled",
        event_id="rotation-event-1",
    )
    rollback_request = _request(
        endpoint="https://receiver-v1.test/risk?mode=controlled",
        event_id="rollback-event-1",
    )
    return rehearse_notification_destination_transition(
        **evidence,
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


def test_complete_transition_rehearsal_is_deterministic_and_no_network(
    tmp_path: Path,
) -> None:
    first = _run(tmp_path / "first")
    second = _run(tmp_path / "second")

    assert first == second
    assert first["model_version"] == MODEL_VERSION
    assert [stage["operation"] for stage in first["stages"]] == [
        "rotate",
        "disable",
        "rollback",
    ]
    assert [stage["request_count"] for stage in first["stages"]] == [2, 0, 1]
    assert first["stages"][0]["receiver_summary"][
        "same_content_duplicate_count"
    ] == 1
    assert first["stages"][1]["authority_id"] is None
    assert first["stages"][1]["receiver_summary"] is None
    assert first["stale_authority_rejections"] == {
        "baseline_for_rotation_target": True,
        "rotated_for_rollback_target": True,
    }
    for flag in (
        "external_request_performed",
        "socket_opened",
        "dns_lookup_performed",
        "delivery_attempt_written",
        "outbox_mutated",
        "acknowledgement_mutated",
        "infrastructure_deployed",
    ):
        assert first[flag] is False

    rendered = json.dumps(first, sort_keys=True)
    assert "https://" not in rendered
    assert "/risk" not in rendered
    assert '"severity"' not in rendered
    assert "postgresql://" not in rendered


def test_rehearsal_rejects_broken_plan_chain(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path, wrong_rollback_parent=True)
    request = _request(
        endpoint="https://receiver-v2.test/risk",
        event_id="event-1",
    )
    with pytest.raises(ValidationError, match="exact disablement plan"):
        rehearse_notification_destination_transition(
            **evidence,
            rotate_allowed_hosts=["receiver-v2.test"],
            rollback_allowed_hosts=["receiver-v1.test"],
            rotate_requests=[request],
            rollback_requests=[
                _request(
                    endpoint="https://receiver-v1.test/risk",
                    event_id="event-2",
                )
            ],
            started_at=STARTED_AT,
            clock=_clock(
                STARTED_AT + timedelta(seconds=1),
                STARTED_AT + timedelta(seconds=2),
            ),
        )


def test_rehearsal_rejects_wrong_current_authority(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)
    evidence["baseline_authority"] = evidence["rotate_authority"]
    with pytest.raises(ValidationError, match="current plan evidence"):
        rehearse_notification_destination_transition(
            **evidence,
            rotate_allowed_hosts=["receiver-v2.test"],
            rollback_allowed_hosts=["receiver-v1.test"],
            rotate_requests=[
                _request(
                    endpoint="https://receiver-v2.test/risk",
                    event_id="event-1",
                )
            ],
            rollback_requests=[
                _request(
                    endpoint="https://receiver-v1.test/risk",
                    event_id="event-2",
                )
            ],
            started_at=STARTED_AT,
            clock=_clock(
                STARTED_AT + timedelta(seconds=1),
                STARTED_AT + timedelta(seconds=2),
            ),
        )


def test_rehearsal_rejects_mismatched_checklist(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)
    evidence["rollback_checklist"] = evidence["rotate_checklist"]
    with pytest.raises(ValidationError, match="does not match authority"):
        rehearse_notification_destination_transition(
            **evidence,
            rotate_allowed_hosts=["receiver-v2.test"],
            rollback_allowed_hosts=["receiver-v1.test"],
            rotate_requests=[
                _request(
                    endpoint="https://receiver-v2.test/risk",
                    event_id="event-1",
                )
            ],
            rollback_requests=[
                _request(
                    endpoint="https://receiver-v1.test/risk",
                    event_id="event-2",
                )
            ],
            started_at=STARTED_AT,
            clock=_clock(
                STARTED_AT + timedelta(seconds=1),
                STARTED_AT + timedelta(seconds=2),
            ),
        )


def test_rehearsal_requires_requests_only_for_active_stages(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)
    with pytest.raises(ValidationError, match="between 1 and"):
        rehearse_notification_destination_transition(
            **evidence,
            rotate_allowed_hosts=["receiver-v2.test"],
            rollback_allowed_hosts=["receiver-v1.test"],
            rotate_requests=[],
            rollback_requests=[
                _request(
                    endpoint="https://receiver-v1.test/risk",
                    event_id="event-2",
                )
            ],
            started_at=STARTED_AT,
            clock=_clock(STARTED_AT + timedelta(seconds=1)),
        )


def test_rehearsal_rejects_receipts_before_start_or_out_of_order(
    tmp_path: Path,
) -> None:
    evidence = _evidence(tmp_path)
    request_one = _request(
        endpoint="https://receiver-v2.test/risk",
        event_id="event-1",
    )
    request_two = _request(
        endpoint="https://receiver-v1.test/risk",
        event_id="event-2",
    )
    with pytest.raises(ValidationError, match="precedes rehearsal start"):
        rehearse_notification_destination_transition(
            **evidence,
            rotate_allowed_hosts=["receiver-v2.test"],
            rollback_allowed_hosts=["receiver-v1.test"],
            rotate_requests=[request_one],
            rollback_requests=[request_two],
            started_at=STARTED_AT,
            clock=_clock(
                STARTED_AT - timedelta(seconds=1),
                STARTED_AT + timedelta(seconds=1),
            ),
        )

    with pytest.raises(ValidationError, match="precede rotation completion"):
        rehearse_notification_destination_transition(
            **evidence,
            rotate_allowed_hosts=["receiver-v2.test"],
            rollback_allowed_hosts=["receiver-v1.test"],
            rotate_requests=[request_one],
            rollback_requests=[request_two],
            started_at=STARTED_AT,
            clock=_clock(
                STARTED_AT + timedelta(seconds=2),
                STARTED_AT + timedelta(seconds=1),
            ),
        )
