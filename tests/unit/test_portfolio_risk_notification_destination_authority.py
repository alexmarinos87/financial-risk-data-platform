from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration.portfolio_risk_notification_destination_authority import (
    MODEL_VERSION,
    resolve_notification_destination_authority,
)


def _payload(*, enabled: bool = True) -> dict[str, Any]:
    activation: dict[str, Any]
    if enabled:
        activation = {
            "enabled": True,
            "change_request_id": "CHG-2026-001",
            "reviewed_by": ["risk-control-reviewer"],
            "reviewed_at": "2026-01-01T00:00:00Z",
            "review_expires_at": "2026-12-31T00:00:00Z",
        }
    else:
        activation = {
            "enabled": False,
            "change_request_id": None,
            "reviewed_by": [],
            "reviewed_at": None,
            "review_expires_at": None,
        }
    return {
        "model_version": "portfolio-risk-notification-destination-v1",
        "destinations": {
            "risk-operations-webhook": {
                "channel": "webhook",
                "endpoint_env": "RISK_NOTIFICATION_WEBHOOK_URL",
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
                "activation": activation,
            }
        },
    }


def _write_config(tmp_path: Path, *, enabled: bool = True) -> Path:
    path = tmp_path / "destinations.yaml"
    path.write_text(
        yaml.safe_dump(_payload(enabled=enabled), sort_keys=False),
        encoding="utf-8",
    )
    return path


def test_active_destination_authority_is_deterministic_and_secret_free(
    tmp_path: Path,
) -> None:
    path = _write_config(tmp_path)
    kwargs = {
        "destination_config_path": path,
        "destination_id": "risk-operations-webhook",
        "delivery_endpoint_env": "RISK_NOTIFICATION_WEBHOOK_URL",
        "evaluated_at": datetime(2026, 6, 1, tzinfo=timezone.utc),
        "event_types": ["breach_opened", "breach_opened", "breach_resolved"],
    }

    first = resolve_notification_destination_authority(**kwargs)
    second = resolve_notification_destination_authority(**kwargs)

    assert first == second
    assert first["model_version"] == MODEL_VERSION
    assert first["active"] is True
    assert first["activation"]["status"] == "active"
    assert first["evaluated_event_types"] == [
        "breach_opened",
        "breach_resolved",
    ]
    rendered = json.dumps(first, sort_keys=True)
    assert "https://" not in rendered
    assert "RISK_NOTIFICATION_WEBHOOK_URL" in rendered
    assert first["endpoint_value_recorded"] is False
    assert first["external_request_performed"] is False


def test_execution_authority_rejects_inactive_destination(tmp_path: Path) -> None:
    path = _write_config(tmp_path, enabled=False)

    with pytest.raises(ValidationError, match="not active: disabled"):
        resolve_notification_destination_authority(
            destination_config_path=path,
            destination_id="risk-operations-webhook",
            delivery_endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL",
            evaluated_at="2026-06-01T00:00:00Z",
        )

    evidence = resolve_notification_destination_authority(
        destination_config_path=path,
        destination_id="risk-operations-webhook",
        delivery_endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL",
        evaluated_at="2026-06-01T00:00:00Z",
        require_active=False,
    )
    assert evidence["active"] is False
    assert evidence["activation"]["status"] == "disabled"


def test_execution_authority_rejects_expired_or_future_review(
    tmp_path: Path,
) -> None:
    path = _write_config(tmp_path)

    with pytest.raises(ValidationError, match="review_expired"):
        resolve_notification_destination_authority(
            destination_config_path=path,
            destination_id="risk-operations-webhook",
            delivery_endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL",
            evaluated_at="2027-01-01T00:00:00Z",
        )

    with pytest.raises(ValidationError, match="not_yet_reviewed"):
        resolve_notification_destination_authority(
            destination_config_path=path,
            destination_id="risk-operations-webhook",
            delivery_endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL",
            evaluated_at="2025-12-31T23:59:59Z",
        )


def test_execution_authority_rejects_endpoint_identity_mismatch(
    tmp_path: Path,
) -> None:
    path = _write_config(tmp_path)

    with pytest.raises(ValidationError, match="does not match"):
        resolve_notification_destination_authority(
            destination_config_path=path,
            destination_id="risk-operations-webhook",
            delivery_endpoint_env="ANOTHER_WEBHOOK_URL",
            evaluated_at="2026-06-01T00:00:00Z",
        )


def test_execution_authority_rejects_unreviewed_event_type(
    tmp_path: Path,
) -> None:
    path = _write_config(tmp_path)

    with pytest.raises(ValidationError, match="does not allow"):
        resolve_notification_destination_authority(
            destination_config_path=path,
            destination_id="risk-operations-webhook",
            delivery_endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL",
            evaluated_at="2026-06-01T00:00:00Z",
            event_types=["breach_deescalated"],
        )


def test_authority_identity_changes_with_time_or_events(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    base = {
        "destination_config_path": path,
        "destination_id": "risk-operations-webhook",
        "delivery_endpoint_env": "RISK_NOTIFICATION_WEBHOOK_URL",
    }
    first = resolve_notification_destination_authority(
        **base,
        evaluated_at="2026-06-01T00:00:00Z",
        event_types=["breach_opened"],
    )
    later = resolve_notification_destination_authority(
        **base,
        evaluated_at="2026-06-01T00:00:01Z",
        event_types=["breach_opened"],
    )
    other_event = resolve_notification_destination_authority(
        **base,
        evaluated_at="2026-06-01T00:00:00Z",
        event_types=["breach_resolved"],
    )

    assert first["authority_id"] != later["authority_id"]
    assert first["authority_id"] != other_event["authority_id"]
