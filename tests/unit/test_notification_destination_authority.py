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
    _write_summary,
    build_destination_execution_authority,
    load_destination_execution_authority,
    main,
    validate_destination_execution_authority,
)

EVALUATED_AT = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)


def _destination_payload(*, enabled: bool = True) -> dict[str, Any]:
    activation: dict[str, Any]
    if enabled:
        activation = {
            "enabled": True,
            "change_request_id": "CHANGE-2026-001",
            "reviewed_by": ["notification-control-reviewer"],
            "reviewed_at": "2026-01-01T00:00:00+00:00",
            "review_expires_at": "2026-12-31T00:00:00+00:00",
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
                    "contact": "risk-operations-owner",
                },
                "purpose": "Deliver reviewed portfolio-risk lifecycle evidence.",
                "recipient_scope": "risk-operations",
                "data_classification": "internal",
                "allowed_event_types": [
                    "breach_deescalated",
                    "breach_escalated",
                    "breach_opened",
                    "breach_resolved",
                ],
                "activation": activation,
            }
        },
    }


def _write_yaml(path: Path, value: MappingLike) -> Path:
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")
    return path


MappingLike = dict[str, Any]


def _write_contracts(
    tmp_path: Path,
    *,
    enabled: bool = True,
    endpoint_env: str = "RISK_NOTIFICATION_WEBHOOK_URL",
) -> tuple[Path, Path]:
    destinations = _write_yaml(
        tmp_path / "destinations.yaml",
        _destination_payload(enabled=enabled),
    )
    delivery = _write_yaml(
        tmp_path / "delivery.yaml",
        {"delivery": {"webhook": {"endpoint_env": endpoint_env}}},
    )
    return destinations, delivery


def _events(event_type: str = "breach_opened") -> list[dict[str, str]]:
    return [
        {
            "event_id": "event-1",
            "event_type": event_type,
            "payload_sha256": "a" * 64,
        },
        {
            "event_id": "event-2",
            "event_type": "breach_resolved",
            "payload_sha256": "b" * 64,
        },
    ]


def _authority(tmp_path: Path) -> dict[str, Any]:
    destinations, delivery = _write_contracts(tmp_path)
    return build_destination_execution_authority(
        destination_config_path=destinations,
        destination_id="risk-operations-webhook",
        delivery_config_path=delivery,
        execution_kind="retry_execution",
        execution_reference_id="retry-plan-1",
        events=_events(),
        evaluated_at=EVALUATED_AT,
    )


def test_authority_is_deterministic_canonical_and_secret_free(tmp_path: Path) -> None:
    first = _authority(tmp_path)
    second = _authority(tmp_path)

    assert first == second
    assert first["model_version"] == MODEL_VERSION
    assert first["destination"]["activation"]["status"] == "active"
    assert first["execution"]["event_count"] == 2
    assert validate_destination_execution_authority(first) == first
    rendered = json.dumps(first)
    assert "https://" not in rendered
    assert "RISK_NOTIFICATION_WEBHOOK_URL" in rendered
    assert all(value is False for value in first["side_effects"].values())


def test_authority_rejects_inactive_destination_before_evidence(tmp_path: Path) -> None:
    destinations, delivery = _write_contracts(tmp_path, enabled=False)
    with pytest.raises(ValidationError, match="not active"):
        build_destination_execution_authority(
            destination_config_path=destinations,
            destination_id="risk-operations-webhook",
            delivery_config_path=delivery,
            execution_kind="initial_delivery",
            execution_reference_id="initial-selection-1",
            events=_events(),
            evaluated_at=EVALUATED_AT,
        )


def test_authority_rejects_expired_review(tmp_path: Path) -> None:
    destinations, delivery = _write_contracts(tmp_path)
    with pytest.raises(ValidationError, match="not active"):
        build_destination_execution_authority(
            destination_config_path=destinations,
            destination_id="risk-operations-webhook",
            delivery_config_path=delivery,
            execution_kind="retry_execution",
            execution_reference_id="retry-plan-1",
            events=_events(),
            evaluated_at="2027-01-01T00:00:00Z",
        )


def test_authority_rejects_endpoint_environment_mismatch(tmp_path: Path) -> None:
    destinations, delivery = _write_contracts(
        tmp_path,
        endpoint_env="OTHER_WEBHOOK_URL",
    )
    with pytest.raises(ValidationError, match="does not match"):
        build_destination_execution_authority(
            destination_config_path=destinations,
            destination_id="risk-operations-webhook",
            delivery_config_path=delivery,
            execution_kind="retry_execution",
            execution_reference_id="retry-plan-1",
            events=_events(),
            evaluated_at=EVALUATED_AT,
        )


def test_authority_rejects_event_outside_allow_list(tmp_path: Path) -> None:
    destinations, delivery = _write_contracts(tmp_path)
    with pytest.raises(ValidationError, match="outside the destination allow-list"):
        build_destination_execution_authority(
            destination_config_path=destinations,
            destination_id="risk-operations-webhook",
            delivery_config_path=delivery,
            execution_kind="retry_execution",
            execution_reference_id="retry-plan-1",
            events=_events("unsupported_event"),
            evaluated_at=EVALUATED_AT,
        )


def test_authority_rejects_duplicate_events_and_invalid_digest(tmp_path: Path) -> None:
    destinations, delivery = _write_contracts(tmp_path)
    duplicated = [_events()[0], _events()[0]]
    with pytest.raises(ValidationError, match="no duplicates"):
        build_destination_execution_authority(
            destination_config_path=destinations,
            destination_id="risk-operations-webhook",
            delivery_config_path=delivery,
            execution_kind="retry_execution",
            execution_reference_id="retry-plan-1",
            events=duplicated,
            evaluated_at=EVALUATED_AT,
        )

    invalid = _events()
    invalid[0]["payload_sha256"] = "not-a-digest"
    with pytest.raises(ValidationError, match="payload_sha256"):
        build_destination_execution_authority(
            destination_config_path=destinations,
            destination_id="risk-operations-webhook",
            delivery_config_path=delivery,
            execution_kind="retry_execution",
            execution_reference_id="retry-plan-1",
            events=invalid,
            evaluated_at=EVALUATED_AT,
        )


def test_loaded_authority_rejects_tampering_and_symbolic_links(tmp_path: Path) -> None:
    authority = _authority(tmp_path)
    authority_path = tmp_path / "authority.json"
    authority_path.write_text(json.dumps(authority), encoding="utf-8")
    assert load_destination_execution_authority(authority_path) == authority

    tampered = dict(authority)
    tampered["evaluated_at"] = "2026-06-01T13:00:00+00:00"
    authority_path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(ValidationError, match="authority_id"):
        load_destination_execution_authority(authority_path)

    target = tmp_path / "target.json"
    target.write_text(json.dumps(authority), encoding="utf-8")
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(ValidationError, match="symbolic link"):
        load_destination_execution_authority(link)


def test_cli_writes_valid_authority_without_delivery(tmp_path: Path) -> None:
    destinations, delivery = _write_contracts(tmp_path)
    events_path = tmp_path / "events.json"
    events_path.write_text(json.dumps(_events()), encoding="utf-8")
    summary_path = tmp_path / "authority.json"

    result = main(
        [
            "--destinations-config",
            str(destinations),
            "--delivery-config",
            str(delivery),
            "--destination-id",
            "risk-operations-webhook",
            "--execution-kind",
            "initial_delivery",
            "--execution-reference-id",
            "initial-selection-1",
            "--events-json",
            str(events_path),
            "--evaluated-at",
            EVALUATED_AT.isoformat(),
            "--summary-json",
            str(summary_path),
        ]
    )

    assert result == 0
    assert load_destination_execution_authority(summary_path)["execution"][
        "kind"
    ] == "initial_delivery"


def test_summary_writer_rejects_symbolic_links(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "summary.json"
    link.symlink_to(target)
    with pytest.raises(Exception, match="symbolic link"):
        _write_summary(link, {"safe": True})
