from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.portfolio_risk_notification_destination_contract import (
    _build_parser,
    _write_summary,
    evaluate_destination_activation,
    load_notification_destinations,
)

EVALUATED_AT = datetime(2026, 4, 1, 12, tzinfo=timezone.utc)


def _payload(*, enabled: bool = False) -> dict[str, Any]:
    activation: dict[str, Any]
    if enabled:
        activation = {
            "enabled": True,
            "change_request_id": "CHG-2026-001",
            "reviewed_by": ["platform-owner"],
            "reviewed_at": "2026-03-01T12:00:00Z",
            "review_expires_at": "2026-06-01T12:00:00Z",
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


def _write(tmp_path: Path, payload: dict[str, Any]) -> Path:
    path = tmp_path / "notification-destinations.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def test_disabled_contract_is_deterministic_and_delivery_free(tmp_path: Path) -> None:
    path = _write(tmp_path, _payload())
    first = load_notification_destinations(path)["risk-operations-webhook"]
    second = load_notification_destinations(path)["risk-operations-webhook"]
    summary = evaluate_destination_activation(first, evaluated_at=EVALUATED_AT)

    assert first.fingerprint == second.fingerprint
    assert summary["activation"]["status"] == "disabled"
    assert summary["external_request_performed"] is False
    assert summary["delivery_attempt_written"] is False
    assert summary["outbox_mutated"] is False
    assert summary["acknowledgement_mutated"] is False
    assert summary["endpoint"] == {
        "environment_variable": "RISK_NOTIFICATION_WEBHOOK_URL",
        "value_recorded": False,
    }
    rendered = json.dumps(summary, sort_keys=True)
    assert "https://" not in rendered


def test_enabled_contract_has_independent_bounded_review(tmp_path: Path) -> None:
    destination = load_notification_destinations(
        _write(tmp_path, _payload(enabled=True))
    )["risk-operations-webhook"]

    assert evaluate_destination_activation(
        destination,
        evaluated_at=EVALUATED_AT,
    )["activation"]["status"] == "active"
    assert evaluate_destination_activation(
        destination,
        evaluated_at="2026-07-01T12:00:00Z",
    )["activation"]["status"] == "review_expired"

    owner_only = _payload(enabled=True)
    owner_only["destinations"]["risk-operations-webhook"]["activation"][
        "reviewed_by"
    ] = ["risk-operations-oncall"]
    with pytest.raises(ValidationError, match="owner contact"):
        load_notification_destinations(_write(tmp_path, owner_only))

    independently_reviewed = _payload(enabled=True)
    independently_reviewed["destinations"]["risk-operations-webhook"][
        "activation"
    ]["reviewed_by"] = ["platform-owner", "risk-operations-oncall"]
    accepted = load_notification_destinations(
        _write(tmp_path, independently_reviewed)
    )["risk-operations-webhook"]
    assert accepted.activation.reviewed_by == (
        "platform-owner",
        "risk-operations-oncall",
    )


def test_committed_endpoint_must_be_an_environment_name(tmp_path: Path) -> None:
    payload = _payload()
    payload["destinations"]["risk-operations-webhook"]["endpoint_env"] = (
        "https://alerts.example.test/risk"
    )
    with pytest.raises(ValidationError, match="environment variable"):
        load_notification_destinations(_write(tmp_path, payload))


def test_unknown_fields_and_unsorted_allow_lists_fail_closed(tmp_path: Path) -> None:
    unknown = _payload()
    unknown["destinations"]["risk-operations-webhook"]["endpoint"] = "secret"
    with pytest.raises(ValidationError, match="unknown"):
        load_notification_destinations(_write(tmp_path, unknown))

    unsorted = _payload()
    unsorted["destinations"]["risk-operations-webhook"][
        "allowed_event_types"
    ] = ["breach_resolved", "breach_opened"]
    with pytest.raises(ValidationError, match="sorted"):
        load_notification_destinations(_write(tmp_path, unsorted))


def test_incomplete_enabled_activation_fails_closed(tmp_path: Path) -> None:
    payload = _payload(enabled=True)
    payload["destinations"]["risk-operations-webhook"]["activation"][
        "change_request_id"
    ] = None
    with pytest.raises(ValidationError, match="complete review evidence"):
        load_notification_destinations(_write(tmp_path, payload))


def test_cli_has_no_execution_switch() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--destination-id",
                "risk-operations-webhook",
                "--evaluated-at",
                EVALUATED_AT.isoformat(),
                "--execute",
            ]
        )


def test_summary_writer_rejects_symbolic_links(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "summary.json"
    link.symlink_to(target)
    with pytest.raises(StorageError, match="symbolic link"):
        _write_summary(link, {"safe": True})


def test_review_duration_is_bounded_by_elapsed_time(tmp_path: Path) -> None:
    accepted = _payload(enabled=True)
    accepted["destinations"]["risk-operations-webhook"]["activation"].update(
        {
            "reviewed_at": "2026-01-01T00:00:00Z",
            "review_expires_at": "2027-01-02T00:00:00Z",
        }
    )
    load_notification_destinations(_write(tmp_path, accepted))

    too_long = _payload(enabled=True)
    too_long["destinations"]["risk-operations-webhook"]["activation"].update(
        {
            "reviewed_at": "2026-01-01T00:00:00Z",
            "review_expires_at": "2027-01-02T00:00:01Z",
        }
    )
    with pytest.raises(ValidationError, match="366 days"):
        load_notification_destinations(_write(tmp_path, too_long))
