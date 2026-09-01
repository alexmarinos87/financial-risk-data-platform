from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    WebhookDeliveryConfig,
)
from src.orchestration.portfolio_risk_notification_destination_contract import (
    DestinationActivation,
    DestinationOwner,
    NotificationDestination,
    load_notification_destinations,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    RetryExecutionPolicy,
)
from src.warehouse.notification_execution_readiness_gate import (
    BLOCKING_REASON_ORDER,
    _build_parser,
    _write_summary,
    canonical_notification_execution_readiness_bytes,
    evaluate_notification_execution_readiness,
    run_notification_execution_readiness_gate,
    validate_notification_execution_readiness_decision,
)

DESTINATION_ID = "risk-operations-webhook"
ENDPOINT_ENV = "RISK_NOTIFICATION_WEBHOOK_URL"
OTHER_ENDPOINT_ENV = "ROTATED_RISK_NOTIFICATION_WEBHOOK_URL"
EVALUATED_AT = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)


def _delivery(
    *,
    enabled: bool = True,
    endpoint_env: str = ENDPOINT_ENV,
) -> WebhookDeliveryConfig:
    return WebhookDeliveryConfig(
        enabled=enabled,
        endpoint_env=endpoint_env,
        timeout_seconds=5,
        max_batch_events=25,
        max_attempts_per_event=3,
        initial_backoff_seconds=1,
    )


def _retry(*, enabled: bool = True) -> RetryExecutionPolicy:
    return RetryExecutionPolicy(
        enabled=enabled,
        max_plan_age_seconds=3600,
        max_events=25,
    )


def _destination(
    *,
    enabled: bool = True,
    endpoint_env: str = ENDPOINT_ENV,
) -> NotificationDestination:
    if enabled:
        activation = DestinationActivation(
            enabled=True,
            change_request_id="CHG-READY-001",
            reviewed_by=("platform-reviewer",),
            reviewed_at=EVALUATED_AT - timedelta(days=1),
            review_expires_at=EVALUATED_AT + timedelta(days=30),
        )
    else:
        activation = DestinationActivation(
            enabled=False,
            change_request_id=None,
            reviewed_by=(),
            reviewed_at=None,
            review_expires_at=None,
        )
    return NotificationDestination(
        destination_id=DESTINATION_ID,
        channel="webhook",
        endpoint_env=endpoint_env,
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
        activation=activation,
    )


def _activation_review(
    destination: NotificationDestination,
    *,
    ready: bool = True,
    status: str = "ready",
) -> dict[str, Any]:
    return {
        "destination_id": destination.destination_id,
        "destination_fingerprint": destination.fingerprint,
        "authority_id": "destination-authority-1",
        "checklist_id": "activation-checklist-1",
        "review_status": status,
        "operational_activation_ready": ready,
    }


def _transition_review(
    destination: NotificationDestination,
    activation: dict[str, Any],
    *,
    status: str = "ready",
) -> dict[str, Any]:
    retained = status == "ready"
    return {
        "destination_id": destination.destination_id,
        "current_destination_fingerprint": destination.fingerprint,
        "current_authority_id": activation["authority_id"],
        "current_checklist_id": activation["checklist_id"],
        "activation_review_status": activation["review_status"],
        "operational_activation_ready": activation[
            "operational_activation_ready"
        ],
        "transition_record_id": "transition-record-1" if retained else None,
        "transition_rehearsal_id": (
            "transition-rehearsal-1" if retained else None
        ),
        "rollback_plan_id": "rollback-plan-1" if retained else None,
        "rollback_authority_id": activation["authority_id"] if retained else None,
        "rollback_checklist_id": activation["checklist_id"] if retained else None,
        "rollback_destination_fingerprint": (
            destination.fingerprint if retained else None
        ),
        "rollback_endpoint_environment_variable": (
            destination.endpoint_env if retained else None
        ),
        "transition_matches_current_activation": retained,
        "transition_review_status": status,
        "transition_ready": retained,
    }


def _ambiguity(*, destination_id: str | None = DESTINATION_ID) -> dict[str, Any]:
    return {
        "event_id": "event-ambiguous-1",
        "uncertainty_record_id": "uncertainty-record-1",
        "latest_execution_record_id": "execution-record-1",
        "destination_id": destination_id,
        "destination_fingerprint": (
            "destination-fingerprint-1" if destination_id is not None else None
        ),
        "endpoint_environment_variable": (
            ENDPOINT_ENV if destination_id is not None else None
        ),
        "destination_bound": destination_id is not None,
        "destination_binding_status": (
            "bound" if destination_id is not None else "destination_binding_missing"
        ),
    }


def _evaluate(
    *,
    execution_kind: str = "initial",
    delivery: WebhookDeliveryConfig | None = None,
    retry: RetryExecutionPolicy | None = None,
    destination: NotificationDestination | None = None,
    activation_review: dict[str, Any] | None | object = object(),
    transition_review: dict[str, Any] | None | object = object(),
    ambiguities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    selected_destination = destination or _destination()
    if not isinstance(activation_review, (dict, type(None))):
        activation = _activation_review(selected_destination)
    else:
        activation = activation_review
    if not isinstance(transition_review, (dict, type(None))):
        assert activation is not None
        transition = _transition_review(selected_destination, activation)
    else:
        transition = transition_review
    return evaluate_notification_execution_readiness(
        execution_kind=execution_kind,
        evaluated_at=EVALUATED_AT,
        delivery_config=delivery or _delivery(),
        retry_policy_fingerprint="retry-planning-policy-1",
        retry_execution_policy=retry or _retry(),
        destination=selected_destination,
        activation_review=activation,
        transition_review=transition,
        ambiguities=ambiguities or [],
    )


@pytest.mark.parametrize("execution_kind", ["initial", "retry"])
def test_ready_decision_is_deterministic_and_read_only(
    execution_kind: str,
) -> None:
    first = _evaluate(execution_kind=execution_kind)
    second = _evaluate(execution_kind=execution_kind)

    assert first == second
    assert first["decision"] == "allow"
    assert first["blocking_reasons"] == []
    assert first["read_only"] is True
    assert first["external_request_performed"] is False
    assert first["delivery_attempt_written"] is False
    assert first["outbox_mutated"] is False
    assert first["acknowledgement_mutated"] is False
    assert validate_notification_execution_readiness_decision(first) == first
    assert canonical_notification_execution_readiness_bytes(first)


def test_execution_kind_and_time_change_decision_identity() -> None:
    initial = _evaluate(execution_kind="initial")
    retry = _evaluate(execution_kind="retry")
    later = evaluate_notification_execution_readiness(
        execution_kind="initial",
        evaluated_at=EVALUATED_AT + timedelta(seconds=1),
        delivery_config=_delivery(),
        retry_policy_fingerprint="retry-planning-policy-1",
        retry_execution_policy=_retry(),
        destination=_destination(),
        activation_review=_activation_review(_destination()),
        transition_review=_transition_review(
            _destination(),
            _activation_review(_destination()),
        ),
        ambiguities=[],
    )

    assert initial["decision_id"] != retry["decision_id"]
    assert initial["decision_id"] != later["decision_id"]


def test_blocking_reasons_use_fixed_canonical_order() -> None:
    destination = _destination(enabled=False, endpoint_env=ENDPOINT_ENV)
    decision = _evaluate(
        execution_kind="retry",
        delivery=_delivery(enabled=False, endpoint_env=OTHER_ENDPOINT_ENV),
        retry=_retry(enabled=False),
        destination=destination,
        activation_review=None,
        transition_review=None,
        ambiguities=[_ambiguity(destination_id=None)],
    )

    assert decision["decision"] == "block"
    assert decision["blocking_reasons"] == [
        "delivery_disabled",
        "retry_execution_disabled",
        "destination_not_active",
        "configuration_mismatch",
        "activation_review_missing",
        "transition_review_missing",
        "persistence_ambiguity",
    ]
    assert decision["blocking_reasons"] == [
        reason
        for reason in BLOCKING_REASON_ORDER
        if reason in decision["blocking_reasons"]
    ]


@pytest.mark.parametrize(
    ("status", "expected_reason"),
    [
        ("transition_rehearsal_missing", "transition_rehearsal_missing"),
        ("transition_rehearsal_superseded", "transition_rehearsal_superseded"),
        ("activation_not_ready", "transition_not_ready"),
    ],
)
def test_transition_review_states_fail_closed(
    status: str,
    expected_reason: str,
) -> None:
    destination = _destination()
    activation = _activation_review(destination)
    transition = _transition_review(destination, activation, status=status)
    decision = _evaluate(
        destination=destination,
        activation_review=activation,
        transition_review=transition,
    )

    assert decision["decision"] == "block"
    assert expected_reason in decision["blocking_reasons"]


def test_identity_mismatch_and_ambiguity_fail_closed() -> None:
    destination = _destination()
    activation = _activation_review(destination)
    transition = _transition_review(destination, activation)
    activation["destination_fingerprint"] = "changed-destination-fingerprint"

    decision = _evaluate(
        destination=destination,
        activation_review=activation,
        transition_review=transition,
        ambiguities=[_ambiguity()],
    )

    assert decision["blocking_reasons"] == [
        "activation_identity_mismatch",
        "persistence_ambiguity",
    ]
    assert decision["ambiguity"]["event_ids"] == ["event-ambiguous-1"]


def test_validator_rejects_tampering_and_side_effect_claims() -> None:
    decision = _evaluate()

    reordered = deepcopy(decision)
    reordered["blocking_reasons"] = [
        "persistence_ambiguity",
        "delivery_disabled",
    ]
    with pytest.raises(ValidationError, match="canonical order"):
        validate_notification_execution_readiness_decision(reordered)

    changed = deepcopy(decision)
    changed["evaluated_at"] = (
        EVALUATED_AT + timedelta(seconds=1)
    ).isoformat()
    with pytest.raises(ValidationError, match="decision_id"):
        validate_notification_execution_readiness_decision(changed)

    side_effect = deepcopy(decision)
    side_effect["external_request_performed"] = True
    with pytest.raises(ValidationError, match="side-effect"):
        validate_notification_execution_readiness_decision(side_effect)

    unknown = deepcopy(decision)
    unknown["execute"] = True
    with pytest.raises(ValidationError, match="fields are invalid"):
        validate_notification_execution_readiness_decision(unknown)


def _write_configs(tmp_path: Path) -> tuple[Path, Path, NotificationDestination]:
    delivery_path = tmp_path / "notification-delivery.yaml"
    delivery_path.write_text(
        yaml.safe_dump(
            {
                "delivery": {
                    "webhook": {
                        "enabled": True,
                        "endpoint_env": ENDPOINT_ENV,
                        "timeout_seconds": 5,
                        "max_batch_events": 25,
                        "max_attempts_per_event": 3,
                        "initial_backoff_seconds": 1,
                    },
                    "retry_planning": {
                        "max_candidate_rows": 500,
                        "max_plan_events": 25,
                        "max_event_age_seconds": 604800,
                        "max_backoff_seconds": 3600,
                        "retryable_http_statuses": [
                            408,
                            425,
                            429,
                            500,
                            502,
                            503,
                            504,
                        ],
                        "retryable_error_codes": ["network_error"],
                    },
                    "retry_execution": {
                        "enabled": True,
                        "max_plan_age_seconds": 3600,
                        "max_events": 25,
                    },
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    destination_path = tmp_path / "notification-destinations.yaml"
    destination_path.write_text(
        yaml.safe_dump(
            {
                "model_version": "portfolio-risk-notification-destination-v1",
                "destinations": {
                    DESTINATION_ID: {
                        "channel": "webhook",
                        "endpoint_env": ENDPOINT_ENV,
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
                        "activation": {
                            "enabled": True,
                            "change_request_id": "CHG-READY-001",
                            "reviewed_by": ["platform-reviewer"],
                            "reviewed_at": "2026-05-01T00:00:00Z",
                            "review_expires_at": "2026-12-01T00:00:00Z",
                        },
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    destination = load_notification_destinations(destination_path)[DESTINATION_ID]
    return delivery_path, destination_path, destination


def test_runner_reads_postgres_evidence_without_execution(tmp_path: Path) -> None:
    delivery_path, destination_path, destination = _write_configs(tmp_path)
    activation = _activation_review(destination)
    transition = _transition_review(destination, activation)
    calls: list[dict[str, Any]] = []

    def reader(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {
            "activation_review": activation,
            "transition_review": transition,
            "ambiguities": [],
        }

    result = run_notification_execution_readiness_gate(
        execution_kind="retry",
        destination_id=DESTINATION_ID,
        evaluated_at=EVALUATED_AT,
        delivery_config_path=delivery_path,
        destination_config_path=destination_path,
        dsn="postgresql://example.invalid/read-only",
        schema_name="risk_platform",
        evidence_reader=reader,
    )

    assert result["decision"] == "allow"
    assert calls == [
        {
            "dsn": "postgresql://example.invalid/read-only",
            "destination_id": DESTINATION_ID,
            "schema_name": "risk_platform",
        }
    ]
    assert result["external_request_performed"] is False


def test_cli_exposes_no_execution_switch() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--execution-kind",
                "initial",
                "--destination-id",
                DESTINATION_ID,
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
        _write_summary(link, _evaluate())
