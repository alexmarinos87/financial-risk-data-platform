from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration import deliver_portfolio_risk_notifications as target


def _write_contracts(tmp_path: Path) -> tuple[Path, Path]:
    delivery_path = tmp_path / "notification_delivery.yaml"
    delivery_path.write_text(
        yaml.safe_dump(
            {
                "delivery": {
                    "webhook": {
                        "enabled": True,
                        "endpoint_env": "RISK_NOTIFICATION_WEBHOOK_URL",
                        "timeout_seconds": 5,
                        "max_batch_events": 25,
                        "max_attempts_per_event": 3,
                        "initial_backoff_seconds": 1,
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    destination_path = tmp_path / "notification_destinations.yaml"
    destination_path.write_text(
        yaml.safe_dump(
            {
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
                        "allowed_event_types": ["breach_opened"],
                        "activation": {
                            "enabled": True,
                            "change_request_id": "CHG-2026-READINESS",
                            "reviewed_by": ["risk-control-reviewer"],
                            "reviewed_at": "2026-01-01T00:00:00Z",
                            "review_expires_at": "2027-01-01T00:00:00Z",
                        },
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return delivery_path, destination_path


def _candidate() -> dict[str, Any]:
    return {
        "event_id": "event-1",
        "event_type": "breach_opened",
        "transition_type": "opened",
        "policy_id": "us-tech-standard",
        "policy_fingerprint": "policy-1",
        "portfolio_id": "us-tech-equal",
        "definition_fingerprint": "definition-1",
        "metric_name": "portfolio_volatility_annualized",
        "subject_type": "portfolio",
        "subject_key": "us-tech-equal",
        "current_status": "critical",
        "ts_event": datetime(2026, 1, 9, tzinfo=timezone.utc),
        "payload_json": {"event_id": "event-1", "severity": "critical"},
        "attempts_so_far": 0,
    }


def _readiness() -> dict[str, Any]:
    return {
        "model_version": (
            "portfolio-risk-notification-execution-readiness-enforcement-v1"
        ),
        "enforcement_id": "readiness-enforcement-1",
        "destination_id": "risk-operations-webhook",
        "execution_kind": "initial",
        "enforced_at": "2026-06-01T12:00:00+00:00",
        "lock": {
            "key_fingerprint": "lock-fingerprint-1",
            "model_version": "portfolio-risk-notification-delivery-lock-v1",
            "scope": "portfolio-risk-notification-delivery",
        },
        "readiness_record_id": "readiness-record-1",
        "readiness_request_id": "readiness-request-1",
        "retained_decision_id": "retained-decision-1",
        "refreshed_decision_id": "refreshed-decision-1",
        "execution_ready": True,
        "readiness_review_status": "allowed",
        "retained_decision_evaluated_at": "2026-06-01T11:59:00+00:00",
        "refreshed_decision_evaluated_at": "2026-06-01T12:00:00+00:00",
        "substantive_evidence_match": True,
    }


def test_readiness_is_enforced_inside_lock_before_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delivery_path, destination_path = _write_contracts(tmp_path)
    events: list[str] = []
    attempts: list[dict[str, Any]] = []

    @contextmanager
    def lock(**_: Any) -> Iterator[Mapping[str, Any]]:
        events.append("lock-acquired")
        yield {
            "model_version": "portfolio-risk-notification-delivery-lock-v1",
            "scope": "portfolio-risk-notification-delivery",
            "key_fingerprint": "lock-fingerprint-1",
            "acquired": True,
        }
        events.append("lock-released")

    def enforce(**kwargs: Any) -> dict[str, Any]:
        events.append("readiness")
        assert kwargs["lock_evidence"]["acquired"] is True
        assert kwargs["evaluated_at"] == datetime(
            2026,
            6,
            1,
            12,
            0,
            tzinfo=timezone.utc,
        )
        return _readiness()

    monkeypatch.setattr(target, "_enforce_initial_delivery_readiness", enforce)
    monkeypatch.setattr(
        target,
        "_validate_initial_delivery_readiness",
        lambda value: dict(value),
    )

    summary = target.deliver_portfolio_risk_notifications(
        config_path=delivery_path,
        destination_config_path=destination_path,
        dsn="dsn",
        execute=True,
        environment={
            "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.example.test/risk"
        },
        reader=lambda **_: events.append("candidate-read") or [_candidate()],
        attempt_writer=lambda attempt: attempts.append(dict(attempt)),
        transport=lambda *_: events.append("transport") or 204,
        lock_factory=lock,
        clock=lambda: datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
    )

    assert events == [
        "lock-acquired",
        "candidate-read",
        "readiness",
        "transport",
        "lock-released",
    ]
    assert len(attempts) == 1
    assert summary["execution_readiness"] == _readiness()
    assert summary["concurrency_control"]["released"] is True


def test_readiness_rejection_prevents_transport_and_attempt_persistence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delivery_path, destination_path = _write_contracts(tmp_path)
    sends: list[bool] = []
    attempts: list[dict[str, Any]] = []

    @contextmanager
    def lock(**_: Any) -> Iterator[Mapping[str, Any]]:
        yield {
            "model_version": "portfolio-risk-notification-delivery-lock-v1",
            "scope": "portfolio-risk-notification-delivery",
            "key_fingerprint": "lock-fingerprint-1",
            "acquired": True,
        }

    def reject(**_: Any) -> dict[str, Any]:
        raise ValidationError("notification execution readiness is not allowed")

    monkeypatch.setattr(target, "_enforce_initial_delivery_readiness", reject)

    with pytest.raises(ValidationError, match="not allowed"):
        target.deliver_portfolio_risk_notifications(
            config_path=delivery_path,
            destination_config_path=destination_path,
            dsn="dsn",
            execute=True,
            environment={
                "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.example.test/risk"
            },
            reader=lambda **_: [_candidate()],
            attempt_writer=lambda attempt: attempts.append(dict(attempt)),
            transport=lambda *_: sends.append(True) or 204,
            lock_factory=lock,
            clock=lambda: datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
        )

    assert sends == []
    assert attempts == []
