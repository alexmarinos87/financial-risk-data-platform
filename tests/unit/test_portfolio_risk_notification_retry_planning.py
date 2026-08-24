from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration.plan_portfolio_risk_notification_retries import (
    CLASSIFICATIONS,
    _build_parser,
    parse_retry_planning_policy,
    plan_portfolio_risk_notification_retries,
    read_notification_retry_candidates,
)
from src.orchestration.deliver_portfolio_risk_notifications import (
    parse_webhook_delivery_config,
)

PLANNED_AT = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)


def _config_payload(
    *,
    max_plan_events: int = 25,
    statuses: list[int] | None = None,
) -> dict[str, Any]:
    return {
        "delivery": {
            "webhook": {
                "enabled": False,
                "endpoint_env": "RISK_NOTIFICATION_WEBHOOK_URL",
                "timeout_seconds": 5,
                "max_batch_events": 25,
                "max_attempts_per_event": 3,
                "initial_backoff_seconds": 1,
            },
            "retry_planning": {
                "max_candidate_rows": 500,
                "max_plan_events": max_plan_events,
                "max_event_age_seconds": 7 * 24 * 60 * 60,
                "max_backoff_seconds": 3600,
                "retryable_http_statuses": statuses
                or [408, 425, 429, 500, 502, 503, 504],
                "retryable_error_codes": ["network_error"],
            },
        }
    }


def _write_config(
    tmp_path: Path,
    *,
    max_plan_events: int = 25,
) -> Path:
    path = tmp_path / "notification-delivery.yaml"
    path.write_text(
        yaml.safe_dump(
            _config_payload(max_plan_events=max_plan_events),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _candidate(
    event_id: str,
    *,
    ts_event: datetime | None = None,
    attempt_count: int = 1,
    last_attempted_at: datetime | None = None,
    http_status: int | None = 503,
    error_code: str | None = "http_503",
    acknowledgement: bool = False,
) -> dict[str, Any]:
    event_time = ts_event or PLANNED_AT - timedelta(hours=2)
    last_time = last_attempted_at or PLANNED_AT - timedelta(minutes=10)
    candidate: dict[str, Any] = {
        "event_id": event_id,
        "outbox_model_version": "portfolio-risk-notification-outbox-v1",
        "event_type": "breach_opened",
        "transition_type": "opened",
        "delivery_disposition": "pending",
        "source_evaluation_calculation_id": f"evaluation-{event_id}",
        "policy_id": "us-tech-standard",
        "policy_fingerprint": "policy-fingerprint",
        "portfolio_id": "us-tech-equal",
        "definition_fingerprint": "definition-fingerprint",
        "metric_name": "portfolio_volatility_annualized",
        "subject_type": "portfolio",
        "subject_key": "us-tech-equal",
        "current_status": "critical",
        "ts_event": event_time,
        "ts_ingest": event_time + timedelta(seconds=1),
        "payload_json": {"event_id": event_id, "severity": "critical"},
        "attempt_count": attempt_count,
        "last_attempt_id": (
            f"attempt-{event_id}-{attempt_count}" if attempt_count else None
        ),
        "last_attempt_number": attempt_count if attempt_count else None,
        "last_attempted_at": last_time if attempt_count else None,
        "last_attempt_outcome": "failed" if attempt_count else None,
        "last_http_status": http_status if attempt_count else None,
        "last_error_code": error_code if attempt_count else None,
        "acknowledgement_id": None,
        "acknowledged_at": None,
        "acknowledgement_disposition": None,
    }
    if acknowledgement:
        candidate.update(
            {
                "acknowledgement_id": f"ack-{event_id}",
                "acknowledged_at": PLANNED_AT - timedelta(minutes=5),
                "acknowledgement_disposition": "investigating",
            }
        )
    return candidate


def test_retry_policy_is_bounded_sorted_and_delivery_compatible() -> None:
    payload = _config_payload()
    delivery = parse_webhook_delivery_config(payload)
    first = parse_retry_planning_policy(payload, delivery)
    second = parse_retry_planning_policy(payload, delivery)

    assert first.fingerprint == second.fingerprint
    assert first.max_plan_events == delivery.max_batch_events

    with pytest.raises(ValidationError, match="sorted"):
        invalid = _config_payload(statuses=[503, 500])
        parse_retry_planning_policy(
            invalid,
            parse_webhook_delivery_config(invalid),
        )

    too_large = _config_payload(max_plan_events=26)
    with pytest.raises(ValidationError, match="max_batch_events"):
        parse_retry_planning_policy(
            too_large,
            parse_webhook_delivery_config(too_large),
        )


def test_plan_classifies_every_supported_dead_letter_state(
    tmp_path: Path,
) -> None:
    candidates = [
        _candidate("event-retryable"),
        _candidate(
            "event-not-yet-eligible",
            attempt_count=2,
            last_attempted_at=PLANNED_AT - timedelta(seconds=1),
            http_status=None,
            error_code="network_error",
        ),
        _candidate("event-attempts-exhausted", attempt_count=3),
        _candidate(
            "event-expired",
            ts_event=PLANNED_AT - timedelta(days=8),
        ),
        _candidate("event-acknowledged", acknowledgement=True),
        _candidate(
            "event-invalid",
            http_status=400,
            error_code="http_400",
        ),
    ]
    summary = plan_portfolio_risk_notification_retries(
        config_path=_write_config(tmp_path),
        dsn="postgresql://secret-value",
        planned_at=PLANNED_AT,
        reader=lambda **_: list(reversed(candidates)),
    )

    assert summary["selection"]["classification_counts"] == {
        classification: 1 for classification in CLASSIFICATIONS
    }
    assert summary["retryable_event_ids"] == ["event-retryable"]
    by_id = {event["event_id"]: event for event in summary["events"]}
    assert by_id["event-not-yet-eligible"]["reason"] == "retry_backoff_active"
    assert by_id["event-attempts-exhausted"]["reason"] == (
        "maximum_attempts_reached"
    )
    assert by_id["event-expired"]["reason"] == "event_age_exceeds_policy"
    assert by_id["event-acknowledged"]["reason"] == (
        "source_breach_acknowledged"
    )
    assert by_id["event-invalid"]["reason"] == "last_failure_not_retryable"
    assert summary["delivery_performed"] is False
    assert summary["delivery_attempt_written"] is False
    assert summary["dead_letter_mutated"] is False
    assert summary["external_request_performed"] is False

    rendered = json.dumps(summary, sort_keys=True)
    assert "postgresql://secret-value" not in rendered
    assert '"severity"' not in rendered


def test_plan_identity_and_event_order_are_input_order_independent(
    tmp_path: Path,
) -> None:
    candidates = [_candidate("event-b"), _candidate("event-a")]
    kwargs = {
        "config_path": _write_config(tmp_path),
        "dsn": "dsn",
        "planned_at": PLANNED_AT,
    }
    first = plan_portfolio_risk_notification_retries(
        **kwargs,
        reader=lambda **_: candidates,
    )
    second = plan_portfolio_risk_notification_retries(
        **kwargs,
        reader=lambda **_: list(reversed(candidates)),
    )

    assert first["plan_id"] == second["plan_id"]
    assert [event["event_id"] for event in first["events"]] == [
        "event-a",
        "event-b",
    ]


def test_retryable_plan_limit_fails_closed_without_truncation(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="max_plan_events"):
        plan_portfolio_risk_notification_retries(
            config_path=_write_config(tmp_path, max_plan_events=1),
            dsn="dsn",
            planned_at=PLANNED_AT,
            reader=lambda **_: [_candidate("event-a"), _candidate("event-b")],
        )


def test_duplicate_event_identity_and_inconsistent_attempts_fail_closed(
    tmp_path: Path,
) -> None:
    duplicate = _candidate("event-duplicate")
    with pytest.raises(ValidationError, match="duplicate event_id"):
        plan_portfolio_risk_notification_retries(
            config_path=_write_config(tmp_path),
            dsn="dsn",
            planned_at=PLANNED_AT,
            reader=lambda **_: [duplicate, dict(duplicate)],
        )

    inconsistent = _candidate("event-inconsistent", attempt_count=2)
    inconsistent["last_attempt_number"] = 1
    result = plan_portfolio_risk_notification_retries(
        config_path=_write_config(tmp_path),
        dsn="dsn",
        planned_at=PLANNED_AT,
        reader=lambda **_: [inconsistent],
    )
    assert result["events"][0]["classification"] == "invalid"
    assert result["events"][0]["reason"] == "attempt_summary_inconsistent"


def test_candidate_reader_rejects_invalid_bounds_before_database_access() -> None:
    with pytest.raises(ValidationError, match="max_candidate_rows"):
        read_notification_retry_candidates(
            dsn="not-used",
            planned_at=PLANNED_AT,
            max_candidate_rows=0,
        )


def test_retry_planner_cli_has_no_execution_switch() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--planned-at",
                PLANNED_AT.isoformat(),
                "--execute",
            ]
        )
