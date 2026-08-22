from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from src.analytics.portfolio_risk_limits import (
    CONCENTRATION_METRIC,
    MODEL_VERSION as RISK_LIMIT_MODEL_VERSION,
    VOLATILITY_METRIC,
)
from src.analytics.portfolio_risk_notification_outbox import (
    MAX_NOTIFICATION_EVENTS,
    MODEL_VERSION,
    build_portfolio_risk_notification_outbox,
)
from src.common.exceptions import ValidationError


def _transition(
    day: int,
    transition_type: str,
    previous_status: str | None,
    status: str,
    *,
    metric_name: str = VOLATILITY_METRIC,
    subject_key: str = "us-tech-equal",
    previous_subject_key: str | None = None,
    calculation_id: str | None = None,
) -> dict[str, object]:
    event_time = datetime(2026, 1, day, tzinfo=timezone.utc)
    is_concentration = metric_name == CONCENTRATION_METRIC
    observed_value = (
        0.2
        if status == "ok"
        else 0.35 if status == "warning" else 0.50
    )
    if is_concentration:
        observed_value = (
            0.50
            if status == "ok"
            else 0.70 if status == "warning" else 0.85
        )
    previous_calculation_id = (
        None if previous_status is None else f"previous-{day}"
    )
    return {
        "calculation_id": calculation_id or f"evaluation-{day}-{metric_name}",
        "model_version": RISK_LIMIT_MODEL_VERSION,
        "policy_id": "us-tech-standard",
        "policy_fingerprint": "risk-limit-policy-a",
        "portfolio_id": "us-tech-equal",
        "base_currency": "USD",
        "definition_fingerprint": "definition-a",
        "attribution_calculation_id": f"attribution-{day}",
        "attribution_model_version": "portfolio-attribution-v1",
        "weighting_method": "constant_weight_daily_rebalanced",
        "covariance_method": "sample_annualized",
        "correlation_method": "pearson",
        "covariance_window": 20,
        "annualization_days": 252,
        "ts_event": event_time,
        "ts_ingest": event_time + timedelta(hours=1),
        "metric_name": metric_name,
        "subject_type": "constituent" if is_concentration else "portfolio",
        "subject_key": subject_key,
        "unit": "absolute_share" if is_concentration else "annualized_decimal",
        "observed_value": observed_value,
        "observed_signed_value": (
            -observed_value if is_concentration else observed_value
        ),
        "warning_threshold": 0.65 if is_concentration else 0.30,
        "critical_threshold": 0.80 if is_concentration else 0.45,
        "status": status,
        "is_breach": status != "ok",
        "breach_threshold": (
            None
            if status == "ok"
            else 0.80 if is_concentration and status == "critical"
            else 0.65 if is_concentration
            else 0.45 if status == "critical"
            else 0.30
        ),
        "breach_excess": (
            0.0
            if status == "ok"
            else observed_value
            - (
                0.80
                if is_concentration and status == "critical"
                else 0.65
                if is_concentration
                else 0.45
                if status == "critical"
                else 0.30
            )
        ),
        "previous_status": previous_status,
        "previous_calculation_id": previous_calculation_id,
        "previous_subject_key": previous_subject_key,
        "transition_type": transition_type,
        "severity_rank": {"ok": 0, "warning": 1, "critical": 2}[status],
        "subject_changed": (
            previous_subject_key is not None
            and previous_subject_key != subject_key
        ),
    }


def test_notification_outbox_maps_actionable_transitions_and_payloads() -> None:
    records = [
        _transition(1, "opened", None, "warning"),
        _transition(2, "escalated", "warning", "critical"),
        _transition(3, "deescalated", "critical", "warning"),
        _transition(4, "resolved", "warning", "ok"),
    ]

    output = build_portfolio_risk_notification_outbox(records)

    assert [event["event_type"] for event in output.events] == [
        "breach_opened",
        "breach_escalated",
        "breach_deescalated",
        "breach_resolved",
    ]
    assert [event["delivery_disposition"] for event in output.events] == [
        "pending",
        "pending",
        "suppressed",
        "pending",
    ]
    assert output.events[2]["suppression_reason"] == "deescalation_not_routed"
    assert output.diagnostics["events_pending"] == 3
    assert output.diagnostics["events_suppressed"] == 1

    payload = json.loads(output.events[0]["payload_json"])
    assert payload["event_id"] == output.events[0]["event_id"]
    assert payload["policy"]["policy_fingerprint"] == "risk-limit-policy-a"
    assert payload["source"]["evaluation_calculation_id"].startswith(
        "evaluation-"
    )
    assert "dsn" not in output.events[0]["payload_json"].lower()
    assert output.events[0]["model_version"] == MODEL_VERSION


def test_concentration_event_retains_signed_subject_change_evidence() -> None:
    record = _transition(
        2,
        "escalated",
        "warning",
        "critical",
        metric_name=CONCENTRATION_METRIC,
        subject_key="alpha_vantage:MSFT",
        previous_subject_key="alpha_vantage:AAPL",
    )

    event = build_portfolio_risk_notification_outbox([record]).events[0]

    assert event["subject_changed"] is True
    assert event["observed_signed_value"] < 0
    payload = json.loads(event["payload_json"])
    assert payload["metric"]["previous_subject_key"] == "alpha_vantage:AAPL"
    assert payload["metric"]["subject_key"] == "alpha_vantage:MSFT"


def test_input_order_and_identical_duplicates_do_not_change_events() -> None:
    records = [
        _transition(1, "opened", None, "warning"),
        _transition(2, "escalated", "warning", "critical"),
    ]

    forward = build_portfolio_risk_notification_outbox([*records, records[0]])
    reverse = build_portfolio_risk_notification_outbox(reversed(records))

    assert forward.events == reverse.events


def test_non_actionable_rows_and_date_filters_are_explicit() -> None:
    unchanged = _transition(1, "opened", None, "warning")
    unchanged["transition_type"] = "unchanged"
    unchanged["previous_status"] = "warning"
    unchanged["previous_calculation_id"] = "previous-1"
    records = [
        unchanged,
        _transition(2, "opened", None, "warning"),
        _transition(3, "escalated", "warning", "critical"),
    ]

    output = build_portfolio_risk_notification_outbox(
        records,
        start_date=date(2026, 1, 3),
        end_date=date(2026, 1, 3),
    )

    assert len(output.events) == 1
    assert output.events[0]["transition_type"] == "escalated"
    assert output.diagnostics["skipped_non_actionable_transitions"] == 1
    assert output.diagnostics["skipped_outside_date_range"] == 1


def test_invalid_transition_contract_and_request_bound_fail_closed() -> None:
    invalid = _transition(1, "escalated", "warning", "critical")
    invalid["status"] = "warning"
    invalid["severity_rank"] = 1
    with pytest.raises(ValidationError, match="transition statuses"):
        build_portfolio_risk_notification_outbox([invalid])

    records = [
        _transition(1, "opened", None, "warning"),
        _transition(2, "escalated", "warning", "critical"),
    ]
    with pytest.raises(ValidationError, match="max_events"):
        build_portfolio_risk_notification_outbox(
            records,
            max_events=1,
        )

    with pytest.raises(ValidationError, match="between 1"):
        build_portfolio_risk_notification_outbox(
            records,
            max_events=MAX_NOTIFICATION_EVENTS + 1,
        )
