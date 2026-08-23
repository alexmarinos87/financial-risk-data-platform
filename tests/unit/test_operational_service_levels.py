from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from src.analytics.operational_service_levels import (
    evaluate_operational_service_levels,
    parse_operational_service_level_policy,
)
from src.common.exceptions import ValidationError


def _policy_payload() -> dict[str, object]:
    return {
        "policies": {
            "us-tech-local": {
                "schedule_id": "us-tech-local",
                "metrics": {
                    "schedule_lag_sessions": {"warning": 1, "critical": 2},
                    "market_freshness_exception_count": {
                        "warning": 1,
                        "critical": 2,
                    },
                    "notification_retry_exhausted_count": {
                        "warning": 1,
                        "critical": 2,
                    },
                    "notification_oldest_dead_letter_age_seconds": {
                        "warning": 900,
                        "critical": 3600,
                    },
                },
            }
        }
    }


def _policy():
    return parse_operational_service_level_policy(
        _policy_payload(),
        "us-tech-local",
    )


def _freshness(status: str = "current") -> list[dict[str, object]]:
    return [
        {
            "source": "alpha_vantage",
            "symbol": symbol,
            "calendar_id": "XNYS",
            "as_of_date": date(2026, 3, 31),
            "freshness_status": status,
            "trailing_missing_session_count": 0 if status != "stale" else 1,
        }
        for symbol in ("AAPL", "MSFT")
    ]


def _evaluate(
    *,
    lag: int | None = 0,
    checkpoint: date | None = date(2026, 3, 31),
    freshness: list[dict[str, object]] | None = None,
    notifications: list[dict[str, object]] | None = None,
):
    return evaluate_operational_service_levels(
        policy=_policy(),
        as_of=datetime(2026, 4, 1, 12, tzinfo=timezone.utc),
        schedule_fingerprint="local-schedule-example",
        latest_expected_session=date(2026, 3, 31),
        schedule_checkpoint=checkpoint,
        schedule_lag_sessions=lag,
        expected_constituents=(
            "alpha_vantage:AAPL",
            "alpha_vantage:MSFT",
        ),
        calendar_id="XNYS",
        freshness_records=_freshness() if freshness is None else freshness,
        notification_records=[] if notifications is None else notifications,
        maximum_notification_attempts=3,
    )


def test_current_evidence_produces_deterministic_ok_report() -> None:
    first = _evaluate()
    second = _evaluate()

    assert first.report == second.report
    assert first.report["overall_status"] == "ok"
    assert first.report["calculation_id"].startswith(
        "operational-service-levels-v1-report-"
    )
    assert {metric["status"] for metric in first.metrics} == {"ok"}
    assert first.report["freshness_exceptions"] == []
    assert first.report["notification_retry_exhausted_events"] == []


def test_missing_checkpoint_and_stale_inputs_produce_critical_report() -> None:
    as_of = datetime(2026, 4, 1, 12, tzinfo=timezone.utc)
    notifications = [
        {
            "event_id": "event-1",
            "ts_event": as_of - timedelta(hours=3),
            "attempt_count": 3,
            "delivered": False,
            "last_attempted_at": as_of - timedelta(hours=2),
        }
    ]
    output = _evaluate(
        lag=None,
        checkpoint=None,
        freshness=_freshness("stale"),
        notifications=notifications,
    )
    metrics = {metric["metric_name"]: metric for metric in output.metrics}

    assert output.report["overall_status"] == "critical"
    assert metrics["schedule_lag_sessions"] == {
        "metric_name": "schedule_lag_sessions",
        "observed_value": None,
        "unit": "sessions",
        "warning_threshold": 1.0,
        "critical_threshold": 2.0,
        "status": "critical",
        "reason": "checkpoint_missing",
    }
    assert metrics["market_freshness_exception_count"]["observed_value"] == 2.0
    assert metrics["notification_retry_exhausted_count"]["status"] == "warning"
    assert (
        metrics["notification_oldest_dead_letter_age_seconds"]["observed_value"]
        == 7200.0
    )
    assert output.report["notification_retry_exhausted_events"] == ["event-1"]


def test_missing_freshness_record_is_counted_as_an_exception() -> None:
    output = _evaluate(freshness=_freshness()[:1])

    assert output.report["freshness_exceptions"] == [
        "alpha_vantage:MSFT:missing"
    ]
    metrics = {metric["metric_name"]: metric for metric in output.metrics}
    assert metrics["market_freshness_exception_count"]["status"] == "warning"


def test_duplicate_evidence_and_future_attempts_fail_closed() -> None:
    with pytest.raises(ValidationError, match="duplicate constituents"):
        _evaluate(freshness=[*_freshness(), _freshness()[0]])

    as_of = datetime(2026, 4, 1, 12, tzinfo=timezone.utc)
    with pytest.raises(ValidationError, match="must not be in the future"):
        _evaluate(
            notifications=[
                {
                    "event_id": "event-future",
                    "ts_event": as_of,
                    "attempt_count": 3,
                    "delivered": False,
                    "last_attempted_at": as_of + timedelta(seconds=1),
                }
            ]
        )


def test_policy_thresholds_and_metric_set_are_strict() -> None:
    payload = _policy_payload()
    candidate = payload["policies"]["us-tech-local"]  # type: ignore[index]
    candidate["metrics"]["schedule_lag_sessions"] = {  # type: ignore[index]
        "warning": 2,
        "critical": 2,
    }
    with pytest.raises(ValidationError, match="greater than warning"):
        parse_operational_service_level_policy(payload, "us-tech-local")

    payload = _policy_payload()
    candidate = payload["policies"]["us-tech-local"]  # type: ignore[index]
    del candidate["metrics"]["schedule_lag_sessions"]  # type: ignore[index]
    with pytest.raises(ValidationError, match="supported metric set"):
        parse_operational_service_level_policy(payload, "us-tech-local")
