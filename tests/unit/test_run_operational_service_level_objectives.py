from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.analytics.operational_service_level_objectives import (
    parse_operational_objective_policy,
)
from src.common.exceptions import ValidationError
from src.orchestration.run_operational_service_level_objectives import (
    run_operational_service_level_objectives,
)


def _objective_policy():
    return parse_operational_objective_policy(
        {
            "objective_policies": {
                "demo": {
                    "operational_policy_id": "operations",
                    "window_sessions": 2,
                    "minimum_observations": 2,
                    "targets": {
                        "schedule_completion_attainment": {
                            "source_metric_name": "schedule_lag_sessions",
                            "success_threshold": 0,
                            "target_ratio": 1,
                        },
                        "market_freshness_attainment": {
                            "source_metric_name": (
                                "market_freshness_exception_count"
                            ),
                            "success_threshold": 0,
                            "target_ratio": 1,
                        },
                        "notification_retry_exhaustion_free_attainment": {
                            "source_metric_name": (
                                "notification_retry_exhausted_count"
                            ),
                            "success_threshold": 0,
                            "target_ratio": 1,
                        },
                        "notification_dead_letter_duration_attainment": {
                            "source_metric_name": (
                                "notification_oldest_dead_letter_age_seconds"
                            ),
                            "success_threshold": 900,
                            "target_ratio": 1,
                        },
                    },
                }
            }
        },
        "demo",
    )


def _report(day: int) -> dict[str, object]:
    names = (
        "schedule_lag_sessions",
        "market_freshness_exception_count",
        "notification_retry_exhausted_count",
        "notification_oldest_dead_letter_age_seconds",
    )
    units = ("sessions", "constituents", "events", "seconds")
    return {
        "calculation_id": (
            f"operational-service-levels-v1-report-{day:024x}"
        ),
        "policy_id": "operations",
        "policy_fingerprint": "operational-slo-policy-" + "a" * 24,
        "schedule_id": "schedule",
        "schedule_fingerprint": "schedule-fingerprint",
        "calendar_id": "XNYS",
        "portfolio_id": "portfolio",
        "risk_limit_policy_id": "risk-policy",
        "mandate_fingerprint": "mandate-fingerprint",
        "as_of": datetime(2026, 1, day, 1, tzinfo=timezone.utc),
        "latest_expected_session": date(2026, 1, day),
        "metrics_json": [
            {
                "metric_name": name,
                "observed_value": 0,
                "unit": unit,
                "warning_threshold": 1,
                "critical_threshold": 2,
                "status": "ok",
                "reason": None,
            }
            for name, unit in zip(names, units, strict=True)
        ],
        "document_sha256": f"{day:064x}",
    }


class _Calendar:
    calendar_id = "XNYS"
    valid_from = date(2026, 1, 1)

    def latest_expected_session(self, value: date) -> date:
        return value

    def sessions_between(self, start: date, end: date) -> tuple[date, ...]:
        del start
        return tuple(date(2026, 1, day) for day in range(1, end.day + 1))


def _run(*, calendar: Any, report_reader: Any):
    return run_operational_service_level_objectives(
        objective_policy_id="demo",
        through_session=date(2026, 1, 2),
        objective_config_path=Path("objectives.yaml"),
        operational_policy_config_path=Path("operations.yaml"),
        schedule_config_path=Path("schedule.yaml"),
        calendar_config_path=Path("calendar.yaml"),
        portfolio_config_path=Path("portfolio.yaml"),
        dsn="postgresql://example",
        objective_policy_loader=lambda *_: _objective_policy(),
        operational_policy_loader=lambda *_: SimpleNamespace(
            policy_id="operations",
            schedule_id="schedule",
            fingerprint="operational-slo-policy-" + "a" * 24,
        ),
        schedule_loader=lambda *_: SimpleNamespace(
            schedule_id="schedule",
            calendar_id="XNYS",
            portfolio_id="portfolio",
            policy_id="risk-policy",
            fingerprint="schedule-fingerprint",
        ),
        calendar_loader=lambda *_: calendar,
        mandate_loader=lambda *_: SimpleNamespace(
            mandate_id="mandate",
            fingerprint="mandate-fingerprint",
        ),
        report_reader=report_reader,
    )


def test_runner_binds_the_exact_current_contract_and_session_window() -> None:
    captured: dict[str, Any] = {}

    def reader(**kwargs: Any) -> list[dict[str, object]]:
        captured.update(kwargs)
        return [_report(1), _report(2)]

    result = _run(calendar=_Calendar(), report_reader=reader)

    assert result["overall_status"] == "met"
    assert result["provider_request_performed"] is False
    assert result["external_delivery_performed"] is False
    assert result["automated_remediation_performed"] is False
    assert captured["operational_policy_fingerprint"] == (
        "operational-slo-policy-" + "a" * 24
    )
    assert captured["window_start_session"] == date(2026, 1, 1)
    assert captured["through_session"] == date(2026, 1, 2)


def test_non_session_request_fails_before_report_read() -> None:
    class NonSessionCalendar(_Calendar):
        def latest_expected_session(self, value: date) -> date:
            del value
            return date(2026, 1, 1)

    called = False

    def reader(**kwargs: Any) -> list[dict[str, object]]:
        nonlocal called
        called = True
        del kwargs
        return []

    with pytest.raises(ValidationError, match="expected market session"):
        _run(calendar=NonSessionCalendar(), report_reader=reader)

    assert called is False
