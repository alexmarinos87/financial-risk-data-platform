from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.market_calendar import MarketCalendar
from src.analytics.operational_service_levels import (
    parse_operational_service_level_policy,
)
from src.analytics.portfolio_mandates import PortfolioMandate
from src.analytics.portfolio_risk import PortfolioConstituent
from src.common.exceptions import ValidationError
from src.orchestration.run_local_portfolio_schedule import LocalPortfolioSchedule
from src.warehouse.operational_readiness_gate import (
    evaluate_operational_readiness,
    parse_operational_readiness_gate_policy,
    run_operational_readiness_gate,
)


def _gate_policy(*, allow_warning: bool = False):
    return parse_operational_readiness_gate_policy(
        {
            "gates": {
                "us-tech-local": {
                    "operational_policy_id": "us-tech-local",
                    "max_report_age_seconds": 3600,
                    "allow_warning": allow_warning,
                }
            }
        },
        "us-tech-local",
    )


def _operational_policy():
    return parse_operational_service_level_policy(
        {
            "policies": {
                "us-tech-local": {
                    "schedule_id": "us-tech-local",
                    "metrics": {
                        "schedule_lag_sessions": {
                            "warning": 1,
                            "critical": 2,
                        },
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
        },
        "us-tech-local",
    )


def _schedule() -> LocalPortfolioSchedule:
    return LocalPortfolioSchedule(
        schedule_id="us-tech-local",
        enabled=False,
        portfolio_id="us-tech-equal",
        policy_id="us-tech-standard",
        calendar_id="XNYS",
        maximum_catch_up_sessions=5,
        volatility_window=20,
        var_window=60,
        var_confidence=0.95,
        covariance_window=20,
        max_snapshots=5,
        max_evaluations=100,
        max_notification_events=100,
    )


def _calendar() -> MarketCalendar:
    return MarketCalendar(
        calendar_id="XNYS",
        timezone_name="America/New_York",
        valid_from=date(2026, 1, 1),
        valid_to=date(2027, 1, 1),
        session_weekdays=(0, 1, 2, 3, 4),
        holidays=frozenset(),
        regular_close_time=time(16, 0),
        early_closes={},
    )


def _mandate() -> PortfolioMandate:
    return PortfolioMandate(
        portfolio_id="us-tech-equal",
        base_currency="USD",
        constituents=(
            PortfolioConstituent(
                source="alpha_vantage",
                symbol="AAPL",
                weight=0.5,
            ),
            PortfolioConstituent(
                source="alpha_vantage",
                symbol="MSFT",
                weight=0.5,
            ),
        ),
        mandate_id="us-tech-2026",
        effective_from=date(2026, 1, 1),
        effective_to=None,
    )


def _report(
    *,
    status: str = "ok",
    as_of: datetime | None = None,
    expected_session: date = date(2026, 3, 31),
) -> dict[str, Any]:
    operational_policy = _operational_policy()
    schedule = _schedule()
    mandate = _mandate()
    return {
        "calculation_id": (
            "operational-service-levels-v1-report-aaaaaaaaaaaaaaaaaaaaaaaa"
        ),
        "policy_id": operational_policy.policy_id,
        "policy_fingerprint": operational_policy.fingerprint,
        "schedule_id": schedule.schedule_id,
        "schedule_fingerprint": schedule.fingerprint,
        "calendar_id": "XNYS",
        "portfolio_id": schedule.portfolio_id,
        "risk_limit_policy_id": schedule.policy_id,
        "mandate_fingerprint": mandate.fingerprint,
        "as_of": as_of or datetime(2026, 4, 1, 11, 55, tzinfo=timezone.utc),
        "latest_expected_session": expected_session,
        "overall_status": status,
        "document_sha256": "b" * 64,
    }


def _evaluate(
    *,
    gate_policy=None,
    report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    operational_policy = _operational_policy()
    schedule = _schedule()
    mandate = _mandate()
    return evaluate_operational_readiness(
        gate_policy=gate_policy or _gate_policy(),
        evaluated_at=datetime(2026, 4, 1, 12, tzinfo=timezone.utc),
        latest_expected_session=date(2026, 3, 31),
        operational_policy_fingerprint=operational_policy.fingerprint,
        schedule_id=schedule.schedule_id,
        schedule_fingerprint=schedule.fingerprint,
        calendar_id="XNYS",
        portfolio_id=schedule.portfolio_id,
        risk_limit_policy_id=schedule.policy_id,
        mandate_fingerprint=mandate.fingerprint,
        report=_report() if report is None else report,
    )


def test_healthy_current_report_allows_deterministically() -> None:
    first = _evaluate()
    second = _evaluate()

    assert first == second
    assert first["decision"] == "allow"
    assert first["reasons"] == []
    assert first["report_age_seconds"] == 300.0
    assert first["report_future_seconds"] == 0.0
    assert first["schedule_executed"] is False
    assert first["provider_request_performed"] is False
    assert first["notification_delivery_performed"] is False
    assert first["cloud_schedule_activated"] is False


def test_warning_policy_is_explicit_and_critical_always_blocks() -> None:
    warning = _report(status="warning")
    blocked = _evaluate(report=warning)
    allowed = _evaluate(
        gate_policy=_gate_policy(allow_warning=True),
        report=warning,
    )
    critical = _evaluate(report=_report(status="critical"))

    assert blocked["decision"] == "block"
    assert blocked["reasons"] == ["report_status_warning"]
    assert allowed["decision"] == "allow"
    assert allowed["reasons"] == []
    assert critical["decision"] == "block"
    assert critical["reasons"] == ["report_status_critical"]


def test_missing_stale_future_and_session_mismatch_reports_block() -> None:
    operational_policy = _operational_policy()
    schedule = _schedule()
    mandate = _mandate()
    missing = evaluate_operational_readiness(
        gate_policy=_gate_policy(),
        evaluated_at=datetime(2026, 4, 1, 12, tzinfo=timezone.utc),
        latest_expected_session=date(2026, 3, 31),
        operational_policy_fingerprint=operational_policy.fingerprint,
        schedule_id=schedule.schedule_id,
        schedule_fingerprint=schedule.fingerprint,
        calendar_id="XNYS",
        portfolio_id=schedule.portfolio_id,
        risk_limit_policy_id=schedule.policy_id,
        mandate_fingerprint=mandate.fingerprint,
        report=None,
    )
    stale = _evaluate(
        report=_report(
            as_of=datetime(2026, 4, 1, 10, tzinfo=timezone.utc),
        )
    )
    future = _evaluate(
        report=_report(
            as_of=datetime(2026, 4, 1, 12, 1, tzinfo=timezone.utc),
        )
    )
    mismatched = _evaluate(
        report=_report(expected_session=date(2026, 3, 30))
    )

    assert missing["reasons"] == ["report_missing"]
    assert stale["reasons"] == ["report_age_exceeds_limit"]
    assert future["reasons"] == ["report_timestamp_future"]
    assert future["report_future_seconds"] == 60.0
    assert mismatched["reasons"] == ["report_session_mismatch"]


def test_gate_policy_contract_is_strict() -> None:
    with pytest.raises(ValidationError, match="invalid contract"):
        parse_operational_readiness_gate_policy(
            {
                "gates": {
                    "us-tech-local": {
                        "operational_policy_id": "us-tech-local",
                        "max_report_age_seconds": 3600,
                    }
                }
            },
            "us-tech-local",
        )
    with pytest.raises(ValidationError, match="between 1"):
        parse_operational_readiness_gate_policy(
            {
                "gates": {
                    "us-tech-local": {
                        "operational_policy_id": "us-tech-local",
                        "max_report_age_seconds": 0,
                        "allow_warning": False,
                    }
                }
            },
            "us-tech-local",
        )


def test_runner_resolves_exact_current_contract_before_read() -> None:
    operational_policy = _operational_policy()
    schedule = _schedule()
    mandate = _mandate()
    calls: list[dict[str, Any]] = []

    def report_reader(**kwargs: Any) -> dict[str, Any]:
        calls.append(dict(kwargs))
        return _report()

    result = run_operational_readiness_gate(
        gate_id="us-tech-local",
        evaluated_at=datetime(2026, 4, 1, 12, tzinfo=timezone.utc),
        dsn="postgresql://example",
        gate_config_path=Path("unused-gate.yaml"),
        operational_policy_config_path=Path("unused-policy.yaml"),
        schedule_config_path=Path("unused-schedule.yaml"),
        calendar_config_path=Path("unused-calendar.yaml"),
        portfolio_config_path=Path("unused-portfolio.yaml"),
        gate_policy_loader=lambda *_: _gate_policy(),
        operational_policy_loader=lambda *_: operational_policy,
        schedule_loader=lambda *_: schedule,
        calendar_loader=lambda *_: _calendar(),
        mandate_loader=lambda *_: mandate,
        report_reader=report_reader,
    )

    assert result["decision"] == "allow"
    assert calls == [
        {
            "dsn": "postgresql://example",
            "operational_policy_id": operational_policy.policy_id,
            "operational_policy_fingerprint": operational_policy.fingerprint,
            "schedule_id": schedule.schedule_id,
            "schedule_fingerprint": schedule.fingerprint,
            "calendar_id": "XNYS",
            "portfolio_id": schedule.portfolio_id,
            "risk_limit_policy_id": schedule.policy_id,
            "mandate_fingerprint": mandate.fingerprint,
            "schema_name": "risk_platform",
        }
    ]
