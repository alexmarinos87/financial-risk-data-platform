from __future__ import annotations

from datetime import date, datetime, time, timezone
from pathlib import Path

import pytest

from src.analytics.market_calendar import MarketCalendar
from src.analytics.operational_service_levels import (
    parse_operational_service_level_policy,
)
from src.analytics.portfolio_mandates import PortfolioMandate
from src.analytics.portfolio_risk import PortfolioConstituent
from src.common.exceptions import ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    WebhookDeliveryConfig,
)
from src.orchestration.run_local_portfolio_schedule import (
    MODEL_VERSION as SCHEDULE_MODEL_VERSION,
)
from src.orchestration.run_local_portfolio_schedule import LocalPortfolioSchedule
from src.orchestration.run_operational_service_levels import (
    run_operational_service_levels,
)


def _policy():
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


def _delivery() -> WebhookDeliveryConfig:
    return WebhookDeliveryConfig(
        enabled=False,
        endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL",
        timeout_seconds=5,
        max_batch_events=25,
        max_attempts_per_event=3,
        initial_backoff_seconds=1,
    )


def _freshness() -> list[dict[str, object]]:
    return [
        {
            "source": "alpha_vantage",
            "symbol": symbol,
            "calendar_id": "XNYS",
            "as_of_date": date(2026, 3, 31),
            "freshness_status": "current",
            "trailing_missing_session_count": 0,
        }
        for symbol in ("AAPL", "MSFT")
    ]


def test_runner_combines_schedule_state_and_postgres_evidence() -> None:
    schedule = _schedule()
    calls: list[dict[str, object]] = []

    def evidence_reader(**kwargs):
        calls.append(dict(kwargs))
        return _freshness(), []

    summary = run_operational_service_levels(
        policy_id="us-tech-local",
        as_of=datetime(2026, 3, 31, 12, tzinfo=timezone.utc),
        policy_config_path=Path("unused-policy.yaml"),
        schedule_config_path=Path("unused-schedule.yaml"),
        calendar_config_path=Path("unused-calendar.yaml"),
        portfolio_config_path=Path("unused-portfolio.yaml"),
        delivery_config_path=Path("unused-delivery.yaml"),
        state_dir=Path("unused-state"),
        dsn="postgresql://example",
        policy_loader=lambda *_: _policy(),
        schedule_loader=lambda *_: schedule,
        calendar_loader=lambda *_: _calendar(),
        mandate_loader=lambda *_: _mandate(),
        delivery_config_loader=lambda *_: _delivery(),
        state_reader=lambda _: {
            "model_version": SCHEDULE_MODEL_VERSION,
            "schedule_id": schedule.schedule_id,
            "schedule_fingerprint": schedule.fingerprint,
            "last_successful_session": "2026-03-30",
        },
        evidence_reader=evidence_reader,
    )

    assert summary["overall_status"] == "warning"
    assert summary["schedule_checkpoint"] == "2026-03-30"
    metrics = {metric["metric_name"]: metric for metric in summary["metrics"]}
    assert metrics["schedule_lag_sessions"]["observed_value"] == 1.0
    assert summary["provider_request_performed"] is False
    assert summary["external_delivery_performed"] is False
    assert summary["cloud_schedule_activated"] is False
    assert calls == [
        {
            "dsn": "postgresql://example",
            "calendar_id": "XNYS",
            "policy_id": "us-tech-standard",
            "portfolio_id": "us-tech-equal",
            "schema_name": "risk_platform",
        }
    ]


def test_missing_state_is_reported_without_executing_the_schedule() -> None:
    summary = run_operational_service_levels(
        policy_id="us-tech-local",
        as_of=datetime(2026, 3, 31, 12, tzinfo=timezone.utc),
        policy_config_path=Path("unused-policy.yaml"),
        schedule_config_path=Path("unused-schedule.yaml"),
        calendar_config_path=Path("unused-calendar.yaml"),
        portfolio_config_path=Path("unused-portfolio.yaml"),
        delivery_config_path=Path("unused-delivery.yaml"),
        state_dir=Path("unused-state"),
        dsn="postgresql://example",
        policy_loader=lambda *_: _policy(),
        schedule_loader=lambda *_: _schedule(),
        calendar_loader=lambda *_: _calendar(),
        mandate_loader=lambda *_: _mandate(),
        delivery_config_loader=lambda *_: _delivery(),
        state_reader=lambda _: None,
        evidence_reader=lambda **_: (_freshness(), []),
    )

    assert summary["overall_status"] == "critical"
    schedule_metric = next(
        metric
        for metric in summary["metrics"]
        if metric["metric_name"] == "schedule_lag_sessions"
    )
    assert schedule_metric["reason"] == "checkpoint_missing"


def test_changed_schedule_state_fails_closed_before_evidence_read() -> None:
    schedule = _schedule()
    called = False

    def evidence_reader(**_):
        nonlocal called
        called = True
        return [], []

    with pytest.raises(ValidationError, match="configuration changed"):
        run_operational_service_levels(
            policy_id="us-tech-local",
            as_of=datetime(2026, 3, 31, 12, tzinfo=timezone.utc),
            policy_config_path=Path("unused-policy.yaml"),
            schedule_config_path=Path("unused-schedule.yaml"),
            calendar_config_path=Path("unused-calendar.yaml"),
            portfolio_config_path=Path("unused-portfolio.yaml"),
            delivery_config_path=Path("unused-delivery.yaml"),
            state_dir=Path("unused-state"),
            dsn="postgresql://example",
            policy_loader=lambda *_: _policy(),
            schedule_loader=lambda *_: schedule,
            calendar_loader=lambda *_: _calendar(),
            mandate_loader=lambda *_: _mandate(),
            delivery_config_loader=lambda *_: _delivery(),
            state_reader=lambda _: {
                "model_version": SCHEDULE_MODEL_VERSION,
                "schedule_id": schedule.schedule_id,
                "schedule_fingerprint": "changed",
                "last_successful_session": "2026-03-31",
            },
            evidence_reader=evidence_reader,
        )

    assert called is False
