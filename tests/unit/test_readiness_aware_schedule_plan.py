from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from src.analytics.market_calendar import parse_market_calendar
from src.analytics.operational_service_levels import (
    parse_operational_service_level_policy,
)
from src.analytics.portfolio_mandates import select_portfolio_mandate
from src.common.exceptions import StorageError, ValidationError
from src.orchestration.plan_readiness_aware_local_schedule import (
    plan_readiness_aware_local_schedule,
)
from src.orchestration.run_local_portfolio_schedule import (
    parse_local_portfolio_schedule,
)
from src.warehouse.operational_readiness_gate import (
    OperationalReadinessGatePolicy,
)


def _schedule():
    return parse_local_portfolio_schedule(
        {
            "schedules": {
                "us-tech-local": {
                    "enabled": False,
                    "portfolio_id": "us-tech-equal",
                    "policy_id": "us-tech-standard",
                    "calendar_id": "XNYS",
                    "maximum_catch_up_sessions": 5,
                    "volatility_window": 20,
                    "var_window": 60,
                    "var_confidence": 0.95,
                    "covariance_window": 20,
                    "max_snapshots": 5,
                    "max_evaluations": 100,
                    "max_notification_events": 100,
                }
            }
        },
        "us-tech-local",
    )


def _calendar():
    return parse_market_calendar(
        {
            "calendars": {
                "XNYS": {
                    "timezone": "America/New_York",
                    "valid_from": "2026-01-01",
                    "valid_to": "2027-01-01",
                    "session_weekdays": [0, 1, 2, 3, 4],
                    "regular_close_time": "16:00",
                    "holidays": ["2026-01-01"],
                    "early_closes": {},
                }
            }
        },
        "XNYS",
    )


def _mandate():
    return select_portfolio_mandate(
        {
            "portfolios": {
                "us-tech-equal": {
                    "mandates": [
                        {
                            "mandate_id": "us-tech-2026",
                            "base_currency": "USD",
                            "effective_from": "2026-01-01",
                            "effective_to": "2027-01-01",
                            "constituents": [
                                {
                                    "source": "alpha_vantage",
                                    "symbol": "AAPL",
                                    "weight": 0.5,
                                },
                                {
                                    "source": "alpha_vantage",
                                    "symbol": "MSFT",
                                    "weight": 0.5,
                                },
                            ],
                        }
                    ]
                }
            }
        },
        "us-tech-equal",
        date(2026, 1, 9),
    )


def _policy(*, schedule_id: str = "us-tech-local"):
    return parse_operational_service_level_policy(
        {
            "policies": {
                "us-tech-local": {
                    "schedule_id": schedule_id,
                    "metrics": {
                        "schedule_lag_sessions": {
                            "warning": 1,
                            "critical": 3,
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


def _gate() -> OperationalReadinessGatePolicy:
    return OperationalReadinessGatePolicy(
        gate_id="us-tech-local",
        operational_policy_id="us-tech-local",
        max_report_age_seconds=3600,
        allow_warning=False,
    )


def _raw_schedule_plan(
    *,
    sessions: list[str] | None = None,
    run_id: str = "random-run",
) -> dict[str, Any]:
    schedule = _schedule()
    calendar = _calendar()
    selected = sessions if sessions is not None else ["2026-01-09"]
    return {
        "run_id": run_id,
        "schedule_id": schedule.schedule_id,
        "schedule_fingerprint": schedule.fingerprint,
        "enabled": schedule.enabled,
        "calendar": {
            "calendar_id": calendar.calendar_id,
            "calendar_fingerprint": calendar.fingerprint,
        },
        "selection": {
            "as_of_date": "2026-01-10",
            "checkpoint_before": None,
            "sessions_selected": len(selected),
            "session_dates": selected,
        },
        "plans": [
            {
                "session_date": session,
                "mandate_id": _mandate().mandate_id,
                "mandate_fingerprint": _mandate().fingerprint,
                "commands": [["python", "-m", "example", session]],
            }
            for session in selected
        ],
        "execution": {
            "requested": False,
            "performed": False,
            "completed_sessions": [],
        },
        "provider_request_performed": False,
        "external_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def _run(
    *,
    readiness: dict[str, Any] | None,
    sessions: list[str] | None = None,
    run_id: str = "random-run",
    policy_schedule_id: str = "us-tech-local",
) -> dict[str, Any]:
    return plan_readiness_aware_local_schedule(
        schedule_id="us-tech-local",
        gate_id="us-tech-local",
        as_of_date=date(2026, 1, 10),
        schedule_config_path=Path("schedule.yaml"),
        gate_config_path=Path("gate.yaml"),
        operational_policy_config_path=Path("policy.yaml"),
        calendar_config_path=Path("calendar.yaml"),
        portfolio_config_path=Path("portfolio.yaml"),
        risk_limit_config_path=Path("limits.yaml"),
        storage_config_path=Path("storage.yaml"),
        state_dir=Path(".scheduler"),
        dsn="not-used",
        python_executable="python",
        schedule_loader=lambda *_: _schedule(),
        gate_loader=lambda *_: _gate(),
        operational_policy_loader=lambda *_: _policy(
            schedule_id=policy_schedule_id
        ),
        calendar_loader=lambda *_: _calendar(),
        mandate_loader=lambda *_: _mandate(),
        readiness_reader=lambda **_: readiness,
        schedule_planner=lambda **_: _raw_schedule_plan(
            sessions=sessions,
            run_id=run_id,
        ),
    )


def test_allowed_readiness_produces_deterministic_would_run_plan() -> None:
    readiness = {
        "decision_id": "decision-1",
        "decision": "allow",
        "reasons": [],
        "document_sha256": "a" * 64,
        "recorded_at": "2026-01-10T12:00:00+00:00",
    }
    first = _run(readiness=readiness, run_id="random-one")
    second = _run(readiness=readiness, run_id="random-two")

    assert first["plan_id"] == second["plan_id"]
    assert first["schedule_effect"]["decision"] == "would_run"
    assert first["execution"]["performed"] is False
    assert first["checkpoint_updated"] is False
    assert "run_id" not in first["schedule_plan"]


def test_blocked_or_missing_readiness_produces_would_block() -> None:
    blocked = _run(
        readiness={
            "decision_id": "decision-blocked",
            "decision": "block",
            "reasons": ["report_status_critical"],
            "document_sha256": "b" * 64,
            "recorded_at": "2026-01-10T12:00:00+00:00",
        }
    )
    missing = _run(readiness=None)

    assert blocked["schedule_effect"]["decision"] == "would_block"
    assert missing["schedule_effect"]["decision"] == "would_block"
    assert missing["readiness"]["reasons"] == ["decision_missing"]


def test_no_selected_sessions_is_no_work_even_when_readiness_is_missing() -> None:
    result = _run(readiness=None, sessions=[])

    assert result["schedule_effect"] == {
        "decision": "no_work",
        "sessions_selected": 0,
        "session_dates": [],
    }


def test_policy_schedule_mismatch_fails_before_reads_or_planning() -> None:
    calls = {"read": 0, "plan": 0}

    with pytest.raises(ValidationError, match="another schedule"):
        plan_readiness_aware_local_schedule(
            schedule_id="us-tech-local",
            gate_id="us-tech-local",
            as_of_date=date(2026, 1, 10),
            schedule_config_path=Path("schedule.yaml"),
            gate_config_path=Path("gate.yaml"),
            operational_policy_config_path=Path("policy.yaml"),
            calendar_config_path=Path("calendar.yaml"),
            portfolio_config_path=Path("portfolio.yaml"),
            risk_limit_config_path=Path("limits.yaml"),
            storage_config_path=Path("storage.yaml"),
            state_dir=Path(".scheduler"),
            dsn="not-used",
            schedule_loader=lambda *_: _schedule(),
            gate_loader=lambda *_: _gate(),
            operational_policy_loader=lambda *_: _policy(schedule_id="other"),
            calendar_loader=lambda *_: _calendar(),
            mandate_loader=lambda *_: _mandate(),
            readiness_reader=lambda **_: calls.__setitem__(
                "read", calls["read"] + 1
            ),
            schedule_planner=lambda **_: calls.__setitem__(
                "plan", calls["plan"] + 1
            ),
        )
    assert calls == {"read": 0, "plan": 0}


def test_schedule_planner_side_effect_evidence_fails_closed() -> None:
    raw = _raw_schedule_plan()
    raw["execution"] = {
        "requested": True,
        "performed": True,
        "completed_sessions": ["2026-01-09"],
    }

    with pytest.raises(StorageError, match="performed execution"):
        plan_readiness_aware_local_schedule(
            schedule_id="us-tech-local",
            gate_id="us-tech-local",
            as_of_date=date(2026, 1, 10),
            schedule_config_path=Path("schedule.yaml"),
            gate_config_path=Path("gate.yaml"),
            operational_policy_config_path=Path("policy.yaml"),
            calendar_config_path=Path("calendar.yaml"),
            portfolio_config_path=Path("portfolio.yaml"),
            risk_limit_config_path=Path("limits.yaml"),
            storage_config_path=Path("storage.yaml"),
            state_dir=Path(".scheduler"),
            dsn="not-used",
            schedule_loader=lambda *_: _schedule(),
            gate_loader=lambda *_: _gate(),
            operational_policy_loader=lambda *_: _policy(),
            calendar_loader=lambda *_: _calendar(),
            mandate_loader=lambda *_: _mandate(),
            readiness_reader=lambda **_: None,
            schedule_planner=lambda **_: raw,
        )
