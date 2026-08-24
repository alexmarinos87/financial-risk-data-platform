from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.analytics.market_calendar import parse_market_calendar
from src.analytics.portfolio_mandates import select_portfolio_mandate
from src.common.exceptions import StorageError, ValidationError
from src.orchestration.operational_readiness_execution_authority import (
    build_operational_readiness_execution_authority,
)
from src.orchestration.run_local_portfolio_schedule import (
    MODEL_VERSION,
    build_session_commands,
    parse_local_portfolio_schedule,
    plan_schedule_sessions,
    run_local_portfolio_schedule,
)
from src.warehouse.local_schedule_run_recorder import validate_local_schedule_run


def _schedule_payload(*, enabled: bool = False, maximum: int = 5):
    return {
        "schedules": {
            "us-tech-local": {
                "enabled": enabled,
                "portfolio_id": "us-tech-equal",
                "policy_id": "us-tech-standard",
                "calendar_id": "XNYS",
                "maximum_catch_up_sessions": maximum,
                "volatility_window": 20,
                "var_window": 60,
                "var_confidence": 0.95,
                "covariance_window": 20,
                "max_snapshots": 5,
                "max_evaluations": 100,
                "max_notification_events": 100,
            }
        }
    }


def _schedule(*, enabled: bool = False, maximum: int = 5):
    return parse_local_portfolio_schedule(
        _schedule_payload(enabled=enabled, maximum=maximum),
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
                    "holidays": ["2026-01-01", "2026-01-19"],
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


def _write_schedule(tmp_path: Path, *, enabled: bool, maximum: int = 5) -> Path:
    path = tmp_path / "schedule.yaml"
    path.write_text(
        yaml.safe_dump(
            _schedule_payload(enabled=enabled, maximum=maximum),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _execution_authority(*, enabled: bool) -> dict[str, Any]:
    schedule = _schedule(enabled=enabled)
    calendar = _calendar()
    mandate = _mandate()
    return build_operational_readiness_execution_authority(
        plan={
            "plan_id": "readiness-aware-schedule-plan-v1-plan-" + "a" * 24,
            "model_version": "readiness-aware-schedule-plan-v1",
            "schedule_id": schedule.schedule_id,
            "schedule_fingerprint": schedule.fingerprint,
            "portfolio_id": schedule.portfolio_id,
            "risk_limit_policy_id": schedule.policy_id,
            "mandate_id": mandate.mandate_id,
            "mandate_fingerprint": mandate.fingerprint,
            "as_of_date": "2026-01-10",
            "latest_expected_session": "2026-01-09",
            "readiness": {
                "status": "current",
                "decision_id": (
                    "operational-readiness-gate-v1-decision-" + "b" * 24
                ),
                "decision": "allow",
                "reasons": [],
                "document_sha256": "c" * 64,
                "gate_id": "us-tech-local",
                "gate_fingerprint": (
                    "operational-readiness-gate-" + "d" * 24
                ),
                "operational_policy_id": "us-tech-local",
                "operational_policy_fingerprint": (
                    "operational-slo-policy-" + "e" * 24
                ),
            },
            "schedule_plan": {
                "calendar": {
                    "calendar_id": calendar.calendar_id,
                    "calendar_fingerprint": calendar.fingerprint,
                }
            },
            "schedule_effect": {
                "decision": "would_run",
                "sessions_selected": 1,
                "session_dates": ["2026-01-09"],
            },
        },
        authorized_at="2026-01-10T12:00:00Z",
    )


def _history_recorder(records: list[dict[str, Any]]):
    def recorder(*, dsn: str, run: dict[str, Any]) -> dict[str, Any]:
        assert dsn
        validated = validate_local_schedule_run(run)
        records.append(validated)
        return {
            "run_id": validated["run_id"],
            "request_id": validated["request_id"],
            "run_status": validated["run_status"],
            "selected_session_count": validated["selected_session_count"],
            "started_session_count": validated["started_session_count"],
            "completed_session_count": validated["completed_session_count"],
            "checkpoint_after": validated["checkpoint_after"],
            "document_sha256": "f" * 64,
            "created": True,
        }

    return recorder


def test_initial_plan_uses_latest_expected_session_on_weekend() -> None:
    sessions = plan_schedule_sessions(
        schedule=_schedule(),
        calendar=_calendar(),
        as_of_date=date(2026, 1, 10),
        state=None,
    )
    assert sessions == (date(2026, 1, 9),)


def test_checkpoint_plans_bounded_sessions_and_rejects_excess() -> None:
    schedule = _schedule(maximum=3)
    state = {
        "model_version": MODEL_VERSION,
        "schedule_id": schedule.schedule_id,
        "schedule_fingerprint": schedule.fingerprint,
        "last_successful_session": "2026-01-06",
    }
    assert plan_schedule_sessions(
        schedule=schedule,
        calendar=_calendar(),
        as_of_date=date(2026, 1, 9),
        state=state,
    ) == (
        date(2026, 1, 7),
        date(2026, 1, 8),
        date(2026, 1, 9),
    )

    with pytest.raises(ValidationError, match="catch-up"):
        plan_schedule_sessions(
            schedule=_schedule(maximum=2),
            calendar=_calendar(),
            as_of_date=date(2026, 1, 9),
            state={
                **state,
                "schedule_fingerprint": _schedule(maximum=2).fingerprint,
            },
        )


def test_command_plan_uses_current_cli_contracts_without_secrets() -> None:
    commands = build_session_commands(
        schedule=_schedule(enabled=True),
        session_date=date(2026, 1, 9),
        mandate=_mandate(),
        python_executable="python",
        portfolio_config_path=Path("portfolios.yaml"),
        risk_limit_config_path=Path("limits.yaml"),
        calendar_config_path=Path("calendar.yaml"),
        storage_config_path=Path("storage.yaml"),
        state_dir=Path(".scheduler"),
    )
    flattened = "\n".join(" ".join(command) for command in commands)

    assert flattened.count("src.orchestration.run_daily_risk") == 2
    assert flattened.count("src.orchestration.run_market_freshness") == 2
    assert "src.orchestration.run_governed_portfolio_cycle" in flattened
    assert "--risk-limit-config limits.yaml" in flattened
    assert "portfolio-risk-limits-warehouse-load" in flattened
    assert "run_optional_portfolio_risk_notification_outbox" in flattened
    assert "portfolio_risk_notification_outbox_loader" in flattened
    assert "postgresql://" not in flattened


def test_dry_run_does_not_execute_or_create_state(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []
    summary = run_local_portfolio_schedule(
        schedule_id="us-tech-local",
        as_of_date=date(2026, 1, 10),
        schedule_config_path=_write_schedule(tmp_path, enabled=False),
        calendar_config_path=Path("calendar.yaml"),
        portfolio_config_path=Path("portfolios.yaml"),
        risk_limit_config_path=Path("limits.yaml"),
        storage_config_path=Path("storage.yaml"),
        state_dir=tmp_path / "state",
        dsn="not-used",
        execute=False,
        python_executable="python",
        command_runner=lambda command, _: calls.append(command),
        mandate_loader=lambda *_: _mandate(),
        calendar_loader=lambda *_: _calendar(),
    )

    assert calls == []
    assert summary["execution"]["performed"] is False
    assert summary["execution_authority"] is None
    assert summary["run_history"] is None
    assert summary["selection"]["session_dates"] == ["2026-01-09"]
    assert not (tmp_path / "state" / "us-tech-local.json").exists()


def test_direct_execution_without_authority_fails_before_commands(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []
    with pytest.raises(ValidationError, match="requires exact"):
        run_local_portfolio_schedule(
            schedule_id="us-tech-local",
            as_of_date=date(2026, 1, 10),
            schedule_config_path=_write_schedule(tmp_path, enabled=True),
            calendar_config_path=Path("calendar.yaml"),
            portfolio_config_path=Path("portfolios.yaml"),
            risk_limit_config_path=Path("limits.yaml"),
            storage_config_path=Path("storage.yaml"),
            state_dir=tmp_path / "state",
            dsn="dsn",
            execute=True,
            python_executable="python",
            command_runner=lambda command, _: calls.append(command),
            mandate_loader=lambda *_: _mandate(),
            calendar_loader=lambda *_: _calendar(),
        )
    assert calls == []
    assert not (tmp_path / "state" / "us-tech-local.json").exists()


def test_disabled_schedule_refuses_authorized_execution_before_commands(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []
    with pytest.raises(ValidationError, match="disabled"):
        run_local_portfolio_schedule(
            schedule_id="us-tech-local",
            as_of_date=date(2026, 1, 10),
            schedule_config_path=_write_schedule(tmp_path, enabled=False),
            calendar_config_path=Path("calendar.yaml"),
            portfolio_config_path=Path("portfolios.yaml"),
            risk_limit_config_path=Path("limits.yaml"),
            storage_config_path=Path("storage.yaml"),
            state_dir=tmp_path / "state",
            dsn="dsn",
            execute=True,
            execution_authority=_execution_authority(enabled=False),
            python_executable="python",
            command_runner=lambda command, _: calls.append(command),
            mandate_loader=lambda *_: _mandate(),
            calendar_loader=lambda *_: _calendar(),
        )
    assert calls == []


def test_successful_authorized_execution_checkpoints_and_records_terminal_history(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []
    environments: list[dict[str, str]] = []
    history: list[dict[str, Any]] = []

    def runner(command: tuple[str, ...], environment: Any) -> None:
        calls.append(command)
        environments.append(dict(environment))

    authority = _execution_authority(enabled=True)
    summary = run_local_portfolio_schedule(
        schedule_id="us-tech-local",
        as_of_date=date(2026, 1, 10),
        schedule_config_path=_write_schedule(tmp_path, enabled=True),
        calendar_config_path=Path("calendar.yaml"),
        portfolio_config_path=Path("portfolios.yaml"),
        risk_limit_config_path=Path("limits.yaml"),
        storage_config_path=Path("storage.yaml"),
        state_dir=tmp_path / "state",
        dsn="postgresql://secret-value",
        execute=True,
        execution_authority=authority,
        python_executable="python",
        command_runner=runner,
        mandate_loader=lambda *_: _mandate(),
        calendar_loader=lambda *_: _calendar(),
        run_history_recorder=_history_recorder(history),
    )

    state = json.loads(
        (tmp_path / "state" / "us-tech-local.json").read_text(encoding="utf-8")
    )
    assert state["last_successful_session"] == "2026-01-09"
    assert summary["execution"]["completed_sessions"] == ["2026-01-09"]
    assert summary["execution_authority"] == authority
    assert summary["run_history"]["run_status"] == "completed"
    assert len(history) == 1
    assert history[0]["request_id"] == authority["authority_id"]
    assert history[0]["run_status"] == "completed"
    assert history[0]["sessions"][0]["status"] == "completed"
    assert history[0]["sessions"][0]["stages"][-1]["stage_name"] == "checkpoint"
    assert len(calls) == 9
    assert all(
        environment["WAREHOUSE_POSTGRES_DSN"] == "postgresql://secret-value"
        for environment in environments
    )
    assert "postgresql://secret-value" not in json.dumps(summary)
    assert "postgresql://secret-value" not in json.dumps(history)


def test_failed_authorized_command_records_failure_without_advancing_checkpoint(
    tmp_path: Path,
) -> None:
    calls = 0
    history: list[dict[str, Any]] = []

    def fail_after_first(command: tuple[str, ...], environment: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise StorageError("failed")

    with pytest.raises(StorageError, match="failed"):
        run_local_portfolio_schedule(
            schedule_id="us-tech-local",
            as_of_date=date(2026, 1, 10),
            schedule_config_path=_write_schedule(tmp_path, enabled=True),
            calendar_config_path=Path("calendar.yaml"),
            portfolio_config_path=Path("portfolios.yaml"),
            risk_limit_config_path=Path("limits.yaml"),
            storage_config_path=Path("storage.yaml"),
            state_dir=tmp_path / "state",
            dsn="dsn",
            execute=True,
            execution_authority=_execution_authority(enabled=True),
            python_executable="python",
            command_runner=fail_after_first,
            mandate_loader=lambda *_: _mandate(),
            calendar_loader=lambda *_: _calendar(),
            run_history_recorder=_history_recorder(history),
        )
    assert not (tmp_path / "state" / "us-tech-local.json").exists()
    assert len(history) == 1
    assert history[0]["run_status"] == "failed"
    assert history[0]["failure_code"] == "command_failed"
    assert history[0]["failed_stage_index"] == 1
    assert history[0]["sessions"][0]["status"] == "failed"
    assert [stage["status"] for stage in history[0]["sessions"][0]["stages"]] == [
        "completed",
        "failed",
    ]
    assert history[0]["checkpoint_after"] is None
