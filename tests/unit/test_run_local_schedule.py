from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.run_local_schedule import (
    LocalSchedule,
    parse_local_schedule,
    plan_due_dates,
    run_local_schedule,
)


def _payload(*, enabled: bool = False) -> dict[str, object]:
    return {
        "schedules": {
            "daily": {
                "enabled": enabled,
                "portfolio_id": "us-tech-equal",
                "policy_id": "us-tech-standard",
                "source": "alpha_vantage",
                "symbols": ["AAPL", "MSFT"],
                "volatility_window": 20,
                "var_window": 60,
                "var_confidence": 0.95,
                "covariance_window": 20,
                "max_snapshots": 100,
                "max_evaluations": 200,
                "max_notifications": 100,
                "max_catchup_days": 3,
                "initial_lookback_days": 1,
            }
        }
    }


def _schedule(*, enabled: bool = False) -> LocalSchedule:
    return parse_local_schedule(_payload(enabled=enabled), "daily")


def _run(tmp_path: Path, **overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "schedule_id": "daily",
        "as_of_date": date(2026, 1, 5),
        "schedule_config_path": Path("schedule.yaml"),
        "portfolio_config_path": Path("portfolios.yaml"),
        "risk_limit_config_path": Path("limits.yaml"),
        "storage_config_path": Path("storage.yaml"),
        "state_dir": tmp_path / "state",
        "lock_base_dir": tmp_path / "locks",
        "schedule_loader": lambda *_: _schedule(),
    }
    defaults.update(overrides)
    return run_local_schedule(**defaults)


def test_config_is_disabled_by_default_and_validates_symbols() -> None:
    schedule = _schedule()

    assert schedule.enabled is False
    assert schedule.symbols == ("AAPL", "MSFT")
    assert schedule.max_catchup_days == 3

    duplicate = _payload()
    duplicate["schedules"]["daily"]["symbols"] = ["AAPL", "AAPL"]  # type: ignore[index]
    with pytest.raises(ValidationError, match="unique"):
        parse_local_schedule(duplicate, "daily")


def test_due_date_plan_is_bounded_and_resumable() -> None:
    schedule = _schedule()

    assert plan_due_dates(
        schedule,
        as_of_date=date(2026, 1, 5),
        last_success_date=None,
    ) == (date(2026, 1, 5),)
    assert plan_due_dates(
        schedule,
        as_of_date=date(2026, 1, 5),
        last_success_date=date(2025, 12, 31),
    ) == (
        date(2026, 1, 1),
        date(2026, 1, 2),
        date(2026, 1, 3),
    )
    assert plan_due_dates(
        schedule,
        as_of_date=date(2026, 1, 5),
        last_success_date=date(2026, 1, 5),
    ) == ()


def test_disabled_schedule_returns_plan_without_stage_execution(
    tmp_path: Path,
) -> None:
    def fail(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("disabled schedule must not execute stages")

    summary = _run(
        tmp_path,
        daily_stage=fail,
        governed_stage=fail,
        notification_stage=fail,
    )

    assert summary["execution"] == {
        "performed": False,
        "reason": "schedule_disabled",
    }
    assert summary["due_dates"] == ["2026-01-05"]
    assert summary["provider_request_performed"] is False


def test_dry_run_never_executes_even_when_schedule_is_enabled(
    tmp_path: Path,
) -> None:
    def fail(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("dry run must not execute stages")

    summary = _run(
        tmp_path,
        dry_run=True,
        schedule_loader=lambda *_: _schedule(enabled=True),
        daily_stage=fail,
        governed_stage=fail,
        notification_stage=fail,
    )

    assert summary["execution"]["reason"] == "dry_run"
    assert not (tmp_path / "state" / "daily.json").exists()


def test_explicit_run_orders_stages_and_updates_checkpoint(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    released: list[list[Path]] = []

    def daily(**kwargs: Any) -> dict[str, Any]:
        calls.append(f"daily:{kwargs['symbol']}")
        assert kwargs["end_date"] == date(2026, 1, 5)
        return {"symbol": kwargs["symbol"]}

    def governed(**kwargs: Any) -> dict[str, Any]:
        calls.append("governed")
        assert kwargs["start_date"] == date(2026, 1, 5)
        assert kwargs["end_date"] == date(2026, 1, 5)
        return {"portfolio_id": kwargs["portfolio_id"]}

    def notifications(**kwargs: Any) -> dict[str, Any]:
        calls.append("notifications")
        assert kwargs["policy_id"] == "us-tech-standard"
        return {"selected": 0}

    summary = _run(
        tmp_path,
        allow_disabled_run=True,
        daily_stage=daily,
        governed_stage=governed,
        notification_stage=notifications,
        lock_acquirer=lambda *args, **kwargs: [Path("lock")],
        lock_releaser=lambda paths: released.append(paths),
    )

    assert calls == ["daily:AAPL", "daily:MSFT", "governed", "notifications"]
    assert released == [[Path("lock")]]
    assert summary["execution"]["completed_run_dates"] == ["2026-01-05"]
    checkpoint = json.loads(
        (tmp_path / "state" / "daily.json").read_text(encoding="utf-8")
    )
    assert checkpoint == {
        "last_success_date": "2026-01-05",
        "schedule_id": "daily",
    }


def test_failure_stops_before_checkpoint_and_releases_lock(
    tmp_path: Path,
) -> None:
    released: list[list[Path]] = []

    with pytest.raises(StorageError, match="failed"):
        _run(
            tmp_path,
            allow_disabled_run=True,
            daily_stage=lambda **_: {"ok": True},
            governed_stage=lambda **_: (_ for _ in ()).throw(
                StorageError("governed failed")
            ),
            notification_stage=lambda **_: {"unexpected": True},
            lock_acquirer=lambda *args, **kwargs: [Path("lock")],
            lock_releaser=lambda paths: released.append(paths),
        )

    assert released == [[Path("lock")]]
    assert not (tmp_path / "state" / "daily.json").exists()


def test_invalid_checkpoint_fails_before_execution(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    (state / "daily.json").write_text("not-json", encoding="utf-8")

    with pytest.raises(StorageError, match="checkpoint"):
        _run(tmp_path, allow_disabled_run=True)
