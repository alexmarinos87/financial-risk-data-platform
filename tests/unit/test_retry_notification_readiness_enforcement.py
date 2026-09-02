from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.run_readiness_enforced_portfolio_risk_notification_retries import (
    MODEL_VERSION,
    execute_readiness_enforced_portfolio_risk_notification_retries,
)


def _lock() -> dict[str, Any]:
    return {
        "acquired": True,
        "key_fingerprint": "notification-lock-test",
        "model_version": "portfolio-risk-notification-delivery-lock-v1",
        "scope": "portfolio-risk-notification-delivery",
    }


def _readiness() -> dict[str, Any]:
    return {
        "enforcement_id": "readiness-enforcement-test",
        "destination_id": "risk-operations-webhook",
        "execution_kind": "retry",
    }


def _executor_summary(lock: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "plan_id": "plan-1",
        "request_id": "request-1",
        "destination_authority": {
            "destination_id": "risk-operations-webhook",
        },
        "concurrency_control": {
            "performed": True,
            "acquired": True,
            "released": True,
            "model_version": lock["model_version"],
            "scope": lock["scope"],
            "key_fingerprint": lock["key_fingerprint"],
        },
    }


def test_retry_readiness_is_enforced_before_executor_and_reuses_one_lock() -> None:
    events: list[str] = []
    physical_lock = _lock()

    @contextmanager
    def lock_factory(*, dsn: str) -> Iterator[Mapping[str, Any]]:
        assert dsn == "dsn"
        events.append("physical-lock-acquired")
        yield physical_lock
        events.append("physical-lock-released")

    def enforcer(**kwargs: Any) -> Mapping[str, Any]:
        events.append("readiness-enforced")
        assert kwargs["execution_kind"] == "retry"
        assert kwargs["lock_evidence"] == physical_lock
        return _readiness()

    def executor(**kwargs: Any) -> dict[str, Any]:
        events.append("executor-started")
        with kwargs["lock_factory"](dsn=kwargs["dsn"]) as reused:
            events.append("held-lock-reused")
            assert reused == physical_lock
        return _executor_summary(physical_lock)

    summary = execute_readiness_enforced_portfolio_risk_notification_retries(
        plan_path=Path("plan.json"),
        confirm_plan_id="plan-1",
        request_id="request-1",
        config_path=Path("config/notification_delivery.yaml"),
        dsn="dsn",
        execute=True,
        destination_config_path=Path("config/notification_destinations.yaml"),
        clock=lambda: datetime(2026, 9, 2, 19, 0, tzinfo=timezone.utc),
        lock_factory=lock_factory,
        readiness_enforcer=enforcer,
        readiness_validator=lambda value: dict(value),
        executor=executor,
        plan_loader=lambda _: {"plan_id": "plan-1"},
    )

    assert events == [
        "physical-lock-acquired",
        "readiness-enforced",
        "executor-started",
        "held-lock-reused",
        "physical-lock-released",
    ]
    assert summary["execution_readiness"] == _readiness()
    assert summary["governed_execution"] == {
        "model_version": MODEL_VERSION,
        "plan_id": "plan-1",
        "destination_id": "risk-operations-webhook",
        "readiness_enforcement_id": "readiness-enforcement-test",
        "single_physical_lock_acquisition": True,
        "nested_lock_reacquisition_performed": False,
        "lock_reused_by_retry_executor": True,
        "outer_lock_released": True,
    }


def test_retry_readiness_denial_rejects_before_executor() -> None:
    calls: list[str] = []

    @contextmanager
    def lock_factory(*, dsn: str) -> Iterator[Mapping[str, Any]]:
        assert dsn == "dsn"
        yield _lock()

    def enforcer(**_: Any) -> Mapping[str, Any]:
        calls.append("enforcer")
        raise ValidationError("notification execution readiness is not allowed")

    with pytest.raises(ValidationError, match="not allowed"):
        execute_readiness_enforced_portfolio_risk_notification_retries(
            plan_path=Path("plan.json"),
            confirm_plan_id="plan-1",
            request_id="request-1",
            config_path=Path("config/notification_delivery.yaml"),
            dsn="dsn",
            execute=True,
            lock_factory=lock_factory,
            readiness_enforcer=enforcer,
            readiness_validator=lambda value: dict(value),
            executor=lambda **_: calls.append("executor") or {},
            plan_loader=lambda _: {"plan_id": "plan-1"},
        )

    assert calls == ["enforcer"]


def test_retry_readiness_requires_retry_kind_and_exact_destination() -> None:
    @contextmanager
    def lock_factory(*, dsn: str) -> Iterator[Mapping[str, Any]]:
        assert dsn == "dsn"
        yield _lock()

    for evidence, message in (
        (
            {
                "enforcement_id": "enforcement-1",
                "destination_id": "risk-operations-webhook",
                "execution_kind": "initial",
            },
            "retry readiness authority",
        ),
        (
            {
                "enforcement_id": "enforcement-1",
                "destination_id": "another-destination",
                "execution_kind": "retry",
            },
            "another destination",
        ),
    ):
        with pytest.raises(ValidationError, match=message):
            execute_readiness_enforced_portfolio_risk_notification_retries(
                plan_path=Path("plan.json"),
                confirm_plan_id="plan-1",
                request_id="request-1",
                config_path=Path("config/notification_delivery.yaml"),
                dsn="dsn",
                execute=True,
                lock_factory=lock_factory,
                readiness_enforcer=lambda **_: evidence,
                readiness_validator=lambda value: dict(value),
                executor=lambda **_: pytest.fail("executor must not run"),
                plan_loader=lambda _: {"plan_id": "plan-1"},
            )


def test_retry_executor_must_reuse_shared_lock_exactly_once() -> None:
    @contextmanager
    def lock_factory(*, dsn: str) -> Iterator[Mapping[str, Any]]:
        assert dsn == "dsn"
        yield _lock()

    with pytest.raises(ValidationError, match="did not execute beneath"):
        execute_readiness_enforced_portfolio_risk_notification_retries(
            plan_path=Path("plan.json"),
            confirm_plan_id="plan-1",
            request_id="request-1",
            config_path=Path("config/notification_delivery.yaml"),
            dsn="dsn",
            execute=True,
            lock_factory=lock_factory,
            readiness_enforcer=lambda **_: _readiness(),
            readiness_validator=lambda value: dict(value),
            executor=lambda **_: _executor_summary(_lock()),
            plan_loader=lambda _: {"plan_id": "plan-1"},
        )


def test_retry_wrapper_requires_execute_and_exact_plan_confirmation() -> None:
    common = {
        "plan_path": Path("plan.json"),
        "request_id": "request-1",
        "config_path": Path("config/notification_delivery.yaml"),
        "dsn": "dsn",
        "plan_loader": lambda _: {"plan_id": "plan-1"},
    }

    with pytest.raises(ValidationError, match="--execute"):
        execute_readiness_enforced_portfolio_risk_notification_retries(
            confirm_plan_id="plan-1",
            execute=False,
            **common,
        )

    with pytest.raises(ValidationError, match="confirm_plan_id"):
        execute_readiness_enforced_portfolio_risk_notification_retries(
            confirm_plan_id="another-plan",
            execute=True,
            **common,
        )
