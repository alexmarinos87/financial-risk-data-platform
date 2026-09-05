from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.plan_notification_worker import (
    _plan_id,
    build_notification_worker_plan,
    load_notification_workers,
    plan_notification_worker,
    validate_notification_worker_plan,
)
from src.orchestration.portfolio_risk_notification_destination_contract import (
    DestinationActivation,
    load_notification_destinations,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    load_retry_execution_contract,
)

WORKER_ID = "risk-operations-managed"
BASE_TIME = datetime(2026, 9, 5, 20, tzinfo=timezone.utc)


def _plan(*, initial_only: bool = False) -> dict[str, Any]:
    worker = load_notification_workers(Path("config/notification_workers.yaml"))[WORKER_ID]
    delivery, policy, execution = load_retry_execution_contract(
        Path("config/notification_delivery.yaml")
    )
    destination = load_notification_destinations(
        Path("config/notification_destinations.yaml")
    )[worker.destination_id]
    destination = replace(destination, activation=DestinationActivation(
        enabled=True,
        change_request_id="CHG-WORKER-VALIDATION",
        reviewed_by=("risk-control-reviewer",),
        reviewed_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        review_expires_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    ))
    return build_notification_worker_plan(
        worker=replace(
            worker, enabled=True,
            execution_kinds=("initial",) if initial_only else ("initial", "retry"),
        ),
        delivery=replace(delivery, enabled=True),
        retry_policy=policy,
        retry_execution=replace(execution, enabled=not initial_only),
        destination=destination,
        planned_at=BASE_TIME,
    )


def _rehash(plan: dict[str, Any]) -> None:
    plan["plan_id"] = _plan_id({key: value for key, value in plan.items() if key != "plan_id"})


@pytest.mark.parametrize(("path", "value"), [
    ("worker.enabled", False),
    ("worker.enabled", 1),
    ("worker.worker_id", "../unreviewed"),
    ("worker.fingerprint", "not-a-fingerprint"),
    ("worker.extra", "unexpected"),
    ("concurrency_control.lock_acquired", True),
    ("concurrency_control.lock_acquired", 0),
    ("concurrency_control.max_concurrency", True),
    ("concurrency_control.max_concurrency", 2),
    ("concurrency_control.scope", "unreviewed-scope"),
    ("concurrency_control.key_fingerprint", "changed-lock"),
    ("delivery.webhook_enabled", False),
    ("delivery.retry_execution_enabled", False),
    ("delivery.max_batch_events", 0),
    ("delivery.max_batch_events", 101),
    ("delivery.delivery_fingerprint", "unreviewed-delivery"),
    ("delivery.endpoint_value", "must-not-be-retained"),
    ("destination.activation_status", "review_expired"),
    ("destination.endpoint_environment_variable", "not an environment name"),
    ("destination.endpoint_value_recorded", True),
    ("destination.allowed_event_types", ["unreviewed_event"]),
    ("destination.allowed_event_types", ["breach_opened", "breach_opened"]),
    ("destination.allowed_event_types", [[]]),
    ("destination.extra", "unexpected"),
    ("execution.execution_timeout_seconds", 0),
    ("execution.execution_timeout_seconds", 3601),
    ("execution.work_items", []),
    ("execution.work_items.0.entrypoint", "unreviewed.module"),
    ("execution.work_items.0.execution_kind", "automatic"),
    ("execution.work_items.0.max_events", 101),
    ("execution.work_items.0.max_events", True),
    ("execution.work_items.0.extra", "unexpected"),
    ("readiness.max_age_seconds", 301),
    ("readiness.required_status", "blocked"),
    ("readiness.refresh_under_shared_lock", False),
    ("readiness.source_view", "unreviewed_view"),
    ("suspension.conditions", []),
    ("suspension.cooldown_seconds", -1),
    ("suspension.max_consecutive_failures", 21),
    ("schedule.mode", "cron"),
    ("schedule.timezone", "Europe/London"),
    ("schedule.interval_seconds", True),
    ("schedule.jitter_seconds", 151),
    ("schedule.boundary_epoch", 0),
    ("schedule.deterministic_jitter_seconds", 31),
    ("schedule.scheduled_for", "2026-09-05T20:09:00+00:00"),
    ("schedule.activation_action", "none"),
    ("side_effects.external_request_performed", True),
    ("side_effects.outbox_mutated", 0),
    ("status", "disabled"),
    ("status", {}),
    ("blocking_reasons", ["worker_disabled"]),
    ("blocking_reasons", [[]]),
    ("planned_at", "2026-09-05T20:00:00Z"),
])
def test_rehashed_nested_tampering_is_rejected(path: str, value: Any) -> None:
    plan = _plan()
    target: Any = plan
    segments = path.split(".")
    for segment in segments[:-1]:
        target = target[int(segment)] if isinstance(target, list) else target[segment]
    target[segments[-1]] = value
    _rehash(plan)
    with pytest.raises(ValidationError):
        validate_notification_worker_plan(plan)


def test_valid_initial_only_plan_does_not_require_retry_enablement() -> None:
    plan = _plan(initial_only=True)
    assert plan["delivery"]["retry_execution_enabled"] is False
    assert plan["status"] == "would_schedule"
    assert validate_notification_worker_plan(plan) == plan


def test_duplicate_or_unsorted_work_is_rejected_after_rehash() -> None:
    for duplicate in (False, True):
        plan = _plan()
        items = plan["execution"]["work_items"]
        plan["execution"]["work_items"] = [items[0], items[0]] if duplicate else list(reversed(items))
        _rehash(plan)
        with pytest.raises(ValidationError, match="sorted and unique"):
            validate_notification_worker_plan(plan)


def test_false_worker_cannot_claim_would_schedule_with_empty_blockers() -> None:
    plan = _plan()
    plan["worker"]["enabled"] = False
    _rehash(plan)
    with pytest.raises(ValidationError, match="contradict"):
        validate_notification_worker_plan(plan)


def test_validation_returns_detached_snapshot() -> None:
    plan = _plan()
    expected = copy.deepcopy(plan)
    retained = validate_notification_worker_plan(plan)
    plan["worker"]["enabled"] = False
    plan["execution"]["work_items"].clear()
    assert retained == expected


def test_retained_plan_size_is_bounded() -> None:
    plan = _plan()
    plan["worker"]["worker_id"] = "x" * 1_048_576
    _rehash(plan)
    with pytest.raises(ValidationError, match="1 MB"):
        validate_notification_worker_plan(plan)


@pytest.mark.parametrize(("instant", "boundary"), [
    ("1969-12-31T23:59:59.500000+00:00", 0),
    ("1970-01-01T00:00:00+00:00", 300),
    ("1970-01-01T00:04:59.999999+00:00", 300),
    ("1970-01-01T00:05:00+00:00", 600),
])
def test_next_boundary_uses_integer_utc_arithmetic(instant: str, boundary: int) -> None:
    plan = plan_notification_worker(worker_id=WORKER_ID, planned_at=instant)
    assert plan["schedule"]["boundary_epoch"] == boundary
    assert validate_notification_worker_plan(plan) == plan


def test_schedule_overflow_is_validation_error() -> None:
    with pytest.raises(ValidationError, match="datetime bounds"):
        plan_notification_worker(
            worker_id=WORKER_ID,
            planned_at=datetime.max.replace(tzinfo=timezone.utc),
        )
