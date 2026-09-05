from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import authority_state
from src.orchestration.plan_notification_worker import (
    _plan_id,
    plan_notification_worker,
    validate_notification_worker_plan,
)
from src.orchestration.reviewed_notification_worker_authority import (
    build_reviewed_worker_authority_transition,
    validate_reviewed_worker_authority_transition,
)

NOW = datetime(2026, 9, 5, 20, tzinfo=timezone.utc)
WORKER_ID = "risk-operations-managed"
DESTINATION_ID = "risk-operations-webhook"


def _write(path: Path, value: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(value, sort_keys=True), encoding="utf-8")


def _read(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@pytest.fixture
def configurations(tmp_path: Path) -> dict[str, Path]:
    paths = {
        "worker_config_path": tmp_path / "workers.yaml",
        "delivery_config_path": tmp_path / "delivery.yaml",
        "destination_config_path": tmp_path / "destinations.yaml",
    }
    workers = _read(Path("config/notification_workers.yaml"))
    workers["workers"][WORKER_ID]["enabled"] = True
    delivery = _read(Path("config/notification_delivery.yaml"))
    delivery["delivery"]["webhook"]["enabled"] = True
    delivery["delivery"]["retry_execution"]["enabled"] = True
    destinations = _read(Path("config/notification_destinations.yaml"))
    destinations["destinations"][DESTINATION_ID]["activation"] = {
        "enabled": True, "change_request_id": "CHG-REVIEWED-WORKER",
        "reviewed_by": ["risk-control-reviewer"],
        "reviewed_at": "2026-09-01T00:00:00+00:00",
        "review_expires_at": "2026-10-01T00:00:00+00:00",
    }
    for key, payload in zip(paths, (workers, delivery, destinations), strict=True):
        _write(paths[key], payload)
    return paths


def _plan(paths: dict[str, Path], instant: datetime = NOW) -> dict[str, Any]:
    return plan_notification_worker(worker_id=WORKER_ID, planned_at=instant, **paths)


def _grant(plan: dict[str, Any], paths: dict[str, Path], **changes: Any) -> dict[str, Any]:
    instant = datetime.fromisoformat(plan["planned_at"]) + timedelta(seconds=1)
    values = {
        "plan": plan, "request_id": "AUTH-REVIEWED-001", "operator_id": "risk-operator",
        "action": "activate", "requested_at": instant, "effective_at": instant,
        "reviewed_by": ["independent-reviewer"],
        "expires_at": datetime.fromisoformat(plan["schedule"]["scheduled_for"]) + timedelta(seconds=60),
        **paths,
    }
    values.update(changes)
    return build_reviewed_worker_authority_transition(**values)


def _review_expiry(paths: dict[str, Path], expiry: datetime) -> None:
    path = paths["destination_config_path"]
    document = _read(path)
    document["destinations"][DESTINATION_ID]["activation"]["review_expires_at"] = expiry.isoformat()
    _write(path, document)


def _rehash(plan: dict[str, Any]) -> None:
    plan["plan_id"] = _plan_id({key: value for key, value in plan.items() if key != "plan_id"})


def test_reviewed_lifecycle_reuses_existing_transition_model(configurations: dict[str, Path]) -> None:
    plan = _plan(configurations)
    original = copy.deepcopy(plan)
    active = _grant(plan, configurations)
    assert validate_reviewed_worker_authority_transition(active, **configurations) == active
    suspended = build_reviewed_worker_authority_transition(
        plan=plan, request_id="AUTH-STOP", operator_id="risk-operator", action="suspend",
        requested_at=NOW + timedelta(seconds=2), effective_at=NOW + timedelta(seconds=2),
        reason_codes=["operator_request"], previous=active, **configurations,
    )
    assert validate_reviewed_worker_authority_transition(
        suspended, previous=active, **configurations
    ) == suspended
    later = NOW + timedelta(minutes=20)
    resumed = _grant(
        _plan(configurations, later), configurations,
        action="resume", previous=suspended, request_id="AUTH-RESUME",
    )
    disabled = build_reviewed_worker_authority_transition(
        plan=resumed["plan"], request_id="AUTH-DISABLE", operator_id="risk-operator",
        action="disable", requested_at=later + timedelta(seconds=2),
        effective_at=later + timedelta(seconds=2), reason_codes=["operator_request"],
        previous=resumed, **configurations,
    )
    assert validate_reviewed_worker_authority_transition(
        resumed, previous=suspended, **configurations
    ) == resumed
    assert validate_reviewed_worker_authority_transition(
        disabled, previous=resumed, **configurations
    ) == disabled
    assert authority_state(disabled, as_of=later + timedelta(days=1)) == "disabled"
    assert plan == original
    for transition in (active, suspended, resumed, disabled):
        assert transition["scheduler_mutated"] is False
        assert transition["external_request_performed"] is False


@pytest.mark.parametrize("group", ["worker", "delivery", "retry", "destination"])
def test_changed_configuration_invalidates_retained_plan(
    configurations: dict[str, Path], group: str
) -> None:
    plan = _plan(configurations)
    key = "delivery_config_path" if group == "retry" else f"{group}_config_path"
    path = configurations[key]
    document = _read(path)
    if group == "worker":
        document["workers"][WORKER_ID]["enabled"] = False
    elif group == "delivery":
        document["delivery"]["webhook"]["timeout_seconds"] = 6
    elif group == "retry":
        document["delivery"]["retry_planning"]["max_backoff_seconds"] = 3001
    else:
        document["destinations"][DESTINATION_ID]["owner"]["contact"] = "different-oncall"
    _write(path, document)
    with pytest.raises(ValidationError, match="does not match"):
        _grant(plan, configurations)


def test_rehashed_internally_valid_alternative_is_not_configuration_evidence(
    configurations: dict[str, Path],
) -> None:
    plan = _plan(configurations)
    plan["execution"]["work_items"][0]["max_events"] = 1
    _rehash(plan)
    assert validate_notification_worker_plan(plan) == plan
    with pytest.raises(ValidationError, match="does not match"):
        _grant(plan, configurations)


def test_removing_endpoint_mismatch_evidence_does_not_grant_authority(
    configurations: dict[str, Path],
) -> None:
    path = configurations["delivery_config_path"]
    document = _read(path)
    document["delivery"]["webhook"]["endpoint_env"] = "OTHER_NOTIFICATION_WEBHOOK_URL"
    _write(path, document)
    plan = _plan(configurations)
    assert plan["blocking_reasons"] == ["endpoint_environment_mismatch"]
    plan["blocking_reasons"] = []
    plan["status"] = "would_schedule"
    plan["schedule"]["activation_action"] = "would_create"
    _rehash(plan)
    assert validate_notification_worker_plan(plan) == plan
    with pytest.raises(ValidationError, match="does not match"):
        _grant(plan, configurations)


def test_expired_destination_review_blocks_effective_authority(
    configurations: dict[str, Path],
) -> None:
    _review_expiry(configurations, NOW + timedelta(seconds=2))
    plan = _plan(configurations)
    assert plan["status"] == "would_schedule"
    with pytest.raises(ValidationError, match="not active at authority effective time"):
        _grant(plan, configurations, effective_at=NOW + timedelta(seconds=3))


def test_authority_cannot_outlive_destination_review(configurations: dict[str, Path]) -> None:
    _review_expiry(configurations, NOW + timedelta(minutes=3))
    with pytest.raises(ValidationError, match="exceeds the destination review expiry"):
        _grant(_plan(configurations), configurations)


def test_exact_review_expiry_is_allowed_as_exclusive_grant_end(
    configurations: dict[str, Path],
) -> None:
    slot = datetime.fromisoformat(_plan(configurations)["schedule"]["scheduled_for"])
    expiry = slot + timedelta(seconds=60)
    _review_expiry(configurations, expiry)
    grant = _grant(_plan(configurations), configurations, expires_at=expiry)
    assert grant["expires_at"] == expiry.isoformat()


def test_disable_after_review_expiry_uses_historical_snapshot(
    configurations: dict[str, Path],
) -> None:
    slot = datetime.fromisoformat(_plan(configurations)["schedule"]["scheduled_for"])
    expiry = slot + timedelta(seconds=60)
    _review_expiry(configurations, expiry)
    active = _grant(_plan(configurations), configurations, expires_at=expiry)
    later = expiry + timedelta(seconds=1)
    stopped = build_reviewed_worker_authority_transition(
        plan=active["plan"], request_id="AUTH-DISABLE-EXPIRED", operator_id="risk-operator",
        action="disable", requested_at=later, effective_at=later,
        reason_codes=["operator_request"], previous=active, **configurations,
    )
    assert stopped["from_state"] == "expired"
    assert stopped["to_state"] == "disabled"


def test_retained_transition_requires_exact_predecessor(configurations: dict[str, Path]) -> None:
    active = _grant(_plan(configurations), configurations)
    with pytest.raises(ValidationError, match="predecessor"):
        validate_reviewed_worker_authority_transition(active, previous=active, **configurations)


def test_configuration_symlink_is_rejected(configurations: dict[str, Path], tmp_path: Path) -> None:
    plan = _plan(configurations)
    link = tmp_path / "linked-workers.yaml"
    link.symlink_to(configurations["worker_config_path"])
    with pytest.raises(ValidationError, match="symbolic link"):
        _grant(plan, {**configurations, "worker_config_path": link})


def test_committed_disabled_plan_cannot_grant_authority() -> None:
    plan = plan_notification_worker(worker_id=WORKER_ID, planned_at=NOW)
    assert plan["status"] == "disabled"
    with pytest.raises(ValidationError, match="disabled or blocked"):
        _grant(plan, {})
