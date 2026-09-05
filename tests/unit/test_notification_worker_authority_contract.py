from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    CONDITIONS, ENTRYPOINTS, LOCK_MODEL, LOCK_SCOPE, PLAN_MODEL_VERSION,
    authority_state, build_worker_authority_transition, canonical_bytes,
    validate_authority_plan, validate_worker_authority_chain,
    validate_worker_authority_transition,
)

NOW = datetime(2026, 9, 5, 20, 0, tzinfo=timezone.utc)


def plan_fixture(*, planned_at: datetime = NOW, worker_id: str = "authority-worker") -> dict[str, Any]:
    fingerprint = "portfolio-risk-notification-worker-config-v1-worker-fixture"
    boundary = ((int(planned_at.timestamp()) // 300) + 1) * 300
    seed = hashlib.sha256(f"{fingerprint}:{boundary}".encode()).digest()
    jitter = int.from_bytes(seed[:8], "big") % 31
    unsigned = int.from_bytes(hashlib.sha256(f"{LOCK_MODEL}:{LOCK_SCOPE}".encode()).digest()[:8], "big")
    signed = unsigned if unsigned < 2**63 else unsigned - 2**64
    p: dict[str, Any] = {
        "model_version": PLAN_MODEL_VERSION, "planned_at": planned_at.isoformat(),
        "worker": {"worker_id": worker_id, "fingerprint": fingerprint, "enabled": True},
        "destination": {
            "destination_id": "risk-operations-webhook", "fingerprint": "destination-fixture",
            "activation_status": "active", "allowed_event_types": ["breach_opened"],
            "endpoint_environment_variable": "RISK_NOTIFICATION_WEBHOOK_URL",
            "endpoint_value_recorded": False,
        },
        "delivery": {
            "delivery_fingerprint": "delivery-fixture", "max_batch_events": 25,
            "retry_execution_enabled": True, "retry_execution_policy_fingerprint": "retry-execution-fixture",
            "retry_planning_policy_fingerprint": "retry-planning-fixture", "webhook_enabled": True,
        },
        "execution": {
            "execution_timeout_seconds": 120,
            "work_items": [{"execution_kind": k, "entrypoint": v, "max_events": 25} for k, v in ENTRYPOINTS.items()],
        },
        "concurrency_control": {
            "key_fingerprint": hashlib.sha256(str(signed).encode("ascii")).hexdigest()[:24],
            "lock_acquired": False, "max_concurrency": 1, "model_version": LOCK_MODEL, "scope": LOCK_SCOPE,
        },
        "readiness": {
            "max_age_seconds": 300, "refresh_under_shared_lock": True,
            "required_status": "allowed",
            "source_view": "risk_platform.current_notification_execution_readiness_review",
        },
        "schedule": {
            "activation_action": "would_create", "boundary_epoch": boundary,
            "deterministic_jitter_seconds": jitter, "interval_seconds": 300,
            "jitter_seconds": 30, "mode": "fixed_interval", "timezone": "UTC",
            "scheduled_for": datetime.fromtimestamp(boundary + jitter, tz=timezone.utc).isoformat(),
        },
        "suspension": {"conditions": list(CONDITIONS), "cooldown_seconds": 900, "max_consecutive_failures": 3},
        "side_effects": {key: False for key in (
            "acknowledgement_mutated", "cloud_schedule_activated", "database_read_performed",
            "delivery_attempt_written", "external_request_performed", "infrastructure_deployed",
            "outbox_mutated", "terraform_apply_performed",
        )},
        "status": "would_schedule", "blocking_reasons": [],
    }
    return rehash(p)


def rehash(plan: dict[str, Any]) -> dict[str, Any]:
    identity = {key: value for key, value in plan.items() if key != "plan_id"}
    plan["plan_id"] = f"{PLAN_MODEL_VERSION}-plan-{hashlib.sha256(canonical_bytes(identity)).hexdigest()[:24]}"
    return plan


def grant(*, plan: dict[str, Any] | None = None, **overrides: Any) -> dict[str, Any]:
    selected = plan if plan is not None else plan_fixture()
    effective = datetime.fromisoformat(selected["planned_at"]) + timedelta(seconds=1)
    args = {
        "plan": selected, "request_id": "AUTH-001", "operator_id": "operator",
        "reviewed_by": ["independent-reviewer"], "action": "activate",
        "requested_at": effective, "effective_at": effective,
        "expires_at": datetime.fromisoformat(selected["schedule"]["scheduled_for"]) + timedelta(seconds=120),
    }
    args.update(overrides)
    return build_worker_authority_transition(**args)


def stop(prior: dict[str, Any], *, action: str = "suspend", **overrides: Any) -> dict[str, Any]:
    args = {
        "plan": prior["plan"], "request_id": "AUTH-STOP", "operator_id": "operator",
        "action": action, "requested_at": NOW + timedelta(seconds=2),
        "effective_at": NOW + timedelta(seconds=2), "previous": prior,
        "reason_codes": ["operator_request"],
    }
    args.update(overrides)
    return build_worker_authority_transition(**args)


def test_full_lifecycle_and_expiry_boundaries() -> None:
    first = grant()
    assert validate_worker_authority_transition(first) == first
    assert authority_state(None, as_of=NOW) == "inactive"
    assert authority_state(first, as_of=NOW) == "inactive"
    assert authority_state(first, as_of=first["effective_at"]) == "active"
    assert authority_state(first, as_of=first["expires_at"]) == "expired"
    suspended = stop(first)
    assert authority_state(suspended, as_of=NOW + timedelta(days=1)) == "suspended"
    later = plan_fixture(planned_at=NOW + timedelta(minutes=20))
    resumed = grant(plan=later, action="resume", previous=suspended, request_id="AUTH-RESUME")
    assert resumed["from_state"] == "suspended"
    disabled = stop(
        resumed, action="disable", request_id="AUTH-DISABLE",
        requested_at=NOW + timedelta(minutes=20, seconds=2),
        effective_at=NOW + timedelta(minutes=20, seconds=2),
    )
    assert authority_state(disabled, as_of=NOW + timedelta(days=1)) == "disabled"
    reactivated = grant(
        plan=plan_fixture(planned_at=NOW + timedelta(minutes=30)),
        previous=disabled, request_id="AUTH-REACTIVATE",
    )
    assert reactivated["from_state"] == "disabled"
    assert reactivated["scheduler_mutated"] is False


@pytest.mark.parametrize("group,key,value", [
    ("worker", "enabled", False), ("worker", "enabled", 1),
    ("readiness", "max_age_seconds", 301), ("readiness", "refresh_under_shared_lock", False),
    ("readiness", "source_view", "other_view"),
    ("concurrency_control", "max_concurrency", True),
    ("concurrency_control", "max_concurrency", 2), ("concurrency_control", "scope", "other"),
    ("concurrency_control", "key_fingerprint", "different-lock"),
    ("suspension", "conditions", ["readiness_failure"]),
    ("suspension", "max_consecutive_failures", 21),
    ("delivery", "webhook_enabled", False), ("delivery", "retry_execution_enabled", False),
    ("schedule", "activation_action", "none"), ("schedule", "boundary_epoch", 1),
    ("schedule", "deterministic_jitter_seconds", 100),
    ("schedule", "timezone", "Europe/London"), ("schedule", "jitter_seconds", 300),
    ("destination", "endpoint_environment_variable", "https://unreviewed.test"),
    ("destination", "endpoint_value_recorded", True),
    ("destination", "activation_status", "disabled"),
    ("side_effects", "cloud_schedule_activated", True),
])
def test_rehashed_unsafe_plan_is_rejected(group: str, key: str, value: Any) -> None:
    p = plan_fixture()
    p[group][key] = value
    with pytest.raises(ValidationError):
        grant(plan=rehash(p))


def test_rehashed_unknown_fields_and_arbitrary_entrypoints_are_rejected() -> None:
    p = plan_fixture()
    p["execution"]["work_items"][0]["entrypoint"] = "os.system"
    with pytest.raises(ValidationError, match="entrypoint"):
        grant(plan=rehash(p))
    p = plan_fixture()
    p["worker"]["unknown"] = "value"
    with pytest.raises(ValidationError, match="exact"):
        grant(plan=rehash(p))


@pytest.mark.parametrize("overrides", [
    {"reviewed_by": []}, {"reviewed_by": ["operator"]}, {"reviewed_by": ["OPERATOR"]},
    {"reviewed_by": ["A", "a"]}, {"reviewed_by": ["b", "a"]},
    {"expires_at": None}, {"expires_at": NOW + timedelta(days=1)},
    {"effective_at": NOW - timedelta(seconds=1)},
    {"requested_at": datetime(2026, 9, 5)}, {"reason_codes": ["operator_request"]},
    {"action": "execute"}, {"request_id": "bad request"},
])
def test_invalid_grants_fail_closed(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        grant(**overrides)


def test_disabled_and_blocked_plans_cannot_grant_authority() -> None:
    for dependency, reason in (("worker", "worker_disabled"), ("delivery", "delivery_disabled")):
        p = plan_fixture()
        p[dependency]["enabled" if dependency == "worker" else "webhook_enabled"] = False
        p["blocking_reasons"] = [reason]
        p["status"] = "disabled" if dependency == "worker" else "blocked"
        p["schedule"]["activation_action"] = "none"
        validate_authority_plan(rehash(p))
        with pytest.raises(ValidationError, match="disabled or blocked"):
            grant(plan=p)


def test_chain_rejects_wrong_predecessor_scope_and_cooldown() -> None:
    first = grant()
    suspended = stop(first)
    with pytest.raises(ValidationError, match="predecessor"):
        validate_worker_authority_chain(suspended, None)
    with pytest.raises(ValidationError, match="cooldown"):
        grant(action="resume", previous=suspended, request_id="EARLY", requested_at=NOW + timedelta(seconds=3), effective_at=NOW + timedelta(seconds=3))
    with pytest.raises(ValidationError, match="worker or destination"):
        grant(
            plan=plan_fixture(planned_at=NOW + timedelta(minutes=20), worker_id="other-worker"),
            action="resume", previous=suspended, request_id="OTHER",
        )
    with pytest.raises(ValidationError, match="exact previously"):
        stop(first, plan=plan_fixture(planned_at=NOW + timedelta(seconds=1)))
    with pytest.raises(ValidationError, match="legal"):
        grant(previous=first, request_id="SECOND", effective_at=NOW + timedelta(seconds=3))


def test_tamper_detection_determinism_and_input_isolation() -> None:
    p = plan_fixture()
    first = grant(plan=p)
    assert first == grant(plan=p)
    p["worker"]["enabled"] = False
    assert first["plan"]["worker"]["enabled"] is True
    for key, value in (("scheduler_mutated", True), ("plan_sha256", "0" * 64), ("to_state", "disabled")):
        changed = copy.deepcopy(first)
        changed[key] = value
        with pytest.raises(ValidationError):
            validate_worker_authority_transition(changed)


def test_real_planner_output_is_accepted(tmp_path: Any) -> None:
    from pathlib import Path

    import yaml

    from src.orchestration.plan_notification_worker import plan_notification_worker

    documents = {}
    for name in ("notification_workers", "notification_delivery", "notification_destinations"):
        documents[name] = yaml.safe_load(Path(f"config/{name}.yaml").read_text())
    documents["notification_workers"]["workers"]["risk-operations-managed"]["enabled"] = True
    delivery = documents["notification_delivery"]["delivery"]
    delivery["webhook"]["enabled"] = True
    delivery["retry_execution"]["enabled"] = True
    documents["notification_destinations"]["destinations"]["risk-operations-webhook"]["activation"] = {
        "enabled": True, "change_request_id": "AUTH-TEST",
        "reviewed_by": ["independent-reviewer"],
        "reviewed_at": "2026-09-01T00:00:00Z", "review_expires_at": "2026-10-01T00:00:00Z",
    }
    for name, document in documents.items():
        (tmp_path / f"{name}.yaml").write_text(yaml.safe_dump(document))
    actual = plan_notification_worker(
        worker_id="risk-operations-managed", planned_at=NOW,
        worker_config_path=tmp_path / "notification_workers.yaml",
        delivery_config_path=tmp_path / "notification_delivery.yaml",
        destination_config_path=tmp_path / "notification_destinations.yaml",
    )
    assert validate_authority_plan(actual) == actual
    assert grant(plan=actual)["plan"]["plan_id"] == actual["plan_id"]
