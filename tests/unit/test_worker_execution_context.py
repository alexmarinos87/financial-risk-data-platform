from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from src.orchestration.notification_worker_execution_context import (
    MODEL_VERSION, build_worker_execution_context, validate_worker_execution_context,
)
from test_notification_worker_authority_contract import grant, plan_fixture, rehash, stop


def context(kind: str = "initial", **changes: Any) -> dict[str, Any]:
    prior = grant()
    args = {"authority": prior, "request_id": "INVOCATION-001", "execution_kind": kind,
            "started_at": prior["plan"]["schedule"]["scheduled_for"]}
    args.update(changes)
    return build_worker_execution_context(**args)


@pytest.mark.parametrize("kind", ["initial", "retry"])
def test_context_derives_scope_and_limits_without_permission(kind: str) -> None:
    prior = grant()
    result = context(kind)
    assert result == context(kind)
    assert validate_worker_execution_context(result, authority=prior) == result
    assert result["authority_transition_id"] == prior["transition_id"]
    assert result["authority_sha256"] == hashlib.sha256(canonical_bytes(prior)).hexdigest()
    assert result["plan_id"] == prior["plan"]["plan_id"]
    assert result["max_events"] == 25
    assert result["destination_id"] == prior["plan"]["destination"]["destination_id"]
    for field in ("current_authority_verified", "readiness_verified", "slot_claimed", "runtime_permission_granted"):
        assert result[field] is False


def test_scope_is_stable_across_invocations_but_not_kinds_or_authorities() -> None:
    first = context()
    later = context(request_id="INVOCATION-002", started_at=datetime.fromisoformat(first["started_at"]) + timedelta(seconds=1))
    assert first["scope_id"] == later["scope_id"]
    assert first["context_id"] != later["context_id"]
    assert first["scope_id"] != context("retry")["scope_id"]
    assert first["scope_id"] != context(authority=grant(request_id="NEW-AUTH"))["scope_id"]


@pytest.mark.parametrize("offset,valid", [(-0.000001, False), (0, True), (119.999999, True), (120, False)])
def test_slot_start_inclusive_and_expiry_exclusive(offset: float, valid: bool) -> None:
    instant = datetime.fromisoformat(grant()["plan"]["schedule"]["scheduled_for"]) + timedelta(seconds=offset)
    if valid:
        assert context(started_at=instant)["started_at"] == instant.isoformat()
    else:
        with pytest.raises(ValidationError, match="outside"):
            context(started_at=instant)


def test_start_uses_authority_expiry_not_whole_planned_timeout() -> None:
    prior = grant()
    slot = datetime.fromisoformat(prior["plan"]["schedule"]["scheduled_for"])
    shorter = grant(expires_at=slot + timedelta(seconds=10))
    with pytest.raises(ValidationError, match="outside"):
        context(authority=shorter, started_at=slot + timedelta(seconds=10))


def test_timestamp_normalization_and_detached_inputs() -> None:
    prior = grant()
    before = copy.deepcopy(prior)
    slot = datetime.fromisoformat(prior["plan"]["schedule"]["scheduled_for"])
    result = context(authority=prior, started_at=slot.astimezone(timezone(timedelta(hours=1))))
    assert result == context()
    assert prior == before
    prior["plan"]["worker"]["worker_id"] = "changed"
    assert result["worker_id"] == "authority-worker"


@pytest.mark.parametrize("action", ["suspend", "disable"])
def test_stopped_snapshot_cannot_create_execution_context(action: str) -> None:
    with pytest.raises(ValidationError, match="active"):
        context(authority=stop(grant(), action=action))


def test_initial_only_plan_cannot_attribute_retry() -> None:
    plan = plan_fixture()
    plan["execution"]["work_items"] = plan["execution"]["work_items"][:1]
    prior = grant(plan=rehash(plan))
    assert context(authority=prior)["execution_kind"] == "initial"
    with pytest.raises(ValidationError, match="not selected"):
        context("retry", authority=prior)


@pytest.mark.parametrize("field,value", [
    ("worker_id", "other"), ("worker_fingerprint", "other"), ("plan_id", "other"),
    ("authority_transition_id", "other"), ("authority_sha256", "0" * 64),
    ("destination_id", "other"), ("destination_fingerprint", "other"),
    ("delivery_fingerprint", "other"), ("retry_planning_policy_fingerprint", "other"),
    ("retry_execution_policy_fingerprint", "other"), ("entrypoint", "os.system"),
    ("max_events", 26), ("max_events", True), ("scope_id", "other"),
    ("expires_at", "2027-01-01T00:00:00+00:00"), ("scheduled_for", "2027-01-01T00:00:00+00:00"),
    ("runtime_permission_granted", True), ("current_authority_verified", True),
    ("readiness_verified", True), ("slot_claimed", True),
])
def test_rehashed_projection_cannot_contradict_authority(field: str, value: Any) -> None:
    result = context()
    result[field] = value
    identity = {key: val for key, val in result.items() if key != "context_id"}
    result["context_id"] = f"{MODEL_VERSION}-{hashlib.sha256(canonical_bytes(identity)).hexdigest()}"
    with pytest.raises(ValidationError):
        validate_worker_execution_context(result, authority=grant())


@pytest.mark.parametrize("changes", [
    {"request_id": ""}, {"request_id": "a" * 513}, {"execution_kind": "execute"},
    {"execution_kind": None}, {"started_at": "2026-09-05T20:05:00"}, {"authority": None},
    {"authority": {"extra": "a" * 1_048_576}},
])
def test_invalid_inputs_are_rejected(changes: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        context(**changes)


def test_validator_rejects_unknown_fields_wrong_authority_and_nonobject() -> None:
    result = context()
    with pytest.raises(ValidationError):
        validate_worker_execution_context(result, authority=grant(request_id="OTHER"))
    result["extra"] = "not-accepted"
    with pytest.raises(ValidationError):
        validate_worker_execution_context(result, authority=grant())
    with pytest.raises(ValidationError):
        validate_worker_execution_context([], authority=grant())  # type: ignore[arg-type]
