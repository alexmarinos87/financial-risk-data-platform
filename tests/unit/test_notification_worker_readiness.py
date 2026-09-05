from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    build_worker_authority_transition, canonical_bytes,
)
from src.orchestration.notification_worker_readiness import (
    MODEL_VERSION, assess_worker_readiness, validate_worker_readiness_assessment,
)
from src.orchestration.plan_notification_worker import _plan_id
from test_reviewed_notification_worker_authority import (
    NOW, _grant, _plan, configurations as configurations,
)


def evidence_fixture(paths: dict[str, Path]) -> dict[str, Any]:
    plan = _plan(paths)
    active = _grant(plan, paths)
    slot = plan["schedule"]["scheduled_for"]
    readiness = []
    health = []
    for item in plan["execution"]["work_items"]:
        kind = item["execution_kind"]
        readiness.append({
            "execution_kind": kind, "record_id": f"readiness-record-{kind}",
            "decision_id": f"readiness-decision-{kind}", "evaluated_at": slot,
            "status": "allowed", "decision": "allow", "matches_current_evidence": True,
            "destination_id": plan["destination"]["destination_id"],
            "destination_fingerprint": plan["destination"]["fingerprint"],
            "delivery_fingerprint": plan["delivery"]["delivery_fingerprint"],
            "retry_policy_fingerprint": plan["delivery"]["retry_planning_policy_fingerprint"],
            "retry_execution_policy_fingerprint": plan["delivery"]["retry_execution_policy_fingerprint"],
            "endpoint_environment_variable": plan["destination"]["endpoint_environment_variable"],
        })
        health.append({
            "execution_kind": kind, "worker_id": plan["worker"]["worker_id"],
            "destination_id": plan["destination"]["destination_id"],
            "observed_at": slot, "evidence_id": f"health-evidence-{kind}",
            "evidence_complete": True, "consecutive_failures": 0,
            "persistence_ambiguity": False,
        })
    return {
        "worker_id": plan["worker"]["worker_id"], "evaluated_at": slot,
        "observed_at": slot, "scheduled_for": slot,
        "selected_authority": active, "current_authority": copy.deepcopy(active),
        "configuration_plan": copy.deepcopy(plan),
        "destination_review_expires_at": "2026-10-01T00:00:00+00:00",
        "readiness": readiness, "health": health,
    }


def retime(evidence: dict[str, Any], instant: datetime) -> None:
    value = instant.isoformat()
    evidence["evaluated_at"] = value
    evidence["observed_at"] = value
    for row in evidence["readiness"]:
        row["evaluated_at"] = value
    for row in evidence["health"]:
        row["observed_at"] = value


def test_healthy_slot_is_deterministic_evidence_not_permission(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    original = copy.deepcopy(evidence)
    result = assess_worker_readiness(evidence)
    assert result == assess_worker_readiness(evidence)
    assert validate_worker_readiness_assessment(result, evidence=evidence) == result
    assert result["assessment"] == "may_run"
    assert result["reasons"] == []
    assert result["read_only"] is True
    for field in ("runtime_permission_granted", "scheduler_mutated", "shared_lock_acquired",
                  "external_request_performed", "database_read_performed"):
        assert result[field] is False
    assert evidence == original


def test_early_but_active_invocation_waits(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    retime(evidence, datetime.fromisoformat(evidence["scheduled_for"]) - timedelta(seconds=1))
    result = assess_worker_readiness(evidence)
    assert result["assessment"] == "wait"
    assert result["reasons"] == ["slot_not_due"]


@pytest.mark.parametrize("action", ["suspend", "disable"])
def test_newer_stop_cannot_be_bypassed_by_old_active_grant(
    configurations: dict[str, Path], action: str,
) -> None:
    evidence = evidence_fixture(configurations)
    prior = evidence["selected_authority"]
    stopped = build_worker_authority_transition(
        plan=prior["plan"], request_id=f"stop-{action}", operator_id="risk-operator",
        action=action, requested_at=NOW + timedelta(seconds=2),
        effective_at=NOW + timedelta(seconds=2), reason_codes=["operator_request"], previous=prior,
    )
    evidence["current_authority"] = stopped
    result = assess_worker_readiness(evidence)
    assert result["assessment"] == "must_suspend"
    assert "authority_superseded" in result["reasons"]
    assert f"authority_{stopped['to_state']}" in result["reasons"]
    evidence["selected_authority"] = stopped
    retime(evidence, NOW + timedelta(minutes=30))
    result = assess_worker_readiness(evidence)
    assert result["assessment"] == "must_suspend"
    assert "authority_superseded" not in result["reasons"]
    assert f"authority_{stopped['to_state']}" in result["reasons"]


def test_exact_expiry_blocks_and_missing_authority_is_inactive(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    retime(evidence, datetime.fromisoformat(evidence["selected_authority"]["expires_at"]))
    assert "authority_expired" in assess_worker_readiness(evidence)["reasons"]
    evidence["selected_authority"] = None
    evidence["current_authority"] = None
    result = assess_worker_readiness(evidence)
    assert result["assessment"] == "must_suspend"
    assert result["authority_state"] == "inactive"
    assert "authority_missing" in result["reasons"]


@pytest.mark.parametrize(("target", "key", "value", "reason"), [
    ("root", "worker_id", "other-worker", "worker_scope_mismatch"),
    ("root", "scheduled_for", "2026-09-05T20:06:00+00:00", "schedule_slot_mismatch"),
    ("root", "destination_review_expires_at", None, "destination_review_missing"),
    ("root", "destination_review_expires_at", "2026-09-05T20:03:00+00:00", "destination_review_expired"),
    ("readiness", "status", "decision_superseded", "readiness_decision_superseded:initial"),
    ("readiness", "status", "decision_stale", "readiness_decision_stale:initial"),
    ("readiness", "destination_id", "other-destination", "readiness_configuration_mismatch:initial"),
    ("readiness", "delivery_fingerprint", "changed-fingerprint", "readiness_configuration_mismatch:initial"),
    ("health", "evidence_complete", False, "health_incomplete:initial"),
    ("health", "worker_id", "other-worker", "health_scope_mismatch:initial"),
    ("health", "consecutive_failures", 3, "repeated_delivery_failure:initial"),
    ("health", "persistence_ambiguity", True, "persistence_ambiguity:initial"),
])
def test_explicit_blocking_evidence(
    configurations: dict[str, Path], target: str, key: str, value: Any, reason: str,
) -> None:
    evidence = evidence_fixture(configurations)
    row = evidence if target == "root" else evidence[target][0]
    row[key] = value
    result = assess_worker_readiness(evidence)
    assert result["assessment"] == "must_suspend"
    assert reason in result["reasons"]


@pytest.mark.parametrize("group", ["readiness", "health"])
@pytest.mark.parametrize("offset", [-301, 1])
def test_stale_and_future_per_kind_evidence_blocks(
    configurations: dict[str, Path], group: str, offset: int,
) -> None:
    evidence = evidence_fixture(configurations)
    field = "evaluated_at" if group == "readiness" else "observed_at"
    evidence[group][0][field] = (
        datetime.fromisoformat(evidence["evaluated_at"]) + timedelta(seconds=offset)
    ).isoformat()
    result = assess_worker_readiness(evidence)
    suffix = "stale" if offset < 0 else "future"
    assert f"{group}_{suffix}:initial" in result["reasons"]
    assert result["assessment"] == "must_suspend"


@pytest.mark.parametrize("group", ["readiness", "health"])
def test_missing_evidence_is_not_healthy_and_both_kinds_are_required(
    configurations: dict[str, Path], group: str,
) -> None:
    evidence = evidence_fixture(configurations)
    evidence[group].pop()
    result = assess_worker_readiness(evidence)
    assert f"{group}_missing:retry" in result["reasons"]
    assert result["assessment"] == "must_suspend"


def test_rehashed_configuration_alternative_does_not_match_grant(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    plan = evidence["configuration_plan"]
    plan["execution"]["work_items"][0]["max_events"] = 1
    plan["plan_id"] = _plan_id({key: value for key, value in plan.items() if key != "plan_id"})
    result = assess_worker_readiness(evidence)
    assert "configuration_mismatch" in result["reasons"]
    assert result["assessment"] == "must_suspend"


def test_rehashed_changed_verdict_is_rejected_against_evidence(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    evidence["health"][0]["persistence_ambiguity"] = True
    result = assess_worker_readiness(evidence)
    result["assessment"] = "may_run"
    result["reasons"] = []
    content = {key: value for key, value in result.items() if key != "assessment_id"}
    result["assessment_id"] = f"{MODEL_VERSION}-{hashlib.sha256(canonical_bytes(content)).hexdigest()}"
    with pytest.raises(ValidationError, match="differs"):
        validate_worker_readiness_assessment(result, evidence=evidence)


@pytest.mark.parametrize("case", ["bool_count", "unknown_field", "duplicate", "unconfigured",
                                 "contradiction", "noncanonical_time", "oversized"])
def test_malformed_observations_fail_closed(configurations: dict[str, Path], case: str) -> None:
    evidence = evidence_fixture(configurations)
    if case == "bool_count":
        evidence["health"][0]["consecutive_failures"] = False
    elif case == "unknown_field":
        evidence["health"][0]["payload"] = "unexpected"
    elif case == "duplicate":
        evidence["readiness"] = [evidence["readiness"][0], evidence["readiness"][0]]
    elif case == "unconfigured":
        evidence["health"][0]["execution_kind"] = "automatic"
    elif case == "contradiction":
        evidence["readiness"][0]["decision"] = "block"
    elif case == "noncanonical_time":
        evidence["evaluated_at"] = "2026-09-05T20:05:00Z"
    else:
        evidence["worker_id"] = "x" * 1_048_576
    with pytest.raises(ValidationError):
        assess_worker_readiness(evidence)


def test_multiple_reasons_are_stable_and_threshold_is_inclusive(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    evidence["health"][0]["consecutive_failures"] = 2
    assert assess_worker_readiness(evidence)["assessment"] == "may_run"
    evidence["health"][0]["consecutive_failures"] = 3
    evidence["health"][0]["persistence_ambiguity"] = True
    result = assess_worker_readiness(evidence)
    assert result["reasons"] == ["persistence_ambiguity:initial", "repeated_delivery_failure:initial"]
    evidence["health"][0]["consecutive_failures"] = 0
    assert result["reasons"] == ["persistence_ambiguity:initial", "repeated_delivery_failure:initial"]
