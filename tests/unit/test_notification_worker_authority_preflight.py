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
from src.orchestration.notification_worker_authority_preflight import (
    MODEL_VERSION, evaluate_worker_authority_preflight, validate_worker_authority_preflight,
)
from src.orchestration.plan_notification_worker import _plan_id
from test_reviewed_notification_worker_authority import (
    NOW, _grant, _plan, configurations as configurations,
)


def preflight_evidence(paths: dict[str, Path]) -> dict[str, Any]:
    plan = _plan(paths)
    active = _grant(plan, paths, requested_at=NOW, effective_at=NOW)
    slot = plan["schedule"]["scheduled_for"]
    return {
        "worker_id": plan["worker"]["worker_id"],
        "selected_transition_id": active["transition_id"],
        "evaluated_at": slot, "observed_at": slot, "scheduled_for": slot,
        "current_authority": active, "configuration_plan": copy.deepcopy(plan),
        "destination_review_expires_at": "2026-10-01T00:00:00+00:00",
    }


def test_due_slot_only_qualifies_for_separate_health_review(configurations: dict[str, Path]) -> None:
    evidence = preflight_evidence(configurations)
    original = copy.deepcopy(evidence)
    result = evaluate_worker_authority_preflight(evidence)
    assert result == evaluate_worker_authority_preflight(evidence)
    assert validate_worker_authority_preflight(result, evidence=evidence) == result
    assert result["outcome"] == "eligible_for_health_review"
    assert result["reasons"] == []
    for field in ("readiness_evaluated", "runtime_permission_granted", "scheduler_mutated",
                  "shared_lock_acquired", "external_request_performed"):
        assert result[field] is False
    assert evidence == original


def test_early_active_slot_waits(configurations: dict[str, Path]) -> None:
    evidence = preflight_evidence(configurations)
    early = datetime.fromisoformat(evidence["scheduled_for"]) - timedelta(seconds=1)
    evidence.update(evaluated_at=early.isoformat(), observed_at=early.isoformat())
    result = evaluate_worker_authority_preflight(evidence)
    assert result["outcome"] == "wait"
    assert result["reasons"] == ["slot_not_due"]


@pytest.mark.parametrize("action", ["suspend", "disable"])
def test_older_selected_grant_cannot_bypass_newer_stop(configurations: dict[str, Path], action: str) -> None:
    evidence = preflight_evidence(configurations)
    prior = evidence["current_authority"]
    stopped = build_worker_authority_transition(
        plan=prior["plan"], request_id=f"preflight-{action}", operator_id="risk-operator",
        action=action, requested_at=NOW + timedelta(seconds=2),
        effective_at=NOW + timedelta(seconds=2), reason_codes=["operator_request"], previous=prior,
    )
    evidence["current_authority"] = stopped
    result = evaluate_worker_authority_preflight(evidence)
    assert result["outcome"] == "blocked"
    assert "authority_superseded" in result["reasons"]
    assert f"authority_{stopped['to_state']}" in result["reasons"]
    evidence["selected_transition_id"] = stopped["transition_id"]
    later = (NOW + timedelta(minutes=30)).isoformat()
    evidence.update(evaluated_at=later, observed_at=later)
    result = evaluate_worker_authority_preflight(evidence)
    assert result["outcome"] == "blocked"
    assert "authority_superseded" not in result["reasons"]
    assert f"authority_{stopped['to_state']}" in result["reasons"]


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("worker_id", "different-worker", "worker_scope_mismatch"),
    ("selected_transition_id", "different-transition", "authority_superseded"),
    ("scheduled_for", "2026-09-05T20:06:00+00:00", "schedule_slot_mismatch"),
    ("configuration_plan", None, "configuration_unverified"),
    ("current_authority", None, "authority_missing"),
    ("destination_review_expires_at", None, "destination_review_missing"),
    ("destination_review_expires_at", "2026-09-05T20:03:00+00:00", "destination_review_expired"),
])
def test_missing_or_mismatched_evidence_blocks(
    configurations: dict[str, Path], field: str, value: Any, reason: str,
) -> None:
    evidence = preflight_evidence(configurations)
    evidence[field] = value
    result = evaluate_worker_authority_preflight(evidence)
    assert result["outcome"] == "blocked"
    assert reason in result["reasons"]


def test_expiry_is_exclusive_and_review_must_cover_authority(configurations: dict[str, Path]) -> None:
    evidence = preflight_evidence(configurations)
    expiry = evidence["current_authority"]["expires_at"]
    evidence["destination_review_expires_at"] = expiry
    assert evaluate_worker_authority_preflight(evidence)["outcome"] == "eligible_for_health_review"
    evidence["destination_review_expires_at"] = (
        datetime.fromisoformat(expiry) - timedelta(seconds=1)
    ).isoformat()
    assert "authority_exceeds_destination_review" in evaluate_worker_authority_preflight(evidence)["reasons"]
    evidence.update(evaluated_at=expiry, observed_at=expiry)
    result = evaluate_worker_authority_preflight(evidence)
    assert result["outcome"] == "blocked"
    assert "authority_expired" in result["reasons"]


@pytest.mark.parametrize(("age", "reason"), [(301, "observation_stale"), (-1, "observation_future")])
def test_snapshot_freshness(configurations: dict[str, Path], age: int, reason: str) -> None:
    evidence = preflight_evidence(configurations)
    evidence["observed_at"] = (
        datetime.fromisoformat(evidence["evaluated_at"]) - timedelta(seconds=age)
    ).isoformat()
    result = evaluate_worker_authority_preflight(evidence)
    assert result["outcome"] == "blocked"
    assert reason in result["reasons"]


def test_exact_freshness_limit_and_not_yet_effective_boundary(configurations: dict[str, Path]) -> None:
    evidence = preflight_evidence(configurations)
    evidence["observed_at"] = (
        datetime.fromisoformat(evidence["evaluated_at"]) - timedelta(seconds=300)
    ).isoformat()
    assert evaluate_worker_authority_preflight(evidence)["outcome"] == "eligible_for_health_review"
    earlier = (NOW - timedelta(seconds=1)).isoformat()
    evidence.update(evaluated_at=earlier, observed_at=earlier)
    result = evaluate_worker_authority_preflight(evidence)
    assert result["outcome"] == "blocked"
    assert "authority_inactive" in result["reasons"]
    assert "authority_not_yet_observed" in result["reasons"]


def test_changed_rehashed_configuration_is_not_same_reviewed_plan(configurations: dict[str, Path]) -> None:
    evidence = preflight_evidence(configurations)
    plan = evidence["configuration_plan"]
    plan["execution"]["work_items"][0]["max_events"] = 1
    plan["plan_id"] = _plan_id({key: value for key, value in plan.items() if key != "plan_id"})
    assert "configuration_mismatch" in evaluate_worker_authority_preflight(evidence)["reasons"]
    plan["worker"]["enabled"] = False
    plan["status"] = "disabled"
    plan["blocking_reasons"] = ["worker_disabled"]
    plan["schedule"]["activation_action"] = "none"
    plan["plan_id"] = _plan_id({key: value for key, value in plan.items() if key != "plan_id"})
    result = evaluate_worker_authority_preflight(evidence)
    assert "configuration_blocked" in result["reasons"]
    assert result["outcome"] == "blocked"


def test_rehashed_changed_verdict_and_permission_are_rejected(configurations: dict[str, Path]) -> None:
    evidence = preflight_evidence(configurations)
    evidence["configuration_plan"] = None
    result = evaluate_worker_authority_preflight(evidence)
    result.update(outcome="eligible_for_health_review", reasons=[], runtime_permission_granted=True)
    content = {key: value for key, value in result.items() if key != "preflight_id"}
    result["preflight_id"] = f"{MODEL_VERSION}-{hashlib.sha256(canonical_bytes(content)).hexdigest()}"
    with pytest.raises(ValidationError, match="differs"):
        validate_worker_authority_preflight(result, evidence=evidence)


@pytest.mark.parametrize("case", ["unknown", "noncanonical_time", "oversized", "invalid_authority", "bool_limit"])
def test_malformed_input_fails_closed(configurations: dict[str, Path], case: str) -> None:
    evidence = preflight_evidence(configurations)
    if case == "unknown":
        evidence["health"] = "belongs to a separate contract"
    elif case == "noncanonical_time":
        evidence["evaluated_at"] = "2026-09-05T20:05:00Z"
    elif case == "oversized":
        evidence["worker_id"] = "x" * 1_048_576
    elif case == "invalid_authority":
        evidence["current_authority"] = {"plan": []}
    else:
        evidence["configuration_plan"]["concurrency_control"]["max_concurrency"] = True
    with pytest.raises(ValidationError):
        evaluate_worker_authority_preflight(evidence)


def test_prior_result_is_detached_from_later_input_changes(configurations: dict[str, Path]) -> None:
    evidence = preflight_evidence(configurations)
    result = evaluate_worker_authority_preflight(evidence)
    evidence["current_authority"]["plan"]["worker"]["worker_id"] = "changed"
    assert result["worker_id"] == "risk-operations-managed"
    assert result["reasons"] == []
