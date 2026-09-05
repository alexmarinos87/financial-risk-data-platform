from __future__ import annotations

import copy
import hashlib
from datetime import timedelta
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from src.orchestration.notification_worker_suspension import (
    MODEL_VERSION, evaluate_worker_suspension, validate_worker_suspension_decision,
)
from test_notification_worker_authority_contract import NOW, grant, plan_fixture, rehash, stop

AT = NOW + timedelta(seconds=10)


def observation(authority: dict[str, Any] | None = None) -> dict[str, Any]:
    prior = grant() if authority is None else authority
    p = prior["plan"]
    return {
        "observation_id": "health-observation-1", "authority_transition_id": prior["transition_id"],
        "observed_at": AT.isoformat(), "review_expires_at": (NOW + timedelta(days=1)).isoformat(),
        "worker_fingerprint": p["worker"]["fingerprint"],
        "readiness": [{
            "execution_kind": item["execution_kind"], "record_id": "ready-" + item["execution_kind"],
            "document_sha256": "a" * 64, "destination_id": p["destination"]["destination_id"],
            "destination_fingerprint": p["destination"]["fingerprint"],
            "delivery_fingerprint": p["delivery"]["delivery_fingerprint"],
            "evaluated_at": AT.isoformat(), "status": "allowed",
        } for item in p["execution"]["work_items"]],
        "failures": [{
            "execution_kind": item["execution_kind"], "history_id": "history-" + item["execution_kind"],
            "history_sha256": "b" * 64, "observed_at": AT.isoformat(),
            "consecutive_failures": 0, "unresolved_persistence_ambiguity": False,
        } for item in p["execution"]["work_items"]],
    }


def decision(snapshot: Any = None, **overrides: Any) -> dict[str, Any]:
    args = {"authority": grant(), "observation": snapshot, "evaluated_at": AT}
    args.update(overrides)
    return evaluate_worker_suspension(**args)


def test_healthy_is_deterministic_and_never_permission() -> None:
    snapshot = observation()
    result = decision(snapshot)
    assert result == decision(snapshot)
    assert result["outcome"] == "no_suspension_required"
    assert result["reason_codes"] == []
    assert result["runtime_permission_granted"] is False
    assert result["scheduler_mutated"] is False
    assert result["external_request_performed"] is False
    assert result["resume_not_before"] is None
    assert validate_worker_suspension_decision(result, authority=grant()) == result
    snapshot["failures"][0]["consecutive_failures"] = 20
    assert result["observation"]["failures"][0]["consecutive_failures"] == 0


def test_missing_observation_does_not_assume_healthy() -> None:
    result = decision()
    assert result["outcome"] == "suspend"
    assert result["reason_codes"] == ["expired_review", "persistence_ambiguity", "readiness_failure"]
    assert result["resume_not_before"] == (AT + timedelta(seconds=900)).isoformat()


@pytest.mark.parametrize("kind", ["initial", "retry"])
@pytest.mark.parametrize("group,reason", [("readiness", "readiness_failure"), ("failures", "persistence_ambiguity")])
def test_every_selected_kind_requires_both_evidence_sources(kind: str, group: str, reason: str) -> None:
    snapshot = observation()
    snapshot[group] = [row for row in snapshot[group] if row["execution_kind"] != kind]
    result = decision(snapshot)
    assert result["outcome"] == "suspend"
    assert result["reason_codes"] == [reason]


@pytest.mark.parametrize("status", ["blocked", "stale", "superseded"])
def test_nonallowed_readiness_requires_suspension(status: str) -> None:
    snapshot = observation()
    snapshot["readiness"][1]["status"] = status
    assert decision(snapshot)["reason_codes"] == ["readiness_failure"]


@pytest.mark.parametrize("field", ["destination_id", "destination_fingerprint", "delivery_fingerprint"])
def test_changed_readiness_scope_fails_closed(field: str) -> None:
    snapshot = observation()
    snapshot["readiness"][0][field] = "changed"
    assert decision(snapshot)["reason_codes"] == ["readiness_failure"]


@pytest.mark.parametrize("count,outcome", [(0, "no_suspension_required"), (2, "no_suspension_required"), (3, "suspend"), (4, "suspend")])
def test_failure_threshold_is_per_kind_and_inclusive(count: int, outcome: str) -> None:
    snapshot = observation()
    snapshot["failures"][1]["consecutive_failures"] = count
    result = decision(snapshot)
    assert result["outcome"] == outcome
    assert result["reason_codes"] == ([] if count < 3 else ["repeated_delivery_failure"])


def test_ambiguity_review_expiry_and_failure_reasons_are_canonical() -> None:
    snapshot = observation()
    snapshot["review_expires_at"] = AT.isoformat()
    snapshot["failures"][0]["unresolved_persistence_ambiguity"] = True
    snapshot["failures"][1]["consecutive_failures"] = 3
    snapshot["readiness"][0]["status"] = "blocked"
    assert decision(snapshot)["reason_codes"] == [
        "expired_review", "persistence_ambiguity", "readiness_failure", "repeated_delivery_failure",
    ]


@pytest.mark.parametrize("age,outcome", [(300, "no_suspension_required"), (300.000001, "suspend")])
def test_freshness_boundary(age: float, outcome: str) -> None:
    assert decision(observation(), evaluated_at=AT + timedelta(seconds=age))["outcome"] == outcome


@pytest.mark.parametrize("group,time_key", [("readiness", "evaluated_at"), ("failures", "observed_at")])
def test_fresh_snapshot_cannot_hide_stale_rows(group: str, time_key: str) -> None:
    snapshot = observation()
    snapshot[group][0][time_key] = (AT - timedelta(seconds=301)).isoformat()
    assert decision(snapshot)["outcome"] == "suspend"


@pytest.mark.parametrize("bad", [True, -1, 1_000_001, 1.5, "3"])
def test_invalid_counts_are_rejected(bad: Any) -> None:
    snapshot = observation()
    snapshot["failures"][0]["consecutive_failures"] = bad
    with pytest.raises(ValidationError):
        decision(snapshot)


@pytest.mark.parametrize("change", ["future", "naive", "other_authority", "duplicate", "extra_kind", "unknown", "digest", "ambiguity", "row_future", "reused_record"])
def test_malformed_or_wrongly_bound_snapshot_is_rejected(change: str) -> None:
    snapshot = observation()
    if change == "future":
        snapshot["observed_at"] = (AT + timedelta(microseconds=1)).isoformat()
    elif change == "naive":
        snapshot["observed_at"] = "2026-09-05T20:00:10"
    elif change == "other_authority":
        snapshot["authority_transition_id"] = "different"
    elif change == "duplicate":
        snapshot["readiness"][1] = copy.deepcopy(snapshot["readiness"][0])
    elif change == "extra_kind":
        snapshot["readiness"][1]["execution_kind"] = "other"
    elif change == "unknown":
        snapshot["endpoint_value"] = "must-not-be-retained"
    elif change == "digest":
        snapshot["readiness"][0]["document_sha256"] = "not-a-digest"
    elif change == "ambiguity":
        snapshot["failures"][0]["unresolved_persistence_ambiguity"] = 0
    elif change == "row_future":
        snapshot["readiness"][0]["evaluated_at"] = (AT + timedelta(seconds=1)).isoformat()
    else:
        snapshot["readiness"][1]["record_id"] = snapshot["readiness"][0]["record_id"]
    with pytest.raises(ValidationError):
        decision(snapshot)


def test_worker_mismatch_and_missing_review_are_not_healthy() -> None:
    snapshot = observation()
    snapshot["worker_fingerprint"] = "changed"
    snapshot["review_expires_at"] = None
    assert decision(snapshot)["reason_codes"] == ["expired_review", "readiness_failure"]


@pytest.mark.parametrize("action", ["suspend", "disable"])
def test_stopped_workers_do_not_resume_after_cooldown(action: str) -> None:
    prior = stop(grant(), action=action)
    result = decision(observation(prior), authority=prior, evaluated_at=NOW + timedelta(days=1))
    assert result["outcome"] == "inactive"
    assert result["authority_state"] == prior["to_state"]
    assert result["runtime_permission_granted"] is False


def test_authority_expiry_is_exclusive_and_not_a_new_suspension() -> None:
    prior = grant()
    result = decision(observation(), evaluated_at=prior["expires_at"])
    assert result["outcome"] == "inactive"
    assert result["authority_state"] == "expired"
    assert "expired_review" in result["reason_codes"]


def test_initial_only_worker_does_not_require_retry_evidence() -> None:
    plan = plan_fixture()
    plan["execution"]["work_items"] = plan["execution"]["work_items"][:1]
    prior = grant(plan=rehash(plan))
    result = decision(observation(prior), authority=prior)
    assert result["outcome"] == "no_suspension_required"


@pytest.mark.parametrize("field,value", [("outcome", "no_suspension_required"), ("reason_codes", []), ("runtime_permission_granted", True), ("resume_not_before", None)])
def test_rehashed_decision_cannot_contradict_inputs(field: str, value: Any) -> None:
    result = decision()
    result[field] = value
    identity = {key: val for key, val in result.items() if key != "decision_id"}
    result["decision_id"] = f"{MODEL_VERSION}-{hashlib.sha256(canonical_bytes(identity)).hexdigest()}"
    with pytest.raises(ValidationError):
        validate_worker_suspension_decision(result, authority=grant())


def test_wrong_authority_and_oversized_evidence_are_rejected() -> None:
    with pytest.raises(ValidationError):
        validate_worker_suspension_decision(decision(), authority=grant(request_id="NEW"))
    snapshot = observation()
    snapshot["observation_id"] = "x" * 1_048_577
    with pytest.raises(ValidationError):
        decision(snapshot)
