from __future__ import annotations

import copy
import hashlib
from datetime import timedelta
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    MODEL_VERSION, canonical_bytes, validate_worker_authority_chain,
)
from src.orchestration.notification_worker_suspension_transition import (
    build_worker_suspension_bundle, validate_worker_suspension_bundle,
)
from test_notification_worker_authority_contract import NOW, grant, stop
from test_notification_worker_suspension import AT, decision, observation


def bundle() -> dict[str, Any]:
    return build_worker_suspension_bundle(authority=grant(), decision=decision(), operator_id="health-observer")


def test_bundle_reuses_exact_lifecycle_and_decision() -> None:
    result = bundle()
    transition = result["transition"]
    assert validate_worker_suspension_bundle(result) == result
    assert validate_worker_authority_chain(transition, result["authority"]) == transition
    assert result == bundle()
    assert transition["model_version"] == MODEL_VERSION
    assert transition["action"] == "suspend"
    assert transition["previous_transition_id"] == result["authority"]["transition_id"]
    assert transition["plan"] == result["authority"]["plan"]
    assert transition["reason_codes"] == result["decision"]["reason_codes"]
    assert transition["effective_at"] == AT.isoformat()
    assert transition["expires_at"] is None
    assert transition["reviewed_by"] == []
    assert transition["request_id"] == "worker-suspension:" + hashlib.sha256(canonical_bytes(result["decision"])).hexdigest()
    assert transition["scheduler_mutated"] is False
    assert transition["external_request_performed"] is False


def test_inputs_are_not_modified_or_aliased() -> None:
    prior, evaluated = grant(), decision()
    expected = copy.deepcopy((prior, evaluated))
    result = build_worker_suspension_bundle(authority=prior, decision=evaluated, operator_id="observer")
    assert (prior, evaluated) == expected
    prior["plan"]["worker"]["enabled"] = False
    evaluated["reason_codes"].clear()
    assert result["authority"]["plan"]["worker"]["enabled"] is True
    assert result["decision"]["reason_codes"]


@pytest.mark.parametrize("kind", ["healthy", "expired", "suspended", "disabled"])
def test_non_suspension_results_never_materialize_a_transition(kind: str) -> None:
    prior = grant()
    at: Any = AT
    if kind == "expired":
        at = prior["expires_at"]
    elif kind in {"suspended", "disabled"}:
        prior = stop(prior, action="suspend" if kind == "suspended" else "disable")
        at = NOW + timedelta(days=1)
    evaluated = decision(observation(prior), authority=prior, evaluated_at=at)
    with pytest.raises(ValidationError, match="only a suspend"):
        build_worker_suspension_bundle(authority=prior, decision=evaluated, operator_id="observer")


def test_same_effective_time_cannot_fabricate_a_later_timestamp() -> None:
    prior = grant()
    evaluated = decision(authority=prior, evaluated_at=prior["effective_at"])
    with pytest.raises(ValidationError, match="strictly time ordered"):
        build_worker_suspension_bundle(authority=prior, decision=evaluated, operator_id="observer")


@pytest.mark.parametrize("key,value", [
    ("previous_transition_id", "other-head"), ("request_id", "arbitrary-request"),
    ("reason_codes", ["operator_request"]), ("effective_at", (AT + timedelta(seconds=1)).isoformat()),
    ("scheduler_mutated", True), ("action", "disable"),
])
def test_rehashed_stop_cannot_change_decision_binding(key: str, value: Any) -> None:
    result = bundle()
    transition = result["transition"]
    transition[key] = value
    identity = {k: v for k, v in transition.items() if k != "transition_id"}
    transition["transition_id"] = f"{MODEL_VERSION}-{hashlib.sha256(canonical_bytes(identity)).hexdigest()}"
    with pytest.raises(ValidationError):
        validate_worker_suspension_bundle(result)


def test_exact_authority_and_exact_bundle_fields_are_required() -> None:
    result = bundle()
    result["authority"] = grant(request_id="OTHER-HEAD")
    with pytest.raises(ValidationError):
        validate_worker_suspension_bundle(result)
    result = bundle()
    result["unknown"] = True
    with pytest.raises(ValidationError):
        validate_worker_suspension_bundle(result)


def test_operator_identity_is_required_but_is_not_authentication() -> None:
    with pytest.raises(ValidationError):
        build_worker_suspension_bundle(authority=grant(), decision=decision(), operator_id="")
    other = build_worker_suspension_bundle(authority=grant(), decision=decision(), operator_id="another-observer")
    first = bundle()
    assert other["transition"]["request_id"] == first["transition"]["request_id"]
    assert other["transition"]["transition_id"] != first["transition"]["transition_id"]


def test_expired_destination_review_can_still_produce_a_stop() -> None:
    snapshot = observation()
    snapshot["review_expires_at"] = AT.isoformat()
    result = build_worker_suspension_bundle(authority=grant(), decision=decision(snapshot), operator_id="observer")
    assert result["transition"]["reason_codes"] == ["expired_review"]
