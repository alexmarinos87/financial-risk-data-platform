from __future__ import annotations

import copy
import hashlib
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from src.orchestration.plan_notification_worker import build_notification_worker_plan, load_notification_workers
from src.orchestration.portfolio_risk_notification_retry_execution_policy import load_retry_execution_contract
from src.warehouse.notification_execution_readiness_gate import evaluate_notification_execution_readiness
from src.warehouse.notification_execution_readiness_history_contract import build_notification_execution_readiness_record
from src.warehouse import notification_worker_readiness_sources as sources
from test_notification_execution_readiness_enforcement import BASE_TIME, _decision, _delivery_config, _destination


def source_fixture(*, initial_only: bool = False, max_age: int = 300) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    worker = load_notification_workers(Path("config/notification_workers.yaml"))["risk-operations-managed"]
    worker = replace(worker, enabled=True, readiness=replace(worker.readiness, max_age_seconds=max_age),
                     execution_kinds=("initial",) if initial_only else ("initial", "retry"))
    _, policy, execution = load_retry_execution_contract(Path("config/notification_delivery.yaml"))
    execution = replace(execution, enabled=True)
    delivery, destination = _delivery_config(), _destination()
    plan = build_notification_worker_plan(
        worker=worker, delivery=delivery, retry_policy=policy, retry_execution=execution,
        destination=destination, planned_at=BASE_TIME,
    )
    rows = []
    for kind in worker.execution_kinds:
        seed = _decision(evaluated_at=BASE_TIME, execution_kind=kind)
        decision = evaluate_notification_execution_readiness(
            execution_kind=kind, evaluated_at=BASE_TIME, delivery_config=delivery,
            retry_policy_fingerprint=policy.fingerprint, retry_execution_policy=execution,
            destination=destination, activation_review=seed["activation_review"],
            transition_review=seed["transition_review"], ambiguities=[],
        )
        record = build_notification_execution_readiness_record(
            request_id=f"source-{kind}", recorded_at=BASE_TIME + timedelta(seconds=1), decision=decision,
        )
        rows.append({
            "destination_id": destination.destination_id, "execution_kind": kind,
            "readiness_record_id": record["record_id"], "readiness_review_status": "allowed",
            "execution_ready": True, "decision_matches_current_evidence": True,
            "record_json": record, "document_sha256": hashlib.sha256(canonical_bytes(record)).hexdigest(),
        })
    return plan, rows


def build(plan: dict[str, Any], rows: list[dict[str, Any]], seconds: float = 5) -> dict[str, Any]:
    return sources.build_worker_readiness_sources(
        plan=plan, sources=rows, observed_at=BASE_TIME + timedelta(seconds=seconds),
    )


def test_real_source_records_are_bound_and_detached() -> None:
    plan, rows = source_fixture()
    original = copy.deepcopy((plan, rows))
    result = build(plan, rows)
    assert result == build(plan, rows)
    assert sources.validate_worker_readiness_sources(result) == result
    assert result["all_sources_allowed"] is True
    assert [row["status"] for row in result["readiness"]] == ["allowed", "allowed"]
    assert all(result[key] is False for key in (
        "runtime_permission_granted", "worker_authority_verified", "failure_history_verified",
    ))
    assert (plan, rows) == original
    rows[0]["record_json"]["decision"]["blocking_reasons"].append("unrelated")
    assert result["sources"] == original[1]


@pytest.mark.parametrize("initial_only", [False, True])
def test_missing_kind_never_becomes_healthy(initial_only: bool) -> None:
    plan, rows = source_fixture(initial_only=initial_only)
    result = build(plan, rows[:-1])
    assert result["all_sources_allowed"] is False
    assert result["readiness"][-1]["status"] == "missing"
    assert result["readiness"][-1]["record_id"] is None


def test_explicit_missing_serving_row_is_preserved() -> None:
    plan, rows = source_fixture()
    rows[0].update(readiness_record_id=None, record_json=None, document_sha256=None,
                   readiness_review_status="decision_missing", execution_ready=False,
                   decision_matches_current_evidence=False)
    result = build(plan, rows)
    assert result["readiness"][0]["status"] == "missing"
    assert result["readiness"][1]["status"] == "allowed"


@pytest.mark.parametrize(("age", "expected"), [(30, "allowed"), (30.000001, "stale")])
def test_stricter_worker_age_is_recomputed_at_the_exact_boundary(age: float, expected: str) -> None:
    plan, rows = source_fixture(max_age=30)
    result = build(plan, rows, age)
    assert all(row["status"] == expected for row in result["readiness"])


@pytest.mark.parametrize("status", ["decision_stale", "decision_superseded"])
def test_serving_restrictions_are_never_upgraded(status: str) -> None:
    plan, rows = source_fixture()
    rows[1].update(readiness_review_status=status, execution_ready=False,
                   decision_matches_current_evidence=status != "decision_superseded")
    result = build(plan, rows)
    assert result["readiness"][0]["status"] == "allowed"
    assert result["readiness"][1]["status"] in {"stale", "superseded"}
    assert result["all_sources_allowed"] is False


@pytest.mark.parametrize(("field", "value"), [
    ("document_sha256", "0" * 64), ("readiness_record_id", "wrong-record"),
    ("destination_id", "wrong-destination"), ("execution_ready", 1),
    ("decision_matches_current_evidence", 1), ("execution_ready", False),
    ("decision_matches_current_evidence", False), ("readiness_review_status", "blocked"),
    ("readiness_review_status", {}), ("record_json", None), ("record_json", []),
    ("execution_kind", "unselected"),
])
def test_contradictory_source_metadata_is_rejected(field: str, value: Any) -> None:
    plan, rows = source_fixture()
    rows[0][field] = value
    with pytest.raises(ValidationError):
        build(plan, rows)


def test_swapped_retained_kind_is_rejected_even_with_matching_digest() -> None:
    plan, rows = source_fixture()
    rows[0].update(record_json=rows[1]["record_json"], document_sha256=rows[1]["document_sha256"],
                   readiness_record_id=rows[1]["readiness_record_id"])
    with pytest.raises(ValidationError, match="scope"):
        build(plan, rows)


@pytest.mark.parametrize("variant", ["duplicate", "unsorted", "extra", "too_many"])
def test_source_grain_and_exact_fields_are_checked(variant: str) -> None:
    plan, rows = source_fixture()
    if variant == "duplicate":
        rows[1] = copy.deepcopy(rows[0])
    elif variant == "unsorted":
        rows.reverse()
    elif variant == "extra":
        rows[0]["endpoint_value"] = "must-not-be-retained"
    else:
        rows.append(copy.deepcopy(rows[0]))
    with pytest.raises(ValidationError):
        build(plan, rows)


def test_future_recording_is_not_a_fresh_observation() -> None:
    plan, rows = source_fixture()
    with pytest.raises(ValidationError, match="chronology"):
        build(plan, rows, 0)


def test_configuration_drift_is_superseded_not_allowed() -> None:
    plan, rows = source_fixture()
    from src.orchestration.plan_notification_worker import _plan_id

    plan["delivery"]["retry_planning_policy_fingerprint"] = (
        plan["delivery"]["retry_planning_policy_fingerprint"][:-24] + "0" * 24
    )
    plan["plan_id"] = _plan_id({k: v for k, v in plan.items() if k != "plan_id"})
    result = build(plan, rows)
    assert all(row["status"] == "superseded" for row in result["readiness"])
    assert result["all_sources_allowed"] is False


@pytest.mark.parametrize("field", ["runtime_permission_granted", "all_sources_allowed", "readiness"])
def test_rehashed_result_does_not_bypass_reconstruction(field: str) -> None:
    plan, rows = source_fixture()
    result = build(plan, rows[:-1])
    result[field] = [] if field == "readiness" else True
    result["snapshot_id"] = sources.MODEL_VERSION + "-" + hashlib.sha256(canonical_bytes(
        {k: v for k, v in result.items() if k != "snapshot_id"}
    )).hexdigest()
    with pytest.raises(ValidationError):
        sources.validate_worker_readiness_sources(result)


def test_source_byte_bound_precedes_record_validation() -> None:
    plan, rows = source_fixture()
    rows[0]["record_json"] = {"unexpected": "x" * sources.MAX_SOURCE_BYTES}
    with pytest.raises(ValidationError, match="byte limit"):
        build(plan, rows)
