from __future__ import annotations

import copy
import hashlib
from datetime import timedelta
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.warehouse.notification_worker_readiness_source import source_bytes
from src.warehouse.notification_worker_readiness_snapshot import (
    MODEL_VERSION, build_worker_readiness_snapshot, validate_worker_readiness_snapshot,
)
from test_notification_execution_readiness_gate import EVALUATED_AT
from test_notification_worker_authority_contract import grant, plan_fixture, rehash, stop
from test_worker_readiness_source import readiness_record

OBSERVED = EVALUATED_AT + timedelta(seconds=2)


def source_entry(kind: str) -> dict[str, Any]:
    record = readiness_record(kind)
    decision = record["decision"]
    activation, transition = decision["activation_review"], decision["transition_review"]
    return {"execution_kind": kind, "record": record,
            "document_sha256": hashlib.sha256(source_bytes(record)).hexdigest(), "review": {
        "destination_id": decision["destination"]["destination_id"], "execution_kind": kind,
        "readiness_record_id": record["record_id"],
        "current_destination_fingerprint": decision["destination"]["fingerprint"],
        "current_authority_id": activation["authority_id"], "current_checklist_id": activation["checklist_id"],
        "activation_review_status": activation["review_status"],
        "operational_activation_ready": activation["operational_activation_ready"],
        "current_transition_record_id": transition["transition_record_id"],
        "current_transition_rehearsal_id": transition["transition_rehearsal_id"],
        "current_transition_review_status": transition["transition_review_status"],
        "current_transition_ready": transition["transition_ready"],
        "current_endpoint_environment_variable": transition["rollback_endpoint_environment_variable"],
    }}


def readiness_authority() -> dict[str, Any]:
    p = plan_fixture(planned_at=EVALUATED_AT - timedelta(seconds=60))
    decision = readiness_record()["decision"]
    p["destination"]["fingerprint"] = decision["destination"]["fingerprint"]
    configuration = decision["configuration"]
    for target, source in (("delivery_fingerprint", "delivery_fingerprint"),
                           ("retry_planning_policy_fingerprint", "retry_policy_fingerprint"),
                           ("retry_execution_policy_fingerprint", "retry_execution_policy_fingerprint")):
        p["delivery"][target] = configuration[source]
    return grant(plan=rehash(p))


def snapshot(sources: Any = None, **changes: Any) -> dict[str, Any]:
    args = {"authority": readiness_authority(), "sources": [source_entry("initial"), source_entry("retry")] if sources is None else sources,
            "observed_at": OBSERVED}
    args.update(changes)
    return build_worker_readiness_snapshot(**args)


def test_verified_sources_do_not_claim_failure_health_or_runtime_permission() -> None:
    result = snapshot()
    assert result["outcome"] == "ready_sources"
    assert result["missing_execution_kinds"] == []
    assert [row["status"] for row in result["readiness"]] == ["allowed", "allowed"]
    assert result["failure_history_verified"] is False
    assert result["current_authority_verified"] is False
    assert result["runtime_permission_granted"] is False
    assert validate_worker_readiness_snapshot(result, authority=readiness_authority(), sources=[source_entry("initial"), source_entry("retry")]) == result
    assert snapshot([source_entry("retry"), source_entry("initial")]) == result


@pytest.mark.parametrize("kind", ["initial", "retry"])
def test_missing_kind_blocks_without_fabricating_zero_failures(kind: str) -> None:
    result = snapshot([source_entry(kind)])
    missing = "retry" if kind == "initial" else "initial"
    assert result["outcome"] == "blocked"
    assert result["missing_execution_kinds"] == [missing]
    assert "failures" not in result


@pytest.mark.parametrize("field", [
    "current_destination_fingerprint", "current_authority_id", "current_checklist_id",
    "activation_review_status", "current_transition_record_id", "current_transition_rehearsal_id",
    "current_transition_review_status", "current_endpoint_environment_variable",
])
def test_changed_current_review_supersedes_an_intact_old_allow(field: str) -> None:
    entries = [source_entry("initial"), source_entry("retry")]
    entries[1]["review"][field] = "OTHER_ENV" if field.endswith("variable") else "changed"
    result = snapshot(entries)
    assert result["outcome"] == "blocked"
    assert result["readiness"][0]["status"] == "allowed"
    assert result["readiness"][1]["status"] == "superseded"


@pytest.mark.parametrize("field", ["operational_activation_ready", "current_transition_ready"])
def test_changed_current_review_boolean_is_not_ignored(field: str) -> None:
    entry = source_entry("initial")
    entry["review"][field] = False
    assert snapshot([entry])["readiness"][0]["status"] == "superseded"
    entry["review"][field] = 0
    with pytest.raises(ValidationError):
        snapshot([entry])


@pytest.mark.parametrize("field", ["delivery_fingerprint", "retry_planning_policy_fingerprint", "retry_execution_policy_fingerprint"])
def test_governed_policy_drift_cannot_pass_on_destination_identity_alone(field: str) -> None:
    prior = readiness_authority()
    p = copy.deepcopy(prior["plan"])
    p["delivery"][field] = "changed-policy"
    result = snapshot(authority=grant(plan=rehash(p)))
    assert result["outcome"] == "blocked"
    assert all(row["status"] == "superseded" for row in result["readiness"])


def test_age_is_recomputed_not_accepted_from_current_view() -> None:
    result = snapshot(observed_at=EVALUATED_AT + timedelta(seconds=301))
    assert all(row["status"] == "stale" for row in result["readiness"])
    assert result["outcome"] == "blocked"


@pytest.mark.parametrize("variant", ["duplicate", "unselected", "extra", "orphan", "wrong_record", "missing_digest", "nonobject"])
def test_inconsistent_source_inventory_is_rejected(variant: str) -> None:
    entry = source_entry("initial")
    entries: Any = [entry]
    if variant == "duplicate":
        entries.append(copy.deepcopy(entry))
    elif variant == "unselected":
        entry["execution_kind"] = "other"
    elif variant == "extra":
        entry["review"]["readiness_review_status"] = "allowed"
    elif variant == "orphan":
        entry["review"] = None
    elif variant == "wrong_record":
        entry["review"]["readiness_record_id"] = "different-record"
    elif variant == "missing_digest":
        entry["document_sha256"] = None
    else:
        entries = [None]
    with pytest.raises(ValidationError):
        snapshot(entries)


def test_missing_current_view_is_explicit_and_stopped_authority_never_passes() -> None:
    entries = [{"execution_kind": "initial", "record": None, "document_sha256": None, "review": None}]
    assert snapshot(entries)["missing_execution_kinds"] == ["initial", "retry"]
    prior = readiness_authority()
    stopped = stop(prior, requested_at=EVALUATED_AT, effective_at=EVALUATED_AT)
    result = snapshot(authority=stopped)
    assert "worker_authority_not_active" in result["blocking_reasons"]


def test_rehashed_summary_cannot_drop_missing_source_blockers() -> None:
    result = snapshot([])
    result["outcome"] = "ready_sources"
    result["blocking_reasons"] = []
    identity = {key: value for key, value in result.items() if key != "snapshot_id"}
    result["snapshot_id"] = f"{MODEL_VERSION}-{hashlib.sha256(source_bytes(identity)).hexdigest()}"
    with pytest.raises(ValidationError):
        validate_worker_readiness_snapshot(result, authority=readiness_authority(), sources=[])


def test_input_evidence_is_detached_and_not_modified() -> None:
    entries = [source_entry("initial"), source_entry("retry")]
    before = copy.deepcopy(entries)
    result = snapshot(entries)
    assert entries == before
    entries[0]["record"]["decision"]["destination"]["fingerprint"] = "mutated"
    assert result["readiness"][0]["destination_fingerprint"] != "mutated"
