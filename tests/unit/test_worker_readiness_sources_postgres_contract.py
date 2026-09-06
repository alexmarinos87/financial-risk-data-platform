from __future__ import annotations

import ast
import hashlib
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from src.orchestration.plan_notification_worker import validate_notification_worker_plan
from src.warehouse import notification_worker_readiness_sources_postgres_contract as proof
from src.warehouse.notification_worker_readiness_sources import build_worker_readiness_sources
from test_notification_execution_readiness_enforcement import BASE_TIME, _decision, _destination


def inputs() -> dict[str, Any]:
    seed = _decision(evaluated_at=BASE_TIME, execution_kind="retry")
    return proof._fixture_inputs(
        destination=_destination(), now=BASE_TIME,
        evidence={"activation_review": seed["activation_review"], "transition_review": seed["transition_review"]},
        prefix="unit-source-proof",
    )


def source_row(record: dict[str, Any]) -> dict[str, Any]:
    row = proof._record_row(record)
    return {
        "destination_id": row["destination_id"], "execution_kind": row["execution_kind"],
        "readiness_record_id": row["record_id"], "record_json": row["record_json"],
        "document_sha256": row["document_sha256"], "decision_matches_current_evidence": True,
        "execution_ready": row["decision"] == "allow",
        "readiness_review_status": "allowed" if row["decision"] == "allow" else "blocked",
    }


def test_live_fixture_inputs_use_real_matching_plans_and_records() -> None:
    fixture = inputs()
    for key in ("plan", "strict_plan", "initial_plan", "missing_plan", "blocked_plan"):
        assert validate_notification_worker_plan(fixture[key]) == fixture[key]
    rows = [source_row(record) for record in fixture["records"]]
    snapshot = build_worker_readiness_sources(plan=fixture["plan"], sources=rows, observed_at=BASE_TIME)
    assert snapshot["all_sources_allowed"] is True
    strict = build_worker_readiness_sources(plan=fixture["strict_plan"], sources=rows, observed_at=BASE_TIME)
    assert [row["status"] for row in strict["readiness"]] == ["stale", "stale"]
    rows[1] = source_row(fixture["blocked_record"])
    blocked = build_worker_readiness_sources(plan=fixture["blocked_plan"], sources=rows, observed_at=BASE_TIME)
    assert blocked["readiness"][1]["status"] == "blocked"
    assert blocked["all_sources_allowed"] is False


def test_insert_projection_reconciles_source_document_and_digest() -> None:
    record = inputs()["records"][0]
    row = proof._record_row(record)
    assert len(row) == 32
    assert row["record_json"] == record
    assert row["decision_json"] == record["decision"]
    assert row["document_sha256"] == hashlib.sha256(canonical_bytes(record)).hexdigest()
    assert row["evaluated_at"] == (BASE_TIME - timedelta(seconds=10)).isoformat()
    assert row["loaded_at"] == row["recorded_at"]


def test_invalid_source_cannot_use_normal_fixture_insert_projection() -> None:
    record = inputs()["records"][0]
    record["record_id"] = "invented-record"
    with pytest.raises(ValidationError):
        proof._record_row(record)


@pytest.mark.parametrize("behavior", ["success", "unredacted"])
def test_negative_probe_cannot_silently_pass(behavior: str, monkeypatch: pytest.MonkeyPatch) -> None:
    def read(*args: Any, **kwargs: Any) -> dict[str, Any]:
        if behavior == "unredacted":
            raise StorageError("private database diagnostic")
        return {"all_sources_allowed": True}

    monkeypatch.setattr(proof, "read_worker_readiness_sources_with_cursor", read)
    with pytest.raises(AssertionError):
        proof._expect_read_failure(None, inputs()["plan"])


def test_new_postgres_proofs_are_wired_into_existing_ci_contract() -> None:
    path = Path("src/warehouse/notification_execution_readiness_postgres_contract_check.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    calls = {node.func.id for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
    assert {"prove_worker_readiness_sources", "prove_worker_readiness_supersession"} <= calls
    assert "worker_readiness_source_proofs" in path.read_text(encoding="utf-8")
    assert "src.warehouse.notification_execution_readiness_postgres_contract_check" in Path("Makefile").read_text()
