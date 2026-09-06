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
    module = "src.warehouse.notification_execution_readiness_postgres_contract_check"
    path = Path("src/warehouse/notification_execution_readiness_postgres_contract_check.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    run = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "run_contract_check")
    calls = {node.func.id for node in ast.walk(run) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
    assert {"prove_worker_readiness_sources", "prove_worker_readiness_supersession"} <= calls
    assert "worker_readiness_source_proofs" in path.read_text(encoding="utf-8")

    # The Make target invokes the receiver fixture, which runs readiness once.
    # Requiring a direct readiness invocation would duplicate its fixed IDs.
    caller = Path("src/warehouse/controlled_receiver_rehearsal_postgres_contract_check.py")
    caller_tree = ast.parse(caller.read_text(encoding="utf-8"))
    aliases = [alias.asname or alias.name for node in caller_tree.body
               if isinstance(node, ast.ImportFrom) and node.module == module
               for alias in node.names if alias.name == "run_contract_check"]
    assert len(aliases) == 1
    caller_run = next(node for node in caller_tree.body
                      if isinstance(node, ast.FunctionDef) and node.name == "run_contract_check")
    invocations = [node for node in ast.walk(caller_run)
                   if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                   and node.func.id == aliases[0]]
    assert len(invocations) == 1
    assert len(invocations[0].args) == 1
    assert isinstance(invocations[0].args[0], ast.Name) and invocations[0].args[0].id == "dsn"
    assert "notification_execution_readiness" in caller.read_text(encoding="utf-8")
    target = Path("Makefile").read_text().split("postgres-contract-check:", 1)[1].split("\nlocal-db-up:", 1)[0]
    assert target.count("-m src.warehouse.controlled_receiver_rehearsal_postgres_contract_check") == 1
    assert f"-m {module}" not in target
