from __future__ import annotations

from copy import deepcopy
from typing import Any
import json

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.warehouse import inspect_worker_readiness as command

WORKER = "inspection-worker"
OBSERVED = "2026-06-01T12:00:00+00:00"


def reader_result(status: str = "ready_sources") -> dict[str, Any]:
    # Boundary fixture only. Real reader integration has its own wiring test.
    snapshot = {
        "model_version": "portfolio-risk-worker-readiness-snapshot-v1",
        "worker_id": WORKER, "authority_transition_id": "authority-1",
        "observed_at": OBSERVED, "outcome": status,
        "snapshot_id": "snapshot-1", "current_authority_verified": False,
        "failure_history_verified": False, "runtime_permission_granted": False,
        "readiness": [{"execution_kind": "initial", "status": "allowed",
                       "record_id": "record-1", "document_sha256": "a" * 64}],
        "missing_execution_kinds": [], "blocking_reasons": [],
    }
    result = {
        "model_version": "portfolio-risk-worker-readiness-read-v1",
        "worker_id": WORKER, "observed_at": OBSERVED,
        "database_read_performed": True, "single_statement_read_only": True,
        **dict.fromkeys(command.NO_AUTHORITY_FLAGS, False),
        "status": status, "authority_sequence": 1,
        "authority_transition_id": "authority-1", "snapshot": snapshot,
    }
    if status == "blocked":
        snapshot["missing_execution_kinds"] = ["retry"]
        snapshot["blocking_reasons"] = ["readiness_missing:retry"]
    elif status == "authority_missing":
        result.update(authority_sequence=None, authority_transition_id=None, snapshot=None)
    return result


def test_default_does_not_read_environment_or_call_reader(monkeypatch: Any, capsys: Any) -> None:
    class NoEnvironment(dict):
        def get(self, key: str, default: Any = None) -> Any:
            if key == "WAREHOUSE_POSTGRES_DSN":
                raise AssertionError("credential lookup is forbidden")
            return super().get(key, default)
    def forbidden(**kwargs: Any) -> Any:
        raise AssertionError("database access is forbidden")
    monkeypatch.setattr(command, "_read", forbidden)
    monkeypatch.setattr(command.os, "environ", NoEnvironment())
    assert command.main(["--worker-id", WORKER]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "not_requested"
    assert result["database_read_attempted"] is False
    assert result["database_read_completed"] is False


@pytest.mark.parametrize("status,code", [("ready_sources", 0), ("blocked", 2), ("authority_missing", 2)])
def test_explicit_read_uses_environment_and_projects_bounded_evidence(status: str, code: int, monkeypatch: Any, capsys: Any) -> None:
    calls = []
    value = reader_result(status)
    value["private_provider_detail"] = "must-not-be-printed"
    def read(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return value
    monkeypatch.setattr(command, "_read", read)
    monkeypatch.setenv("INSPECTION_TEST_DSN", "synthetic")
    assert command.main(["--worker-id", WORKER, "--read-database", "--dsn-env", "INSPECTION_TEST_DSN"]) == code
    output = capsys.readouterr().out
    result = json.loads(output)
    assert calls == [{"dsn": "synthetic", "worker_id": WORKER}]
    assert result["status"] == status
    assert result["database_read_attempted"] is True
    assert result["database_read_completed"] is True
    assert all(result[flag] is False for flag in command.NO_AUTHORITY_FLAGS)
    assert "snapshot" not in result and "synthetic" not in output
    assert "must-not-be-printed" not in output


@pytest.mark.parametrize("failure", [StorageError, ValidationError, RuntimeError, OSError])
def test_provider_or_validation_failure_never_leaks_diagnostics(failure: Any, monkeypatch: Any, capsys: Any) -> None:
    def fail(**kwargs: Any) -> Any:
        raise failure("private-provider-detail")
    monkeypatch.setattr(command, "_read", fail)
    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "synthetic")
    assert command.main(["--worker-id", WORKER, "--read-database"]) == 1
    output = capsys.readouterr()
    assert output.out == "" and "private-provider-detail" not in output.err
    result = json.loads(output.err)
    assert result["status"] == "failed" and result["database_read_attempted"] is True
    assert result["database_read_completed"] is False


@pytest.mark.parametrize("args", [
    ["--worker-id", "not valid"], ["--worker-id", WORKER, "--dsn-env", "not-valid"],
    ["--worker-id", WORKER, "--read-database"],
])
def test_invalid_selection_or_missing_configuration_stops_before_read(args: list[str], monkeypatch: Any, capsys: Any) -> None:
    def fail(**kwargs: Any) -> Any:
        raise AssertionError("reader must not run")
    monkeypatch.setattr(command, "_read", fail)
    monkeypatch.delenv("WAREHOUSE_POSTGRES_DSN", raising=False)
    assert command.main(args) == 1
    result = json.loads(capsys.readouterr().err)
    assert result["database_read_attempted"] is False


@pytest.mark.parametrize("change", [
    "runtime", "database", "worker", "sequence", "missing-authority", "source-status",
    "duplicate-kind", "missing-kind", "reason", "time", "snapshot-outcome", "digest",
])
def test_contradictory_reader_results_cannot_report_success(change: str) -> None:
    value = reader_result()
    if change == "runtime":
        value["runtime_permission_granted"] = True
    elif change == "database":
        value["database_read_performed"] = 1
    elif change == "worker":
        value["worker_id"] = "other-worker"
    elif change == "sequence":
        value["authority_sequence"] = True
    elif change == "missing-authority":
        value["status"] = "authority_missing"
    elif change == "source-status":
        value["snapshot"]["readiness"][0]["status"] = "stale"
    elif change == "duplicate-kind":
        value["snapshot"]["readiness"] *= 2
    elif change == "missing-kind":
        value["snapshot"]["missing_execution_kinds"] = ["retry"]
    elif change == "reason":
        value["snapshot"]["blocking_reasons"] = ["private-provider-detail"]
    elif change == "time":
        value["snapshot"]["observed_at"] = "2026-06-01T12:00:01+00:00"
    elif change == "snapshot-outcome":
        value["snapshot"]["outcome"] = "blocked"
    else:
        value["snapshot"]["readiness"][0]["document_sha256"] = "invalid"
    with pytest.raises(ValidationError):
        command._summary(value, worker_id=WORKER)


def test_summary_does_not_mutate_reader_evidence() -> None:
    value = reader_result("blocked")
    before = deepcopy(value)
    report = command._summary(value, worker_id=WORKER)
    assert value == before
    value["snapshot"]["readiness"][0]["record_id"] = "mutated"
    assert report["readiness"][0]["record_id"] == "record-1"


def test_help_does_not_resolve_database_dependency(monkeypatch: Any, capsys: Any) -> None:
    def fail(**kwargs: Any) -> Any:
        raise AssertionError("reader must not run")
    monkeypatch.setattr(command, "_read", fail)
    with pytest.raises(SystemExit) as caught:
        command.main(["--help"])
    assert caught.value.code == 0
    assert "--read-database" in capsys.readouterr().out


def test_unrecognised_arguments_are_not_echoed(capsys: Any) -> None:
    with pytest.raises(SystemExit) as caught:
        command.main(["--worker-id", WORKER, "--dsn", "private-connection-detail"])
    assert caught.value.code == 2
    assert "private-connection-detail" not in capsys.readouterr().err
