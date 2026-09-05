from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any

import psycopg
import pytest

from src.common.exceptions import ValidationError
from src.orchestration import rehearse_worker_authority_preflight as rehearsal
from src.orchestration.notification_worker_authority_preflight import validate_worker_authority_preflight

PLANNED_AT = "2026-09-06T00:00:00+00:00"


def test_rehearsal_reconciles_ten_real_contract_scenarios_without_config_changes() -> None:
    before = {path: path.read_bytes() for path in rehearsal.CONFIG_PATHS}
    result = rehearsal.rehearse_worker_authority_preflight(planned_at=PLANNED_AT)
    assert result == rehearsal.rehearse_worker_authority_preflight(planned_at=PLANNED_AT)
    assert result["scenario_count"] == result["passed_count"] == 10
    assert result["failed_count"] == 0
    assert result["observations_synthetic"] is True
    outcomes = [case["preflight"]["outcome"] for case in result["scenarios"]]
    assert outcomes.count("eligible_for_health_review") == 1
    assert outcomes.count("wait") == 1
    assert outcomes.count("blocked") == 8
    for case in result["scenarios"]:
        assert case["passed"] is True
        assert validate_worker_authority_preflight(case["preflight"], evidence=case["evidence"]) == case["preflight"]
        assert case["preflight"]["readiness_evaluated"] is False
        assert case["preflight"]["runtime_permission_granted"] is False
    for field in ("configuration_files_modified", "database_read_performed", "readiness_evaluated",
                  "runtime_permission_granted", "scheduler_mutated", "external_request_performed",
                  "shared_lock_acquired"):
        assert result[field] is False
    assert {path: path.read_bytes() for path in rehearsal.CONFIG_PATHS} == before


def test_rehearsal_cannot_resolve_network_or_connect_database(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("rehearsal must not connect or resolve a network address")

    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(psycopg, "connect", forbidden)
    monkeypatch.setenv("RISK_NOTIFICATION_WEBHOOK_URL", "https://unused.example/synthetic-value")
    result = rehearsal.rehearse_worker_authority_preflight(planned_at=PLANNED_AT)
    assert "https://unused.example/synthetic-value" not in json.dumps(result)
    assert result["passed_count"] == 10


def test_equivalent_timezone_input_is_deterministic_but_new_time_changes_identity() -> None:
    first = rehearsal.rehearse_worker_authority_preflight(planned_at=PLANNED_AT)
    equivalent = rehearsal.rehearse_worker_authority_preflight(planned_at="2026-09-06T01:00:00+01:00")
    later = rehearsal.rehearse_worker_authority_preflight(planned_at="2026-09-07T00:00:00Z")
    assert first == equivalent
    assert first["rehearsal_id"] != later["rehearsal_id"]


def test_cli_writes_only_bounded_rehearsal_report(tmp_path: Path, capsys: Any) -> None:
    target = tmp_path / "rehearsal.json"
    assert rehearsal.main(["--planned-at", PLANNED_AT, "--summary-json", str(target)]) == 0
    result = json.loads(target.read_text(encoding="utf-8"))
    assert json.loads(capsys.readouterr().out) == result
    assert result["observations_synthetic"] is True
    assert target.stat().st_size <= rehearsal.MAX_SUMMARY_BYTES
    assert list(tmp_path.glob(".rehearsal.json.*.tmp")) == []


def test_cli_rejects_symlinked_summary_without_touching_victim(tmp_path: Path, capsys: Any) -> None:
    victim = tmp_path / "unrelated.txt"
    victim.write_text("preserve evidence", encoding="utf-8")
    target = tmp_path / "rehearsal.json"
    target.symlink_to(victim)
    assert rehearsal.main(["--planned-at", PLANNED_AT, "--summary-json", str(target)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "symbolic link" in captured.err
    assert victim.read_text(encoding="utf-8") == "preserve evidence"


def test_cli_rejects_output_into_configuration_tree(capsys: Any) -> None:
    target = rehearsal.CONFIG_PATHS[0]
    before = target.read_bytes()
    assert rehearsal.main(["--planned-at", PLANNED_AT, "--summary-json", str(target)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "must not replace repository configuration" in captured.err
    assert target.read_bytes() == before


@pytest.mark.parametrize("option", ["--execute", "--record"])
def test_cli_has_no_execution_or_recording_option(option: str) -> None:
    with pytest.raises(SystemExit) as exc:
        rehearsal.main(["--planned-at", PLANNED_AT, option])
    assert exc.value.code == 2


@pytest.mark.parametrize("instant", [
    "not-a-time", "2026-09-06T00:00:00", "2026-09-06T00:00:00.5Z", "9999-12-31T23:59:59Z",
])
def test_invalid_or_unrepresentable_time_produces_no_success(instant: str, capsys: Any) -> None:
    assert rehearsal.main(["--planned-at", instant]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err


def test_unknown_worker_is_rejected() -> None:
    with pytest.raises(ValidationError, match="does not exist"):
        rehearsal.rehearse_worker_authority_preflight(planned_at=PLANNED_AT, worker_id="missing-worker")


def test_inconsistent_evaluator_result_cannot_be_reported_as_success(monkeypatch: pytest.MonkeyPatch) -> None:
    original = rehearsal.evaluate_worker_authority_preflight

    def inconsistent(evidence: Any) -> dict[str, Any]:
        result = original(evidence)
        result["outcome"] = "blocked" if result["outcome"] != "blocked" else "eligible_for_health_review"
        return result

    monkeypatch.setattr(rehearsal, "evaluate_worker_authority_preflight", inconsistent)
    with pytest.raises(ValidationError):
        rehearsal.rehearse_worker_authority_preflight(planned_at=PLANNED_AT)
