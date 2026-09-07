from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration import validate_notification_evidence as command
from src.warehouse.controlled_receiver_rehearsal_contract import canonical_controlled_receiver_rehearsal_bytes
from src.warehouse.notification_destination_transition_rehearsal_contract import (
    build_notification_destination_transition_rehearsal_record,
    canonical_transition_rehearsal_record_bytes,
)
from src.warehouse.notification_execution_readiness_history_contract import (
    build_notification_execution_readiness_record,
    canonical_notification_execution_readiness_record_bytes,
)
from test_controlled_receiver_rehearsal_contract import build as receiver_record
from test_notification_destination_transition_rehearsal_contract import STARTED_AT, _rehearsal
from test_notification_execution_readiness_enforcement import BASE_TIME, _decision, _delivery_config, _record

CANONICALIZERS = {
    "readiness": canonical_notification_execution_readiness_record_bytes,
    "receiver": canonical_controlled_receiver_rehearsal_bytes,
    "transition": canonical_transition_rehearsal_record_bytes,
}


def _fixture(kind: str, root: Path) -> dict[str, Any]:
    if kind == "readiness":
        return _record()
    if kind == "receiver":
        return receiver_record()
    return build_notification_destination_transition_rehearsal_record(
        request_id="OFFLINE-TRANSITION-001",
        recorded_at=STARTED_AT + timedelta(seconds=4),
        rehearsal=_rehearsal(root / "configuration"),
    )


@pytest.fixture
def no_external_access(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    import socket

    import psycopg

    calls: list[str] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append("external access")
        pytest.fail("offline evidence validation attempted external access")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(psycopg, "connect", forbidden)
    return calls


@pytest.mark.parametrize("kind", command.RECORD_KINDS)
def test_real_contract_round_trip_and_expected_digest(
    tmp_path: Path, kind: str, no_external_access: list[str], capsys: pytest.CaptureFixture[str],
) -> None:
    record = _fixture(kind, tmp_path)
    path = tmp_path / "record.json"
    raw = json.dumps(record, indent=2).encode() + b"\n"
    path.write_bytes(raw)
    canonical = CANONICALIZERS[kind](record)
    digest = hashlib.sha256(canonical).hexdigest()
    assert command.main([
        "--kind", kind, "--input", str(path), "--expected-sha256", digest,
    ]) == 0
    output = capsys.readouterr()
    report = json.loads(output.out)
    assert report["record_id"] == record["record_id"]
    assert report["document_sha256"] == digest
    assert report["canonical_bytes"] == len(canonical)
    assert report["expected_digest_checked"] is True
    assert report["status"] == "valid"
    assert report["runtime_permission_granted"] is False
    assert report["source_authenticated"] is False
    assert report["current_state_verified"] is False
    assert "decision" not in report and "receipts" not in report
    assert path.read_bytes() == raw
    assert no_external_access == []
    assert output.err == ""


@pytest.mark.parametrize("kind", command.RECORD_KINDS)
def test_semantically_tampered_record_is_rejected_without_external_access(
    tmp_path: Path, kind: str, no_external_access: list[str], capsys: pytest.CaptureFixture[str],
) -> None:
    record = _fixture(kind, tmp_path)
    record["record_id"] = "private-altered-id"
    path = tmp_path / "private-record.json"
    path.write_text(json.dumps(record))
    assert command.main(["--kind", kind, "--input", str(path)]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "rejected"
    assert "private" not in output.out + output.err
    assert no_external_access == []


@pytest.mark.parametrize("kind", command.RECORD_KINDS)
def test_wrong_record_family_is_not_accepted(tmp_path: Path, kind: str, no_external_access: list[str]) -> None:
    record = _fixture(kind, tmp_path)
    path = tmp_path / "record.json"
    path.write_text(json.dumps(record))
    other = command.RECORD_KINDS[(command.RECORD_KINDS.index(kind) + 1) % 3]
    with pytest.raises(ValidationError, match="semantic validation"):
        command.validate_notification_evidence_file(record_kind=other, path=path)
    assert no_external_access == []


@pytest.mark.parametrize("kind", ["readiness", "receiver"])
def test_valid_retained_negative_outcome_does_not_grant_permission(
    tmp_path: Path, kind: str, no_external_access: list[str],
) -> None:
    if kind == "readiness":
        decision = _decision(evaluated_at=BASE_TIME, delivery_config=_delivery_config(enabled=False))
        assert decision["decision"] == "block"
        record = build_notification_execution_readiness_record(
            request_id="OFFLINE-BLOCK-001", recorded_at=BASE_TIME + timedelta(seconds=1), decision=decision,
        )
    else:
        record = receiver_record(
            terminal_status="rejected_before_request", failure_code="validation_error",
            attempted_request_count=0, receiver_summary=None,
        )
    path = tmp_path / "record.json"
    path.write_text(json.dumps(record))
    report = command.validate_notification_evidence_file(record_kind=kind, path=path)
    assert report["status"] == "valid"
    assert report["validation_scope"] == "retained_record_only"
    assert report["current_state_verified"] is False
    assert report["runtime_permission_granted"] is False
    assert no_external_access == []


# A clean interpreter proves the CLI does not rely on already-imported recorder
# or database modules. Raised exceptions also fail the required zero exit code.
ISOLATED_ENTRYPOINT = '''
import builtins
import runpy
import socket
import ssl
import sys
original_import = builtins.__import__
def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    names = [name] + [name + "." + item for item in (fromlist or ())]
    if any(item == "psycopg" or item.startswith("psycopg.") or
           (item.startswith("src.") and "recorder" in item) for item in names):
        raise AssertionError("offline command imported a forbidden module")
    return original_import(name, globals, locals, fromlist, level)
def forbidden(*args, **kwargs):
    raise AssertionError("offline command attempted network access")
builtins.__import__ = guarded_import
socket.socket = forbidden
socket.getaddrinfo = forbidden
socket.create_connection = forbidden
sys.argv = ["validate_notification_evidence", *sys.argv[1:]]
runpy.run_module("src.orchestration.validate_notification_evidence", run_name="__main__")
'''


@pytest.mark.parametrize("kind", ["help", *command.RECORD_KINDS])
def test_clean_cli_never_imports_recorders_or_database_driver(tmp_path: Path, kind: str) -> None:
    if kind == "help":
        args = ["--help"]
    else:
        record = _fixture(kind, tmp_path)
        path = tmp_path / "record.json"
        path.write_text(json.dumps(record))
        args = ["--kind", kind, "--input", str(path)]
    result = subprocess.run(
        [sys.executable, "-c", ISOLATED_ENTRYPOINT, *args],
        check=False, capture_output=True, text=True, timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    if kind != "help":
        assert json.loads(result.stdout)["status"] == "valid"
