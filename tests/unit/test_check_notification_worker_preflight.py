from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration import check_notification_worker_preflight as command
from src.warehouse.notification_worker_authority_snapshot import read_worker_authority_snapshot_with_cursor
from test_notification_worker_authority_snapshot import SnapshotCursor, snapshot_row
from test_reviewed_notification_worker_authority import WORKER_ID, configurations as configurations
from test_reviewed_notification_worker_preflight import reviewed_snapshot


def arguments(paths: dict[str, Path], captured: dict[str, Any], source: list[str]) -> list[str]:
    current = captured["transition"]
    return [*source, "--worker-id", WORKER_ID,
            "--selected-transition-id", current["transition_id"],
            "--scheduled-for", current["plan"]["schedule"]["scheduled_for"],
            "--worker-config", str(paths["worker_config_path"]),
            "--delivery-config", str(paths["delivery_config_path"]),
            "--destination-config", str(paths["destination_config_path"])]


def write_snapshot(tmp_path: Path, captured: dict[str, Any]) -> Path:
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(captured), encoding="utf-8")
    return path


def forbidden_reader(**kwargs: Any) -> dict[str, Any]:
    raise AssertionError("database reader must not be called")


def test_offline_default_never_reads_database_even_with_dsn(
    configurations: dict[str, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any,
) -> None:
    captured = reviewed_snapshot(configurations)
    path = write_snapshot(tmp_path, captured)
    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "do-not-use")
    monkeypatch.setattr(command, "read_worker_authority_snapshot", forbidden_reader)
    before = {p: p.read_bytes() for p in configurations.values()}
    assert command.main(arguments(configurations, captured, ["--snapshot", str(path)])) == 0
    output = capsys.readouterr()
    report = json.loads(output.out)
    assert output.err == ""
    assert report["source_mode"] == "retained_file"
    assert report["database_read_performed"] is False
    assert report["runtime_permission_granted"] is False
    assert report["result"]["evaluation_scope"] == "captured_database_instant"
    assert before == {p: p.read_bytes() for p in configurations.values()}
    assert "do-not-use" not in output.out


def test_explicit_read_delegates_only_dsn_and_worker(
    configurations: dict[str, Path], monkeypatch: pytest.MonkeyPatch, capsys: Any,
) -> None:
    captured = reviewed_snapshot(configurations)
    calls: list[dict[str, Any]] = []

    def reader(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return captured

    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "injected-dsn")
    monkeypatch.setattr(command, "read_worker_authority_snapshot", reader)
    assert command.main(arguments(configurations, captured, ["--read-current"])) == 0
    output = capsys.readouterr()
    assert calls == [{"dsn": "injected-dsn", "worker_id": WORKER_ID}]
    report = json.loads(output.out)
    assert report["source_mode"] == "live_database_read"
    assert report["database_read_performed"] is True
    assert report["runtime_permission_granted"] is False
    assert "injected-dsn" not in output.out


def test_live_mode_requires_dsn_before_reader(
    configurations: dict[str, Path], monkeypatch: pytest.MonkeyPatch, capsys: Any,
) -> None:
    captured = reviewed_snapshot(configurations)
    monkeypatch.delenv("WAREHOUSE_POSTGRES_DSN", raising=False)
    monkeypatch.setattr(command, "read_worker_authority_snapshot", forbidden_reader)
    assert command.main(arguments(configurations, captured, ["--read-current"])) == 1
    assert capsys.readouterr().out == ""


def test_provider_failure_is_sanitized(
    configurations: dict[str, Path], monkeypatch: pytest.MonkeyPatch, capsys: Any,
) -> None:
    captured = reviewed_snapshot(configurations)

    def failed(**kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("credential-bearing-provider-diagnostic")

    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "private-dsn")
    monkeypatch.setattr(command, "read_worker_authority_snapshot", failed)
    assert command.main(arguments(configurations, captured, ["--read-current"])) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "Worker preflight failed; no execution permission granted\n"


@pytest.mark.parametrize(("offset", "expected"), [(-1, 3), (0, 0), (60, 4)])
def test_wait_due_and_expiry_exit_codes(
    configurations: dict[str, Path], tmp_path: Path, capsys: Any, offset: int, expected: int,
) -> None:
    baseline = reviewed_snapshot(configurations)
    slot = datetime.fromisoformat(baseline["transition"]["plan"]["schedule"]["scheduled_for"])
    captured = reviewed_snapshot(configurations, observed=slot + timedelta(seconds=offset))
    path = write_snapshot(tmp_path, captured)
    assert command.main(arguments(configurations, captured, ["--snapshot", str(path)])) == expected
    assert json.loads(capsys.readouterr().out)["runtime_permission_granted"] is False


def test_unknown_current_authority_is_blocked(
    configurations: dict[str, Path], monkeypatch: pytest.MonkeyPatch, capsys: Any,
) -> None:
    selected = reviewed_snapshot(configurations)
    missing = read_worker_authority_snapshot_with_cursor(
        SnapshotCursor(snapshot_row(None)), worker_id=WORKER_ID,
    )
    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "injected")
    monkeypatch.setattr(command, "read_worker_authority_snapshot", lambda **kwargs: missing)
    assert command.main(arguments(configurations, selected, ["--read-current"])) == 4
    result = json.loads(capsys.readouterr().out)["result"]
    assert "authority_missing" in result["preflight"]["reasons"]
    assert result["configuration_validated"] is False


@pytest.mark.parametrize("flag", ["--execute", "--record", "--dsn", "--read"])
def test_execution_recording_dsn_and_abbreviated_flags_are_rejected(
    configurations: dict[str, Path], flag: str,
) -> None:
    captured = reviewed_snapshot(configurations)
    with pytest.raises(SystemExit) as caught:
        command.main(arguments(configurations, captured, ["--read-current"]) + [flag])
    assert caught.value.code == 2


def test_invalid_request_is_rejected_before_database(
    configurations: dict[str, Path], monkeypatch: pytest.MonkeyPatch, capsys: Any,
) -> None:
    captured = reviewed_snapshot(configurations)
    args = arguments(configurations, captured, ["--read-current"])
    args[args.index("--worker-id") + 1] = "invalid worker"
    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "injected")
    monkeypatch.setattr(command, "read_worker_authority_snapshot", forbidden_reader)
    assert command.main(args) == 1
    assert capsys.readouterr().out == ""


def test_snapshot_worker_must_match_explicit_selection(
    configurations: dict[str, Path], tmp_path: Path, capsys: Any,
) -> None:
    captured = reviewed_snapshot(configurations)
    path = write_snapshot(tmp_path, captured)
    args = arguments(configurations, captured, ["--snapshot", str(path)])
    args[args.index("--worker-id") + 1] = "another-worker"
    assert command.main(args) == 1
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("raw", [b"{}", b"[]", b"null", b"\xff", b'{"worker_id":"a","worker_id":"b"}', b"[NaN]", b"[" * 1500])
def test_invalid_or_duplicate_json_is_rejected(tmp_path: Path, raw: bytes) -> None:
    path = tmp_path / "invalid.json"
    path.write_bytes(raw)
    with pytest.raises(ValidationError):
        command.load_authority_snapshot(path)


def test_oversize_input_symlink_directory_and_fifo_are_rejected(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b" " * (command.MAX_SNAPSHOT_BYTES + 1))
    link = tmp_path / "link.json"
    link.symlink_to(oversized)
    paths = [oversized, link, tmp_path]
    if hasattr(os, "mkfifo"):
        fifo = tmp_path / "fifo"
        os.mkfifo(fifo)
        paths.append(fifo)
    for path in paths:
        with pytest.raises(ValidationError):
            command.load_authority_snapshot(path)


def test_read_modes_are_mutually_exclusive_and_required() -> None:
    for args in ([], ["--read-current", "--snapshot", "unused"]):
        with pytest.raises(SystemExit) as caught:
            command.main(args)
        assert caught.value.code == 2
