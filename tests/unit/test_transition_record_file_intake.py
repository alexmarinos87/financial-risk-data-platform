from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from src.common import bounded_json
from src.common.exceptions import ValidationError
from src.warehouse import notification_destination_transition_rehearsal_recorder as recorder
from src.warehouse.notification_destination_transition_rehearsal_contract import (
    build_notification_destination_transition_rehearsal_record,
    validate_notification_destination_transition_rehearsal_record,
)
from test_notification_destination_transition_rehearsal_contract import STARTED_AT, _rehearsal

LIMIT = 1_048_576


def _build(tmp_path: Path) -> dict[str, Any]:
    return build_notification_destination_transition_rehearsal_record(
        request_id="TRANSITION-INTAKE-001",
        recorded_at=STARTED_AT + timedelta(seconds=4),
        rehearsal=_rehearsal(tmp_path / "configuration"),
    )


def _reject_before_recording(
    path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[dict[str, Any]] = []

    def forbidden(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        pytest.fail("intake failure reached persistence")

    monkeypatch.setattr(recorder, "record_notification_destination_transition_rehearsal", forbidden)
    assert recorder.main(["--record", str(path), "--dsn", "private-unused-dsn"]) == 1
    captured = capsys.readouterr()
    assert calls == []
    assert captured.out == ""
    assert captured.err.startswith("Destination transition rehearsal rejected:")
    assert "private" not in captured.err
    assert str(path) not in captured.err


def test_canonical_transition_record_round_trips(tmp_path: Path) -> None:
    record = _build(tmp_path)
    path = tmp_path / "record.json"
    raw = json.dumps(record, indent=2).encode("utf-8") + b"\n"
    path.write_bytes(raw)
    loaded = recorder._read_record(path)
    assert loaded == record
    assert validate_notification_destination_transition_rehearsal_record(loaded) == record
    assert path.read_bytes() == raw


@pytest.mark.parametrize("target", ["root", "rehearsal", "escaped_root"])
def test_duplicate_cannot_hide_behind_canonical_last_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], target: str,
) -> None:
    record = _build(tmp_path)
    text = json.dumps(record)
    if target == "rehearsal":
        marker = '"rehearsal_summary": {'
        assert marker in text
        text = text.replace(marker, marker + '"rehearsal_id":"private-shadow",', 1)
    else:
        key = 'record_id' if target == "root" else r'\u0072ecord_id'
        text = '{"' + key + '":"private-shadow",' + text[1:]
    # The legacy decoder loses the conflicting first field, even though the
    # remaining last-value document satisfies the real semantic validator.
    assert json.loads(text) == record
    assert validate_notification_destination_transition_rehearsal_record(json.loads(text)) == record
    path = tmp_path / "private-duplicate.json"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ValidationError, match="duplicate fields"):
        recorder._read_record(path)
    _reject_before_recording(path, monkeypatch, capsys)


@pytest.mark.parametrize("extra", [0, 1])
def test_exact_byte_ceiling(tmp_path: Path, extra: int) -> None:
    record = _build(tmp_path)
    raw = json.dumps(record).encode("utf-8")
    assert len(raw) < LIMIT
    raw += b" " * (LIMIT + extra - len(raw))
    path = tmp_path / "record.json"
    path.write_bytes(raw)
    if extra:
        with pytest.raises(ValidationError, match="byte limit"):
            recorder._read_record(path)
    else:
        assert recorder._read_record(path) == record


@pytest.mark.parametrize("raw", [
    b'{"v":NaN}', b'{"v":Infinity}', b'{"v":-Infinity}', b'{"v":1e9999}',
    b'{"private":"\xff"}', b'[]', b'null', b'{"private":',
    b'{"v":' * 65 + b'0' + b'}' * 65,
])
def test_malformed_intake_never_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], raw: bytes,
) -> None:
    path = tmp_path / "private-input.json"
    path.write_bytes(raw)
    _reject_before_recording(path, monkeypatch, capsys)


@pytest.mark.parametrize("kind", ["directory", "missing", "symlink", "dangling", "fifo"])
def test_unsafe_file_never_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], kind: str,
) -> None:
    path = tmp_path / "private-input"
    if kind == "directory":
        path.mkdir()
    elif kind in {"symlink", "dangling"}:
        target = tmp_path / "target"
        if kind == "symlink":
            target.write_text("{}", encoding="utf-8")
        path.symlink_to(target)
    elif kind == "fifo":
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO support is unavailable")
        os.mkfifo(path)
    _reject_before_recording(path, monkeypatch, capsys)


def test_file_growth_is_bounded_before_recording(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "private-growing.json"
    path.write_text("{}", encoding="utf-8")
    real_open, real_read = os.open, os.read
    consumed: list[int] = []

    def growing(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        path.write_bytes(b" " * (LIMIT + 100))
        return real_open(file, flags, *args, **kwargs)

    def measured(fd: int, count: int) -> bytes:
        chunk = real_read(fd, count)
        consumed.append(len(chunk))
        return chunk

    monkeypatch.setattr(bounded_json.os, "open", growing)
    monkeypatch.setattr(bounded_json.os, "read", measured)
    _reject_before_recording(path, monkeypatch, capsys)
    assert sum(consumed) == LIMIT + 1


def test_exact_loader_delegation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = {"only_intake": True}
    calls: list[tuple[Path, int]] = []

    def load(path: Path, *, max_bytes: int) -> dict[str, Any]:
        calls.append((path, max_bytes))
        return result

    monkeypatch.setattr(bounded_json, "load_bounded_json_object", load)
    path = tmp_path / "not-read"
    assert recorder._read_record(path) is result
    assert calls == [(path, LIMIT)]


def test_valid_cli_delegates_without_altering_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    record = _build(tmp_path)
    path = tmp_path / "record.json"
    path.write_text(json.dumps(record), encoding="utf-8")
    calls: list[dict[str, Any]] = []

    def capture(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"record_id": kwargs["record"]["record_id"], "created": False}

    monkeypatch.setattr(recorder, "record_notification_destination_transition_rehearsal", capture)
    assert recorder.main(["--record", str(path), "--dsn", "injected-dsn"]) == 0
    assert calls == [{"dsn": "injected-dsn", "record": record}]
    output = capsys.readouterr()
    assert json.loads(output.out) == {"record_id": record["record_id"], "created": False}
    assert output.err == ""


def test_semantic_validation_still_precedes_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    import psycopg

    record = _build(tmp_path)
    record["record_id"] = "tampered-identity"
    path = tmp_path / "record.json"
    path.write_text(json.dumps(record), encoding="utf-8")
    calls: list[tuple[Any, ...]] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append(args)
        pytest.fail("semantic rejection reached database connection")

    monkeypatch.setattr(psycopg, "connect", forbidden)
    assert recorder.main(["--record", str(path), "--dsn", "unused-dsn"]) == 1
    assert calls == []
    assert capsys.readouterr().out == ""


def test_io_error_is_redacted_before_recording(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "private-input.json"
    path.write_text("{}", encoding="utf-8")

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise OSError("private filesystem diagnostic")

    monkeypatch.setattr(bounded_json.os, "open", fail)
    _reject_before_recording(path, monkeypatch, capsys)


def test_cli_still_has_no_execute_flag() -> None:
    with pytest.raises(SystemExit) as error:
        recorder._build_parser().parse_args(["--record", "unused.json", "--execute"])
    assert error.value.code == 2


def test_transition_fixture_and_loader_are_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket

    import psycopg

    calls: list[str] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append("external")
        pytest.fail("offline fixture attempted network or database access")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(psycopg, "connect", forbidden)
    record = _build(tmp_path)
    path = tmp_path / "record.json"
    path.write_text(json.dumps(record), encoding="utf-8")
    assert recorder._read_record(path) == record
    assert calls == []
