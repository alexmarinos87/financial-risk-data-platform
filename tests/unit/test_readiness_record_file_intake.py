from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from src.common import bounded_json
from src.common.exceptions import ValidationError
from src.warehouse import notification_execution_readiness_recorder as recorder
from src.warehouse.notification_execution_readiness_history_contract import (
    validate_notification_execution_readiness_record,
)
from test_notification_execution_readiness_enforcement import _record


def test_real_canonical_record_round_trips_unchanged(tmp_path: Path) -> None:
    record = _record()
    raw = json.dumps(record, ensure_ascii=False).encode()
    path = tmp_path / "record.json"
    path.write_bytes(raw)
    loaded = recorder._read_record(path)
    assert loaded == record
    assert validate_notification_execution_readiness_record(loaded) == record
    assert path.read_bytes() == raw


@pytest.mark.parametrize("nested", [False, True])
def test_duplicate_field_cannot_hide_behind_valid_last_occurrence(tmp_path: Path, nested: bool) -> None:
    record = _record()
    text = json.dumps(record)
    if nested:
        text = text.replace('"decision": {', '"decision": {"decision":"discarded",', 1)
    else:
        text = '{"record_id":"discarded",' + text[1:]
    # Demonstrate that the old parser loses the conflicting earlier value.
    assert json.loads(text) == record
    path = tmp_path / "duplicate.json"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ValidationError, match="duplicate fields"):
        recorder._read_record(path)


@pytest.mark.parametrize("extra", [0, 1])
def test_record_file_keeps_exact_one_megabyte_limit(tmp_path: Path, extra: int) -> None:
    record = _record()
    raw = json.dumps(record).encode()
    raw += b" " * (recorder.MAX_RECORD_BYTES + extra - len(raw))
    path = tmp_path / "bounded.json"
    path.write_bytes(raw)
    if extra:
        with pytest.raises(ValidationError, match="byte limit"):
            recorder._read_record(path)
    else:
        assert recorder._read_record(path) == record


@pytest.mark.parametrize("raw", [
    b'{"value":NaN}', b'{"value":Infinity}', b'{"value":1e9999}',
    b'{"private":"\xff"}', b'{"key":1,"key":2}', b'[]', b'null',
    b'{"private":', b'{"v":' * 65 + b'0' + b'}' * 65,
])
def test_cli_rejects_bad_intake_before_recording(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], raw: bytes,
) -> None:
    path = tmp_path / "private-record-path.json"
    path.write_bytes(raw)
    calls: list[dict[str, Any]] = []

    def forbidden(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        raise AssertionError("invalid file must not reach recording")

    monkeypatch.setattr(recorder, "record_notification_execution_readiness", forbidden)
    assert recorder.main(["--record", str(path), "--dsn", "unused-test-dsn"]) == 1
    captured = capsys.readouterr()
    assert calls == []
    assert captured.out == ""
    assert captured.err.startswith("Notification readiness record rejected:")
    assert "private" not in captured.err
    assert "unused-test-dsn" not in captured.err


@pytest.mark.parametrize("dangling", [False, True])
def test_record_input_symlink_is_rejected(tmp_path: Path, dangling: bool) -> None:
    target = tmp_path / "record.json"
    if not dangling:
        target.write_text(json.dumps(_record()))
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(ValidationError, match="symbolic link"):
        recorder._read_record(link)
    assert link.is_symlink()


def test_record_input_directory_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="regular file"):
        recorder._read_record(tmp_path)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO support is unavailable")
def test_record_input_fifo_is_rejected_without_waiting(tmp_path: Path) -> None:
    path = tmp_path / "fifo"
    os.mkfifo(path)
    with pytest.raises(ValidationError, match="regular file"):
        recorder._read_record(path)


def test_loader_delegates_the_existing_byte_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    record = _record()
    calls: list[tuple[Path, int]] = []

    def load(path: Path, *, max_bytes: int) -> dict[str, Any]:
        calls.append((path, max_bytes))
        return record

    path = tmp_path / "not-read.json"
    monkeypatch.setattr(bounded_json, "load_bounded_json_object", load)
    assert recorder._read_record(path) == record
    assert calls == [(path, 1_048_576)]


def test_cli_valid_input_delegates_unchanged_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    record = _record()
    path = tmp_path / "record.json"
    path.write_text(json.dumps(record), encoding="utf-8")
    calls: list[dict[str, Any]] = []

    def record_once(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        assert validate_notification_execution_readiness_record(kwargs["record"]) == record
        return {"record_id": record["record_id"], "created": False}

    monkeypatch.setattr(recorder, "record_notification_execution_readiness", record_once)
    assert recorder.main(["--record", str(path), "--dsn", "injected-dsn"]) == 0
    assert calls == [{"dsn": "injected-dsn", "record": record}]
    assert json.loads(capsys.readouterr().out) == {"record_id": record["record_id"], "created": False}


def test_valid_json_does_not_replace_semantic_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    import psycopg

    record = _record()
    record["record_id"] = "not-the-canonical-identity"
    path = tmp_path / "record.json"
    path.write_text(json.dumps(record), encoding="utf-8")

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("invalid semantic evidence must not connect")

    monkeypatch.setattr(psycopg, "connect", forbidden)
    assert recorder.main(["--record", str(path), "--dsn", "unused-dsn"]) == 1
    assert capsys.readouterr().out == ""
