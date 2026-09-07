from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration import validate_notification_evidence as command

FLAGS = ("database_access_performed", "external_request_performed", "source_authenticated",
         "current_state_verified", "runtime_permission_granted")


def _encode(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


@pytest.fixture
def isolated_contract(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    calls: list[Any] = []

    def select(kind: str):
        calls.append(kind)

        def validate(value: Any) -> dict[str, Any]:
            calls.append(value)
            return dict(value)

        return validate, _encode

    monkeypatch.setattr(command, "_contract", select)
    return calls


@pytest.mark.parametrize("kind", command.RECORD_KINDS)
def test_projection_contains_no_source_body(
    tmp_path: Path, isolated_contract: list[Any], kind: str,
) -> None:
    value = {"record_id": "record-1", "private_source_field": "not-for-output"}
    path = tmp_path / "private-file.json"
    path.write_text(json.dumps(value))
    result = command.validate_notification_evidence_file(record_kind=kind, path=path)
    assert result["status"] == "valid"
    assert result["document_sha256"] == hashlib.sha256(_encode(value)).hexdigest()
    assert result["canonical_bytes"] == len(_encode(value))
    assert result["expected_digest_checked"] is False
    assert all(result[key] is False for key in FLAGS)
    assert set(result) == {
        "model_version", "status", "validation_scope", *FLAGS,
        "record_kind", "record_id", "document_sha256", "canonical_bytes",
        "expected_digest_checked",
    }
    assert "private" not in json.dumps(result)
    assert "not-for-output" not in json.dumps(result)
    assert isolated_contract == [kind, value]


def test_canonical_digest_not_raw_file_digest(tmp_path: Path, isolated_contract: list[Any]) -> None:
    value = {"record_id": "record-1", "a": 1}
    path = tmp_path / "record.json"
    raw = json.dumps(value, indent=4).encode() + b"\n"
    path.write_bytes(raw)
    expected = hashlib.sha256(_encode(value)).hexdigest()
    assert expected != hashlib.sha256(raw).hexdigest()
    result = command.validate_notification_evidence_file(
        record_kind="readiness", path=path, expected_sha256=expected,
    )
    assert result["expected_digest_checked"] is True
    assert path.read_bytes() == raw
    with pytest.raises(ValidationError, match="does not match"):
        command.validate_notification_evidence_file(
            record_kind="readiness", path=path, expected_sha256="0" * 64,
        )


@pytest.mark.parametrize("digest", ["", "f" * 63, "f" * 65, "F" * 64, True, 1, []])
def test_bad_digest_fails_before_file_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, digest: Any,
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("invalid digest caused file access")

    monkeypatch.setattr(command, "load_bounded_json_object", forbidden)
    with pytest.raises(ValidationError, match="lowercase SHA-256"):
        command.validate_notification_evidence_file(
            record_kind="readiness", path=tmp_path / "absent", expected_sha256=digest,
        )


@pytest.mark.parametrize("kind", ["", "other", "private.module", None, []])
def test_unknown_kind_does_not_select_a_contract(tmp_path: Path, kind: Any) -> None:
    with pytest.raises(ValidationError, match="unsupported"):
        command.validate_notification_evidence_file(record_kind=kind, path=tmp_path / "absent")


@pytest.mark.parametrize("raw", [
    b'{"private":', b'{"x":1,"x":2}', b'{"x":NaN}', b'[]',
    b'{"x":"\\ud800"}', b'{"x":"\xff"}', b' ' * (command.MAX_JSON_BYTES + 1),
])
def test_bad_file_never_loads_domain_contracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], raw: bytes,
) -> None:
    path = tmp_path / "private-input.json"
    path.write_bytes(raw)

    def forbidden(*args: Any) -> Any:
        pytest.fail("malformed intake loaded semantic contracts")

    monkeypatch.setattr(command, "_contract", forbidden)
    assert command.main(["--kind", "readiness", "--input", str(path)]) == 1
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert result["status"] == "rejected"
    assert result["error_code"] == "invalid_evidence"
    assert all(result[key] is False for key in FLAGS)
    assert "private" not in output.out + output.err
    assert output.err == ""


@pytest.mark.parametrize("args", [
    [], ["--kind", "private-kind", "--input", "private-file"],
    ["--kind", "readiness", "--input", "file", "--dsn", "private-value"],
    ["--kind", "readiness", "--input", "file", "--execute"],
    ["--ki", "readiness", "--input", "private-file"],
    ["--kind", "readiness", "--input", "file", "--expected", "private-value"],
])
def test_argument_errors_do_not_echo_values(args: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as error:
        command.main(args)
    assert error.value.code == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "Invalid evidence validation arguments; use --help.\n"


def test_help_never_loads_domain_contracts(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(command, "_contract", lambda kind: pytest.fail("help loaded contracts"))
    with pytest.raises(SystemExit) as error:
        command.main(["--help"])
    assert error.value.code == 0
    assert "--kind" in capsys.readouterr().out


@pytest.mark.parametrize("exception", [ValidationError("private-field"), RuntimeError("private-provider")])
def test_contract_failures_are_sanitized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], exception: Exception,
) -> None:
    path = tmp_path / "record.json"
    path.write_text("{}")

    def validate(value: Any) -> Any:
        raise exception

    monkeypatch.setattr(command, "_contract", lambda kind: (validate, _encode))
    assert command.main(["--kind", "receiver", "--input", str(path)]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "rejected"
    assert "private" not in output.out + output.err


def test_unavailable_contract_is_reported_without_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "record.json"
    path.write_text("{}")

    def unavailable(kind: str) -> Any:
        raise ImportError("private-environment")

    monkeypatch.setattr(command, "_contract", unavailable)
    assert command.main(["--kind", "transition", "--input", str(path)]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["error_code"] == "validation_failed"
    assert "private" not in output.out + output.err


@pytest.mark.parametrize("identity", [None, "bad\nidentity", "x" * 513, "../private"])
def test_projection_rejects_unbounded_or_unsafe_identity(
    tmp_path: Path, isolated_contract: list[Any], identity: Any,
) -> None:
    path = tmp_path / "record.json"
    path.write_text(json.dumps({"record_id": identity}))
    with pytest.raises(ValidationError, match="identity"):
        command.validate_notification_evidence_file(record_kind="receiver", path=path)


def test_success_output_is_bounded_and_validation_only(
    tmp_path: Path, isolated_contract: list[Any], capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "record.json"
    path.write_text(json.dumps({"record_id": "x" * 512}))
    assert command.main(["--kind", "receiver", "--input", str(path)]) == 0
    output = capsys.readouterr()
    assert len(output.out.encode()) <= command.MAX_REPORT_BYTES
    assert json.loads(output.out)["validation_scope"] == "retained_record_only"
    assert output.err == ""


@pytest.mark.parametrize("encoded", [b"", "not-bytes", b"x" * (command.MAX_JSON_BYTES + 1)])
def test_canonical_size_and_type_are_checked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, encoded: Any,
) -> None:
    path = tmp_path / "record.json"
    path.write_text('{"record_id":"safe-id"}')
    monkeypatch.setattr(command, "_contract", lambda kind: (lambda value: value, lambda value: encoded))
    with pytest.raises(ValidationError, match="size contract"):
        command.validate_notification_evidence_file(record_kind="readiness", path=path)


def test_output_failure_does_not_report_success(
    tmp_path: Path, isolated_contract: list[Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "record.json"
    path.write_text('{"record_id":"safe-id"}')

    def fail(*args: Any, **kwargs: Any) -> None:
        raise BrokenPipeError("private-output-error")

    monkeypatch.setattr("builtins.print", fail)
    assert command.main(["--kind", "readiness", "--input", str(path)]) == 1
