from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any

import pytest

from src.common import bounded_json as reader
from src.common.exceptions import ValidationError


def write(tmp_path: Path, content: bytes) -> Path:
    path = tmp_path / "private-input.json"
    path.write_bytes(content)
    return path


def test_valid_object_is_detached_and_input_is_unchanged(tmp_path: Path) -> None:
    expected = {"text": 'quotes " and \\ and [ { ] } Ω', "nested": [{"ok": True}], "number": 1.25}
    raw = json.dumps(expected, ensure_ascii=False).encode()
    path = write(tmp_path, raw)
    result = reader.load_bounded_json_object(path)
    assert result == expected
    result["nested"].clear()
    assert reader.load_bounded_json_object(path) == expected
    assert path.read_bytes() == raw


@pytest.mark.parametrize("extra", [0, 1])
def test_actual_byte_limit_accepts_exact_size_only(tmp_path: Path, extra: int) -> None:
    maximum = 40
    raw = b'{"value":"' + b"x" * (maximum - len(b'{"value":""}\n') + extra) + b'"}\n'
    assert len(raw) == maximum + extra
    path = write(tmp_path, raw)
    if extra:
        with pytest.raises(ValidationError, match="byte limit"):
            reader.load_bounded_json_object(path, max_bytes=maximum)
    else:
        assert reader.load_bounded_json_object(path, max_bytes=maximum)["value"]


@pytest.mark.parametrize("value", [True, False, 0, -1, 1.5, "10", None, reader.MAX_JSON_BYTES + 1])
def test_invalid_limits_fail_before_io(tmp_path: Path, value: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("invalid request must not open a file")

    monkeypatch.setattr(reader.os, "open", forbidden)
    with pytest.raises(ValidationError, match="byte limit"):
        reader.load_bounded_json_object(tmp_path / "missing", max_bytes=value)


@pytest.mark.parametrize("raw", [
    b'{"key":1,"key":2}', b'{"outer":{"key":1,"key":2}}',
    b'{"key":1,"\\u006bey":2}',
    b'{"value":NaN}', b'{"value":Infinity}', b'{"value":-Infinity}',
    b'{"value":1e9999}', b'{"value":-1e9999}',
    b'[]', b'null', b'123', b'"text"', b'true',
    b'{"private":', b'{"private":"\xff"}', b'{} {}', b'{"value":01}',
])
def test_invalid_json_never_echoes_source(tmp_path: Path, raw: bytes) -> None:
    path = write(tmp_path, raw)
    with pytest.raises(ValidationError) as error:
        reader.load_bounded_json_object(path)
    assert "private" not in str(error.value)
    assert str(path) not in str(error.value)


@pytest.mark.parametrize("depth", [reader.MAX_JSON_DEPTH, reader.MAX_JSON_DEPTH + 1])
def test_container_depth_boundary(tmp_path: Path, depth: int) -> None:
    raw = b'{"v":' * depth + b'0' + b'}' * depth
    path = write(tmp_path, raw)
    if depth > reader.MAX_JSON_DEPTH:
        with pytest.raises(ValidationError, match="nesting"):
            reader.load_bounded_json_object(path)
    else:
        assert reader.load_bounded_json_object(path)


def test_syntax_inside_escaped_strings_does_not_count_as_depth(tmp_path: Path) -> None:
    value = {"v": ('\\"[{' * 100) + ('}]"\\' * 100)}
    path = write(tmp_path, json.dumps(value).encode())
    assert reader.load_bounded_json_object(path) == value


def test_short_reads_are_accumulated_and_descriptor_is_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = write(tmp_path, b'{"value":12345}')
    real_read = os.read
    seen: list[int] = []

    def short_read(fd: int, size: int) -> bytes:
        seen.append(fd)
        return real_read(fd, min(size, 2))

    monkeypatch.setattr(reader.os, "read", short_read)
    assert reader.load_bounded_json_object(path) == {"value": 12345}
    assert len(seen) > 2
    with pytest.raises(OSError):
        os.fstat(seen[0])


def test_growth_after_path_check_cannot_exceed_actual_read_budget(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = write(tmp_path, b'{}')
    real_open, real_read = os.open, os.read
    read_sizes: list[int] = []

    def grow_before_open(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        path.write_bytes(b'{"value":"' + b"x" * 200 + b'"}')
        return real_open(file, flags, *args, **kwargs)

    def counted(fd: int, size: int) -> bytes:
        read_sizes.append(size)
        return real_read(fd, size)

    monkeypatch.setattr(reader.os, "open", grow_before_open)
    monkeypatch.setattr(reader.os, "read", counted)
    with pytest.raises(ValidationError, match="byte limit"):
        reader.load_bounded_json_object(path, max_bytes=32)
    assert sum(read_sizes) == 33


@pytest.mark.parametrize("dangling", [False, True])
def test_symbolic_links_are_rejected(tmp_path: Path, dangling: bool) -> None:
    target = tmp_path / "target.json"
    if not dangling:
        target.write_text("{}")
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(ValidationError, match="symbolic link"):
        reader.load_bounded_json_object(link)
    assert link.is_symlink()


def test_directory_is_rejected_before_open(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="regular file"):
        reader.load_bounded_json_object(tmp_path)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO support is unavailable")
def test_fifo_is_rejected_without_opening(tmp_path: Path) -> None:
    path = tmp_path / "fifo"
    os.mkfifo(path)
    with pytest.raises(ValidationError, match="regular file"):
        reader.load_bounded_json_object(path)


@pytest.mark.skipif(not hasattr(os, "O_NOFOLLOW"), reason="no-follow open is unavailable")
def test_symlink_swap_between_check_and_open_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = write(tmp_path, b'{}')
    victim = tmp_path / "victim.json"
    victim.write_text('{"private":true}')
    real_open = os.open

    def swapped(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        path.unlink()
        path.symlink_to(victim)
        assert flags & os.O_NOFOLLOW
        return real_open(file, flags, *args, **kwargs)

    monkeypatch.setattr(reader.os, "open", swapped)
    with pytest.raises(ValidationError):
        reader.load_bounded_json_object(path)
    assert victim.read_text() == '{"private":true}'


@pytest.mark.parametrize("failure", ["read", "fstat", "close", "descriptor_type"])
def test_descriptor_errors_close_and_redact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    path = write(tmp_path, b'{}')
    real_open, real_close, real_fstat = os.open, os.close, os.fstat
    seen: list[int] = []

    def capture(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        fd = real_open(file, flags, *args, **kwargs)
        seen.append(fd)
        return fd

    def fail(*args: Any) -> Any:
        raise OSError("private-provider-details")

    def fail_close(fd: int) -> None:
        real_close(fd)
        raise OSError("private-close-details")

    def wrong_type(fd: int) -> os.stat_result:
        fields = list(real_fstat(fd))
        fields[0] = stat.S_IFCHR
        return os.stat_result(fields)

    monkeypatch.setattr(reader.os, "open", capture)
    if failure == "close":
        monkeypatch.setattr(reader.os, "close", fail_close)
    elif failure == "descriptor_type":
        monkeypatch.setattr(reader.os, "fstat", wrong_type)
    else:
        monkeypatch.setattr(reader.os, failure, fail)
    with pytest.raises(ValidationError) as error:
        reader.load_bounded_json_object(path)
    assert "private" not in str(error.value)
    assert len(seen) == 1
    with pytest.raises(OSError):
        real_fstat(seen[0])


def test_missing_path_is_redacted(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="unable to read valid JSON evidence"):
        reader.load_bounded_json_object(tmp_path / "private-missing.json")


def test_default_byte_ceiling_accepts_exact_document(tmp_path: Path) -> None:
    raw = b'{"v":"' + b'x' * (reader.MAX_JSON_BYTES - 8) + b'"}'
    assert len(raw) == reader.MAX_JSON_BYTES
    assert reader.load_bounded_json_object(write(tmp_path, raw))["v"]


def test_regular_file_replacement_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = write(tmp_path, b'{}')
    replacement = tmp_path / "replacement.json"
    replacement.write_text('{"other":true}')
    real_open = os.open

    def replaced(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        replacement.replace(path)
        return real_open(file, flags, *args, **kwargs)

    monkeypatch.setattr(reader.os, "open", replaced)
    with pytest.raises(ValidationError, match="changed while opening"):
        reader.load_bounded_json_object(path)


@pytest.mark.skipif(not Path("/dev/null").exists(), reason="device file unavailable")
def test_device_file_is_rejected() -> None:
    with pytest.raises(ValidationError, match="regular file"):
        reader.load_bounded_json_object(Path("/dev/null"))


def test_non_path_argument_is_a_validation_error() -> None:
    with pytest.raises(ValidationError, match="must be a Path"):
        reader.load_bounded_json_object("not-a-path-object")  # type: ignore[arg-type]
