from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from src.common.bounded_json import MAX_JSON_DEPTH, load_bounded_json_object
from src.common.exceptions import ValidationError


@pytest.mark.parametrize("escaped", [
    r"\ud800", r"\udbff", r"\udc00", r"\udfff",
    r"\ud800\ud800", r"\udc00\ud800", r"\ud800x\udc00",
    r"\ud83d\ude00\ud800",
])
@pytest.mark.parametrize("location", ["value", "key", "array"])
def test_unpaired_surrogates_are_rejected_at_every_location(
    tmp_path: Path, escaped: str, location: str,
) -> None:
    text = {
        "value": '{"nested":{"private":"' + escaped + '"}}',
        "key": '{"nested":{"' + escaped + '":"private"}}',
        "array": '{"nested":[0,[true,"' + escaped + '"]] }',
    }[location]
    path = tmp_path / "private-evidence.json"
    path.write_bytes(text.encode("ascii"))
    with pytest.raises(ValidationError) as error:
        load_bounded_json_object(path)
    assert str(error.value) == "JSON evidence contains an unpaired Unicode surrogate"
    assert "private" not in str(error.value)
    assert path.read_bytes() == text.encode("ascii")


@pytest.mark.parametrize(("escaped", "value"), [
    (r"\ud800\udc00", "\U00010000"),
    (r"\udbff\udfff", "\U0010ffff"),
    (r"\ud83d\ude00", "\U0001f600"),
    (r"\uD83D\uDE00", "\U0001f600"),
    (r"\u03a9", "Ω"),
])
def test_correct_pairs_and_bmp_escapes_remain_unchanged(
    tmp_path: Path, escaped: str, value: str,
) -> None:
    path = tmp_path / "record.json"
    raw = ('{"' + escaped + '":["' + escaped + '"]}').encode("ascii")
    path.write_bytes(raw)
    expected = {value: [value]}
    result = load_bounded_json_object(path)
    assert result == expected
    assert json.dumps(result, ensure_ascii=False).encode("utf-8")
    before = json.dumps(json.loads(raw), sort_keys=True, separators=(",", ":")).encode()
    after = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    assert before == after
    assert hashlib.sha256(before).digest() == hashlib.sha256(after).digest()
    assert path.read_bytes() == raw


def test_literal_escape_text_is_not_treated_as_a_surrogate(tmp_path: Path) -> None:
    expected = {r"\ud800": [r"\udc00", "quotes \" and backslash \\", "中文 Ω 😀"]}
    path = tmp_path / "record.json"
    path.write_text(json.dumps(expected, ensure_ascii=False), encoding="utf-8")
    assert load_bounded_json_object(path) == expected


def test_normalization_forms_and_json_control_escapes_are_preserved(tmp_path: Path) -> None:
    expected = {"é": "composed", "e\u0301": "decomposed", "control": "\n\t\x00"}
    path = tmp_path / "record.json"
    path.write_text(json.dumps(expected, ensure_ascii=False), encoding="utf-8")
    result = load_bounded_json_object(path)
    assert result == expected
    assert len(result) == 3


def test_equivalent_supplementary_keys_still_trigger_duplicate_rejection(tmp_path: Path) -> None:
    path = tmp_path / "record.json"
    path.write_text('{"😀":1,"\\ud83d\\ude00":2}', encoding="utf-8")
    with pytest.raises(ValidationError, match="duplicate fields"):
        load_bounded_json_object(path)


@pytest.mark.parametrize("valid", [False, True])
def test_unicode_validation_reaches_the_existing_depth_boundary(tmp_path: Path, valid: bool) -> None:
    leaf = b'"\\ud83d\\ude00"' if valid else b'"\\ud800"'
    raw = b'{"v":' * MAX_JSON_DEPTH + leaf + b'}' * MAX_JSON_DEPTH
    path = tmp_path / "record.json"
    path.write_bytes(raw)
    if valid:
        assert load_bounded_json_object(path)
    else:
        with pytest.raises(ValidationError, match="unpaired Unicode surrogate"):
            load_bounded_json_object(path)


def test_surrogate_failure_happens_after_descriptor_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "record.json"
    path.write_bytes(b'{"value":"\\ud800"}')
    real_open = os.open
    descriptors: list[int] = []

    def capture(*args: Any, **kwargs: Any) -> int:
        descriptor = real_open(*args, **kwargs)
        descriptors.append(descriptor)
        return descriptor

    monkeypatch.setattr(os, "open", capture)
    with pytest.raises(ValidationError, match="unpaired Unicode surrogate"):
        load_bounded_json_object(path)
    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


def test_plain_decoder_acceptance_does_not_imply_utf8_interoperability(tmp_path: Path) -> None:
    raw = b'{"value":"\\ud800"}'
    decoded = json.loads(raw)
    with pytest.raises(UnicodeEncodeError):
        json.dumps(decoded, ensure_ascii=False).encode("utf-8")
    path = tmp_path / "record.json"
    path.write_bytes(raw)
    with pytest.raises(ValidationError, match="unpaired Unicode surrogate"):
        load_bounded_json_object(path)
