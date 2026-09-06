from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.common.exceptions import ValidationError
from src.warehouse.notification_destination_transition_rehearsal_recorder import (
    MAX_RECORD_BYTES,
    _read_record,
)


def test_transition_record_reader_accepts_regular_bounded_json(tmp_path: Path) -> None:
    path = tmp_path / "record.json"
    path.write_text(json.dumps({"record_id": "record-1"}), encoding="utf-8")

    assert _read_record(path) == {"record_id": "record-1"}


def test_transition_record_reader_rejects_symlink_and_non_file(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "record.json"
    link.symlink_to(target)

    with pytest.raises(ValidationError, match="symbolic link"):
        _read_record(link)
    with pytest.raises(ValidationError, match="regular file"):
        _read_record(tmp_path)


def test_transition_record_reader_rejects_oversized_or_malformed_json(
    tmp_path: Path,
) -> None:
    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b"x" * (MAX_RECORD_BYTES + 1))
    with pytest.raises(ValidationError, match="byte limit"):
        _read_record(oversized)

    malformed = tmp_path / "malformed.json"
    malformed.write_text("{not-json", encoding="utf-8")
    with pytest.raises(ValidationError, match="unable to read valid JSON evidence"):
        _read_record(malformed)
