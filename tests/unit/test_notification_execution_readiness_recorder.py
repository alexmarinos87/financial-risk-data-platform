from __future__ import annotations

from pathlib import Path

import pytest

from src.common.exceptions import ValidationError
from src.warehouse.notification_execution_readiness_recorder import (
    MAX_RECORD_BYTES,
    _build_parser,
    _read_record,
)


def test_record_reader_rejects_symbolic_links(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "record.json"
    link.symlink_to(target)

    with pytest.raises(ValidationError, match="symbolic link"):
        _read_record(link)


def test_record_reader_rejects_non_object_and_oversized_files(
    tmp_path: Path,
) -> None:
    array = tmp_path / "array.json"
    array.write_text("[]", encoding="utf-8")
    with pytest.raises(ValidationError, match="must be an object"):
        _read_record(array)

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b" " * (MAX_RECORD_BYTES + 1))
    with pytest.raises(ValidationError, match="byte limit"):
        _read_record(oversized)


def test_recorder_cli_has_no_execution_switch() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--record", "decision.json", "--execute"])
