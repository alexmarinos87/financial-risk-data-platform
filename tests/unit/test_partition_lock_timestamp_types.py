"""A JSON number resembling a date is not acquisition-time evidence."""

import json
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import OverlapError
from src.orchestration import locks


@pytest.mark.parametrize("acquired_at", [
    19700101, 20000101, 0, None, True, ["19700101"], {"text": "19700101"},
])
def test_nontext_acquisition_times_remain_blocked(tmp_path: Path, acquired_at: Any) -> None:
    path = tmp_path / ".lock"
    path.write_text(json.dumps({"acquired_at": acquired_at}), encoding="utf-8")
    before = path.read_bytes()
    assert locks._is_stale_lock(path, 3600) is False
    assert path.read_bytes() == before


@pytest.mark.parametrize("acquired_at", [
    "1970-01-01T00:00:00+00:00",
    "1970-01-01T05:30:00+05:30",
    "1970-01-01T00:00:00",
    "19700101",
])
def test_previously_accepted_text_formats_keep_their_policy(
    tmp_path: Path, acquired_at: str,
) -> None:
    path = tmp_path / ".lock"
    path.write_text(json.dumps({"acquired_at": acquired_at}), encoding="utf-8")
    before = path.read_bytes()
    assert locks._is_stale_lock(path, 3600) is True
    assert path.read_bytes() == before


def test_numeric_date_cannot_replace_owner_or_abandon_earlier_lock(tmp_path: Path) -> None:
    target = tmp_path / ".orchestration_locks/b/.lock"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps({
        "owner": "original", "partition": "b", "acquired_at": 19700101,
    }), encoding="utf-8")
    before = target.read_bytes()
    with pytest.raises(OverlapError):
        locks.acquire_partition_locks(tmp_path, ["a", "b"], "contender", stale_after_seconds=3600)
    assert target.read_bytes() == before
    assert not (tmp_path / ".orchestration_locks/a/.lock").exists()
    assert list(tmp_path.rglob(".lock")) == [target]


def test_disabled_takeover_does_not_open_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_open(*args: Any, **kwargs: Any) -> int:
        raise AssertionError("disabled takeover must not inspect a file")

    monkeypatch.setattr(locks.os, "open", unexpected_open)
    assert locks._is_stale_lock(tmp_path / ".lock", None) is False
