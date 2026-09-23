"""Ambiguous lock JSON must never authorize deleting another owner's entry."""

import json
from pathlib import Path

import pytest

from src.common.exceptions import OverlapError
from src.orchestration.locks import acquire_partition_locks, release_partition_locks

OLD = '"2000-01-01T00:00:00+00:00"'
FUTURE = '"9999-01-01T00:00:00+00:00"'


@pytest.mark.parametrize("body", [
    '{"acquired_at":' + FUTURE + ',"acquired_at":' + OLD + '}',
    '{"acquired_at":' + OLD + ',"acquired_at":' + FUTURE + '}',
    '{"acquired_at":' + OLD + ',"acquired_at":' + OLD + '}',
    '{"acquired_at":' + OLD + ',"\\u0061cquired_at":' + OLD + '}',
    '{"owner":"first","owner":"second","acquired_at":' + OLD + '}',
    '{"partition":"b","partition":"c","acquired_at":' + OLD + '}',
    '{"extra":{"owner":"first","owner":"second"},"acquired_at":' + OLD + '}',
], ids=["future-then-old", "old-then-future", "identical-timestamps", "escaped-key",
        "owners", "partitions", "nested-object"])
def test_ambiguous_metadata_blocks_and_rolls_back_earlier_lock(
    tmp_path: Path, body: str,
) -> None:
    target = tmp_path / ".orchestration_locks" / "b" / ".lock"
    target.parent.mkdir(parents=True)
    target.write_text(body, encoding="utf-8")
    before = target.read_bytes()
    with pytest.raises(OverlapError):
        acquire_partition_locks(tmp_path, ["b", "a"], "contender", stale_after_seconds=60)
    assert target.read_bytes() == before
    assert not (target.parent.parent / "a" / ".lock").exists()
    retry = acquire_partition_locks(tmp_path, ["a"], "retry")
    release_partition_locks(retry)
    assert target.read_bytes() == before


@pytest.mark.parametrize("timestamp", [
    "2000-01-01T00:00:00+00:00", "2000-01-01T00:00:00", "2000-01-01T03:00:00+03:00",
])
def test_unambiguous_legacy_metadata_remains_replaceable(tmp_path: Path, timestamp: str) -> None:
    target = tmp_path / ".orchestration_locks" / "b" / ".lock"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps({
        "acquired_at": timestamp,
        "owner": "original",
        # Equal field names in different objects are not duplicates.
        "extra": {"owner": "nested", "text": "acquired_at acquired_at"},
    }), encoding="utf-8")
    paths = acquire_partition_locks(tmp_path, ["b"], "replacement", stale_after_seconds=60)
    assert json.loads(target.read_text(encoding="utf-8"))["owner"] == "replacement"
    release_partition_locks(paths)
    assert not target.exists()


def test_unique_future_lock_stays_blocked(tmp_path: Path) -> None:
    target = tmp_path / ".orchestration_locks" / "b" / ".lock"
    target.parent.mkdir(parents=True)
    body = '{"acquired_at":' + FUTURE + '}'
    target.write_text(body, encoding="utf-8")
    with pytest.raises(OverlapError):
        acquire_partition_locks(tmp_path, ["b"], "contender", stale_after_seconds=60)
    assert target.read_text(encoding="utf-8") == body
