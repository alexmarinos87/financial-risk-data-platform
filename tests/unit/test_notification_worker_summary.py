from __future__ import annotations

import json
import os
import stat
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError
from src.orchestration.notification_worker_summary import (
    MAX_SUMMARY_BYTES,
    write_notification_worker_summary,
)
from src.orchestration.plan_notification_worker import _write_summary

Writer = Callable[[Path, Mapping[str, Any]], None]


@pytest.fixture(params=[write_notification_worker_summary, _write_summary])
def writer(request: pytest.FixtureRequest) -> Writer:
    return request.param


def test_summary_replaces_complete_document(writer: Writer, tmp_path: Path) -> None:
    target = tmp_path / "summary.json"
    target.write_text("old evidence", encoding="utf-8")
    writer(target, {"worker_id": "reviewed-worker"})
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "worker_id": "reviewed-worker"
    }
    assert list(tmp_path.glob(".summary.json.*.tmp")) == []
    if os.name == "posix":
        assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_summary_never_follows_legacy_temporary_symlink(
    writer: Writer, tmp_path: Path
) -> None:
    target = tmp_path / "summary.json"
    victim = tmp_path / "unrelated.txt"
    victim.write_text("preserve this", encoding="utf-8")
    legacy = target.with_suffix(target.suffix + ".tmp")
    legacy.symlink_to(victim)
    writer(target, {"safe": True})
    assert victim.read_text(encoding="utf-8") == "preserve this"
    assert legacy.is_symlink()
    assert json.loads(target.read_text(encoding="utf-8")) == {"safe": True}


@pytest.mark.parametrize("dangling", [False, True])
def test_summary_rejects_destination_symlink(
    writer: Writer, tmp_path: Path, dangling: bool
) -> None:
    target = tmp_path / "summary.json"
    victim = tmp_path / "unrelated.txt"
    if not dangling:
        victim.write_text("preserve this", encoding="utf-8")
    target.symlink_to(victim)
    with pytest.raises(StorageError, match="symbolic link"):
        writer(target, {"safe": True})
    assert target.is_symlink()
    assert victim.exists() is not dangling
    if not dangling:
        assert victim.read_text(encoding="utf-8") == "preserve this"


def test_summary_rejects_directory(writer: Writer, tmp_path: Path) -> None:
    target = tmp_path / "summary.json"
    target.mkdir()
    with pytest.raises(StorageError, match="regular file"):
        writer(target, {"safe": True})
    assert target.is_dir()


@pytest.mark.parametrize("value", [float("nan"), object()])
def test_invalid_json_has_no_filesystem_effect(
    writer: Writer, tmp_path: Path, value: Any
) -> None:
    target = tmp_path / "not-created" / "summary.json"
    with pytest.raises(StorageError, match="valid JSON"):
        writer(target, {"value": value})
    assert not target.parent.exists()


def test_summary_byte_limit_precedes_filesystem_mutation(
    writer: Writer, tmp_path: Path
) -> None:
    target = tmp_path / "not-created" / "summary.json"
    with pytest.raises(StorageError, match="1 MB"):
        writer(target, {"value": "x" * MAX_SUMMARY_BYTES})
    assert not target.parent.exists()


def test_summary_accepts_exact_byte_limit(writer: Writer, tmp_path: Path) -> None:
    overhead = len((json.dumps({"value": ""}, indent=2, sort_keys=True) + "\n").encode())
    target = tmp_path / "summary.json"
    writer(target, {"value": "x" * (MAX_SUMMARY_BYTES - overhead)})
    assert target.stat().st_size == MAX_SUMMARY_BYTES


def test_failed_replacement_preserves_old_summary_and_cleans_temporary(
    writer: Writer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "summary.json"
    target.write_text("old evidence", encoding="utf-8")

    def fail_replace(self: Path, destination: Path) -> Path:
        raise OSError("injected replacement failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(StorageError, match="unable to write"):
        writer(target, {"safe": True})
    assert target.read_text(encoding="utf-8") == "old evidence"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["summary.json"]


def test_parent_creation_error_is_normalized(writer: Writer, tmp_path: Path) -> None:
    parent = tmp_path / "not-a-directory"
    parent.write_text("preserve this", encoding="utf-8")
    with pytest.raises(StorageError, match="unable to write"):
        writer(parent / "summary.json", {"safe": True})
    assert parent.read_text(encoding="utf-8") == "preserve this"
