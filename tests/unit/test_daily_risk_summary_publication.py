"""Summary writers must own their staging area and leave other paths alone."""

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
from threading import Barrier, Lock
from typing import Any

import pytest

from src.common.exceptions import StorageError
from src.orchestration import run_daily_risk as runner


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(path.relative_to(root)): path.read_bytes()
            for path in root.rglob("*") if path.is_file()}


@pytest.mark.parametrize("name", ["summary.json", "summary", "summary.with.dots.json"])
def test_summary_format_and_replacement_remain_compatible(tmp_path: Path, name: str) -> None:
    path = tmp_path / "new-directory" / name
    first = {"z": [1, 2], "a": "α"}
    second = {"run_id": "second", "metric": None}
    for summary in (first, second):
        assert runner._write_summary(path, summary) is None
        assert path.read_text(encoding="utf-8") == json.dumps(
            summary, indent=2, sort_keys=True,
        ) + "\n"
        assert list(path.parent.iterdir()) == [path]


@pytest.mark.parametrize("kind", ["file", "symlink"])
def test_predictable_temporary_name_is_never_written_or_removed(
    tmp_path: Path, kind: str,
) -> None:
    path = tmp_path / "summary.json"
    legacy = tmp_path / "summary.json.tmp"
    victim = tmp_path / "unrelated.txt"
    victim.write_bytes(b"keep this unrelated file")
    if kind == "symlink":
        legacy.symlink_to(victim)
    else:
        legacy.write_bytes(b"another writer's temporary data")
    before = legacy.read_bytes()
    runner._write_summary(path, {"run_id": "new"})
    assert victim.read_bytes() == b"keep this unrelated file"
    assert legacy.read_bytes() == before
    assert legacy.is_symlink() is (kind == "symlink")
    assert not path.is_symlink()
    assert json.loads(path.read_text()) == {"run_id": "new"}


def test_concurrent_writers_have_distinct_staging_and_complete_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "summary.json"
    payloads = [{"run_id": "first", "values": list(range(200))},
                {"run_id": "second", "values": list(range(400))}]
    barrier = Barrier(2, timeout=5)
    guard = Lock()
    stages: list[Path] = []
    original_replace = Path.replace

    def replace(source: Path, target: Any) -> Path:
        assert Path(target) == path
        with guard:
            stages.append(source)
        # Both writers finish writing before either is allowed to publish.
        barrier.wait()
        return original_replace(source, target)

    monkeypatch.setattr(Path, "replace", replace)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(runner._write_summary, path, payload) for payload in payloads]
        for future in futures:
            assert future.result(timeout=10) is None
    assert len(set(stages)) == 2
    assert json.loads(path.read_text()) in payloads
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("phase", ["write", "replace"])
@pytest.mark.parametrize("failure_type", [OSError, KeyboardInterrupt])
def test_failure_preserves_old_summary_and_cleans_only_owned_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str,
    failure_type: type[BaseException],
) -> None:
    path = tmp_path / "summary.json"
    path.write_bytes(b'{"run_id":"previous"}\n')
    legacy = tmp_path / "summary.json.tmp"
    legacy.write_bytes(b"unrelated temporary state")
    before = _snapshot(tmp_path)
    original_write = Path.write_text
    original_replace = Path.replace
    failure = failure_type("synthetic publication failure")

    def write(target: Path, data: str, *args: Any, **kwargs: Any) -> int:
        if phase == "write":
            original_write(target, data[:3], *args, **kwargs)
            raise failure
        return original_write(target, data, *args, **kwargs)

    def replace(source: Path, destination: Any) -> Path:
        if phase == "replace":
            raise failure
        return original_replace(source, destination)

    monkeypatch.setattr(Path, "write_text", write)
    monkeypatch.setattr(Path, "replace", replace)
    if failure_type is OSError:
        with pytest.raises(StorageError, match="^Unable to write the daily risk summary$"):
            runner._write_summary(path, {"run_id": "candidate"})
    else:
        with pytest.raises(KeyboardInterrupt) as caught:
            runner._write_summary(path, {"run_id": "candidate"})
        assert caught.value is failure
    assert _snapshot(tmp_path) == before
    assert not any(item.is_dir() for item in tmp_path.iterdir())


def test_serialization_failure_preserves_output_and_cleans_staging(tmp_path: Path) -> None:
    path = tmp_path / "summary.json"
    path.write_bytes(b"previous")
    with pytest.raises(TypeError):
        runner._write_summary(path, {"unsupported": object()})
    assert list(tmp_path.iterdir()) == [path]
    assert path.read_bytes() == b"previous"


def test_parent_directory_failure_uses_the_public_storage_error(tmp_path: Path) -> None:
    obstruction = tmp_path / "not-a-directory"
    obstruction.write_bytes(b"leave intact")
    with pytest.raises(StorageError, match="^Unable to write the daily risk summary$"):
        runner._write_summary(obstruction / "summary.json", {"run_id": "candidate"})
    assert obstruction.read_bytes() == b"leave intact"


def test_destination_symlink_is_replaced_without_writing_its_target(tmp_path: Path) -> None:
    victim = tmp_path / "other.json"
    victim.write_bytes(b"other owner's content")
    path = tmp_path / "summary.json"
    path.symlink_to(victim)
    runner._write_summary(path, {"run_id": "candidate"})
    assert not path.is_symlink()
    assert victim.read_bytes() == b"other owner's content"
    assert json.loads(path.read_text()) == {"run_id": "candidate"}
