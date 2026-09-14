"""Release must attempt every supplied lock without hiding cleanup failures."""

from pathlib import Path

import pytest

from src.common.exceptions import OverlapError
from src.orchestration.locks import acquire_partition_locks, release_partition_locks


@pytest.mark.parametrize("failed_index", [0, 1, 2])
def test_release_failure_does_not_abandon_other_locks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_index: int,
) -> None:
    partitions = ["part=a", "part=b", "part=c"]
    paths = acquire_partition_locks(tmp_path, partitions, owner="release-test")
    before = list(paths)
    failed = paths[failed_index]
    payload = failed.read_bytes()
    original_unlink = Path.unlink
    calls: list[Path] = []
    failure = PermissionError("synthetic release failure")

    def unlink(path: Path, missing_ok: bool = False) -> None:
        calls.append(path)
        assert missing_ok is True
        if path == failed:
            raise failure
        original_unlink(path, missing_ok=missing_ok)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", unlink)
        with pytest.raises(PermissionError) as error:
            release_partition_locks(paths)
    assert error.value is failure
    assert calls == paths  # Preserve existing caller order, not stack order.
    assert paths == before
    assert [path.exists() for path in paths] == [path == failed for path in paths]
    assert failed.read_bytes() == payload
    with pytest.raises(OverlapError):
        acquire_partition_locks(tmp_path, [partitions[failed_index]], owner="contender")
    available = [part for index, part in enumerate(partitions) if index != failed_index]
    retry = acquire_partition_locks(tmp_path, available, owner="retry")
    release_partition_locks(retry)
    release_partition_locks(paths)  # A later explicit retry can release the residual.
    assert not any(path.exists() for path in paths)


def test_multiple_release_failures_are_all_retained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = acquire_partition_locks(tmp_path, ["a", "b", "c"], owner="release-test")
    failures = {paths[0]: PermissionError("first"), paths[1]: OSError("second")}
    original_unlink = Path.unlink
    calls: list[Path] = []

    def unlink(path: Path, missing_ok: bool = False) -> None:
        calls.append(path)
        if path in failures:
            raise failures[path]
        original_unlink(path, missing_ok=missing_ok)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", unlink)
        with pytest.raises(ExceptionGroup) as error:
            release_partition_locks(paths)
    assert calls == paths
    assert error.value.exceptions == (failures[paths[0]], failures[paths[1]])
    assert paths[0].exists() and paths[1].exists() and not paths[2].exists()
    release_partition_locks(paths)


def test_release_keeps_original_processing_failure_in_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = acquire_partition_locks(tmp_path, ["a", "b"], owner="release-test")
    original_unlink = Path.unlink
    processing_failure = RuntimeError("processing failed")
    release_failure = PermissionError("release failed")

    def unlink(path: Path, missing_ok: bool = False) -> None:
        if path == paths[0]:
            raise release_failure
        original_unlink(path, missing_ok=missing_ok)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", unlink)
        with pytest.raises(PermissionError) as error:
            try:
                raise processing_failure
            finally:
                release_partition_locks(paths)
    assert error.value is release_failure
    assert error.value.__context__ is processing_failure
    assert not paths[1].exists()
    release_partition_locks(paths)


@pytest.mark.parametrize("failure_type", [KeyboardInterrupt, SystemExit])
def test_interruption_unwinds_remaining_registered_releases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_type: type[BaseException],
) -> None:
    paths = acquire_partition_locks(tmp_path, ["a", "b"], owner="release-test")
    original_unlink = Path.unlink
    failure = failure_type("synthetic operation-boundary interruption")

    def unlink(path: Path, missing_ok: bool = False) -> None:
        if path == paths[0]:
            raise failure
        original_unlink(path, missing_ok=missing_ok)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", unlink)
        with pytest.raises(failure_type) as error:
            release_partition_locks(paths)
    assert error.value is failure
    assert not paths[1].exists()
    release_partition_locks(paths)


def test_empty_missing_and_duplicate_paths_remain_idempotent(tmp_path: Path) -> None:
    paths = acquire_partition_locks(tmp_path, ["a", "b"], owner="release-test")
    unrelated = tmp_path / "unrelated"
    unrelated.write_text("leave unchanged", encoding="utf-8")
    paths[0].unlink()
    assert release_partition_locks([]) is None
    assert release_partition_locks([paths[0], paths[1], paths[1]]) is None
    assert release_partition_locks(paths) is None
    assert unrelated.read_text(encoding="utf-8") == "leave unchanged"


def test_mixed_cleanup_and_interruption_failures_are_not_hidden(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = acquire_partition_locks(tmp_path, ["a", "b", "c"], owner="release-test")
    first = PermissionError("synthetic permission failure")
    second = KeyboardInterrupt("synthetic interruption")
    original_unlink = Path.unlink

    def unlink(path: Path, missing_ok: bool = False) -> None:
        if path == paths[0]:
            raise first
        if path == paths[1]:
            raise second
        original_unlink(path, missing_ok=missing_ok)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", unlink)
        with pytest.raises(BaseExceptionGroup) as error:
            release_partition_locks(paths)
    assert type(error.value) is BaseExceptionGroup
    assert error.value.exceptions == (first, second)
    assert not paths[2].exists()
    release_partition_locks(paths)
