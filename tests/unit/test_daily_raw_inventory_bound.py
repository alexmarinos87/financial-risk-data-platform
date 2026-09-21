"""Reject oversized inventories without exhausting every matching file path."""

from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration import run_daily_risk as runner


def storage(tmp_path: Path) -> tuple[dict[str, Any], Path]:
    raw = tmp_path / "raw" / "events"
    raw.mkdir(parents=True)
    return {"storage": {
        "base_dir": str(tmp_path), "format": "parquet",
        "partitioning": {"granularity": "hour"},
        "raw": {"base_path": str(raw.parent), "dataset": raw.name},
        "curated": {"base_path": str(tmp_path / "curated"), "datasets": {
            "daily_returns": "daily_returns", "daily_volatility": "daily_volatility",
            "daily_risk_summary": "daily_risk_summary",
        }},
    }}, raw


@pytest.mark.parametrize("limit", [2, 2048])
def test_inventory_consumes_only_limit_plus_one_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit: int,
) -> None:
    config, raw = storage(tmp_path)
    yielded: list[Path] = []

    def inventory(path: Path, pattern: str) -> Iterator[Path]:
        assert path == raw and pattern == "*.parquet"
        for index in range(limit + 5):
            item = raw / f"{index}.parquet"
            yielded.append(item)
            yield item

    monkeypatch.setattr(runner, "MAX_RAW_FILES", limit)
    monkeypatch.setattr(Path, "rglob", inventory)
    with pytest.raises(StorageError, match="^Raw daily storage exceeds the file scan limit$"):
        runner._raw_parquet_files(config)
    assert len(yielded) == limit + 1


def test_inventory_does_not_request_failing_tail_after_known_excess(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, raw = storage(tmp_path)

    def inventory(path: Path, pattern: str) -> Iterator[Path]:
        yield from (raw / f"{index}.parquet" for index in range(3))
        raise AssertionError("Enumeration continued after the request was already too large")

    monkeypatch.setattr(runner, "MAX_RAW_FILES", 2)
    monkeypatch.setattr(Path, "rglob", inventory)
    with pytest.raises(StorageError, match="file scan limit"):
        runner._raw_parquet_files(config)


@pytest.mark.parametrize("names", [["z.parquet"], ["z.parquet", "a.parquet"]])
def test_accepted_inventory_keeps_sorted_paths_and_file_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, names: list[str],
) -> None:
    config, raw = storage(tmp_path)
    paths = [raw / name for name in names]
    for path in paths:
        path.write_bytes(b"inventory fixture; no Parquet decoding required")
    before = {path: path.read_bytes() for path in paths}
    monkeypatch.setattr(runner, "MAX_RAW_FILES", 2)
    monkeypatch.setattr(Path, "rglob", lambda path, pattern: iter(paths))
    assert runner._raw_parquet_files(config) == sorted(paths)
    assert {path: path.read_bytes() for path in paths} == before


def test_empty_inventory_still_rejects(tmp_path: Path) -> None:
    config, _ = storage(tmp_path)
    with pytest.raises(ValidationError, match="No local raw market data"):
        runner._raw_parquet_files(config)


def test_real_over_limit_tree_is_not_returned_as_a_truncated_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, raw = storage(tmp_path)
    for index in range(3):
        (raw / f"{index}.parquet").write_bytes(b"unchanged")
    monkeypatch.setattr(runner, "MAX_RAW_FILES", 2)
    with pytest.raises(StorageError, match="file scan limit"):
        runner._raw_parquet_files(config)
    assert len(list(raw.glob("*.parquet"))) == 3


@pytest.mark.parametrize("unsafe", ["directory", "symlink"])
def test_under_limit_inventory_still_rejects_unsafe_entries(
    tmp_path: Path, unsafe: str,
) -> None:
    config, raw = storage(tmp_path)
    path = raw / "unsafe.parquet"
    if unsafe == "directory":
        path.mkdir()
    else:
        target = tmp_path / "target"
        target.write_bytes(b"leave alone")
        path.symlink_to(target)
    with pytest.raises(StorageError, match="unsafe file type"):
        runner._raw_parquet_files(config)


def test_under_limit_inventory_still_enforces_byte_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, raw = storage(tmp_path)
    (raw / "a.parquet").write_bytes(b"12345")
    monkeypatch.setattr(runner, "MAX_RAW_BYTES", 4)
    with pytest.raises(StorageError, match="byte scan limit"):
        runner._raw_parquet_files(config)


def test_runner_aborts_before_publication_without_consuming_the_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, raw = storage(tmp_path)
    writes: list[bool] = []

    def inventory(path: Path, pattern: str) -> Iterator[Path]:
        yield from (raw / f"{index}.parquet" for index in range(3))
        raise AssertionError("Unnecessary raw scan")

    def writer(*args: Any, **kwargs: Any) -> int:
        writes.append(True)
        return 1

    monkeypatch.setattr(runner, "MAX_RAW_FILES", 2)
    monkeypatch.setattr(Path, "rglob", inventory)
    with pytest.raises(StorageError, match="^Raw daily storage exceeds the file scan limit$"):
        runner.run_daily_risk(
            symbol="IBM", start_date=None, end_date=date(2026, 1, 4),
            volatility_window=2, var_window=2, var_confidence=0.95,
            storage_config_path=tmp_path / "unused.yaml", config_loader=lambda path: config,
            writer=writer,
        )
    assert writes == []
    assert not (tmp_path / "curated").exists()
