from pathlib import Path
from threading import Barrier
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest

import src.storage.parquet_io as parquet_io
from src.common.exceptions import StorageError
from src.storage.parquet_io import batch_file_name, create_parquet_file


def test_batch_filename_preserves_the_legacy_digest_contract() -> None:
    assert batch_file_name([{"value": 1}], "parquet") == "batch_48208f9428d64634.parquet"


def test_create_parquet_file_publishes_private_valid_output(tmp_path: Path) -> None:
    target = tmp_path / "dataset" / "batch.parquet"

    assert create_parquet_file([{"value": 1}], target) is True

    assert target.stat().st_mode & 0o777 == 0o600
    with duckdb.connect() as connection:
        assert connection.execute(
            "SELECT value FROM read_parquet(?)",
            [str(target)],
        ).fetchall() == [(1,)]


def test_link_failure_leaves_no_visible_or_staged_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "dataset" / "batch.parquet"

    def fail_link(*args: object, **kwargs: object) -> None:
        raise OSError("simulated publication failure")

    monkeypatch.setattr(parquet_io.os, "link", fail_link)
    with pytest.raises(StorageError, match="Unable to publish"):
        create_parquet_file([{"value": 1}], target)

    assert not target.exists()
    assert not list(target.parent.glob(".parquet-stage-*"))


def test_create_parquet_file_never_overwrites_existing_target(tmp_path: Path) -> None:
    target = tmp_path / "dataset" / "batch.parquet"
    assert create_parquet_file([{"value": 1}], target) is True
    original = target.read_bytes()

    assert create_parquet_file([{"value": 2}], target) is False

    assert target.read_bytes() == original


def test_concurrent_publishers_create_one_complete_file(tmp_path: Path) -> None:
    target = tmp_path / "dataset" / "batch.parquet"
    barrier = Barrier(2)

    def publish() -> bool:
        barrier.wait()
        return create_parquet_file([{"value": 1}], target)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish), executor.submit(publish)]
        results = [future.result() for future in futures]

    assert sorted(results) == [False, True]
    with duckdb.connect() as connection:
        assert connection.execute(
            "SELECT value FROM read_parquet(?)",
            [str(target)],
        ).fetchall() == [(1,)]
