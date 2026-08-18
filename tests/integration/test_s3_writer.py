from __future__ import annotations

import json
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from threading import Barrier

import duckdb
import pytest

import src.storage.raw_event_writer as raw_event_writer
from src.common.exceptions import RawEventConflictError, StorageError
from src.ingestion.alpha_vantage_client import parse_alpha_vantage_daily_response
from src.storage.parquet_io import create_parquet_file
from src.storage.partitioning import partition_path
from src.storage.s3_writer import validate_raw_storage_destination, write_records


def _build_storage_config(tmp_path: Path) -> dict:
    return {
        "storage": {
            "base_dir": str(tmp_path),
            "raw": {
                "base_path": str(tmp_path / "raw"),
                "dataset": "market_events",
            },
            "curated": {
                "base_path": str(tmp_path / "curated"),
                "datasets": {
                    "returns_1m": "returns_1m",
                    "volatility_5m": "volatility_5m",
                    "data_quality_metrics": "data_quality_metrics",
                    "risk_summary": "risk_summary",
                },
            },
            "format": "parquet",
            "partitioning": {
                "granularity": "hourly",
            },
        }
    }


def _parquet_row_count(path: Path) -> int:
    escaped = str(path).replace("'", "''")
    with duckdb.connect() as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM read_parquet('{escaped}')").fetchone()[0])


def _event(
    event_id: str = "evt-1",
    *,
    symbol: str = "AAPL",
    price: float = 100.0,
    volume: int = 10,
    ts_event: datetime | None = None,
    ts_ingest: datetime | None = None,
    source: str = "stooq",
) -> dict:
    return {
        "event_id": event_id,
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "ts_event": ts_event or datetime(2025, 1, 20, 10, 0, tzinfo=timezone.utc),
        "ts_ingest": ts_ingest or datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc),
        "source": source,
    }


class _FailsOnSecondOffset(tzinfo):
    def __init__(self) -> None:
        self.calls = 0

    def utcoffset(self, value: datetime | None) -> timedelta:
        self.calls += 1
        if self.calls > 1:
            raise RuntimeError("sentinel-secret-timezone")
        return timedelta(0)

    def dst(self, value: datetime | None) -> timedelta:
        return timedelta(0)


def _raw_records(tmp_path: Path) -> list[dict]:
    files = sorted((tmp_path / "raw" / "market_events").rglob("*.parquet"))
    if not files:
        return []
    with duckdb.connect() as connection:
        frame = connection.execute(
            "SELECT * FROM read_parquet(?, union_by_name=true, hive_partitioning=false)",
            [[str(path) for path in files]],
        ).df()
    return frame.to_dict("records")


def _write_legacy_batch(tmp_path: Path, records: list[dict], *, digest: str) -> Path:
    partition = partition_path(records[0]["ts_ingest"])
    target = tmp_path / "raw" / "market_events" / partition / f"batch_{digest}.parquet"
    assert create_parquet_file(records, target) is True
    return target


def test_write_records_raw_partitioned_and_idempotent(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    ts_ingest = datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc)
    records = [
        {
            "event_id": "evt-1",
            "symbol": "AAPL",
            "price": 100.0,
            "volume": 10,
            "ts_event": datetime(2025, 1, 20, 10, 0, tzinfo=timezone.utc),
            "ts_ingest": ts_ingest,
            "source": "stooq",
        },
        {
            "event_id": "evt-2",
            "symbol": "AAPL",
            "price": 101.0,
            "volume": 11,
            "ts_event": datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc),
            "ts_ingest": ts_ingest.isoformat(),
            "source": "stooq",
        },
    ]

    written = write_records(records, kind="raw", storage_config=config)
    assert written == 2

    partition = partition_path(ts_ingest)
    dataset_dir = tmp_path / "raw" / "market_events" / partition
    parquet_files = list(dataset_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    assert _parquet_row_count(parquet_files[0]) == 2

    written_again = write_records(records, kind="raw", storage_config=config)
    assert written_again == 0
    assert len(list(dataset_dir.glob("*.parquet"))) == 1


def test_raw_inventory_does_not_require_pytz(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _build_storage_config(tmp_path)
    first = _event()
    assert write_records([first], kind="raw", storage_config=config) == 1

    monkeypatch.setitem(sys.modules, "pytz", None)

    replay = _event(ts_ingest=first["ts_ingest"] + timedelta(hours=1))
    assert write_records([replay], kind="raw", storage_config=config) == 0


def test_write_records_curated_without_ts_ingest(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    records = [{"late_rate": 0.2, "duplicate_rate": 0.1}]

    written = write_records(
        records,
        kind="curated",
        dataset="risk_summary",
        storage_config=config,
    )
    assert written == 1

    dataset_dir = tmp_path / "curated" / "risk_summary"
    parquet_files = list(dataset_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    assert _parquet_row_count(parquet_files[0]) == 1


def test_raw_replay_ignores_later_ingest_time_and_preserves_first_seen(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    first = _event()
    replay = _event(ts_ingest=datetime(2025, 1, 21, 11, 0, tzinfo=timezone.utc))

    assert write_records([first], kind="raw", storage_config=config) == 1
    assert write_records([replay], kind="raw", storage_config=config) == 0

    rows = _raw_records(tmp_path)
    assert len(rows) == 1
    assert rows[0]["event_id"] == "evt-1"
    assert rows[0]["ts_ingest"].to_pydatetime() == first["ts_ingest"]
    assert len(list((tmp_path / "raw" / "market_events").rglob("*.parquet"))) == 1


@pytest.mark.parametrize(
    "updates",
    [
        {"symbol": "MSFT"},
        {"price": 101.0},
        {"volume": 11},
        {"ts_event": datetime(2025, 1, 20, 10, 5, tzinfo=timezone.utc)},
        {"source": "alpha_vantage"},
    ],
)
def test_raw_correction_conflicts_without_overwriting(
    tmp_path: Path,
    updates: dict,
) -> None:
    config = _build_storage_config(tmp_path)
    original = _event()
    assert write_records([original], kind="raw", storage_config=config) == 1

    corrected = _event(ts_ingest=datetime(2025, 1, 21, 11, 0, tzinfo=timezone.utc))
    corrected.update(updates)
    with pytest.raises(RawEventConflictError, match="immutable first version"):
        write_records([corrected], kind="raw", storage_config=config)

    rows = _raw_records(tmp_path)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["price"] == 100.0
    assert rows[0]["volume"] == 10
    assert rows[0]["source"] == "stooq"


def test_raw_conflict_preflights_entire_batch_before_publication(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    assert write_records([_event()], kind="raw", storage_config=config) == 1
    new_event = _event("evt-2", symbol="MSFT")
    correction = _event(price=101.0)

    with pytest.raises(RawEventConflictError):
        write_records([new_event, correction], kind="raw", storage_config=config)

    assert {row["event_id"] for row in _raw_records(tmp_path)} == {"evt-1"}


def test_raw_incoming_duplicate_collapses_to_earliest_ingest_time(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    later = _event(ts_ingest=datetime(2025, 1, 21, 11, 0, tzinfo=timezone.utc))
    earlier = _event(ts_ingest=datetime(2025, 1, 20, 9, 0, tzinfo=timezone.utc))

    assert write_records([later, earlier], kind="raw", storage_config=config) == 1

    rows = _raw_records(tmp_path)
    assert len(rows) == 1
    assert rows[0]["ts_ingest"].to_pydatetime() == earlier["ts_ingest"]


def test_raw_incoming_correction_fails_before_creating_dataset(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    with pytest.raises(RawEventConflictError, match="Incoming batch"):
        write_records(
            [_event(), _event(price=101.0)],
            kind="raw",
            storage_config=config,
        )

    assert not (tmp_path / "raw" / "market_events").exists()


def test_concurrent_identical_raw_writers_land_one_fact(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    barrier = Barrier(2)

    def publish() -> int:
        barrier.wait()
        return write_records([_event()], kind="raw", storage_config=config)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [future.result() for future in [executor.submit(publish), executor.submit(publish)]]

    assert sorted(results) == [0, 1]
    assert len(_raw_records(tmp_path)) == 1


def test_concurrent_correction_never_overwrites_winner(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    barrier = Barrier(2)

    def publish(record: dict) -> int | str:
        barrier.wait()
        try:
            return write_records([record], kind="raw", storage_config=config)
        except RawEventConflictError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish, _event()), executor.submit(publish, _event(price=101.0))]
        results = [future.result() for future in futures]

    assert sorted(str(result) for result in results) == ["1", "conflict"]
    rows = _raw_records(tmp_path)
    assert len(rows) == 1
    assert rows[0]["price"] in {100.0, 101.0}


def test_legacy_raw_batch_participates_in_identity_checks(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    _write_legacy_batch(tmp_path, [_event()], digest="0" * 16)

    replay = _event(ts_ingest=datetime(2025, 1, 21, 11, 0, tzinfo=timezone.utc))
    assert write_records([replay], kind="raw", storage_config=config) == 0
    with pytest.raises(RawEventConflictError):
        write_records([_event(price=101.0)], kind="raw", storage_config=config)


def test_duplicate_legacy_keys_block_new_publication(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    first = _event()
    second = _event(ts_ingest=datetime(2025, 1, 21, 11, 0, tzinfo=timezone.utc))
    _write_legacy_batch(tmp_path, [first], digest="0" * 16)
    _write_legacy_batch(tmp_path, [second], digest="1" * 16)

    with pytest.raises(StorageError, match="duplicate records"):
        write_records([_event("evt-2")], kind="raw", storage_config=config)

    assert {row["event_id"] for row in _raw_records(tmp_path)} == {"evt-1"}


@pytest.mark.parametrize(
    "invalid_record",
    [
        {**_event(), "unexpected": "value"},
        {key: value for key, value in _event().items() if key != "source"},
        _event(price=float("nan")),
        _event(price=True),
        _event(price="1" * 4_097),
        _event(volume=-1),
        _event(volume=True),
        _event(volume=1.5),
        _event(volume="1e1000000"),
        _event(volume=9_223_372_036_854_775_808),
        _event(event_id=123),
        _event(event_id="x" * 65_537),
        _event(ts_event="not-a-timestamp"),
        _event(ts_event="1" * 4_097),
    ],
)
def test_raw_contract_rejects_unsafe_or_ambiguous_records(
    tmp_path: Path,
    invalid_record: dict,
) -> None:
    with pytest.raises(StorageError):
        write_records(
            [invalid_record],
            kind="raw",
            storage_config=_build_storage_config(tmp_path),
        )

    assert not list(tmp_path.rglob("*.parquet"))


def test_raw_contract_preserves_existing_text_timestamp_and_integer_compatibility(
    tmp_path: Path,
) -> None:
    config = _build_storage_config(tmp_path)
    event = _event(
        event_id="vendor event/événement 1",
        symbol="BRK/B",
        volume=9_007_199_254_740_993,
        ts_event=datetime(999, 1, 20, 10, 0),
        ts_ingest=datetime(999, 1, 20, 10, 1),
        source="vendor feed",
    )

    assert write_records([event], kind="raw", storage_config=config) == 1
    assert write_records([event], kind="raw", storage_config=config) == 0

    files = list((tmp_path / "raw" / "market_events").rglob("*.parquet"))
    with duckdb.connect() as connection:
        row = connection.execute(
            "SELECT event_id, symbol, volume, source FROM read_parquet(?)",
            [[str(path) for path in files]],
        ).fetchone()
    assert row == (
        event["event_id"],
        "BRK/B",
        9_007_199_254_740_993,
        "vendor feed",
    )


def test_raw_contract_sanitizes_stateful_timezone_failures(tmp_path: Path) -> None:
    broken_time = datetime(2025, 1, 20, 10, tzinfo=_FailsOnSecondOffset())

    with pytest.raises(StorageError) as captured:
        write_records(
            [_event(ts_event=broken_time)],
            kind="raw",
            storage_config=_build_storage_config(tmp_path),
        )

    assert captured.value.__context__ is None
    assert "sentinel-secret" not in "".join(traceback.format_exception(captured.value))
    assert not list(tmp_path.rglob("*.parquet"))


def test_raw_inventory_accepts_microsecond_aligned_legacy_timestamp_ns(
    tmp_path: Path,
) -> None:
    config = _build_storage_config(tmp_path)
    event = _event(
        ts_event=datetime(2025, 1, 20, 10, 0, 0, 123456),
        ts_ingest=datetime(2025, 1, 20, 10, 1, 0, 654321),
    )

    _write_legacy_batch(tmp_path, [event], digest="0" * 16)
    assert write_records([event], kind="raw", storage_config=config) == 0


def test_raw_dataset_rejects_base_escape_and_partition_symlink(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    config["storage"]["raw"]["base_path"] = str(tmp_path.parent / "outside-raw")
    with pytest.raises(StorageError, match="storage.base_dir"):
        write_records([_event()], kind="raw", storage_config=config)

    config = _build_storage_config(tmp_path)
    dataset = tmp_path / "raw" / "market_events"
    dataset.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (dataset / "year=2025").symlink_to(outside, target_is_directory=True)
    with pytest.raises(StorageError, match="symbolic links"):
        write_records([_event()], kind="raw", storage_config=config)
    assert not list(outside.rglob("*.parquet"))


def test_raw_dataset_rejects_dataset_traversal_and_redacts_filesystem_errors(
    tmp_path: Path,
) -> None:
    config = _build_storage_config(tmp_path)
    config["storage"]["raw"]["dataset"] = ".."
    with pytest.raises(StorageError, match="safe path segment"):
        write_records([_event()], kind="raw", storage_config=config)
    assert not list(tmp_path.glob("year=*"))

    sentinel = tmp_path / "sentinel-secret-raw-base"
    sentinel.write_text("not a directory", encoding="utf-8")
    config = _build_storage_config(tmp_path)
    config["storage"]["raw"]["base_path"] = str(sentinel)
    with pytest.raises(StorageError) as captured:
        write_records([_event()], kind="raw", storage_config=config)

    error = captured.value
    rendered = "".join(traceback.format_exception(error))
    assert error.__context__ is None
    assert str(sentinel) not in str(error)
    assert str(sentinel) not in rendered


def test_raw_destination_preflight_is_non_mutating_and_rejects_non_directories(
    tmp_path: Path,
) -> None:
    config = _build_storage_config(tmp_path)

    assert validate_raw_storage_destination(config) == "market_events"
    assert list(tmp_path.iterdir()) == []

    dataset = tmp_path / "raw" / "market_events"
    dataset.parent.mkdir(parents=True)
    dataset.write_text("not a directory", encoding="utf-8")

    with pytest.raises(StorageError, match="must be a regular directory"):
        validate_raw_storage_destination(config)


def test_raw_writer_validates_destination_before_empty_input_return(tmp_path: Path) -> None:
    with pytest.raises(StorageError, match="Unsupported storage format"):
        raw_event_writer.write_raw_event_records(
            [],
            dataset_path=tmp_path / "raw" / "market_events",
            raw_base_path=tmp_path / "raw",
            storage_base_dir=tmp_path,
            file_format="csv",
        )


@pytest.mark.parametrize("path_field", ["base_dir", "raw_base_path"])
def test_raw_dataset_redacts_embedded_nul_paths(
    tmp_path: Path,
    path_field: str,
) -> None:
    config = _build_storage_config(tmp_path)
    if path_field == "base_dir":
        config["storage"]["base_dir"] = "bad\0sentinel-secret"
    else:
        config["storage"]["raw"]["base_path"] = "bad\0sentinel-secret"

    with pytest.raises(StorageError) as captured:
        write_records([_event()], kind="raw", storage_config=config)

    assert captured.value.__context__ is None
    assert "sentinel-secret" not in "".join(traceback.format_exception(captured.value))


def test_raw_dataset_rejects_symlinked_path_components_before_creation(
    tmp_path: Path,
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    config = _build_storage_config(tmp_path)
    config["storage"]["raw"]["base_path"] = str(alias / "raw")

    with pytest.raises(StorageError, match="must not contain symbolic links"):
        write_records([_event()], kind="raw", storage_config=config)

    assert not (real / "raw").exists()


@pytest.mark.parametrize("lock_kind", ["directory", "fifo"])
def test_raw_dataset_lock_rejects_non_regular_files_without_path_context(
    tmp_path: Path,
    lock_kind: str,
) -> None:
    dataset = tmp_path / "raw" / "market_events"
    dataset.mkdir(parents=True)
    lock_path = dataset / ".raw-write.lock"
    if lock_kind == "directory":
        lock_path.mkdir()
    else:
        os.mkfifo(lock_path)

    with pytest.raises(StorageError, match="Unable to open") as captured:
        write_records(
            [_event()],
            kind="raw",
            storage_config=_build_storage_config(tmp_path),
        )

    assert captured.value.__context__ is None
    assert str(tmp_path) not in "".join(traceback.format_exception(captured.value))


def test_raw_dataset_lock_rejects_hardlink_without_mutating_its_target(
    tmp_path: Path,
) -> None:
    dataset = tmp_path / "raw" / "market_events"
    dataset.mkdir(parents=True)
    sentinel = tmp_path / "sentinel.txt"
    sentinel.write_text("unchanged", encoding="utf-8")
    sentinel.chmod(0o644)
    os.link(sentinel, dataset / ".raw-write.lock")

    with pytest.raises(StorageError, match="Unable to open"):
        write_records(
            [_event()],
            kind="raw",
            storage_config=_build_storage_config(tmp_path),
        )

    assert sentinel.read_text(encoding="utf-8") == "unchanged"
    assert sentinel.stat().st_mode & 0o777 == 0o644
    assert not list(tmp_path.rglob("*.parquet"))


def test_raw_retry_recovers_private_staging_link_after_publication_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _build_storage_config(tmp_path)
    event = _event()
    assert write_records([event], kind="raw", storage_config=config) == 1
    final = next((tmp_path / "raw" / "market_events").rglob("*.parquet"))
    staging = final.parent / ".parquet-stage-interrupted"
    staging.mkdir(mode=0o700)
    os.link(final, staging / "payload.tmp")
    assert final.stat().st_nlink == 2
    monkeypatch.setattr(raw_event_writer, "MAX_INVENTORY_ENTRIES", 6)

    replay = _event(ts_ingest=datetime(2025, 1, 21, 11, tzinfo=timezone.utc))
    assert write_records([replay], kind="raw", storage_config=config) == 0

    assert final.stat().st_nlink == 1
    assert not staging.exists()


def test_raw_inventory_rejects_incompatible_physical_types(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    string_record = {key: str(value) for key, value in _event().items()}
    partition = partition_path(_event()["ts_ingest"])
    target = (
        tmp_path
        / "raw"
        / "market_events"
        / partition
        / f"batch_{'0' * 16}.parquet"
    )
    assert create_parquet_file([string_record], target) is True

    with pytest.raises(StorageError, match="incompatible physical types"):
        write_records([_event("evt-2")], kind="raw", storage_config=config)

    assert len(list((tmp_path / "raw" / "market_events").rglob("*.parquet"))) == 1


@pytest.mark.parametrize(
    "price_sql",
    [
        "100.1::FLOAT",
        "1.0000000000000000001::DECIMAL(20, 19)",
        "9007199254740993::BIGINT",
    ],
)
def test_raw_inventory_rejects_noncanonical_price_storage(
    tmp_path: Path,
    price_sql: str,
) -> None:
    config = _build_storage_config(tmp_path)
    partition = partition_path(_event()["ts_ingest"])
    target = (
        tmp_path
        / "raw"
        / "market_events"
        / partition
        / f"batch_{'0' * 16}.parquet"
    )
    target.parent.mkdir(parents=True)
    with duckdb.connect() as connection:
        connection.execute(
            "COPY (SELECT "
            "'evt-1'::VARCHAR AS event_id, 'AAPL'::VARCHAR AS symbol, "
            f"{price_sql} AS price, 10::BIGINT AS volume, "
            "TIMESTAMPTZ '2025-01-20 10:00:00+00' AS ts_event, "
            "TIMESTAMPTZ '2025-01-20 10:01:00+00' AS ts_ingest, "
            "'stooq'::VARCHAR AS source) TO ? (FORMAT PARQUET)",
            [str(target)],
        )

    with pytest.raises(StorageError, match="incompatible physical types"):
        write_records([_event()], kind="raw", storage_config=config)


def test_raw_inventory_rejects_coarse_timestamp_storage(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    partition = partition_path(_event()["ts_ingest"])
    target = (
        tmp_path
        / "raw"
        / "market_events"
        / partition
        / f"batch_{'0' * 16}.parquet"
    )
    target.parent.mkdir(parents=True)
    with duckdb.connect() as connection:
        connection.execute(
            "COPY (SELECT "
            "'evt-1'::VARCHAR AS event_id, 'AAPL'::VARCHAR AS symbol, "
            "100.0::DOUBLE AS price, 10::BIGINT AS volume, "
            "CAST(TIMESTAMP '2025-01-20 10:00:00.123456' AS TIMESTAMP_MS) "
            "AS ts_event, TIMESTAMP '2025-01-20 10:01:00' AS ts_ingest, "
            "'stooq'::VARCHAR AS source) TO ? (FORMAT PARQUET)",
            [str(target)],
        )

    with pytest.raises(StorageError, match="incompatible physical types"):
        write_records([_event()], kind="raw", storage_config=config)


def test_raw_inventory_rejects_submicrosecond_legacy_timestamps(
    tmp_path: Path,
) -> None:
    config = _build_storage_config(tmp_path)
    partition = partition_path(_event()["ts_ingest"])
    target = (
        tmp_path
        / "raw"
        / "market_events"
        / partition
        / f"batch_{'0' * 16}.parquet"
    )
    target.parent.mkdir(parents=True)
    with duckdb.connect() as connection:
        connection.execute(
            "COPY (SELECT "
            "'evt-1'::VARCHAR AS event_id, 'AAPL'::VARCHAR AS symbol, "
            "100.0::DOUBLE AS price, 10::BIGINT AS volume, "
            "CAST('2025-01-20 10:00:00.123456789' AS TIMESTAMP_NS) AS ts_event, "
            "CAST('2025-01-20 10:01:00' AS TIMESTAMP_NS) AS ts_ingest, "
            "'stooq'::VARCHAR AS source) TO ? (FORMAT PARQUET)",
            [str(target)],
        )

    with pytest.raises(StorageError, match="microsecond precision"):
        write_records([_event()], kind="raw", storage_config=config)


def test_raw_text_caps_count_duplicates_and_utf8_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(raw_event_writer, "MAX_INPUT_TEXT_BYTES", 20)
    with pytest.raises(StorageError, match="UTF-8 byte limit"):
        write_records(
            [_event(), _event()],
            kind="raw",
            storage_config=_build_storage_config(tmp_path),
        )
    assert not list(tmp_path.rglob("*.parquet"))

    monkeypatch.setattr(raw_event_writer, "MAX_INPUT_TEXT_BYTES", 16_000_000)
    config = _build_storage_config(tmp_path)
    assert write_records([_event(event_id="é")], kind="raw", storage_config=config) == 1
    monkeypatch.setattr(raw_event_writer, "MAX_TEXT_FIELD_BYTES", 1)
    with pytest.raises(StorageError, match="text exceeds"):
        write_records(
            [_event(event_id="a", symbol="A", source="s")],
            kind="raw",
            storage_config=config,
        )


def test_raw_publication_enforces_prospective_inventory_caps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _build_storage_config(tmp_path)
    assert write_records([_event()], kind="raw", storage_config=config) == 1
    existing = next((tmp_path / "raw" / "market_events").rglob("*.parquet"))

    monkeypatch.setattr(raw_event_writer, "MAX_INVENTORY_FILES", 1)
    with pytest.raises(StorageError, match="file limit"):
        write_records(
            [
                _event(
                    "evt-2",
                    ts_ingest=datetime(2025, 1, 21, 11, tzinfo=timezone.utc),
                )
            ],
            kind="raw",
            storage_config=config,
        )
    assert list((tmp_path / "raw" / "market_events").rglob("*.parquet")) == [
        existing
    ]

    monkeypatch.setattr(raw_event_writer, "MAX_INVENTORY_FILES", 10_000)
    monkeypatch.setattr(raw_event_writer, "MAX_INVENTORY_ENTRIES", 6)
    with pytest.raises(StorageError, match="entry limit"):
        write_records([_event("evt-entry")], kind="raw", storage_config=config)

    monkeypatch.setattr(raw_event_writer, "MAX_INVENTORY_ENTRIES", 60_000)
    monkeypatch.setattr(raw_event_writer, "MAX_INVENTORY_ROWS", 1)
    with pytest.raises(StorageError, match="row limit"):
        write_records([_event("evt-3")], kind="raw", storage_config=config)

    monkeypatch.setattr(raw_event_writer, "MAX_INVENTORY_ROWS", 1_000_000)
    monkeypatch.setattr(raw_event_writer, "MAX_INVENTORY_TEXT_BYTES", 14)
    with pytest.raises(StorageError, match="UTF-8 byte limit"):
        write_records([_event("evt-4")], kind="raw", storage_config=config)

    monkeypatch.setattr(raw_event_writer, "MAX_INVENTORY_TEXT_BYTES", 512_000_000)
    monkeypatch.setattr(raw_event_writer, "MAX_INVENTORY_BYTES", existing.stat().st_size)
    with pytest.raises(StorageError, match="byte limit"):
        write_records([_event("evt-5")], kind="raw", storage_config=config)

    assert list((tmp_path / "raw" / "market_events").rglob("*.parquet")) == [
        existing
    ]


def test_raw_publication_rejects_oversized_staged_file_before_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(raw_event_writer, "MAX_FILE_BYTES", 1)

    with pytest.raises(StorageError, match="Unable to publish raw parquet"):
        write_records(
            [_event()],
            kind="raw",
            storage_config=_build_storage_config(tmp_path),
        )

    assert not list(tmp_path.rglob("*.parquet"))


def test_corrupt_or_mispartitioned_raw_inventory_fails_closed(tmp_path: Path) -> None:
    config = _build_storage_config(tmp_path)
    partition = partition_path(_event()["ts_ingest"])
    corrupt = tmp_path / "raw" / "market_events" / partition / f"batch_{'0' * 16}.parquet"
    corrupt.parent.mkdir(parents=True)
    corrupt.write_bytes(b"not parquet")
    with pytest.raises(StorageError, match="unreadable"):
        write_records([_event("evt-2")], kind="raw", storage_config=config)

    corrupt.unlink()
    misplaced = _event(ts_ingest=datetime(2025, 1, 21, 11, 0, tzinfo=timezone.utc))
    _write_legacy_batch(tmp_path, [misplaced], digest="1" * 16)
    actual = next((tmp_path / "raw" / "market_events").rglob(f"batch_{'1' * 16}.parquet"))
    wrong_directory = tmp_path / "raw" / "market_events" / partition
    wrong_directory.mkdir(parents=True, exist_ok=True)
    wrong = wrong_directory / actual.name
    actual.replace(wrong)
    with pytest.raises(StorageError, match="ingest-hour partition"):
        write_records([_event("evt-2")], kind="raw", storage_config=config)


def test_alpha_vantage_bar_repoll_and_correction_use_raw_identity_contract(
    tmp_path: Path,
) -> None:
    def payload(close: str, volume: str) -> bytes:
        return json.dumps(
            {
                "Meta Data": {"2. Symbol": "IBM"},
                "Time Series (Daily)": {
                    "2025-01-03": {
                        "1. open": "100.00",
                        "2. high": "103.00",
                        "3. low": "99.00",
                        "4. close": close,
                        "5. volume": volume,
                    }
                },
            }
        ).encode("utf-8")

    first = parse_alpha_vantage_daily_response(
        payload("101.00", "1200"),
        symbol="IBM",
        ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
    )[0].model_dump()
    replay = parse_alpha_vantage_daily_response(
        payload("101.00", "1200"),
        symbol="IBM",
        ingested_at=datetime(2025, 1, 5, tzinfo=timezone.utc),
    )[0].model_dump()
    correction = parse_alpha_vantage_daily_response(
        payload("102.00", "1250"),
        symbol="IBM",
        ingested_at=datetime(2025, 1, 6, tzinfo=timezone.utc),
    )[0].model_dump()
    config = _build_storage_config(tmp_path)

    assert first["event_id"] == replay["event_id"] == correction["event_id"]
    assert write_records([first], kind="raw", storage_config=config) == 1
    assert write_records([replay], kind="raw", storage_config=config) == 0
    with pytest.raises(RawEventConflictError):
        write_records([correction], kind="raw", storage_config=config)

    assert len(_raw_records(tmp_path)) == 1
