from __future__ import annotations

from datetime import datetime, timezone

import duckdb

from src.storage.s3_writer import write_records


def _sample_records() -> list[dict]:
    return [
        {
            "event_id": "evt-1",
            "symbol": "AAPL",
            "price": 101.0,
            "volume": 10,
            "ts_event": datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            "ts_ingest": datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc),
            "source": "stooq",
        },
        {
            "event_id": "evt-2",
            "symbol": "AAPL",
            "price": 102.0,
            "volume": 9,
            "ts_event": datetime(2025, 1, 1, 1, 0, tzinfo=timezone.utc),
            "ts_ingest": datetime(2025, 1, 1, 1, 2, tzinfo=timezone.utc),
            "source": "stooq",
        },
    ]


def test_write_records_is_partitioned_and_idempotent(tmp_path):
    root_path = tmp_path / "lake"
    run_id = "test-run-001"

    written_first = write_records(
        _sample_records(),
        layer="bronze",
        dataset="market_events",
        root_path=root_path,
        partition_field="ts_ingest",
        run_id=run_id,
        contract_version="v1",
    )
    written_second = write_records(
        _sample_records(),
        layer="bronze",
        dataset="market_events",
        root_path=root_path,
        partition_field="ts_ingest",
        run_id=run_id,
        contract_version="v1",
    )

    assert written_first == 2
    assert written_second == 2

    files = sorted(
        root_path.glob(
            "bronze/market_events/contract_version=v1/**/run_id=test-run-001.parquet"
        )
    )
    assert len(files) == 2

    parquet_glob = (
        root_path
        / "bronze/market_events/contract_version=v1/**/run_id=test-run-001.parquet"
    )
    conn = duckdb.connect()
    try:
        row_count = conn.execute(
            "SELECT COUNT(*) "
            f"FROM read_parquet('{parquet_glob.as_posix()}', hive_partitioning=true)"
        ).fetchone()[0]
    finally:
        conn.close()

    assert row_count == 2
