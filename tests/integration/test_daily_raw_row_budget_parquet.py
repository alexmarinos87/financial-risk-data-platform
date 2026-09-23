"""Bound real Parquet fetches when a known input file changes after counting."""

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest

from src.common.exceptions import StorageError
from src.ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from src.orchestration import run_daily_risk as runner
from src.storage.parquet_io import create_parquet_file
from tests.storage_config_helpers import build_storage_config, write_storage_config


def _land(path: Path, count: int) -> None:
    records = [
        {
            "event_id": alpha_vantage_daily_event_id("IBM", date(2026, 1, day)),
            "symbol": "IBM", "price": 100.0 + day, "volume": 10,
            "ts_event": datetime(2026, 1, day, tzinfo=timezone.utc),
            "ts_ingest": datetime(2026, 1, day, 1, tzinfo=timezone.utc),
            "source": "alpha_vantage",
        }
        for day in range(1, count + 1)
    ]
    assert create_parquet_file(records, path)


@pytest.mark.parametrize("actual", [3, 4, 7])
def test_changed_file_never_bypasses_the_fetch_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, actual: int,
) -> None:
    config_path = write_storage_config(tmp_path)
    target = tmp_path / "raw/market_events/fixture.parquet"
    replacement = tmp_path / "staged/expanded.parquet"
    _land(target, 2)
    _land(replacement, actual)
    expected_bytes = replacement.read_bytes()
    real_connect = duckdb.connect
    fetched: list[int] = []
    closed: list[bool] = []
    counts: list[int] = []

    class Connection:
        def __init__(self) -> None:
            self.connection = real_connect()

        def __enter__(self) -> "Connection":
            return self

        def __exit__(self, *args: Any) -> None:
            self.connection.close()
            closed.append(True)

        def execute(self, *args: Any, **kwargs: Any) -> "Connection":
            self.connection.execute(*args, **kwargs)
            return self

        def fetchone(self) -> Any:
            row = self.connection.fetchone()
            assert row == (2,)
            counts.append(row[0])
            # Explicitly model an external file replacement. The canonical
            # immutable writer does not perform this mutation in normal use.
            replacement.replace(target)
            return row

        def fetchall(self) -> Any:
            rows = self.connection.fetchall()
            fetched.append(len(rows))
            return rows

    monkeypatch.setattr(duckdb, "connect", Connection)
    monkeypatch.setattr(runner, "MAX_RAW_ROWS", 3)
    if actual <= 3:
        events = runner.load_alpha_vantage_daily_events(
            storage_config=build_storage_config(tmp_path), symbol="IBM", end_date=date(2026, 1, 9),
        )
        assert [event.event_id for event in events] == [
            alpha_vantage_daily_event_id("IBM", date(2026, 1, day))
            for day in range(1, actual + 1)
        ]
    else:
        with pytest.raises(StorageError, match="^Raw daily storage exceeds the row scan limit$"):
            runner.run_daily_risk(
                symbol="IBM", start_date=None, end_date=date(2026, 1, 9),
                volatility_window=2, var_window=2, var_confidence=0.95,
                storage_config_path=config_path,
            )
    assert counts == [2]
    assert fetched == [min(actual, 4)]
    assert closed == [True]
    assert target.read_bytes() == expected_bytes
    assert not (tmp_path / "curated").exists()
