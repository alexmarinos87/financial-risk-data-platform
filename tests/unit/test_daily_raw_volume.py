"""The raw reader must validate volumes before any lossy integer conversion."""

import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.common.exceptions import StorageError
from src.orchestration import run_daily_risk as runner


@pytest.fixture
def raw_rows(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    rows = []
    for day in (1, 2):
        instant = datetime(2026, 1, day, tzinfo=timezone.utc)
        micros = (instant - epoch) // timedelta(microseconds=1)
        rows.append((f"event-{day}", "IBM", 100.0 + day, 10, micros, micros, "alpha_vantage"))

    class Connection:
        def __enter__(self) -> "Connection":
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def execute(self, *args: Any) -> "Connection":
            return self

        def fetchone(self) -> tuple[int]:
            return (len(rows),)

        def fetchall(self) -> list[tuple[Any, ...]]:
            return rows

    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=Connection))
    monkeypatch.setattr(runner, "_raw_parquet_files", lambda config: [Path("fixture.parquet")])
    monkeypatch.setattr(
        runner, "alpha_vantage_daily_event_id", lambda symbol, day: f"event-{day.day}",
    )
    return rows


def _config() -> dict[str, Any]:
    return {"storage": {
        "base_dir": "unused", "format": "parquet", "partitioning": {"granularity": "hourly"},
        "raw": {"base_path": "unused/raw", "dataset": "market_events"},
        "curated": {"base_path": "unused/curated", "datasets": {
            dataset: dataset for dataset in runner.DAILY_DATASETS.values()
        }},
    }}


@pytest.mark.parametrize("volume", [0.5, -0.5, Decimal("1.5"), Decimal("-0.5"), True, False])
@pytest.mark.parametrize("through_runner", [False, True])
def test_invalid_volume_cannot_be_truncated_or_published(
    raw_rows: list[tuple[Any, ...]], volume: Any, through_runner: bool,
) -> None:
    raw_rows[1] = (*raw_rows[1][:3], volume, *raw_rows[1][4:])
    before = list(raw_rows)
    writes: list[Any] = []

    def writer(records: Any, **kwargs: Any) -> int:
        writes.append(records)
        return len(records)

    with pytest.raises(StorageError, match="^Raw Alpha Vantage daily records are incompatible$"):
        if through_runner:
            runner.run_daily_risk(
                symbol="IBM", start_date=None, end_date=date(2026, 1, 2),
                volatility_window=2, var_window=2, var_confidence=0.95,
                storage_config_path=Path("unused.yaml"), config_loader=lambda path: _config(),
                writer=writer,
            )
        else:
            runner.load_alpha_vantage_daily_events(
                storage_config=_config(), symbol="IBM", end_date=date(2026, 1, 2),
            )
    assert writes == []
    assert raw_rows == before


@pytest.mark.parametrize("volume", [0, 10, 10.0, Decimal("10"), "10"])
def test_exact_integer_representations_still_validate(
    raw_rows: list[tuple[Any, ...]], volume: Any,
) -> None:
    raw_rows[1] = (*raw_rows[1][:3], volume, *raw_rows[1][4:])
    events = runner.load_alpha_vantage_daily_events(
        storage_config=_config(), symbol="IBM", end_date=date(2026, 1, 2),
    )
    assert len(events) == 2
    assert events[1].volume == int(volume)
    assert type(events[1].volume) is int
