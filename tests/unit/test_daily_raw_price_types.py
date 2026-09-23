"""Physical Boolean prices must not be converted into plausible numeric prices."""

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
def rows(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    source = [
        (f"event-{day}", "IBM", 100.0, 10,
         (datetime(2026, 1, day, tzinfo=timezone.utc) - epoch) // timedelta(microseconds=1),
         (datetime(2026, 1, day, tzinfo=timezone.utc) - epoch) // timedelta(microseconds=1),
         "alpha_vantage")
        for day in (1, 2)
    ]

    class Connection:
        def __enter__(self) -> "Connection":
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def execute(self, *args: Any) -> "Connection":
            return self

        def fetchone(self) -> tuple[int]:
            return (len(source),)

        def fetchall(self) -> list[tuple[Any, ...]]:
            return source

    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=Connection))
    monkeypatch.setattr(runner, "_raw_parquet_files", lambda config: [Path("fixture.parquet")])
    monkeypatch.setattr(
        runner, "alpha_vantage_daily_event_id", lambda symbol, day: f"event-{day.day}",
    )
    return source


def _config() -> dict[str, Any]:
    return {"storage": {
        "base_dir": "unused", "format": "parquet", "partitioning": {"granularity": "hourly"},
        "raw": {"base_path": "unused/raw", "dataset": "market_events"},
        "curated": {"base_path": "unused/curated", "datasets": {
            name: name for name in runner.DAILY_DATASETS.values()
        }},
    }}


@pytest.mark.parametrize("price", [True, False])
@pytest.mark.parametrize("through_runner", [False, True])
def test_boolean_price_rejects_before_building_or_publishing(
    rows: list[tuple[Any, ...]], monkeypatch: pytest.MonkeyPatch,
    price: bool, through_runner: bool,
) -> None:
    rows[1] = (*rows[1][:2], price, *rows[1][3:])
    original = list(rows)
    calls: list[str] = []

    def build(*args: Any, **kwargs: Any) -> Any:
        calls.append("build")
        raise AssertionError("invalid source must not reach analytics")

    def writer(*args: Any, **kwargs: Any) -> int:
        calls.append("write")
        return 1

    monkeypatch.setattr(runner, "build_daily_risk_outputs", build)
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
    assert calls == []
    assert rows == original


@pytest.mark.parametrize("price", [1, 1.0, Decimal("1.25"), "1.25"])
def test_existing_numeric_representations_retain_their_values(
    rows: list[tuple[Any, ...]], price: Any,
) -> None:
    rows[1] = (*rows[1][:2], price, *rows[1][3:])
    events = runner.load_alpha_vantage_daily_events(
        storage_config=_config(), symbol="IBM", end_date=date(2026, 1, 2),
    )
    assert events[1].price == float(price)
    assert type(events[1].price) is float
