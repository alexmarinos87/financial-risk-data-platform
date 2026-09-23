"""Real Boolean Parquet columns are not daily closing prices."""

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError
from src.ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from src.orchestration.run_daily_risk import run_daily_risk
from src.storage.parquet_io import create_parquet_file
from tests.storage_config_helpers import write_storage_config


def _land(root: Path, price: Any, days: tuple[int, int]) -> Path:
    rows = [{
        "event_id": alpha_vantage_daily_event_id("IBM", date(2026, 1, day)),
        "symbol": "IBM", "source": "alpha_vantage", "price": price, "volume": 10,
        "ts_event": datetime(2026, 1, day, tzinfo=timezone.utc),
        "ts_ingest": datetime(2026, 1, day, 1, tzinfo=timezone.utc),
    } for day in days]
    target = root / "raw/market_events" / f"prices-{days[0]}.parquet"
    # Bypass the canonical writer to retain the malformed upstream physical type.
    assert create_parquet_file(rows, target)
    return target


def _run(config: Path, end_day: int) -> dict[str, Any]:
    return run_daily_risk(
        symbol="IBM", start_date=None, end_date=date(2026, 1, end_day),
        volatility_window=2, var_window=2, var_confidence=0.95, storage_config_path=config,
    )


def _bytes(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*.parquet")}


@pytest.mark.parametrize("price", [True, False])
def test_boolean_parquet_price_is_rejected_without_curated_output(
    tmp_path: Path, price: bool,
) -> None:
    config = write_storage_config(tmp_path)
    _land(tmp_path, price, (1, 2))
    before = _bytes(tmp_path)
    with pytest.raises(StorageError, match="^Raw Alpha Vantage daily records are incompatible$"):
        _run(config, 2)
    assert _bytes(tmp_path) == before
    assert not (tmp_path / "curated").exists()


@pytest.mark.parametrize("price", [1, 1.0, Decimal("1.25")])
def test_numeric_parquet_prices_still_publish_and_replay(tmp_path: Path, price: Any) -> None:
    config = write_storage_config(tmp_path)
    _land(tmp_path, price, (1, 2))
    first = _run(config, 2)
    assert first["latest_metrics"]["price_close"] == float(price)
    assert first["latest_metrics"]["return_1d"] == 0.0
    assert sum(row["records_written"] for row in first["curated_output"].values()) == 2
    before = _bytes(tmp_path)
    replay = _run(config, 2)
    assert all(row["records_written"] == 0 for row in replay["curated_output"].values())
    assert _bytes(tmp_path) == before
