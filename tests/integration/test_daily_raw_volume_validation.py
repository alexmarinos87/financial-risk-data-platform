"""Malformed physical raw volumes must not be silently repaired during reads."""

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError
from src.ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from src.orchestration.run_daily_risk import load_alpha_vantage_daily_events, run_daily_risk
from src.storage.parquet_io import create_parquet_file
from tests.storage_config_helpers import build_storage_config, write_storage_config


def _land_unvalidated(root: Path, volume: Any) -> Path:
    records = [
        {
            "event_id": alpha_vantage_daily_event_id("IBM", date(2026, 1, day)),
            "symbol": "IBM", "price": 100.0 + day, "volume": volume,
            "ts_event": datetime(2026, 1, day, tzinfo=timezone.utc),
            "ts_ingest": datetime(2026, 1, day, 1, tzinfo=timezone.utc),
            "source": "alpha_vantage",
        }
        for day in (1, 2)
    ]
    # Deliberately bypass the canonical raw-event writer to model malformed
    # upstream files. Serialization and the subsequent reader are real.
    target = root / "raw/market_events/fixture.parquet"
    assert create_parquet_file(records, target)
    return target


@pytest.mark.parametrize("volume", [1.5, -0.5, Decimal("1.5"), True, False])
def test_invalid_physical_volume_blocks_real_pipeline(tmp_path: Path, volume: Any) -> None:
    config_path = write_storage_config(tmp_path)
    target = _land_unvalidated(tmp_path, volume)
    before = target.read_bytes()
    with pytest.raises(StorageError, match="^Raw Alpha Vantage daily records are incompatible$"):
        run_daily_risk(
            symbol="IBM", start_date=None, end_date=date(2026, 1, 2),
            volatility_window=2, var_window=2, var_confidence=0.95,
            storage_config_path=config_path,
        )
    assert target.read_bytes() == before
    assert not (tmp_path / "curated").exists()


@pytest.mark.parametrize("volume", [10, 10.0, Decimal("10")])
def test_integral_physical_volume_still_reads(tmp_path: Path, volume: Any) -> None:
    target = _land_unvalidated(tmp_path, volume)
    before = target.read_bytes()
    events = load_alpha_vantage_daily_events(
        storage_config=build_storage_config(tmp_path), symbol="IBM", end_date=date(2026, 1, 2),
    )
    assert [event.volume for event in events] == [10, 10]
    assert all(type(event.volume) is int for event in events)
    assert target.read_bytes() == before
