from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from src.ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from src.ingestion.schemas import MarketEvent
from src.orchestration.run_daily_risk import run_daily_risk
from src.storage.s3_writer import write_records
from tests.storage_config_helpers import build_storage_config, write_storage_config


def _raw_events() -> list[dict]:
    ingested_at = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    return [
        MarketEvent(
            event_id=alpha_vantage_daily_event_id("IBM", date(2026, 1, day)),
            symbol="IBM",
            price=price,
            volume=1_000 + day,
            ts_event=datetime(2026, 1, day, tzinfo=timezone.utc),
            ts_ingest=ingested_at + timedelta(minutes=day),
            source="alpha_vantage",
        ).model_dump()
        for day, price in [(1, 100.0), (2, 101.0), (3, 99.0), (4, 102.0)]
    ]


def test_daily_risk_reads_immutable_raw_and_replays_curated_writes(tmp_path: Path) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    assert write_records(
        _raw_events(),
        kind="raw",
        dataset="market_events",
        storage_config=storage_config,
    ) == 4

    first = run_daily_risk(
        symbol="IBM",
        start_date=None,
        end_date=date(2026, 1, 4),
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
        storage_config_path=storage_config_path,
    )
    second = run_daily_risk(
        symbol="IBM",
        start_date=None,
        end_date=date(2026, 1, 4),
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
        storage_config_path=storage_config_path,
    )

    assert first["curated_output"]["daily_returns"]["records_written"] == 3
    assert first["curated_output"]["daily_volatility"]["records_written"] == 2
    assert first["curated_output"]["daily_risk_summary"]["records_written"] == 3
    assert second["curated_output"]["daily_returns"]["records_written"] == 0
    assert second["curated_output"]["daily_volatility"]["records_written"] == 0
    assert second["curated_output"]["daily_risk_summary"]["records_written"] == 0
    assert second["latest_metrics"]["calculation_id"] == first["latest_metrics"]["calculation_id"]
