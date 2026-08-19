from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError
from src.ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from src.ingestion.schemas import MarketEvent
from src.orchestration.run_daily_risk import DAILY_DATASETS, run_daily_risk


def _events() -> list[MarketEvent]:
    ingested = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    return [
        MarketEvent(
            event_id=alpha_vantage_daily_event_id("IBM", date(2026, 1, day)),
            symbol="IBM",
            price=price,
            volume=1_000,
            ts_event=datetime(2026, 1, day, tzinfo=timezone.utc),
            ts_ingest=ingested + timedelta(minutes=day),
            source="alpha_vantage",
        )
        for day, price in [(1, 100.0), (2, 101.0), (3, 99.0), (4, 102.0)]
    ]


def _config() -> dict[str, Any]:
    return {
        "storage": {
            "base_dir": "unused",
            "raw": {"base_path": "unused/raw", "dataset": "market_events"},
            "curated": {
                "base_path": "unused/curated",
                "datasets": {dataset: dataset for dataset in DAILY_DATASETS.values()},
            },
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }


def test_run_daily_risk_publishes_one_idempotent_record_per_call() -> None:
    writes: list[tuple[str, dict[str, Any]]] = []

    def reader(**_: Any) -> list[MarketEvent]:
        return _events()

    def writer(records: list[dict[str, Any]], *, dataset: str, **_: Any) -> int:
        assert len(records) == 1
        writes.append((dataset, records[0]))
        return 1

    summary = run_daily_risk(
        symbol=" ibm ",
        start_date=None,
        end_date=date(2026, 1, 4),
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
        storage_config_path=Path("unused.yaml"),
        reader=reader,
        writer=writer,
        config_loader=lambda _: _config(),
    )

    assert summary["symbol"] == "IBM"
    assert summary["selection"]["raw_observations"] == 4
    assert summary["curated_output"]["daily_returns"]["records_written"] == 3
    assert summary["curated_output"]["daily_volatility"]["records_written"] == 2
    assert summary["curated_output"]["daily_risk_summary"]["records_written"] == 3
    assert len(writes) == 8
    assert {dataset for dataset, _ in writes} == set(DAILY_DATASETS.values())


def test_run_daily_risk_reports_replayed_curated_records() -> None:
    summary = run_daily_risk(
        symbol="IBM",
        start_date=date(2026, 1, 4),
        end_date=date(2026, 1, 4),
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
        storage_config_path=Path("unused.yaml"),
        reader=lambda **_: _events(),
        writer=lambda *_args, **_kwargs: 0,
        config_loader=lambda _: _config(),
    )

    assert summary["curated_output"]["daily_returns"] == {
        "records_selected": 1,
        "records_written": 0,
        "records_already_present": 1,
    }
    assert summary["curated_output"]["daily_risk_summary"]["records_already_present"] == 1


def test_run_daily_risk_requires_configured_daily_datasets() -> None:
    config = _config()
    del config["storage"]["curated"]["datasets"]["daily_volatility"]

    with pytest.raises(StorageError, match="missing daily curated datasets"):
        run_daily_risk(
            symbol="IBM",
            start_date=None,
            end_date=date(2026, 1, 4),
            volatility_window=2,
            var_window=2,
            var_confidence=0.95,
            storage_config_path=Path("unused.yaml"),
            reader=lambda **_: _events(),
            writer=lambda *_args, **_kwargs: 1,
            config_loader=lambda _: config,
        )


def test_run_daily_risk_rejects_invalid_writer_results() -> None:
    with pytest.raises(StorageError, match="invalid result"):
        run_daily_risk(
            symbol="IBM",
            start_date=None,
            end_date=date(2026, 1, 4),
            volatility_window=2,
            var_window=2,
            var_confidence=0.95,
            storage_config_path=Path("unused.yaml"),
            reader=lambda **_: _events(),
            writer=lambda *_args, **_kwargs: 2,
            config_loader=lambda _: _config(),
        )
