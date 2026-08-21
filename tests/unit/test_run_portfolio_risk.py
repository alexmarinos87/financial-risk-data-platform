from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.portfolio_risk import parse_portfolio_definition
from src.common.exceptions import StorageError, ValidationError
from src.orchestration.run_portfolio_risk import OUTPUT_DATASETS, run_portfolio_risk


def _definition():
    return parse_portfolio_definition(
        {
            "portfolios": {
                "us-tech-equal": {
                    "base_currency": "USD",
                    "constituents": [
                        {"source": "alpha_vantage", "symbol": "AAPL", "weight": 0.5},
                        {"source": "alpha_vantage", "symbol": "MSFT", "weight": 0.5},
                    ],
                }
            }
        },
        "us-tech-equal",
    )


def _records() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day, aapl, msft in [(2, 0.10, 0.02), (3, -0.04, 0.0), (4, 0.06, 0.02)]:
        event = datetime(2026, 1, day, tzinfo=timezone.utc)
        for symbol, value in [("AAPL", aapl), ("MSFT", msft)]:
            rows.append(
                {
                    "model_version": "daily-risk-v2",
                    "calculation_id": f"{symbol}-{day}",
                    "source": "alpha_vantage",
                    "symbol": symbol,
                    "source_event_id": f"{symbol}-event-{day}",
                    "ts_event": event,
                    "ts_ingest": event + timedelta(hours=1),
                    "return_1d": value,
                }
            )
    return rows


def _storage_config() -> dict[str, Any]:
    datasets = {
        "daily_returns": "daily_returns",
        "portfolio_daily_returns": "portfolio_daily_returns",
        "portfolio_daily_risk_summary": "portfolio_daily_risk_summary",
    }
    return {
        "storage": {
            "base_dir": "unused",
            "raw": {"base_path": "unused/raw", "dataset": "market_events"},
            "curated": {"base_path": "unused/curated", "datasets": datasets},
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }


def test_run_portfolio_risk_publishes_versioned_outputs() -> None:
    writes: list[tuple[str, dict[str, Any]]] = []

    def writer(records: list[dict[str, Any]], *, dataset: str, **_: Any) -> int:
        assert len(records) == 1
        writes.append((dataset, records[0]))
        return 1

    summary = run_portfolio_risk(
        portfolio_id="us-tech-equal",
        portfolio_config_path=Path("unused.yaml"),
        start_date=None,
        end_date=date(2026, 1, 4),
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
        storage_config_path=Path("unused-storage.yaml"),
        reader=lambda **_: _records(),
        writer=writer,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda _path, _portfolio_id: _definition(),
    )

    assert summary["portfolio_id"] == "us-tech-equal"
    assert summary["selection"]["aligned_dates"] == 3
    assert summary["latest_metrics"]["history_status"] == "ready"
    assert summary["curated_output"]["portfolio_daily_returns"]["records_written"] == 3
    assert (
        summary["curated_output"]["portfolio_daily_risk_summary"]["records_written"]
        == 3
    )
    assert len(writes) == 6
    assert {dataset for dataset, _ in writes} == set(OUTPUT_DATASETS.values())


def test_run_portfolio_risk_reports_idempotent_replay() -> None:
    summary = run_portfolio_risk(
        portfolio_id="us-tech-equal",
        portfolio_config_path=Path("unused.yaml"),
        start_date=date(2026, 1, 4),
        end_date=date(2026, 1, 4),
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
        storage_config_path=Path("unused-storage.yaml"),
        reader=lambda **_: _records(),
        writer=lambda *_args, **_kwargs: 0,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda _path, _portfolio_id: _definition(),
    )

    assert summary["curated_output"]["portfolio_daily_returns"] == {
        "records_selected": 1,
        "records_written": 0,
        "records_already_present": 1,
    }
    assert (
        summary["curated_output"]["portfolio_daily_risk_summary"][
            "records_already_present"
        ]
        == 1
    )


def test_run_portfolio_risk_requires_configured_datasets() -> None:
    config = _storage_config()
    del config["storage"]["curated"]["datasets"]["portfolio_daily_risk_summary"]

    with pytest.raises(StorageError, match="missing portfolio datasets"):
        run_portfolio_risk(
            portfolio_id="us-tech-equal",
            portfolio_config_path=Path("unused.yaml"),
            start_date=None,
            end_date=date(2026, 1, 4),
            volatility_window=2,
            var_window=2,
            var_confidence=0.95,
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda **_: _records(),
            writer=lambda *_args, **_kwargs: 1,
            storage_config_loader=lambda _: config,
            definition_loader=lambda _path, _portfolio_id: _definition(),
        )


def test_run_portfolio_risk_rejects_invalid_writer_result() -> None:
    with pytest.raises(StorageError, match="invalid result"):
        run_portfolio_risk(
            portfolio_id="us-tech-equal",
            portfolio_config_path=Path("unused.yaml"),
            start_date=None,
            end_date=date(2026, 1, 4),
            volatility_window=2,
            var_window=2,
            var_confidence=0.95,
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda **_: _records(),
            writer=lambda *_args, **_kwargs: 2,
            storage_config_loader=lambda _: _storage_config(),
            definition_loader=lambda _path, _portfolio_id: _definition(),
        )


def test_run_portfolio_risk_rejects_invalid_dates_before_reading() -> None:
    with pytest.raises(ValidationError, match="start_date"):
        run_portfolio_risk(
            portfolio_id="us-tech-equal",
            portfolio_config_path=Path("unused.yaml"),
            start_date=date(2026, 1, 5),
            end_date=date(2026, 1, 4),
            volatility_window=2,
            var_window=2,
            var_confidence=0.95,
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda **_: (_ for _ in ()).throw(AssertionError("reader called")),
            writer=lambda *_args, **_kwargs: 1,
            storage_config_loader=lambda _: _storage_config(),
            definition_loader=lambda _path, _portfolio_id: _definition(),
        )
