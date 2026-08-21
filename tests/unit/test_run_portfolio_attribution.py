from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.portfolio_risk import (
    WEIGHTING_METHOD,
    PortfolioDefinition,
    parse_portfolio_definition,
)
from src.common.exceptions import StorageError
from src.orchestration.run_portfolio_attribution import (
    OUTPUT_DATASET,
    run_portfolio_attribution,
)


def _definition_payload() -> dict[str, object]:
    return {
        "portfolios": {
            "us-tech-equal": {
                "base_currency": "USD",
                "constituents": [
                    {
                        "source": "alpha_vantage",
                        "symbol": "AAPL",
                        "weight": 0.5,
                    },
                    {
                        "source": "alpha_vantage",
                        "symbol": "MSFT",
                        "weight": 0.5,
                    },
                ],
            }
        }
    }


def _definition() -> PortfolioDefinition:
    return parse_portfolio_definition(_definition_payload(), "us-tech-equal")


def _records() -> list[dict[str, object]]:
    definition = _definition()
    records: list[dict[str, object]] = []
    for day, aapl, msft in [(2, 0.10, 0.02), (3, -0.04, 0.0), (4, 0.06, 0.02)]:
        ts_event = datetime(2026, 1, day, tzinfo=timezone.utc)
        component_returns = {
            "alpha_vantage:AAPL": aapl,
            "alpha_vantage:MSFT": msft,
        }
        records.append(
            {
                "model_version": "portfolio-risk-v1",
                "calculation_id": f"portfolio-{day}",
                "portfolio_id": "us-tech-equal",
                "base_currency": "USD",
                "definition_fingerprint": definition.fingerprint,
                "weighting_method": WEIGHTING_METHOD,
                "ts_event": ts_event,
                "ts_ingest": ts_event + timedelta(hours=1),
                "constituent_count": 2,
                "weights_json": json.dumps(
                    {"alpha_vantage:AAPL": 0.5, "alpha_vantage:MSFT": 0.5}
                ),
                "component_calculation_ids_json": json.dumps(
                    {
                        "alpha_vantage:AAPL": f"AAPL-{day}",
                        "alpha_vantage:MSFT": f"MSFT-{day}",
                    }
                ),
                "component_returns_json": json.dumps(component_returns),
                "portfolio_return_1d": 0.5 * aapl + 0.5 * msft,
            }
        )
    return records


def _storage_config() -> dict[str, Any]:
    return {
        "storage": {
            "base_dir": "unused",
            "raw": {"base_path": "unused/raw", "dataset": "market_events"},
            "curated": {
                "base_path": "unused/curated",
                "datasets": {
                    "portfolio_daily_returns": "portfolio_daily_returns",
                    OUTPUT_DATASET: OUTPUT_DATASET,
                },
            },
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }


def test_run_portfolio_attribution_publishes_one_snapshot() -> None:
    writes: list[dict[str, Any]] = []

    def reader(**kwargs: Any) -> list[dict[str, object]]:
        assert kwargs["portfolio_id"] == "us-tech-equal"
        assert kwargs["definition_fingerprint"] == _definition().fingerprint
        return _records()

    def writer(records: list[dict[str, Any]], *, dataset: str, **_: Any) -> int:
        assert dataset == OUTPUT_DATASET
        assert len(records) == 1
        writes.append(records[0])
        return 1

    summary = run_portfolio_attribution(
        portfolio_id="us-tech-equal",
        portfolio_config_path=Path("unused.yaml"),
        end_date=date(2026, 1, 4),
        covariance_window=3,
        storage_config_path=Path("unused-storage.yaml"),
        reader=reader,
        writer=writer,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda *_: _definition(),
    )

    assert summary["curated_output"][OUTPUT_DATASET] == {
        "records_selected": 1,
        "records_written": 1,
        "records_already_present": 0,
    }
    assert summary["latest_metrics"]["portfolio_volatility_annualized"] > 0
    assert summary["latest_metrics"]["largest_absolute_component"] == (
        "alpha_vantage:AAPL"
    )
    assert len(writes) == 1


def test_run_portfolio_attribution_reports_replay() -> None:
    summary = run_portfolio_attribution(
        portfolio_id="us-tech-equal",
        portfolio_config_path=Path("unused.yaml"),
        end_date=date(2026, 1, 4),
        covariance_window=3,
        storage_config_path=Path("unused-storage.yaml"),
        reader=lambda **_: _records(),
        writer=lambda *_args, **_kwargs: 0,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda *_: _definition(),
    )

    assert summary["curated_output"][OUTPUT_DATASET]["records_already_present"] == 1


def test_run_portfolio_attribution_requires_configured_output_dataset() -> None:
    config = _storage_config()
    del config["storage"]["curated"]["datasets"][OUTPUT_DATASET]

    with pytest.raises(StorageError, match="missing portfolio attribution datasets"):
        run_portfolio_attribution(
            portfolio_id="us-tech-equal",
            portfolio_config_path=Path("unused.yaml"),
            end_date=date(2026, 1, 4),
            covariance_window=3,
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda **_: _records(),
            writer=lambda *_args, **_kwargs: 1,
            storage_config_loader=lambda _: config,
            definition_loader=lambda *_: _definition(),
        )


def test_run_portfolio_attribution_rejects_invalid_writer_result() -> None:
    with pytest.raises(StorageError, match="invalid result"):
        run_portfolio_attribution(
            portfolio_id="us-tech-equal",
            portfolio_config_path=Path("unused.yaml"),
            end_date=date(2026, 1, 4),
            covariance_window=3,
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda **_: _records(),
            writer=lambda *_args, **_kwargs: 2,
            storage_config_loader=lambda _: _storage_config(),
            definition_loader=lambda *_: _definition(),
        )
