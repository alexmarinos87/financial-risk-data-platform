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
from src.common.exceptions import StorageError, ValidationError
from src.orchestration.run_portfolio_attribution_history import (
    OUTPUT_DATASET,
    run_portfolio_attribution_history,
)


def _definition() -> PortfolioDefinition:
    return parse_portfolio_definition(
        {
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
        },
        "us-tech-equal",
    )


def _records() -> list[dict[str, object]]:
    definition = _definition()
    records: list[dict[str, object]] = []
    for day, aapl, msft in [
        (2, 0.10, 0.02),
        (3, -0.04, 0.00),
        (4, 0.06, 0.02),
        (5, 0.01, -0.01),
    ]:
        event = datetime(2026, 1, day, tzinfo=timezone.utc)
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
                "ts_event": event,
                "ts_ingest": event + timedelta(hours=1),
                "constituent_count": 2,
                "weights_json": json.dumps(
                    {
                        "alpha_vantage:AAPL": 0.5,
                        "alpha_vantage:MSFT": 0.5,
                    }
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
            "raw": {
                "base_path": "unused/raw",
                "dataset": "market_events",
            },
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


def test_runner_publishes_each_selected_snapshot() -> None:
    writes: list[dict[str, Any]] = []

    def writer(
        records: list[dict[str, Any]],
        *,
        dataset: str,
        **_: Any,
    ) -> int:
        assert dataset == OUTPUT_DATASET
        assert len(records) == 1
        writes.append(records[0])
        return 1

    summary = run_portfolio_attribution_history(
        portfolio_id="us-tech-equal",
        portfolio_config_path=Path("unused.yaml"),
        end_date=date(2026, 1, 5),
        covariance_window=3,
        max_snapshots=10,
        storage_config_path=Path("unused-storage.yaml"),
        reader=lambda **_: _records(),
        writer=writer,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda *_: _definition(),
    )

    assert summary["curated_output"][OUTPUT_DATASET] == {
        "records_selected": 2,
        "records_written": 2,
        "records_already_present": 0,
    }
    assert [record["ts_event"].date() for record in writes] == [
        date(2026, 1, 4),
        date(2026, 1, 5),
    ]
    assert summary["latest_metrics"]["portfolio_volatility_annualized"] >= 0


def test_runner_start_date_filters_and_reports_replay() -> None:
    summary = run_portfolio_attribution_history(
        portfolio_id="us-tech-equal",
        portfolio_config_path=Path("unused.yaml"),
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        covariance_window=3,
        max_snapshots=10,
        storage_config_path=Path("unused-storage.yaml"),
        reader=lambda **_: _records(),
        writer=lambda *_args, **_kwargs: 0,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda *_: _definition(),
    )

    assert summary["curated_output"][OUTPUT_DATASET] == {
        "records_selected": 1,
        "records_written": 0,
        "records_already_present": 1,
    }
    assert summary["selection"]["first_snapshot_date"] == "2026-01-05"


def test_runner_rejects_invalid_range_before_io() -> None:
    with pytest.raises(ValidationError, match="start_date"):
        run_portfolio_attribution_history(
            portfolio_id="us-tech-equal",
            portfolio_config_path=Path("unused.yaml"),
            start_date=date(2026, 1, 6),
            end_date=date(2026, 1, 5),
            covariance_window=3,
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda **_: (_ for _ in ()).throw(AssertionError()),
            writer=lambda *_args, **_kwargs: 1,
            storage_config_loader=lambda _: (_ for _ in ()).throw(
                AssertionError()
            ),
            definition_loader=lambda *_: _definition(),
        )


def test_runner_rejects_invalid_writer_result() -> None:
    with pytest.raises(StorageError, match="invalid result"):
        run_portfolio_attribution_history(
            portfolio_id="us-tech-equal",
            portfolio_config_path=Path("unused.yaml"),
            end_date=date(2026, 1, 5),
            covariance_window=3,
            max_snapshots=10,
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda **_: _records(),
            writer=lambda *_args, **_kwargs: 2,
            storage_config_loader=lambda _: _storage_config(),
            definition_loader=lambda *_: _definition(),
        )
