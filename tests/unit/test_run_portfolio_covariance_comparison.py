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
from src.orchestration.run_portfolio_covariance_comparison import (
    run_portfolio_covariance_comparison,
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
        (3, -0.04, 0.0),
        (4, 0.06, 0.02),
    ]:
        ts_event = datetime(2026, 1, day, tzinfo=timezone.utc)
        component_returns = {
            "alpha_vantage:AAPL": aapl,
            "alpha_vantage:MSFT": msft,
        }
        records.append(
            {
                "model_version": "portfolio-risk-v1",
                "calculation_id": f"portfolio-{day}",
                "portfolio_id": definition.portfolio_id,
                "base_currency": definition.base_currency,
                "definition_fingerprint": definition.fingerprint,
                "weighting_method": WEIGHTING_METHOD,
                "ts_event": ts_event,
                "ts_ingest": ts_event + timedelta(hours=1),
                "constituent_count": 2,
                "weights_json": json.dumps(
                    {
                        "alpha_vantage:AAPL": 0.5,
                        "alpha_vantage:MSFT": 0.5,
                    },
                    sort_keys=True,
                ),
                "component_calculation_ids_json": json.dumps(
                    {
                        "alpha_vantage:AAPL": f"AAPL-{day}",
                        "alpha_vantage:MSFT": f"MSFT-{day}",
                    },
                    sort_keys=True,
                ),
                "component_returns_json": json.dumps(
                    component_returns,
                    sort_keys=True,
                ),
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
                    "portfolio_risk_attribution": "portfolio_risk_attribution",
                },
            },
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }


def test_comparison_publishes_aligned_sample_and_ewma_snapshots() -> None:
    writes: list[dict[str, Any]] = []

    def reader(**kwargs: Any) -> list[dict[str, object]]:
        assert kwargs["portfolio_id"] == "us-tech-equal"
        assert kwargs["definition_fingerprint"] == _definition().fingerprint
        assert kwargs["end_date"] == date(2026, 1, 4)
        return _records()

    def writer(records: list[dict[str, Any]], *, dataset: str, **_: Any) -> int:
        assert dataset == "portfolio_risk_attribution"
        assert len(records) == 1
        writes.append(records[0])
        return 1

    summary = run_portfolio_covariance_comparison(
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

    assert summary["curated_output"]["portfolio_risk_attribution"] == {
        "records_selected": 2,
        "records_written": 2,
        "records_already_present": 0,
    }
    assert [record["model_version"] for record in writes] == [
        "portfolio-attribution-v1",
        "portfolio-attribution-ewma-v1",
    ]
    assert writes[0]["input_calculation_ids_json"] == writes[1][
        "input_calculation_ids_json"
    ]
    assert summary["models"]["sample"]["covariance_method"] == (
        "sample_annualized"
    )
    assert summary["models"]["ewma"]["covariance_method"] == (
        "ewma_zero_mean_lambda_0_94_annualized"
    )
    assert summary["parameters"]["ewma_decay"] == 0.94
    assert summary["comparison"]["ewma_minus_sample_volatility"] > 0
    assert summary["comparison"]["higher_volatility_model"] == "ewma"


def test_comparison_reports_replay_counts_per_snapshot() -> None:
    results = iter([0, 1])

    summary = run_portfolio_covariance_comparison(
        portfolio_id="us-tech-equal",
        portfolio_config_path=Path("unused.yaml"),
        end_date=date(2026, 1, 4),
        covariance_window=3,
        storage_config_path=Path("unused-storage.yaml"),
        reader=lambda **_: _records(),
        writer=lambda *_args, **_kwargs: next(results),
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda *_: _definition(),
    )

    assert summary["curated_output"]["portfolio_risk_attribution"] == {
        "records_selected": 2,
        "records_written": 1,
        "records_already_present": 1,
    }


def test_comparison_rejects_invalid_writer_result() -> None:
    with pytest.raises(StorageError, match="invalid result"):
        run_portfolio_covariance_comparison(
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


def test_comparison_requires_both_attribution_datasets() -> None:
    config = _storage_config()
    del config["storage"]["curated"]["datasets"][
        "portfolio_risk_attribution"
    ]

    with pytest.raises(StorageError, match="missing portfolio attribution datasets"):
        run_portfolio_covariance_comparison(
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
