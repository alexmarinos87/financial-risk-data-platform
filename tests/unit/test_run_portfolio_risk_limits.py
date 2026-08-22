from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.portfolio_risk import parse_portfolio_definition
from src.analytics.portfolio_risk_limits import parse_portfolio_risk_limit_policy
from src.common.exceptions import StorageError
from src.orchestration.run_portfolio_risk_limits import (
    OUTPUT_DATASET,
    run_portfolio_risk_limits,
)


def _definition():
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


def _policy():
    return parse_portfolio_risk_limit_policy(
        {
            "policies": {
                "test-policy": {
                    "portfolio_id": "us-tech-equal",
                    "covariance_window": 3,
                    "annualization_days": 252,
                    "limits": {
                        "portfolio_volatility_annualized": {
                            "warning": 0.30,
                            "critical": 0.45,
                        },
                        "largest_absolute_component_contribution_share": {
                            "warning": 0.65,
                            "critical": 0.80,
                        },
                    },
                }
            }
        },
        "test-policy",
    )


def _storage_config() -> dict[str, Any]:
    return {
        "storage": {
            "base_dir": "unused",
            "raw": {"base_path": "unused/raw", "dataset": "market_events"},
            "curated": {
                "base_path": "unused/curated",
                "datasets": {
                    "portfolio_risk_attribution": "portfolio_risk_attribution",
                    OUTPUT_DATASET: OUTPUT_DATASET,
                },
            },
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }


def _records() -> list[dict[str, object]]:
    definition = _definition()
    records: list[dict[str, object]] = []
    for day, volatility, aapl, msft in [
        (3, 0.20, 0.55, 0.45),
        (4, 0.50, 0.82, 0.18),
    ]:
        event = datetime(2026, 1, day, tzinfo=timezone.utc)
        records.append(
            {
                "model_version": "portfolio-attribution-v1",
                "calculation_id": f"attribution-{day}",
                "portfolio_id": "us-tech-equal",
                "base_currency": "USD",
                "definition_fingerprint": definition.fingerprint,
                "weighting_method": "constant_weight_daily_rebalanced",
                "covariance_method": "sample_annualized",
                "correlation_method": "pearson",
                "covariance_window": 3,
                "annualization_days": 252,
                "ts_event": event,
                "ts_ingest": event + timedelta(hours=1),
                "portfolio_volatility_annualized": volatility,
                "volatility_status": "positive",
                "component_contribution_share_json": json.dumps(
                    {
                        "alpha_vantage:AAPL": aapl,
                        "alpha_vantage:MSFT": msft,
                    }
                ),
            }
        )
    return records


def test_runner_publishes_and_summarises_limit_evaluations() -> None:
    writes: list[dict[str, Any]] = []

    def writer(records: list[dict[str, Any]], *, dataset: str, **_: Any) -> int:
        assert dataset == OUTPUT_DATASET
        writes.append(records[0])
        return 1

    summary = run_portfolio_risk_limits(
        policy_id="test-policy",
        limits_config_path=Path("unused-limits.yaml"),
        portfolio_config_path=Path("unused-portfolios.yaml"),
        start_date=date(2026, 1, 4),
        end_date=date(2026, 1, 4),
        storage_config_path=Path("unused-storage.yaml"),
        reader=lambda _: _records(),
        writer=writer,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda *_: _definition(),
        policy_loader=lambda *_: _policy(),
    )

    assert summary["curated_output"][OUTPUT_DATASET] == {
        "records_selected": 2,
        "records_written": 2,
        "records_already_present": 0,
    }
    assert summary["latest_status"]["status"] == "critical"
    assert len(summary["latest_status"]["metrics"]) == 2
    assert len(writes) == 2


def test_runner_reports_replay_and_rejects_invalid_writer_result() -> None:
    replay = run_portfolio_risk_limits(
        policy_id="test-policy",
        limits_config_path=Path("unused-limits.yaml"),
        portfolio_config_path=Path("unused-portfolios.yaml"),
        end_date=date(2026, 1, 4),
        storage_config_path=Path("unused-storage.yaml"),
        reader=lambda _: _records(),
        writer=lambda *_args, **_kwargs: 0,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda *_: _definition(),
        policy_loader=lambda *_: _policy(),
    )
    assert replay["curated_output"][OUTPUT_DATASET]["records_written"] == 0
    assert replay["curated_output"][OUTPUT_DATASET][
        "records_already_present"
    ] == 4

    with pytest.raises(StorageError, match="invalid result"):
        run_portfolio_risk_limits(
            policy_id="test-policy",
            limits_config_path=Path("unused-limits.yaml"),
            portfolio_config_path=Path("unused-portfolios.yaml"),
            end_date=date(2026, 1, 4),
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda _: _records(),
            writer=lambda *_args, **_kwargs: 2,
            storage_config_loader=lambda _: _storage_config(),
            definition_loader=lambda *_: _definition(),
            policy_loader=lambda *_: _policy(),
        )
