from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.portfolio_attribution_ewma import (
    CORRELATION_METHOD,
    COVARIANCE_METHOD,
    MODEL_VERSION as ATTRIBUTION_MODEL_VERSION,
)
from src.analytics.portfolio_risk import (
    WEIGHTING_METHOD,
    parse_portfolio_definition,
)
from src.analytics.portfolio_risk_limit_method_policies import (
    EWMA_METHOD_CONTRACT,
    MethodAwarePortfolioRiskLimitPolicy,
)
from src.analytics.portfolio_risk_limit_policies import (
    EffectiveDatedPortfolioRiskLimitPolicy,
)
from src.analytics.portfolio_risk_limits import RiskLimitThresholds
from src.common.exceptions import StorageError
from src.orchestration.run_method_aware_portfolio_risk_limits import (
    OUTPUT_DATASET,
    run_method_aware_portfolio_risk_limits,
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


def _policy() -> MethodAwarePortfolioRiskLimitPolicy:
    base = EffectiveDatedPortfolioRiskLimitPolicy(
        policy_id="us-tech-standard",
        portfolio_id="us-tech-equal",
        covariance_window=20,
        annualization_days=252,
        portfolio_volatility=RiskLimitThresholds(warning=0.30, critical=0.45),
        component_concentration=RiskLimitThresholds(
            warning=0.65,
            critical=0.80,
        ),
        policy_version_id="us-tech-standard-v1",
        effective_from=date(2026, 1, 1),
        effective_to=None,
    )
    return MethodAwarePortfolioRiskLimitPolicy(
        method_policy_id="us-tech-ewma",
        base_policy=base,
        method=EWMA_METHOD_CONTRACT,
    )


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
                    "portfolio_risk_attribution": (
                        "portfolio_risk_attribution"
                    ),
                    OUTPUT_DATASET: OUTPUT_DATASET,
                },
            },
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }


def _records() -> list[dict[str, object]]:
    definition = _definition()
    ts_event = datetime(2026, 1, 20, tzinfo=timezone.utc)
    return [
        {
            "calculation_id": "ewma-attribution-20",
            "model_version": ATTRIBUTION_MODEL_VERSION,
            "portfolio_id": "us-tech-equal",
            "base_currency": "USD",
            "definition_fingerprint": definition.fingerprint,
            "weighting_method": WEIGHTING_METHOD,
            "covariance_method": COVARIANCE_METHOD,
            "correlation_method": CORRELATION_METHOD,
            "covariance_window": 20,
            "annualization_days": 252,
            "ts_event": ts_event,
            "ts_ingest": ts_event + timedelta(hours=1),
            "portfolio_volatility_annualized": 0.48,
            "volatility_status": "positive",
            "component_contribution_share_json": json.dumps(
                {
                    "alpha_vantage:AAPL": 0.82,
                    "alpha_vantage:MSFT": 0.18,
                },
                sort_keys=True,
            ),
        }
    ]


def test_runner_publishes_method_bound_evaluations() -> None:
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

    summary = run_method_aware_portfolio_risk_limits(
        method_policy_id="us-tech-ewma",
        method_policies_config_path=Path("unused-methods.yaml"),
        limits_config_path=Path("unused-limits.yaml"),
        portfolio_config_path=Path("unused-portfolios.yaml"),
        end_date=date(2026, 1, 20),
        storage_config_path=Path("unused-storage.yaml"),
        reader=lambda _: _records(),
        writer=writer,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda *_: _definition(),
        method_policy_loader=lambda **_: _policy(),
    )

    assert summary["method_policy"]["method_policy_id"] == "us-tech-ewma"
    assert summary["method_policy"]["covariance_method"] == COVARIANCE_METHOD
    assert summary["curated_output"][OUTPUT_DATASET] == {
        "records_selected": 2,
        "records_written": 2,
        "records_already_present": 0,
    }
    assert summary["latest_status"]["status"] == "critical"
    assert len(writes) == 2
    assert {row["attribution_model_version"] for row in writes} == {
        ATTRIBUTION_MODEL_VERSION
    }


def test_runner_reports_replay_without_duplicate_publication() -> None:
    summary = run_method_aware_portfolio_risk_limits(
        method_policy_id="us-tech-ewma",
        method_policies_config_path=Path("unused-methods.yaml"),
        limits_config_path=Path("unused-limits.yaml"),
        portfolio_config_path=Path("unused-portfolios.yaml"),
        end_date=date(2026, 1, 20),
        storage_config_path=Path("unused-storage.yaml"),
        reader=lambda _: _records(),
        writer=lambda *_args, **_kwargs: 0,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda *_: _definition(),
        method_policy_loader=lambda **_: _policy(),
    )

    assert summary["curated_output"][OUTPUT_DATASET][
        "records_already_present"
    ] == 2


def test_runner_rejects_invalid_writer_result() -> None:
    with pytest.raises(StorageError, match="invalid result"):
        run_method_aware_portfolio_risk_limits(
            method_policy_id="us-tech-ewma",
            method_policies_config_path=Path("unused-methods.yaml"),
            limits_config_path=Path("unused-limits.yaml"),
            portfolio_config_path=Path("unused-portfolios.yaml"),
            end_date=date(2026, 1, 20),
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda _: _records(),
            writer=lambda *_args, **_kwargs: 2,
            storage_config_loader=lambda _: _storage_config(),
            definition_loader=lambda *_: _definition(),
            method_policy_loader=lambda **_: _policy(),
        )


def test_runner_requires_output_dataset() -> None:
    config = _storage_config()
    del config["storage"]["curated"]["datasets"][OUTPUT_DATASET]

    with pytest.raises(StorageError, match="missing method-aware"):
        run_method_aware_portfolio_risk_limits(
            method_policy_id="us-tech-ewma",
            method_policies_config_path=Path("unused-methods.yaml"),
            limits_config_path=Path("unused-limits.yaml"),
            portfolio_config_path=Path("unused-portfolios.yaml"),
            end_date=date(2026, 1, 20),
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda _: _records(),
            writer=lambda *_args, **_kwargs: 1,
            storage_config_loader=lambda _: config,
            definition_loader=lambda *_: _definition(),
            method_policy_loader=lambda **_: _policy(),
        )
