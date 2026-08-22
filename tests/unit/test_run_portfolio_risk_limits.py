from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.portfolio_risk import parse_portfolio_definition
from src.analytics.portfolio_risk_limit_policies import (
    EffectiveDatedPortfolioRiskLimitPolicy,
)
from src.analytics.portfolio_risk_limits import RiskLimitThresholds
from src.common.exceptions import StorageError, ValidationError
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


def _policy(
    *,
    effective_from: date = date(2026, 1, 1),
    effective_to: date | None = None,
) -> EffectiveDatedPortfolioRiskLimitPolicy:
    return EffectiveDatedPortfolioRiskLimitPolicy(
        policy_id="test-policy",
        portfolio_id="us-tech-equal",
        covariance_window=3,
        annualization_days=252,
        portfolio_volatility=RiskLimitThresholds(
            warning=0.30,
            critical=0.45,
        ),
        component_concentration=RiskLimitThresholds(
            warning=0.65,
            critical=0.80,
        ),
        policy_version_id="test-policy-v1",
        effective_from=effective_from,
        effective_to=effective_to,
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

    policy = _policy()
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
        policy_loader=lambda *_: policy,
    )

    assert summary["curated_output"][OUTPUT_DATASET] == {
        "records_selected": 2,
        "records_written": 2,
        "records_already_present": 0,
    }
    assert summary["latest_status"]["status"] == "critical"
    assert len(summary["latest_status"]["metrics"]) == 2
    assert summary["policy_version"] == {
        "policy_id": "test-policy",
        "policy_version_id": "test-policy-v1",
        "policy_fingerprint": policy.fingerprint,
        "limit_definition_fingerprint": policy.limit_definition_fingerprint,
        "effective_from": "2026-01-01",
        "effective_to": None,
    }
    assert len(writes) == 2


def test_runner_passes_end_date_to_policy_loader() -> None:
    received: list[tuple[Path, str, date]] = []

    def loader(
        path: Path,
        policy_id: str,
        as_of_date: date,
    ) -> EffectiveDatedPortfolioRiskLimitPolicy:
        received.append((path, policy_id, as_of_date))
        return _policy()

    run_portfolio_risk_limits(
        policy_id="test-policy",
        limits_config_path=Path("limits.yaml"),
        portfolio_config_path=Path("portfolios.yaml"),
        end_date=date(2026, 1, 4),
        storage_config_path=Path("storage.yaml"),
        reader=lambda _: _records(),
        writer=lambda *_args, **_kwargs: 1,
        storage_config_loader=lambda _: _storage_config(),
        definition_loader=lambda *_: _definition(),
        policy_loader=loader,
    )

    assert received == [(Path("limits.yaml"), "test-policy", date(2026, 1, 4))]


def test_runner_rejects_policy_boundary_before_reading_input() -> None:
    reader_called = False

    def reader(_: Path) -> list[dict[str, object]]:
        nonlocal reader_called
        reader_called = True
        return _records()

    with pytest.raises(ValidationError, match="crosses"):
        run_portfolio_risk_limits(
            policy_id="test-policy",
            limits_config_path=Path("unused-limits.yaml"),
            portfolio_config_path=Path("unused-portfolios.yaml"),
            start_date=date(2026, 1, 3),
            end_date=date(2026, 1, 4),
            storage_config_path=Path("unused-storage.yaml"),
            reader=reader,
            writer=lambda *_args, **_kwargs: 1,
            storage_config_loader=lambda _: _storage_config(),
            definition_loader=lambda *_: _definition(),
            policy_loader=lambda *_: _policy(effective_from=date(2026, 1, 4)),
        )

    assert reader_called is False


def test_runner_rejects_end_date_outside_selected_policy() -> None:
    with pytest.raises(ValidationError, match="outside"):
        run_portfolio_risk_limits(
            policy_id="test-policy",
            limits_config_path=Path("unused-limits.yaml"),
            portfolio_config_path=Path("unused-portfolios.yaml"),
            end_date=date(2026, 1, 4),
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda _: _records(),
            writer=lambda *_args, **_kwargs: 1,
            storage_config_loader=lambda _: _storage_config(),
            definition_loader=lambda *_: _definition(),
            policy_loader=lambda *_: _policy(effective_to=date(2026, 1, 4)),
        )


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


def test_runner_rejects_invalid_policy_loader_output() -> None:
    with pytest.raises(ValidationError, match="invalid policy version"):
        run_portfolio_risk_limits(
            policy_id="test-policy",
            limits_config_path=Path("unused-limits.yaml"),
            portfolio_config_path=Path("unused-portfolios.yaml"),
            end_date=date(2026, 1, 4),
            storage_config_path=Path("unused-storage.yaml"),
            reader=lambda _: _records(),
            writer=lambda *_args, **_kwargs: 1,
            storage_config_loader=lambda _: _storage_config(),
            definition_loader=lambda *_: _definition(),
            policy_loader=lambda *_: object(),  # type: ignore[return-value]
        )
