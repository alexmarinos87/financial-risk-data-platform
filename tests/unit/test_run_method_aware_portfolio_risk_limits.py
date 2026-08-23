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
from src.common.exceptions import StorageError, ValidationError
from src.orchestration.run_method_aware_portfolio_risk_limits import (
    OUTPUT_DATASET,
    run_method_aware_portfolio_risk_limits,
)
from src.warehouse.model_approval_contract import build_model_contract
from src.warehouse.model_approval_gate import (
    GATE_MODEL_VERSION,
    RISK_LIMIT_USE_CASE,
    ModelApprovalGateEvidence,
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


def _approval_evidence() -> ModelApprovalGateEvidence:
    policy = _policy()
    contract = build_model_contract(
        attribution_model_version=policy.method.attribution_model_version,
        weighting_method=policy.method.weighting_method,
        covariance_method=policy.method.covariance_method,
        correlation_method=policy.method.correlation_method,
    )
    return ModelApprovalGateEvidence(
        gate_evidence_id="model-approval-gate-v1-test",
        model_version=GATE_MODEL_VERSION,
        use_case=RISK_LIMIT_USE_CASE,
        method_policy_fingerprint=policy.fingerprint,
        contract_fingerprint=contract.contract_fingerprint,
        attribution_model_version=contract.attribution_model_version,
        weighting_method=contract.weighting_method,
        covariance_method=contract.covariance_method,
        correlation_method=contract.correlation_method,
        approval_required=True,
        decision="approved",
        approval_id="model-approval-v1-0123456789abcdef01234567",
        approved_at=datetime(2026, 1, 10, 12, tzinfo=timezone.utc),
        approved_by="model-risk@example.test",
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


def _runner_kwargs() -> dict[str, Any]:
    return {
        "method_policy_id": "us-tech-ewma",
        "method_policies_config_path": Path("unused-methods.yaml"),
        "limits_config_path": Path("unused-limits.yaml"),
        "portfolio_config_path": Path("unused-portfolios.yaml"),
        "end_date": date(2026, 1, 20),
        "storage_config_path": Path("unused-storage.yaml"),
        "storage_config_loader": lambda _: _storage_config(),
        "definition_loader": lambda *_: _definition(),
        "method_policy_loader": lambda **_: _policy(),
        "approval_gate_resolver": lambda **_: _approval_evidence(),
    }


def test_runner_publishes_method_bound_evaluations_after_approval() -> None:
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
        **_runner_kwargs(),
        reader=lambda _: _records(),
        writer=writer,
    )

    assert summary["method_policy"]["method_policy_id"] == "us-tech-ewma"
    assert summary["method_policy"]["covariance_method"] == COVARIANCE_METHOD
    assert summary["model_approval_gate"]["decision"] == "approved"
    assert summary["model_approval_gate"]["approval_id"] == (
        "model-approval-v1-0123456789abcdef01234567"
    )
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
        **_runner_kwargs(),
        reader=lambda _: _records(),
        writer=lambda *_args, **_kwargs: 0,
    )

    assert summary["curated_output"][OUTPUT_DATASET][
        "records_already_present"
    ] == 2


def test_runner_fails_approval_before_attribution_read_or_publication() -> None:
    reads = 0
    writes = 0

    def reader(_: Path) -> list[dict[str, object]]:
        nonlocal reads
        reads += 1
        return _records()

    def writer(*_args: Any, **_kwargs: Any) -> int:
        nonlocal writes
        writes += 1
        return 1

    kwargs = _runner_kwargs()
    kwargs["approval_gate_resolver"] = lambda **_: (_ for _ in ()).throw(
        ValidationError("current approval missing")
    )
    with pytest.raises(ValidationError, match="current approval missing"):
        run_method_aware_portfolio_risk_limits(
            **kwargs,
            reader=reader,
            writer=writer,
        )

    assert reads == 0
    assert writes == 0


def test_runner_rejects_invalid_writer_result() -> None:
    with pytest.raises(StorageError, match="invalid result"):
        run_method_aware_portfolio_risk_limits(
            **_runner_kwargs(),
            reader=lambda _: _records(),
            writer=lambda *_args, **_kwargs: 2,
        )


def test_runner_requires_output_dataset() -> None:
    config = _storage_config()
    del config["storage"]["curated"]["datasets"][OUTPUT_DATASET]
    kwargs = _runner_kwargs()
    kwargs["storage_config_loader"] = lambda _: config

    with pytest.raises(StorageError, match="missing method-aware"):
        run_method_aware_portfolio_risk_limits(
            **kwargs,
            reader=lambda _: _records(),
            writer=lambda *_args, **_kwargs: 1,
        )
