from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.analytics.portfolio_attribution import (
    CORRELATION_METHOD as SAMPLE_CORRELATION_METHOD,
    COVARIANCE_METHOD as SAMPLE_COVARIANCE_METHOD,
    MODEL_VERSION as SAMPLE_MODEL_VERSION,
)
from src.analytics.portfolio_attribution_ewma import (
    CORRELATION_METHOD as EWMA_CORRELATION_METHOD,
    COVARIANCE_METHOD as EWMA_COVARIANCE_METHOD,
    MODEL_VERSION as EWMA_MODEL_VERSION,
)
from src.analytics.portfolio_risk import WEIGHTING_METHOD
from src.analytics.portfolio_risk_limit_method_evaluations import (
    evaluate_method_aware_portfolio_risk_limits,
)
from src.analytics.portfolio_risk_limit_method_policies import (
    EWMA_METHOD_CONTRACT,
    MODEL_VERSION,
    SAMPLE_METHOD_CONTRACT,
    MethodAwarePortfolioRiskLimitPolicy,
    load_method_aware_portfolio_risk_limit_policy,
)
from src.analytics.portfolio_risk_limit_policies import (
    EffectiveDatedPortfolioRiskLimitPolicy,
)
from src.analytics.portfolio_risk_limits import RiskLimitThresholds
from src.common.exceptions import ValidationError


def _base_policy() -> EffectiveDatedPortfolioRiskLimitPolicy:
    return EffectiveDatedPortfolioRiskLimitPolicy(
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


def _policy(
    *,
    method_policy_id: str,
    ewma: bool,
) -> MethodAwarePortfolioRiskLimitPolicy:
    return MethodAwarePortfolioRiskLimitPolicy(
        method_policy_id=method_policy_id,
        base_policy=_base_policy(),
        method=EWMA_METHOD_CONTRACT if ewma else SAMPLE_METHOD_CONTRACT,
    )


def _record(
    *,
    day: int,
    ewma: bool,
    calculation_id: str,
    volatility: float,
    ingested_at: datetime | None = None,
    shares: dict[str, float] | None = None,
) -> dict[str, object]:
    ts_event = datetime(2026, 1, day, tzinfo=timezone.utc)
    return {
        "calculation_id": calculation_id,
        "model_version": EWMA_MODEL_VERSION if ewma else SAMPLE_MODEL_VERSION,
        "portfolio_id": "us-tech-equal",
        "base_currency": "USD",
        "definition_fingerprint": "definition-a",
        "weighting_method": WEIGHTING_METHOD,
        "covariance_method": (
            EWMA_COVARIANCE_METHOD if ewma else SAMPLE_COVARIANCE_METHOD
        ),
        "correlation_method": (
            EWMA_CORRELATION_METHOD if ewma else SAMPLE_CORRELATION_METHOD
        ),
        "covariance_window": 20,
        "annualization_days": 252,
        "ts_event": ts_event,
        "ts_ingest": ingested_at or ts_event + timedelta(hours=1),
        "portfolio_volatility_annualized": volatility,
        "volatility_status": "positive" if volatility > 0 else "zero",
        "component_contribution_share_json": json.dumps(
            shares
            or {
                "alpha_vantage:AAPL": 0.70,
                "alpha_vantage:MSFT": 0.30,
            },
            sort_keys=True,
        ),
    }


def test_bundled_method_bindings_are_explicit_and_distinct() -> None:
    sample = load_method_aware_portfolio_risk_limit_policy(
        method_policy_config_path=Path(
            "config/portfolio_risk_limit_methods.yaml"
        ),
        limits_config_path=Path("config/portfolio_risk_limits.yaml"),
        method_policy_id="us-tech-sample",
        as_of_date=date(2026, 3, 31),
    )
    ewma = load_method_aware_portfolio_risk_limit_policy(
        method_policy_config_path=Path(
            "config/portfolio_risk_limit_methods.yaml"
        ),
        limits_config_path=Path("config/portfolio_risk_limits.yaml"),
        method_policy_id="us-tech-ewma",
        as_of_date=date(2026, 3, 31),
    )

    assert sample.policy_id == ewma.policy_id == "us-tech-standard"
    assert sample.base_policy.fingerprint == ewma.base_policy.fingerprint
    assert sample.method == SAMPLE_METHOD_CONTRACT
    assert ewma.method == EWMA_METHOD_CONTRACT
    assert sample.fingerprint != ewma.fingerprint


def test_method_binding_rejects_a_mixed_unsupported_contract(
    tmp_path: Path,
) -> None:
    config = tmp_path / "methods.yaml"
    config.write_text(
        """
method_policies:
  broken:
    base_policy_id: us-tech-standard
    attribution:
      model_version: portfolio-attribution-ewma-v1
      weighting_method: constant_weight_daily_rebalanced
      covariance_method: sample_annualized
      correlation_method: pearson
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match="unsupported"):
        load_method_aware_portfolio_risk_limit_policy(
            method_policy_config_path=config,
            limits_config_path=Path("config/portfolio_risk_limits.yaml"),
            method_policy_id="broken",
            as_of_date=date(2026, 3, 31),
        )


def test_sample_and_ewma_rows_can_coexist_without_cross_method_failure() -> None:
    records = [
        _record(
            day=2,
            ewma=False,
            calculation_id="sample-2",
            volatility=0.32,
        ),
        _record(
            day=2,
            ewma=True,
            calculation_id="ewma-2",
            volatility=0.48,
        ),
    ]

    sample = evaluate_method_aware_portfolio_risk_limits(
        records,
        policy=_policy(method_policy_id="sample", ewma=False),
        definition_fingerprint="definition-a",
    )
    ewma = evaluate_method_aware_portfolio_risk_limits(
        records,
        policy=_policy(method_policy_id="ewma", ewma=True),
        definition_fingerprint="definition-a",
    )

    assert {item["attribution_calculation_id"] for item in sample.evaluations} == {
        "sample-2"
    }
    assert {item["attribution_calculation_id"] for item in ewma.evaluations} == {
        "ewma-2"
    }
    assert sample.diagnostics["ignored_nonmatching_contract_records"] == 1
    assert ewma.diagnostics["ignored_nonmatching_contract_records"] == 1
    assert {item["model_version"] for item in sample.evaluations} == {
        MODEL_VERSION
    }
    assert {item["model_version"] for item in ewma.evaluations} == {
        MODEL_VERSION
    }


def test_method_specific_statuses_and_identities_are_retained() -> None:
    records = [
        _record(
            day=2,
            ewma=False,
            calculation_id="sample-2",
            volatility=0.32,
        ),
        _record(
            day=2,
            ewma=True,
            calculation_id="ewma-2",
            volatility=0.48,
        ),
    ]
    sample = evaluate_method_aware_portfolio_risk_limits(
        records,
        policy=_policy(method_policy_id="sample", ewma=False),
        definition_fingerprint="definition-a",
    )
    ewma = evaluate_method_aware_portfolio_risk_limits(
        records,
        policy=_policy(method_policy_id="ewma", ewma=True),
        definition_fingerprint="definition-a",
    )

    sample_volatility = next(
        item
        for item in sample.evaluations
        if item["metric_name"] == "portfolio_volatility_annualized"
    )
    ewma_volatility = next(
        item
        for item in ewma.evaluations
        if item["metric_name"] == "portfolio_volatility_annualized"
    )
    assert sample_volatility["status"] == "warning"
    assert ewma_volatility["status"] == "critical"
    assert sample_volatility["policy_fingerprint"] != ewma_volatility[
        "policy_fingerprint"
    ]
    assert sample_volatility["calculation_id"] != ewma_volatility[
        "calculation_id"
    ]


def test_latest_correction_is_selected_within_the_chosen_method() -> None:
    later = datetime(2026, 2, 1, tzinfo=timezone.utc)
    records = [
        _record(
            day=2,
            ewma=False,
            calculation_id="sample-old",
            volatility=0.32,
        ),
        _record(
            day=2,
            ewma=False,
            calculation_id="sample-new",
            volatility=0.50,
            ingested_at=later,
        ),
        _record(
            day=2,
            ewma=True,
            calculation_id="ewma-2",
            volatility=0.40,
        ),
    ]

    output = evaluate_method_aware_portfolio_risk_limits(
        records,
        policy=_policy(method_policy_id="sample", ewma=False),
        definition_fingerprint="definition-a",
    )

    assert {item["attribution_calculation_id"] for item in output.evaluations} == {
        "sample-new"
    }
    assert all(item["ts_ingest"] == later for item in output.evaluations)


def test_conflicting_calculation_id_reuse_fails_closed() -> None:
    records = [
        _record(
            day=2,
            ewma=True,
            calculation_id="same",
            volatility=0.35,
        ),
        _record(
            day=3,
            ewma=True,
            calculation_id="same",
            volatility=0.40,
        ),
    ]

    with pytest.raises(ValidationError, match="conflicting records"):
        evaluate_method_aware_portfolio_risk_limits(
            records,
            policy=_policy(method_policy_id="ewma", ewma=True),
            definition_fingerprint="definition-a",
        )


def test_evaluation_request_bound_is_enforced_before_output() -> None:
    records = [
        _record(
            day=day,
            ewma=False,
            calculation_id=f"sample-{day}",
            volatility=0.31,
        )
        for day in (2, 3)
    ]

    with pytest.raises(ValidationError, match="max_evaluations"):
        evaluate_method_aware_portfolio_risk_limits(
            records,
            policy=_policy(method_policy_id="sample", ewma=False),
            definition_fingerprint="definition-a",
            max_evaluations=2,
        )
