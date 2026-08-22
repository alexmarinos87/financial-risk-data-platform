from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.portfolio_mandates import (
    PortfolioMandate,
    select_portfolio_mandate,
)
from src.analytics.portfolio_risk_limit_policies import (
    EffectiveDatedPortfolioRiskLimitPolicy,
)
from src.analytics.portfolio_risk_limits import RiskLimitThresholds
from src.common.exceptions import StorageError, ValidationError
from src.orchestration.run_governed_portfolio_cycle import (
    STAGES,
    run_governed_portfolio_cycle,
)


def _mandate() -> PortfolioMandate:
    payload = {
        "portfolios": {
            "us-tech-equal": {
                "mandates": [
                    {
                        "mandate_id": "us-tech-2026",
                        "base_currency": "USD",
                        "effective_from": "2026-01-02",
                        "effective_to": "2026-02-01",
                        "constituents": [
                            {"source": "alpha_vantage", "symbol": "AAPL", "weight": 0.5},
                            {"source": "alpha_vantage", "symbol": "MSFT", "weight": 0.5},
                        ],
                    }
                ]
            }
        }
    }
    return select_portfolio_mandate(payload, "us-tech-equal", date(2026, 1, 4))


def _policy(
    *,
    portfolio_id: str = "us-tech-equal",
    covariance_window: int = 3,
    annualization_days: int = 252,
    effective_from: date = date(2026, 1, 1),
    effective_to: date | None = date(2026, 2, 1),
) -> EffectiveDatedPortfolioRiskLimitPolicy:
    return EffectiveDatedPortfolioRiskLimitPolicy(
        policy_id="us-tech-standard",
        portfolio_id=portfolio_id,
        covariance_window=covariance_window,
        annualization_days=annualization_days,
        portfolio_volatility=RiskLimitThresholds(warning=0.30, critical=0.45),
        component_concentration=RiskLimitThresholds(warning=0.65, critical=0.80),
        policy_version_id="us-tech-standard-2026-h1",
        effective_from=effective_from,
        effective_to=effective_to,
    )


def _record(day: int) -> dict[str, object]:
    return {
        "ts_event": datetime(2026, 1, day, tzinfo=timezone.utc),
        "calculation_id": f"calculation-{day}",
    }


def _run(**overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "portfolio_id": "us-tech-equal",
        "policy_id": "us-tech-standard",
        "portfolio_config_path": Path("portfolios.yaml"),
        "risk_limit_config_path": Path("limits.yaml"),
        "start_date": None,
        "end_date": date(2026, 1, 4),
        "volatility_window": 2,
        "var_window": 2,
        "var_confidence": 0.95,
        "covariance_window": 3,
        "max_snapshots": 10,
        "max_evaluations": 20,
        "storage_config_path": Path("storage.yaml"),
        "mandate_loader": lambda *_: _mandate(),
        "policy_loader": lambda *_: _policy(),
        "lock_base_dir": Path("locks"),
    }
    defaults.update(overrides)
    return run_governed_portfolio_cycle(**defaults)


def test_cycle_uses_one_mandate_and_effective_policy_across_stages() -> None:
    calls: list[str] = []
    released: list[list[Path]] = []
    mandate = _mandate()

    def portfolio_stage(**kwargs: Any) -> dict[str, Any]:
        calls.append("portfolio_risk")
        selected = kwargs["definition_loader"](Path("unused"), "us-tech-equal")
        assert selected.fingerprint == mandate.fingerprint
        filtered = kwargs["reader"](storage_config={}, end_date=date(2026, 1, 4))
        assert [row["calculation_id"] for row in filtered] == [
            "calculation-2",
            "calculation-3",
        ]
        assert kwargs["start_date"] == date(2026, 1, 2)
        return {"stage": "portfolio"}

    def attribution_stage(**kwargs: Any) -> dict[str, Any]:
        calls.append("portfolio_attribution_history")
        filtered = kwargs["reader"](
            storage_config={},
            portfolio_id="us-tech-equal",
            definition_fingerprint=mandate.fingerprint,
            end_date=date(2026, 1, 4),
        )
        assert [row["calculation_id"] for row in filtered] == [
            "calculation-2",
            "calculation-3",
        ]
        return {"stage": "attribution"}

    def risk_limit_stage(**kwargs: Any) -> dict[str, Any]:
        calls.append("portfolio_risk_limits")
        filtered = kwargs["reader"](Path("unused"))
        assert [row["calculation_id"] for row in filtered] == [
            "calculation-2",
            "calculation-3",
        ]
        selected_policy = kwargs["policy_loader"](
            Path("unused"), "us-tech-standard", date(2026, 1, 4)
        )
        assert selected_policy.fingerprint == _policy().fingerprint
        return {"stage": "limits"}

    summary = _run(
        portfolio_stage=portfolio_stage,
        attribution_stage=attribution_stage,
        risk_limit_stage=risk_limit_stage,
        daily_reader=lambda **_: [_record(1), _record(2), _record(3)],
        portfolio_reader=lambda **_: [_record(1), _record(2), _record(3)],
        attribution_reader=lambda _: [_record(1), _record(2), _record(3)],
        lock_acquirer=lambda *args, **kwargs: [Path("lock")],
        lock_releaser=lambda paths: released.append(paths),
    )

    assert calls == list(STAGES)
    assert released == [[Path("lock")]]
    assert summary["mandate"]["mandate_id"] == "us-tech-2026"
    assert summary["policy_version"]["policy_version_id"] == "us-tech-standard-2026-h1"
    assert summary["selection"]["effective_start_date"] == "2026-01-02"
    assert summary["execution"]["completed_stages"] == list(STAGES)
    assert summary["delivery"]["performed"] is False


def test_dry_run_validates_without_lock_or_stage_execution() -> None:
    def fail(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("dry run must not execute stages or acquire locks")

    summary = _run(
        dry_run=True,
        portfolio_stage=fail,
        attribution_stage=fail,
        risk_limit_stage=fail,
        lock_acquirer=fail,
    )

    assert summary["execution"] == {
        "performed": False,
        "lock_acquired": False,
        "planned_stages": list(STAGES),
    }
    assert summary["stages"] == {}
    assert summary["policy_version"]["effective_from"] == "2026-01-01"


def test_mandate_and_policy_boundaries_fail_before_writes() -> None:
    stage_calls: list[str] = []

    def stage(**_: Any) -> dict[str, Any]:
        stage_calls.append("called")
        return {}

    with pytest.raises(ValidationError, match="mandate boundary"):
        _run(
            start_date=date(2026, 1, 1),
            portfolio_stage=stage,
            attribution_stage=stage,
            risk_limit_stage=stage,
        )

    with pytest.raises(ValidationError, match="policy boundary"):
        _run(
            policy_loader=lambda *_: _policy(effective_from=date(2026, 1, 3)),
            portfolio_stage=stage,
            attribution_stage=stage,
            risk_limit_stage=stage,
        )

    with pytest.raises(ValidationError, match="policy portfolio"):
        _run(
            policy_loader=lambda *_: _policy(portfolio_id="other"),
            portfolio_stage=stage,
            attribution_stage=stage,
            risk_limit_stage=stage,
        )

    assert stage_calls == []


def test_policy_window_and_annualisation_must_match_attribution() -> None:
    with pytest.raises(ValidationError, match="covariance_window"):
        _run(policy_loader=lambda *_: _policy(covariance_window=20))

    with pytest.raises(ValidationError, match="annualization_days"):
        _run(policy_loader=lambda *_: _policy(annualization_days=365))


def test_stage_failure_stops_downstream_and_releases_lock() -> None:
    calls: list[str] = []
    released: list[list[Path]] = []

    def portfolio_stage(**_: Any) -> dict[str, Any]:
        calls.append("portfolio")
        return {"ok": True}

    def attribution_stage(**_: Any) -> dict[str, Any]:
        calls.append("attribution")
        raise StorageError("failed")

    def risk_limit_stage(**_: Any) -> dict[str, Any]:
        calls.append("limits")
        return {"unexpected": True}

    with pytest.raises(StorageError, match="failed"):
        _run(
            portfolio_stage=portfolio_stage,
            attribution_stage=attribution_stage,
            risk_limit_stage=risk_limit_stage,
            lock_acquirer=lambda *args, **kwargs: [Path("lock")],
            lock_releaser=lambda paths: released.append(paths),
        )

    assert calls == ["portfolio", "attribution"]
    assert released == [[Path("lock")]]


def test_invalid_stage_summary_releases_lock_and_fails_closed() -> None:
    released: list[list[Path]] = []

    with pytest.raises(StorageError, match="invalid run summary"):
        _run(
            portfolio_stage=lambda **_: None,
            attribution_stage=lambda **_: {},
            risk_limit_stage=lambda **_: {},
            lock_acquirer=lambda *args, **kwargs: [Path("lock")],
            lock_releaser=lambda paths: released.append(paths),
        )

    assert released == [[Path("lock")]]
