from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.plan_governed_portfolio_history import (
    plan_governed_portfolio_history,
)
from src.orchestration.run_governed_portfolio_cycle import STAGES


def _constituents() -> list[dict[str, object]]:
    return [
        {"source": "alpha_vantage", "symbol": "AAPL", "weight": 0.5},
        {"source": "alpha_vantage", "symbol": "MSFT", "weight": 0.5},
    ]


def _portfolio_payload() -> dict[str, object]:
    return {
        "portfolios": {
            "us-tech-equal": {
                "mandates": [
                    {
                        "mandate_id": "mandate-a",
                        "base_currency": "USD",
                        "effective_from": "2026-01-01",
                        "effective_to": "2026-02-01",
                        "constituents": _constituents(),
                    },
                    {
                        "mandate_id": "mandate-b",
                        "base_currency": "USD",
                        "effective_from": "2026-02-01",
                        "constituents": _constituents(),
                    },
                ]
            }
        }
    }


def _policy_payload() -> dict[str, object]:
    limits = {
        "portfolio_volatility_annualized": {
            "warning": 0.30,
            "critical": 0.45,
        },
        "largest_absolute_component_contribution_share": {
            "warning": 0.65,
            "critical": 0.80,
        },
    }
    return {
        "policies": {
            "us-tech-standard": {
                "versions": [
                    {
                        "policy_version_id": "policy-a",
                        "portfolio_id": "us-tech-equal",
                        "effective_from": "2026-01-01",
                        "effective_to": "2026-01-15",
                        "covariance_window": 20,
                        "annualization_days": 252,
                        "limits": limits,
                    },
                    {
                        "policy_version_id": "policy-b",
                        "portfolio_id": "us-tech-equal",
                        "effective_from": "2026-01-15",
                        "covariance_window": 20,
                        "annualization_days": 252,
                        "limits": limits,
                    },
                ]
            }
        }
    }


def _loader(path: Path) -> Any:
    if path.name == "portfolios.yaml":
        return _portfolio_payload()
    if path.name == "limits.yaml":
        return _policy_payload()
    raise AssertionError(f"unexpected path: {path}")


def test_run_returns_plan_only_segment_evidence() -> None:
    summary = plan_governed_portfolio_history(
        portfolio_id="us-tech-equal",
        policy_id="us-tech-standard",
        portfolio_config_path=Path("portfolios.yaml"),
        risk_limit_config_path=Path("limits.yaml"),
        start_date=date(2026, 1, 10),
        end_date=date(2026, 2, 10),
        covariance_window=20,
        max_segments=10,
        config_loader=_loader,
    )

    assert summary["plan_id"].startswith("governed-plan-")
    assert summary["selection"]["segment_count"] == 3
    assert summary["execution"] == {
        "performed": False,
        "planned_segment_runs": 3,
        "planned_stage_invocations": 3 * len(STAGES),
        "planned_stages": list(STAGES),
        "reason": "plan_only",
    }
    assert [segment["start_date"] for segment in summary["segments"]] == [
        "2026-01-10",
        "2026-01-15",
        "2026-02-01",
    ]
    assert summary["segments"][0]["mandate"]["mandate_id"] == "mandate-a"
    assert summary["segments"][1]["policy_version"][
        "policy_version_id"
    ] == "policy-b"


def test_run_rejects_invalid_loader_output_and_load_failure() -> None:
    with pytest.raises(ValidationError, match="must be a mapping"):
        plan_governed_portfolio_history(
            portfolio_id="us-tech-equal",
            policy_id="us-tech-standard",
            portfolio_config_path=Path("portfolios.yaml"),
            risk_limit_config_path=Path("limits.yaml"),
            start_date=date(2026, 1, 10),
            end_date=date(2026, 1, 20),
            covariance_window=20,
            config_loader=lambda _: [],
        )

    def fail(_: Path) -> Any:
        raise OSError("failed")

    with pytest.raises(ValidationError, match="could not be loaded"):
        plan_governed_portfolio_history(
            portfolio_id="us-tech-equal",
            policy_id="us-tech-standard",
            portfolio_config_path=Path("portfolios.yaml"),
            risk_limit_config_path=Path("limits.yaml"),
            start_date=date(2026, 1, 10),
            end_date=date(2026, 1, 20),
            covariance_window=20,
            config_loader=fail,
        )
