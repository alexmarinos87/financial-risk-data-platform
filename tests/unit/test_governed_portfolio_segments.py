from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from src.analytics.governed_portfolio_segments import (
    MODEL_VERSION,
    plan_governed_portfolio_segments,
)
from src.analytics.portfolio_mandates import parse_portfolio_mandates
from src.analytics.portfolio_risk_limit_policies import (
    parse_portfolio_risk_limit_policies,
)
from src.common.exceptions import ValidationError


def _constituents() -> list[dict[str, object]]:
    return [
        {"source": "alpha_vantage", "symbol": "AAPL", "weight": 0.5},
        {"source": "alpha_vantage", "symbol": "MSFT", "weight": 0.5},
    ]


def _mandates(*, gap: bool = False):
    second_start = "2026-02-02" if gap else "2026-02-01"
    payload = {
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
                        "effective_from": second_start,
                        "constituents": _constituents(),
                    },
                ]
            }
        }
    }
    return parse_portfolio_mandates(payload, "us-tech-equal")


def _policies(*, second_window: int = 20, gap: bool = False):
    second_start = "2026-01-16" if gap else "2026-01-15"
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
    payload = {
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
                        "effective_from": second_start,
                        "covariance_window": second_window,
                        "annualization_days": 252,
                        "limits": limits,
                    },
                ]
            }
        }
    }
    return parse_portfolio_risk_limit_policies(payload, "us-tech-standard")


def _plan(mandates=None, policies=None, **overrides):
    kwargs = {
        "portfolio_id": "us-tech-equal",
        "policy_id": "us-tech-standard",
        "start_date": date(2026, 1, 10),
        "end_date": date(2026, 2, 10),
        "covariance_window": 20,
    }
    kwargs.update(overrides)
    return plan_governed_portfolio_segments(
        mandates or _mandates(),
        policies or _policies(),
        **kwargs,
    )


def test_plan_intersects_every_temporal_boundary_in_order() -> None:
    output = _plan()

    assert [
        (segment.start_date, segment.end_date)
        for segment in output.segments
    ] == [
        (date(2026, 1, 10), date(2026, 1, 14)),
        (date(2026, 1, 15), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 2, 10)),
    ]
    assert [segment.mandate.mandate_id for segment in output.segments] == [
        "mandate-a",
        "mandate-a",
        "mandate-b",
    ]
    assert [
        segment.policy.policy_version_id for segment in output.segments
    ] == ["policy-a", "policy-b", "policy-b"]
    assert sum(segment.calendar_days for segment in output.segments) == 32
    assert output.diagnostics["segment_count"] == 3
    assert output.diagnostics["mandates_used"] == 2
    assert output.diagnostics["policy_versions_used"] == 2
    assert output.diagnostics["model_version"] == MODEL_VERSION


def test_plan_is_deterministic_for_reordered_inputs() -> None:
    forward = _plan()
    reverse = _plan(
        mandates=tuple(reversed(_mandates())),
        policies=tuple(reversed(_policies())),
    )

    assert forward.segments == reverse.segments
    assert forward.diagnostics == reverse.diagnostics
    assert [item.segment_id for item in forward.segments] == [
        item.segment_id for item in reverse.segments
    ]


def test_plan_rejects_mandate_and_policy_coverage_gaps() -> None:
    with pytest.raises(ValidationError, match="coverage gap"):
        _plan(
            mandates=_mandates(gap=True),
            start_date=date(2026, 1, 30),
            end_date=date(2026, 2, 3),
        )

    with pytest.raises(ValidationError, match="coverage gap"):
        _plan(
            policies=_policies(gap=True),
            start_date=date(2026, 1, 14),
            end_date=date(2026, 1, 17),
        )


def test_plan_rejects_defensive_overlap_and_parameter_mismatch() -> None:
    mandates = _mandates()
    overlapping = (
        mandates[0],
        replace(mandates[1], effective_from=date(2026, 1, 20)),
    )
    with pytest.raises(ValidationError, match="must not overlap"):
        _plan(mandates=overlapping)

    with pytest.raises(ValidationError, match="covariance_window"):
        _plan(policies=_policies(second_window=30))


def test_plan_rejects_invalid_range_and_output_limit() -> None:
    with pytest.raises(ValidationError, match="on or before"):
        _plan(
            start_date=date(2026, 2, 10),
            end_date=date(2026, 1, 10),
        )

    with pytest.raises(ValidationError, match="max_segments"):
        _plan(max_segments=2)


def test_plan_identity_changes_with_requested_range() -> None:
    first = _plan(end_date=date(2026, 2, 9))
    second = _plan(end_date=date(2026, 2, 10))

    assert first.diagnostics["plan_id"] != second.diagnostics["plan_id"]
