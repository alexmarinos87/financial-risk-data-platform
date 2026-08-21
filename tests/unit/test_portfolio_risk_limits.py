from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from src.analytics.portfolio_risk_limits import (
    CONCENTRATION_METRIC,
    MODEL_VERSION,
    VOLATILITY_METRIC,
    evaluate_portfolio_risk_limits,
    parse_portfolio_risk_limit_policy,
)
from src.common.exceptions import ValidationError

FINGERPRINT = "portfolio-definition-test"


def _policy_payload(
    volatility_warning: float = 0.30,
) -> dict[str, object]:
    return {
        "policies": {
            "test-policy": {
                "portfolio_id": "us-tech-equal",
                "covariance_window": 3,
                "annualization_days": 252,
                "limits": {
                    VOLATILITY_METRIC: {
                        "warning": volatility_warning,
                        "critical": 0.45,
                    },
                    CONCENTRATION_METRIC: {
                        "warning": 0.65,
                        "critical": 0.80,
                    },
                },
            }
        }
    }


def _record(
    day: int,
    volatility: float,
    aapl_share: float,
    msft_share: float,
    *,
    calculation_id: str | None = None,
    ingested_at: datetime | None = None,
) -> dict[str, object]:
    event = datetime(2026, 1, day, tzinfo=timezone.utc)
    return {
        "model_version": "portfolio-attribution-v1",
        "calculation_id": calculation_id or f"attribution-{day}",
        "portfolio_id": "us-tech-equal",
        "base_currency": "USD",
        "definition_fingerprint": FINGERPRINT,
        "weighting_method": "constant_weight_daily_rebalanced",
        "covariance_method": "sample_annualized",
        "correlation_method": "pearson",
        "covariance_window": 3,
        "annualization_days": 252,
        "ts_event": event,
        "ts_ingest": ingested_at or event + timedelta(hours=1),
        "portfolio_volatility_annualized": volatility,
        "volatility_status": "positive" if volatility > 0 else "zero",
        "component_contribution_share_json": json.dumps(
            {
                "alpha_vantage:AAPL": aapl_share,
                "alpha_vantage:MSFT": msft_share,
            },
            sort_keys=True,
        ),
    }


def _policy(volatility_warning: float = 0.30):
    return parse_portfolio_risk_limit_policy(
        _policy_payload(volatility_warning),
        "test-policy",
    )


def test_evaluates_two_metrics_with_deterministic_statuses() -> None:
    output = evaluate_portfolio_risk_limits(
        [_record(4, 0.50, 0.82, 0.18)],
        policy=_policy(),
        definition_fingerprint=FINGERPRINT,
    )

    assert len(output.evaluations) == 2
    by_metric = {item["metric_name"]: item for item in output.evaluations}
    assert by_metric[VOLATILITY_METRIC]["status"] == "critical"
    assert by_metric[VOLATILITY_METRIC]["breach_excess"] == pytest.approx(0.05)
    concentration = by_metric[CONCENTRATION_METRIC]
    assert concentration["status"] == "critical"
    assert concentration["subject_key"] == "alpha_vantage:AAPL"
    assert concentration["observed_signed_value"] == pytest.approx(0.82)
    assert all(item["model_version"] == MODEL_VERSION for item in output.evaluations)


def test_latest_attribution_version_wins_and_input_order_is_irrelevant() -> None:
    late = datetime(2026, 2, 1, tzinfo=timezone.utc)
    records = [
        _record(4, 0.20, 0.55, 0.45, calculation_id="old"),
        _record(
            4,
            0.35,
            -0.70,
            0.30,
            calculation_id="new",
            ingested_at=late,
        ),
    ]
    forward = evaluate_portfolio_risk_limits(
        records,
        policy=_policy(),
        definition_fingerprint=FINGERPRINT,
    )
    reverse = evaluate_portfolio_risk_limits(
        reversed(records),
        policy=_policy(),
        definition_fingerprint=FINGERPRINT,
    )

    assert forward.evaluations == reverse.evaluations
    assert {item["attribution_calculation_id"] for item in forward.evaluations} == {
        "new"
    }
    concentration = next(
        item
        for item in forward.evaluations
        if item["metric_name"] == CONCENTRATION_METRIC
    )
    assert concentration["observed_value"] == pytest.approx(0.70)
    assert concentration["observed_signed_value"] == pytest.approx(-0.70)


def test_policy_change_creates_new_fingerprint_and_evaluation_ids() -> None:
    record = _record(4, 0.35, 0.60, 0.40)
    original = evaluate_portfolio_risk_limits(
        [record],
        policy=_policy(0.30),
        definition_fingerprint=FINGERPRINT,
    )
    changed = evaluate_portfolio_risk_limits(
        [record],
        policy=_policy(0.25),
        definition_fingerprint=FINGERPRINT,
    )

    assert original.diagnostics["policy_fingerprint"] != changed.diagnostics[
        "policy_fingerprint"
    ]
    assert {item["calculation_id"] for item in original.evaluations}.isdisjoint(
        item["calculation_id"] for item in changed.evaluations
    )


def test_invalid_policy_conflict_and_request_bound_fail_closed() -> None:
    invalid = _policy_payload()
    invalid["policies"]["test-policy"]["limits"][VOLATILITY_METRIC] = {
        "warning": 0.50,
        "critical": 0.40,
    }
    with pytest.raises(ValidationError, match="warning < critical"):
        parse_portfolio_risk_limit_policy(invalid, "test-policy")

    conflicting = [
        _record(4, 0.20, 0.50, 0.50, calculation_id="same"),
        _record(4, 0.30, 0.50, 0.50, calculation_id="same"),
    ]
    with pytest.raises(ValidationError, match="conflicting records"):
        evaluate_portfolio_risk_limits(
            conflicting,
            policy=_policy(),
            definition_fingerprint=FINGERPRINT,
        )

    with pytest.raises(ValidationError, match="max_evaluations"):
        evaluate_portfolio_risk_limits(
            [_record(4, 0.20, 0.50, 0.50)],
            policy=_policy(),
            definition_fingerprint=FINGERPRINT,
            max_evaluations=1,
        )


def test_start_date_filters_evaluation_dates() -> None:
    output = evaluate_portfolio_risk_limits(
        [
            _record(3, 0.20, 0.50, 0.50),
            _record(4, 0.35, 0.70, 0.30),
        ],
        policy=_policy(),
        definition_fingerprint=FINGERPRINT,
        start_date=date(2026, 1, 4),
        end_date=date(2026, 1, 4),
    )

    assert len(output.evaluations) == 2
    assert {item["ts_event"].date() for item in output.evaluations} == {
        date(2026, 1, 4)
    }
