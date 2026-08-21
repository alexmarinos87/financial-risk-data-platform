from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from src.analytics.portfolio_attribution import (
    MODEL_VERSION,
    build_portfolio_attribution,
)
from src.analytics.portfolio_risk import (
    WEIGHTING_METHOD,
    PortfolioDefinition,
    parse_portfolio_definition,
)
from src.common.exceptions import ValidationError


def _definition_payload() -> dict[str, object]:
    return {
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
    }


def _definition() -> PortfolioDefinition:
    return parse_portfolio_definition(_definition_payload(), "us-tech-equal")


def _record(
    day: int,
    aapl: float,
    msft: float,
    *,
    calculation_id: str | None = None,
    ingested_at: datetime | None = None,
    portfolio_id: str = "us-tech-equal",
    fingerprint: str | None = None,
) -> dict[str, object]:
    definition = _definition()
    ts_event = datetime(2026, 1, day, tzinfo=timezone.utc)
    weights = {"alpha_vantage:AAPL": 0.5, "alpha_vantage:MSFT": 0.5}
    component_returns = {
        "alpha_vantage:AAPL": aapl,
        "alpha_vantage:MSFT": msft,
    }
    return {
        "model_version": "portfolio-risk-v1",
        "calculation_id": calculation_id or f"portfolio-{day}",
        "portfolio_id": portfolio_id,
        "base_currency": "USD",
        "definition_fingerprint": fingerprint or definition.fingerprint,
        "weighting_method": WEIGHTING_METHOD,
        "ts_event": ts_event,
        "ts_ingest": ingested_at or ts_event + timedelta(hours=1),
        "constituent_count": 2,
        "weights_json": json.dumps(weights, sort_keys=True),
        "component_calculation_ids_json": json.dumps(
            {
                "alpha_vantage:AAPL": f"AAPL-{day}",
                "alpha_vantage:MSFT": f"MSFT-{day}",
            },
            sort_keys=True,
        ),
        "component_returns_json": json.dumps(component_returns, sort_keys=True),
        "portfolio_return_1d": 0.5 * aapl + 0.5 * msft,
    }


def _records() -> list[dict[str, object]]:
    return [
        _record(2, 0.10, 0.02),
        _record(3, -0.04, 0.0),
        _record(4, 0.06, 0.02),
    ]


def test_build_attribution_calculates_covariance_correlation_and_euler_risk() -> None:
    output = build_portfolio_attribution(
        _records(),
        definition=_definition(),
        covariance_window=3,
        end_date=date(2026, 1, 4),
    )
    snapshot = output.snapshot

    covariance = json.loads(snapshot["covariance_annualized_json"])
    correlation = json.loads(snapshot["correlation_json"])
    components = json.loads(snapshot["component_volatility_contribution_json"])
    shares = json.loads(snapshot["component_contribution_share_json"])

    assert snapshot["model_version"] == MODEL_VERSION
    assert snapshot["calculation_id"].startswith(f"{MODEL_VERSION}-snapshot-")
    assert snapshot["portfolio_variance_annualized"] == pytest.approx(0.4368)
    assert snapshot["portfolio_volatility_annualized"] == pytest.approx(
        0.6609084656743323
    )
    assert covariance["alpha_vantage:AAPL"]["alpha_vantage:AAPL"] == pytest.approx(
        1.3104
    )
    assert covariance["alpha_vantage:AAPL"]["alpha_vantage:MSFT"] == pytest.approx(
        0.2016
    )
    assert correlation["alpha_vantage:AAPL"]["alpha_vantage:MSFT"] == pytest.approx(
        0.9607689228305227
    )
    assert sum(components.values()) == pytest.approx(
        snapshot["portfolio_volatility_annualized"]
    )
    assert sum(shares.values()) == pytest.approx(1.0)
    assert components["alpha_vantage:AAPL"] == pytest.approx(0.5719400183720182)
    assert snapshot["correlation_status"] == "complete"
    assert snapshot["volatility_status"] == "positive"
    assert snapshot["euler_residual"] == pytest.approx(0.0)


def test_input_order_does_not_change_attribution_identity() -> None:
    forward = build_portfolio_attribution(
        _records(), definition=_definition(), covariance_window=3
    )
    reverse = build_portfolio_attribution(
        reversed(_records()), definition=_definition(), covariance_window=3
    )

    assert forward.snapshot == reverse.snapshot
    assert forward.diagnostics == reverse.diagnostics


def test_latest_portfolio_return_version_wins_for_the_same_date() -> None:
    late = datetime(2026, 2, 1, tzinfo=timezone.utc)
    records = [
        _record(2, 0.10, 0.02),
        _record(3, -0.04, 0.0, calculation_id="old"),
        _record(
            3,
            0.04,
            0.0,
            calculation_id="new",
            ingested_at=late,
        ),
        _record(4, 0.06, 0.02),
    ]

    output = build_portfolio_attribution(
        records,
        definition=_definition(),
        covariance_window=3,
    )

    assert output.snapshot["ts_ingest"] == late
    assert json.loads(output.snapshot["input_calculation_ids_json"]) == [
        "portfolio-2",
        "new",
        "portfolio-4",
    ]


def test_conflicting_calculation_id_reuse_fails_closed() -> None:
    records = [
        _record(2, 0.10, 0.02, calculation_id="same"),
        _record(3, -0.04, 0.0, calculation_id="same"),
        _record(4, 0.06, 0.02),
    ]

    with pytest.raises(ValidationError, match="conflicting records"):
        build_portfolio_attribution(
            records,
            definition=_definition(),
            covariance_window=2,
        )


def test_unrelated_portfolio_records_are_ignored() -> None:
    records = [
        _record(1, 0.9, 0.9, portfolio_id="other", fingerprint="other-fingerprint"),
        *_records(),
    ]

    output = build_portfolio_attribution(
        records,
        definition=_definition(),
        covariance_window=3,
    )

    assert output.diagnostics["matched_input_records"] == 3


def test_zero_variance_has_explicit_correlation_and_contribution_status() -> None:
    records = [
        _record(2, 0.01, 0.02),
        _record(3, 0.01, 0.02),
        _record(4, 0.01, 0.02),
    ]
    output = build_portfolio_attribution(
        records,
        definition=_definition(),
        covariance_window=3,
    )

    correlation = json.loads(output.snapshot["correlation_json"])
    components = json.loads(output.snapshot["component_volatility_contribution_json"])
    assert output.snapshot["portfolio_volatility_annualized"] == pytest.approx(0.0)
    assert output.snapshot["volatility_status"] == "zero"
    assert output.snapshot["correlation_status"] == "undefined_zero_variance"
    assert output.snapshot["undefined_correlation_cells"] == 4
    assert all(value is None for row in correlation.values() for value in row.values())
    assert components == {"alpha_vantage:AAPL": 0.0, "alpha_vantage:MSFT": 0.0}


def test_insufficient_window_and_invalid_evidence_fail_closed() -> None:
    with pytest.raises(ValidationError, match="requires at least 4"):
        build_portfolio_attribution(
            _records(),
            definition=_definition(),
            covariance_window=4,
        )

    invalid = _records()
    invalid[0] = dict(invalid[0])
    invalid[0]["portfolio_return_1d"] = 0.9
    with pytest.raises(ValidationError, match="does not equal"):
        build_portfolio_attribution(
            invalid,
            definition=_definition(),
            covariance_window=3,
        )
