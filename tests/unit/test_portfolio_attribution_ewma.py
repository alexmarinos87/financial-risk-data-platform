from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from src.analytics.portfolio_attribution import build_portfolio_attribution
from src.analytics.portfolio_attribution_ewma import (
    CORRELATION_METHOD,
    COVARIANCE_METHOD,
    EWMA_DECAY,
    MODEL_VERSION,
    build_portfolio_ewma_attribution,
)
from src.analytics.portfolio_risk import (
    WEIGHTING_METHOD,
    PortfolioDefinition,
    parse_portfolio_definition,
)
from src.common.exceptions import ValidationError


def _definition() -> PortfolioDefinition:
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


def _record(
    day: int,
    aapl: float,
    msft: float,
    *,
    calculation_id: str | None = None,
    ingested_at: datetime | None = None,
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
        "portfolio_id": definition.portfolio_id,
        "base_currency": definition.base_currency,
        "definition_fingerprint": definition.fingerprint,
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
        "component_returns_json": json.dumps(
            component_returns,
            sort_keys=True,
        ),
        "portfolio_return_1d": 0.5 * aapl + 0.5 * msft,
    }


def _records() -> list[dict[str, object]]:
    return [
        _record(2, 0.10, 0.02),
        _record(3, -0.04, 0.0),
        _record(4, 0.06, 0.02),
    ]


def test_ewma_attribution_calculates_expected_matrix_and_euler_risk() -> None:
    output = build_portfolio_ewma_attribution(
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
    assert snapshot["covariance_method"] == COVARIANCE_METHOD
    assert snapshot["correlation_method"] == CORRELATION_METHOD
    assert snapshot["calculation_id"].startswith(f"{MODEL_VERSION}-snapshot-")
    assert snapshot["portfolio_variance_annualized"] == pytest.approx(
        0.4602471738206545
    )
    assert snapshot["portfolio_volatility_annualized"] == pytest.approx(
        0.6784151927991107
    )
    assert covariance["alpha_vantage:AAPL"]["alpha_vantage:AAPL"] == (
        pytest.approx(1.2441139028475716)
    )
    assert covariance["alpha_vantage:AAPL"]["alpha_vantage:MSFT"] == (
        pytest.approx(0.2648159758278854)
    )
    assert correlation["alpha_vantage:AAPL"]["alpha_vantage:MSFT"] == (
        pytest.approx(0.9155690359077874)
    )
    assert sum(components.values()) == pytest.approx(
        snapshot["portfolio_volatility_annualized"]
    )
    assert sum(shares.values()) == pytest.approx(1.0)
    assert output.diagnostics["ewma_decay"] == EWMA_DECAY
    assert output.diagnostics["newest_observation_weight"] > output.diagnostics[
        "oldest_observation_weight"
    ]


def test_sample_and_ewma_share_inputs_but_keep_distinct_versions() -> None:
    sample = build_portfolio_attribution(
        _records(),
        definition=_definition(),
        covariance_window=3,
    )
    ewma = build_portfolio_ewma_attribution(
        _records(),
        definition=_definition(),
        covariance_window=3,
    )

    assert sample.snapshot["input_calculation_ids_json"] == ewma.snapshot[
        "input_calculation_ids_json"
    ]
    assert sample.snapshot["window_start"] == ewma.snapshot["window_start"]
    assert sample.snapshot["window_end"] == ewma.snapshot["window_end"]
    assert sample.snapshot["calculation_id"] != ewma.snapshot["calculation_id"]
    assert sample.snapshot["model_version"] != ewma.snapshot["model_version"]
    assert sample.snapshot["covariance_method"] != ewma.snapshot[
        "covariance_method"
    ]


def test_input_order_does_not_change_ewma_output() -> None:
    forward = build_portfolio_ewma_attribution(
        _records(),
        definition=_definition(),
        covariance_window=3,
    )
    reverse = build_portfolio_ewma_attribution(
        reversed(_records()),
        definition=_definition(),
        covariance_window=3,
    )

    assert forward.snapshot == reverse.snapshot
    assert forward.diagnostics == reverse.diagnostics


def test_recent_shock_receives_more_weight_than_an_old_shock() -> None:
    old_shock = [
        _record(2, 0.20, 0.0),
        _record(3, 0.0, 0.0),
        _record(4, 0.0, 0.0),
        _record(5, 0.0, 0.0),
    ]
    recent_shock = [
        _record(2, 0.0, 0.0),
        _record(3, 0.0, 0.0),
        _record(4, 0.0, 0.0),
        _record(5, 0.20, 0.0),
    ]

    old_output = build_portfolio_ewma_attribution(
        old_shock,
        definition=_definition(),
        covariance_window=4,
    )
    recent_output = build_portfolio_ewma_attribution(
        recent_shock,
        definition=_definition(),
        covariance_window=4,
    )

    assert recent_output.snapshot["portfolio_volatility_annualized"] > (
        old_output.snapshot["portfolio_volatility_annualized"]
    )


def test_zero_returns_have_explicit_zero_and_undefined_correlation_status() -> None:
    output = build_portfolio_ewma_attribution(
        [
            _record(2, 0.0, 0.0),
            _record(3, 0.0, 0.0),
            _record(4, 0.0, 0.0),
        ],
        definition=_definition(),
        covariance_window=3,
    )

    correlation = json.loads(output.snapshot["correlation_json"])
    components = json.loads(
        output.snapshot["component_volatility_contribution_json"]
    )
    assert output.snapshot["portfolio_variance_annualized"] == 0.0
    assert output.snapshot["portfolio_volatility_annualized"] == 0.0
    assert output.snapshot["volatility_status"] == "zero"
    assert output.snapshot["correlation_status"] == "undefined_zero_variance"
    assert output.snapshot["undefined_correlation_cells"] == 4
    assert all(value is None for row in correlation.values() for value in row.values())
    assert components == {
        "alpha_vantage:AAPL": 0.0,
        "alpha_vantage:MSFT": 0.0,
    }


def test_latest_corrected_portfolio_return_version_is_used() -> None:
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

    output = build_portfolio_ewma_attribution(
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


def test_insufficient_window_and_conflicting_identity_fail_closed() -> None:
    with pytest.raises(ValidationError, match="requires at least 4"):
        build_portfolio_ewma_attribution(
            _records(),
            definition=_definition(),
            covariance_window=4,
        )

    conflicting = [
        _record(2, 0.10, 0.02, calculation_id="same"),
        _record(3, -0.04, 0.0, calculation_id="same"),
        _record(4, 0.06, 0.02),
    ]
    with pytest.raises(ValidationError, match="conflicting records"):
        build_portfolio_ewma_attribution(
            conflicting,
            definition=_definition(),
            covariance_window=2,
        )
