from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from src.analytics.portfolio_attribution_history import (
    build_portfolio_attribution_history,
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


def _record(day: int, aapl: float, msft: float) -> dict[str, object]:
    definition = _definition()
    event = datetime(2026, 1, day, tzinfo=timezone.utc)
    component_returns = {
        "alpha_vantage:AAPL": aapl,
        "alpha_vantage:MSFT": msft,
    }
    return {
        "model_version": "portfolio-risk-v1",
        "calculation_id": f"portfolio-{day}",
        "portfolio_id": "us-tech-equal",
        "base_currency": "USD",
        "definition_fingerprint": definition.fingerprint,
        "weighting_method": WEIGHTING_METHOD,
        "ts_event": event,
        "ts_ingest": event + timedelta(hours=1),
        "constituent_count": 2,
        "weights_json": json.dumps(
            {
                "alpha_vantage:AAPL": 0.5,
                "alpha_vantage:MSFT": 0.5,
            },
            sort_keys=True,
        ),
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
        _record(3, -0.04, 0.00),
        _record(4, 0.06, 0.02),
        _record(5, 0.01, -0.01),
        _record(6, -0.02, 0.03),
    ]


def test_history_emits_every_complete_rolling_window() -> None:
    output = build_portfolio_attribution_history(
        _records(),
        definition=_definition(),
        covariance_window=3,
        end_date=date(2026, 1, 6),
    )

    assert [snapshot["ts_event"].date() for snapshot in output.snapshots] == [
        date(2026, 1, 4),
        date(2026, 1, 5),
        date(2026, 1, 6),
    ]
    assert [
        snapshot["window_start"].date() for snapshot in output.snapshots
    ] == [
        date(2026, 1, 2),
        date(2026, 1, 3),
        date(2026, 1, 4),
    ]
    assert len({snapshot["calculation_id"] for snapshot in output.snapshots}) == 3
    assert output.diagnostics["available_snapshot_dates"] == 3
    assert output.diagnostics["snapshots_selected"] == 3


def test_start_date_filters_emission_but_preserves_prior_window_context() -> None:
    output = build_portfolio_attribution_history(
        _records(),
        definition=_definition(),
        covariance_window=3,
        start_date=date(2026, 1, 6),
        end_date=date(2026, 1, 6),
    )

    assert len(output.snapshots) == 1
    assert output.snapshots[0]["window_start"].date() == date(2026, 1, 4)
    assert output.snapshots[0]["ts_event"].date() == date(2026, 1, 6)
    assert output.diagnostics["snapshots_skipped_before_start_date"] == 2


def test_history_is_input_order_independent() -> None:
    forward = build_portfolio_attribution_history(
        _records(),
        definition=_definition(),
        covariance_window=3,
    )
    reverse = build_portfolio_attribution_history(
        reversed(_records()),
        definition=_definition(),
        covariance_window=3,
    )

    assert forward == reverse


def test_history_range_and_snapshot_limit_fail_closed() -> None:
    with pytest.raises(ValidationError, match="start_date"):
        build_portfolio_attribution_history(
            _records(),
            definition=_definition(),
            covariance_window=3,
            start_date=date(2026, 1, 6),
            end_date=date(2026, 1, 5),
        )

    with pytest.raises(ValidationError, match="exceeds max_snapshots"):
        build_portfolio_attribution_history(
            _records(),
            definition=_definition(),
            covariance_window=3,
            max_snapshots=2,
        )
