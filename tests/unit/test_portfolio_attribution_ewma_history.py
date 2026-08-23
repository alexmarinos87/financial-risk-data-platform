from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from src.analytics.portfolio_attribution_ewma import MODEL_VERSION
from src.analytics.portfolio_attribution_ewma_history import (
    build_portfolio_ewma_attribution_history,
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
        "weights_json": json.dumps(
            {"alpha_vantage:AAPL": 0.5, "alpha_vantage:MSFT": 0.5},
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
        _record(3, -0.04, 0.0),
        _record(4, 0.06, 0.02),
        _record(5, 0.01, -0.01),
        _record(6, -0.02, 0.03),
    ]


def test_ewma_history_emits_every_complete_window_in_date_order() -> None:
    output = build_portfolio_ewma_attribution_history(
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
    assert all(
        snapshot["model_version"] == MODEL_VERSION
        for snapshot in output.snapshots
    )
    assert len({snapshot["calculation_id"] for snapshot in output.snapshots}) == 3
    assert output.diagnostics["snapshots_selected"] == 3
    assert output.diagnostics["available_snapshot_dates"] == 3


def test_start_date_filters_outputs_but_preserves_prior_window_context() -> None:
    output = build_portfolio_ewma_attribution_history(
        _records(),
        definition=_definition(),
        covariance_window=3,
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 6),
    )

    assert [snapshot["ts_event"].date() for snapshot in output.snapshots] == [
        date(2026, 1, 5),
        date(2026, 1, 6),
    ]
    assert json.loads(output.snapshots[0]["input_calculation_ids_json"]) == [
        "portfolio-3",
        "portfolio-4",
        "portfolio-5",
    ]
    assert output.diagnostics["snapshots_skipped_before_start_date"] == 1


def test_input_order_does_not_change_ewma_history() -> None:
    forward = build_portfolio_ewma_attribution_history(
        _records(),
        definition=_definition(),
        covariance_window=3,
    )
    reverse = build_portfolio_ewma_attribution_history(
        reversed(_records()),
        definition=_definition(),
        covariance_window=3,
    )

    assert forward.snapshots == reverse.snapshots
    assert forward.diagnostics == reverse.diagnostics


def test_late_correction_changes_every_affected_window_identity() -> None:
    original = build_portfolio_ewma_attribution_history(
        _records(),
        definition=_definition(),
        covariance_window=3,
    )
    corrected_records = [
        *_records(),
        _record(
            4,
            0.20,
            0.02,
            calculation_id="portfolio-4-corrected",
            ingested_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        ),
    ]
    corrected = build_portfolio_ewma_attribution_history(
        corrected_records,
        definition=_definition(),
        covariance_window=3,
    )

    assert [snapshot["ts_event"] for snapshot in original.snapshots] == [
        snapshot["ts_event"] for snapshot in corrected.snapshots
    ]
    assert all(
        original_snapshot["calculation_id"]
        != corrected_snapshot["calculation_id"]
        for original_snapshot, corrected_snapshot in zip(
            original.snapshots,
            corrected.snapshots,
            strict=True,
        )
    )


def test_invalid_ranges_caps_and_missing_windows_fail_before_output() -> None:
    with pytest.raises(ValidationError, match="start_date"):
        build_portfolio_ewma_attribution_history(
            _records(),
            definition=_definition(),
            covariance_window=3,
            start_date=date(2026, 1, 6),
            end_date=date(2026, 1, 5),
        )

    with pytest.raises(ValidationError, match="exceeds max_snapshots"):
        build_portfolio_ewma_attribution_history(
            _records(),
            definition=_definition(),
            covariance_window=3,
            max_snapshots=2,
        )

    with pytest.raises(ValidationError, match="requires at least 6"):
        build_portfolio_ewma_attribution_history(
            _records(),
            definition=_definition(),
            covariance_window=6,
        )

    with pytest.raises(ValidationError, match="no EWMA"):
        build_portfolio_ewma_attribution_history(
            _records(),
            definition=_definition(),
            covariance_window=3,
            start_date=date(2026, 2, 1),
        )
