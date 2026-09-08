from __future__ import annotations

import math
from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import pytest

from src.analytics import daily_risk
from src.common.exceptions import ValidationError
from src.ingestion.schemas import MarketEvent


def events(prices: list[float], *, symbol: str = "IBM") -> list[MarketEvent]:
    return [
        MarketEvent(
            event_id=f"event-{i}" if symbol == "IBM" else f"{symbol}-{i}",
            symbol=symbol, price=price, volume=1, source="alpha_vantage",
            ts_event=datetime(2026, 1, i, tzinfo=timezone.utc),
            ts_ingest=datetime(2026, 1, i, 1, tzinfo=timezone.utc),
        ) for i, price in enumerate(prices, 1)
    ]


def build(prices: list[float], **kwargs: Any) -> daily_risk.DailyRiskOutputs:
    return daily_risk.build_daily_risk_outputs(
        events(prices), volatility_window=2, var_window=2, **kwargs,
    )


@pytest.mark.parametrize("prices", [
    [1e-308, 1e308], [1e-308, 1e308, 1e308], [1, 1e-308, 1e308],
])
def test_finite_positive_prices_cannot_emit_infinite_returns(prices: list[float]) -> None:
    assert all(math.isfinite(price) and price > 0 for price in prices)
    with pytest.raises(ValidationError, match="Daily returns"):
        build(prices)


def test_two_observation_history_is_checked_without_quantile_call(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("short history must not calculate a quantile")

    monkeypatch.setattr(daily_risk, "value_at_risk", forbidden)
    with pytest.raises(ValidationError, match="Daily returns"):
        build([1e-308, 1e308])


def test_finite_returns_can_still_overflow_sample_variance() -> None:
    prices = [1.0, 1e200, 1.0]
    returns = pd.Series(prices).pct_change(fill_method=None).iloc[1:]
    assert all(math.isfinite(value) for value in returns)
    with pytest.raises(ValidationError, match="Daily volatility"):
        build(prices)


def test_start_date_cannot_hide_nonfinite_retained_history() -> None:
    with pytest.raises(ValidationError, match="Daily returns"):
        build([1e-308, 1e308, 1e308, 1e308], start_date=date(2026, 1, 4))


def test_end_date_still_excludes_future_prices() -> None:
    output = build([1e-308, 1e-308, 1e308], end_date=date(2026, 1, 2))
    assert len(output.returns) == 1
    assert output.returns[0]["return_1d"] == 0.0
    assert output.risk_summary[0]["historical_var_loss"] is None


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_quantile_is_checked_before_the_loss_clamp(monkeypatch: pytest.MonkeyPatch, value: float) -> None:
    monkeypatch.setattr(daily_risk, "value_at_risk", lambda *args, **kwargs: value)
    with pytest.raises(ValidationError, match="quantile is not finite"):
        build([100, 110, 99])


@pytest.mark.parametrize(("quantile", "loss"), [(0.1, 0.0), (-0.1, 0.1), (0.0, 0.0)])
def test_valid_quantile_sign_convention_is_unchanged(
    monkeypatch: pytest.MonkeyPatch, quantile: float, loss: float,
) -> None:
    monkeypatch.setattr(daily_risk, "value_at_risk", lambda *args, **kwargs: quantile)
    assert build([100, 110, 99]).risk_summary[-1]["historical_var_loss"] == loss


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_missing_or_nonfinite_warmed_volatility_is_not_warmup(
    monkeypatch: pytest.MonkeyPatch, value: float,
) -> None:
    rolling_type = type(pd.Series([1.0]).rolling(2))
    monkeypatch.setattr(rolling_type, "std", lambda *args, **kwargs: pd.Series([np.nan, np.nan, value]))
    with pytest.raises(ValidationError, match="Daily volatility"):
        build([100, 110, 99])


def test_invalid_drawdown_cannot_be_hidden_by_cumulative_minimum(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pd.Series, "cummax", lambda *args, **kwargs: pd.Series([100.0, 0.0, 110.0]))
    with pytest.raises(ValidationError, match="Daily drawdown"):
        build([100, 110, 99])


@pytest.mark.parametrize("window", [2, 3, 20])
def test_expected_warmup_remains_explicit(window: int) -> None:
    output = daily_risk.build_daily_risk_outputs(
        events([100, 101]), volatility_window=window, var_window=window,
    )
    assert output.volatility == ()
    summary = output.risk_summary[0]
    assert summary["volatility_annualized"] is None
    assert summary["historical_var_loss"] is None
    assert summary["history_status"] == "partial"
    assert math.isfinite(summary["return_1d"])


@pytest.mark.parametrize("prices", [[100, 100, 100, 100], [100, 110, 121, 133.1],
                                    [100, 90, 81, 72.9], [100, 110, 99, 108.9]])
def test_ordinary_output_metrics_remain_finite_and_input_unchanged(prices: list[float]) -> None:
    source = events(prices)
    before = [event.model_dump() for event in source]
    output = daily_risk.build_daily_risk_outputs(source, volatility_window=2, var_window=2)
    assert [event.model_dump() for event in source] == before
    for rows in asdict(output).values():
        for row in rows:
            assert all(math.isfinite(value) for value in row.values() if isinstance(value, float))
    replay = daily_risk.build_daily_risk_outputs(reversed(source), volatility_window=2, var_window=2)
    assert replay == output


def test_ordinary_golden_calculation_ids_and_metrics_are_unchanged() -> None:
    output = build([100, 110, 99, 108.9])
    assert daily_risk.MODEL_VERSION == "daily-risk-v2"
    assert [r["calculation_id"] for r in output.risk_summary] == [
        "daily-risk-v2-summary-266a81273ddc59da5c1ed81c",
        "daily-risk-v2-summary-2bfe4cabbf730ed87aa673e0",
        "daily-risk-v2-summary-c814cee1e10401b49f84f60a",
    ]
    assert output.risk_summary[-1]["historical_var_loss"] == pytest.approx(0.09)
    assert output.risk_summary[-1]["maximum_drawdown"] == pytest.approx(-0.1)
    assert output.risk_summary[-1]["history_status"] == "ready"


def test_numeric_failure_is_a_validation_error_under_strict_numpy_policy() -> None:
    with np.errstate(over="raise", invalid="raise", divide="raise"):
        with pytest.raises(ValidationError, match="Daily returns"):
            build([1e-308, 1e308])


def test_failure_does_not_disclose_source_identity() -> None:
    with pytest.raises(ValidationError) as error:
        daily_risk.build_daily_risk_outputs(
            events([1e-308, 1e308], symbol="PRIVATE-SYMBOL"), volatility_window=2, var_window=2,
        )
    assert str(error.value) == "Daily returns must contain only finite calculated values"
    assert "PRIVATE" not in str(error.value)
