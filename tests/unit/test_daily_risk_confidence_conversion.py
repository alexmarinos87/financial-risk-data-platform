"""Confidence conversion must fail through the public validation contract."""

from __future__ import annotations

import math
from collections.abc import Iterator
from typing import Any

import pytest

from src.analytics.daily_risk import build_daily_risk_outputs
from src.common.exceptions import ValidationError
from src.ingestion.schemas import MarketEvent
from test_daily_risk_finite_outputs import events

CONFIDENCE_ERROR = "var_confidence must be a number between 0 and 1"
OVERSIZED_CONFIDENCES = [
    pytest.param(10**1000, id="positive-overflow"),
    pytest.param(-(10**1000), id="negative-overflow"),
]


@pytest.mark.parametrize("confidence", OVERSIZED_CONFIDENCES)
@pytest.mark.parametrize("prices", [[], [100.0, 101.0], [100.0, 110.0, 99.0, 108.9]])
def test_oversized_confidence_uses_public_validation_error(
    confidence: int, prices: list[float],
) -> None:
    with pytest.raises(ValidationError) as error:
        build_daily_risk_outputs(events(prices), var_confidence=confidence)
    assert str(error.value) == CONFIDENCE_ERROR
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__ is True


@pytest.mark.parametrize("confidence", OVERSIZED_CONFIDENCES)
def test_invalid_confidence_rejects_before_consuming_history(confidence: int) -> None:
    consumed: list[bool] = []

    def history() -> Iterator[MarketEvent]:
        consumed.append(True)
        yield from events([100.0, 101.0])

    with pytest.raises(ValidationError, match=f"^{CONFIDENCE_ERROR}$"):
        build_daily_risk_outputs(history(), var_confidence=confidence)
    assert consumed == []


@pytest.mark.parametrize("failure", [TypeError, ValueError, OverflowError])
def test_conversion_failures_do_not_expose_custom_numeric_diagnostics(
    failure: type[Exception],
) -> None:
    class InvalidFloat(float):
        def __float__(self) -> float:
            raise failure("synthetic-private-conversion-detail")

    with pytest.raises(ValidationError) as error:
        build_daily_risk_outputs([], var_confidence=InvalidFloat(0.95))
    assert str(error.value) == CONFIDENCE_ERROR
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__ is True


def test_unexpected_conversion_failure_is_not_reclassified_as_invalid_input() -> None:
    class BrokenFloat(float):
        def __float__(self) -> float:
            raise RuntimeError("unexpected conversion failure")

    with pytest.raises(RuntimeError, match="^unexpected conversion failure$"):
        build_daily_risk_outputs([], var_confidence=BrokenFloat(0.95))


@pytest.mark.parametrize("confidence", [0.5, 0.95, 0.99])
def test_valid_numeric_subclass_preserves_complete_output(confidence: float) -> None:
    class Confidence(float):
        pass

    source = events([100.0, 110.0, 99.0, 108.9])
    before = [event.model_dump() for event in source]
    options: dict[str, Any] = {"volatility_window": 2, "var_window": 2}
    ordinary = build_daily_risk_outputs(source, var_confidence=confidence, **options)
    converted = build_daily_risk_outputs(source, var_confidence=Confidence(confidence), **options)
    assert converted == ordinary
    assert [event.model_dump() for event in source] == before


@pytest.mark.parametrize("confidence", [math.nextafter(0.0, 1.0), math.nextafter(1.0, 0.0)])
@pytest.mark.parametrize("prices", [[100.0, 101.0], [100.0, 110.0, 99.0, 108.9]])
def test_open_interval_boundaries_still_accept_finite_floats(
    confidence: float, prices: list[float],
) -> None:
    output = build_daily_risk_outputs(
        events(prices), var_confidence=confidence, volatility_window=2, var_window=2,
    )
    assert len(output.returns) == len(prices) - 1
    assert all(row["var_confidence"] == confidence for row in output.risk_summary)
