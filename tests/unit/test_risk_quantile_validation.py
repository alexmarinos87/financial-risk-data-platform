from __future__ import annotations

from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd
import pytest

from src.analytics.risk_metrics import value_at_risk
from src.common.exceptions import ValidationError


@pytest.mark.parametrize("values", [
    [-0.2, -0.1, 0.05, 0.1], [0.1, 0.2, 0.3], [-0.3, -0.2, -0.1],
    [0.0, 0.0], [0.25], [-0.1, np.nan, 0.2],
])
@pytest.mark.parametrize("confidence", [0.5, 0.95, 0.99])
def test_finite_float_history_preserves_pandas_quantile(values: list[float], confidence: float) -> None:
    series = pd.Series(values, dtype="float64", name="returns", index=range(20, 20 + len(values)))
    before = series.copy(deep=True)
    result = value_at_risk(series, confidence)
    assert type(result) is float
    assert result == float(series.quantile(1 - confidence))
    pd.testing.assert_series_equal(series, before)


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("confidence", [True, False, np.bool_(True), 0, 1, -0.1, 1.1,
                                       float("nan"), float("inf"), -float("inf"),
                                       "0.95", None, [], 10**1000])
def test_invalid_confidence_is_checked_even_for_empty_input(empty: bool, confidence: Any) -> None:
    series = pd.Series([] if empty else [0.1, 0.2], dtype="float64")
    with pytest.raises(ValidationError, match="confidence"):
        value_at_risk(series, confidence)


@pytest.mark.parametrize("values", [None, [], [0.1], {}, np.array([0.1]), pd.DataFrame({"x": [0.1]})])
def test_input_must_be_a_series(values: Any) -> None:
    with pytest.raises(ValidationError, match="pandas Series"):
        value_at_risk(values)


@pytest.mark.parametrize("series", [pd.Series(dtype="float64"), pd.Series(dtype="object")])
def test_actual_empty_input_keeps_legacy_sentinel(series: pd.Series) -> None:
    assert value_at_risk(series) == 0.0


@pytest.mark.parametrize("series", [pd.Series([float("nan")]), pd.Series([None], dtype="object"),
                                    pd.Series([pd.NA, pd.NA], dtype="Float64")])
def test_all_missing_nonempty_input_is_not_a_zero_risk_estimate(series: pd.Series) -> None:
    with pytest.raises(ValidationError, match="no observed returns"):
        value_at_risk(series)


@pytest.mark.parametrize("value", [float("inf"), -float("inf"), True, np.bool_(False),
                                   "private-return", complex(1, 2), Decimal("0.1"), 10**1000])
def test_invalid_observations_fail_without_echoing_values(value: Any) -> None:
    with pytest.raises(ValidationError) as error:
        value_at_risk(pd.Series([0.1, value], dtype="object"))
    assert str(error.value) == "Risk observations must be finite real numbers"
    assert "private" not in str(error.value)


@pytest.mark.parametrize("dtype", ["float32", "int64", "uint64", "Float64", "Int64", "object"])
def test_real_numeric_representations(dtype: str) -> None:
    series = pd.Series([1, 2, 3], dtype=dtype)
    assert value_at_risk(series, np.float64(0.5)) == 2.0


def test_nullable_missing_values_are_excluded_without_mutation() -> None:
    series = pd.Series([1, pd.NA, 3], dtype="Int64")
    before = series.copy(deep=True)
    assert value_at_risk(series, 0.5) == 2.0
    pd.testing.assert_series_equal(series, before)


def test_integer_interpolation_does_not_wrap() -> None:
    values = pd.Series([-(2**63), 2**63 - 1], dtype="int64")
    # The public return is a binary64 float, not arbitrary-precision integer arithmetic.
    assert value_at_risk(values, 0.5) == 0.0


@pytest.mark.parametrize("confidence", [0.5, 0.95])
def test_finite_extreme_interpolation_cannot_emit_infinity(confidence: float) -> None:
    with pytest.raises(ValidationError, match="finite number"):
        value_at_risk(pd.Series([-1e308, 1e308]), confidence)


@pytest.mark.parametrize("bad_result", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_backend_result_is_rejected(monkeypatch: pytest.MonkeyPatch, bad_result: float) -> None:
    monkeypatch.setattr(pd.Series, "quantile", lambda *args, **kwargs: bad_result)
    with pytest.raises(ValidationError, match="finite number"):
        value_at_risk(pd.Series([0.1, 0.2]))


def test_interpolation_failure_uses_fixed_diagnostic(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: Any, **kwargs: Any) -> Any:
        raise FloatingPointError("private-backend-detail")

    monkeypatch.setattr(pd.Series, "quantile", fail)
    with pytest.raises(ValidationError) as error:
        value_at_risk(pd.Series([0.1, 0.2]))
    assert str(error.value) == "Risk quantile could not be calculated as a finite number"
