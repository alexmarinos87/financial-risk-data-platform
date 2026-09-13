"""Risk validation must precede any settings-dependent missing-value filtering."""

from __future__ import annotations

import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd
import pytest

from src.analytics.risk_metrics import value_at_risk
from src.common.exceptions import ValidationError

OBSERVATION_ERROR = "^Risk observations must be finite real numbers$"


@contextmanager
def missing_option(enabled: bool) -> Iterator[None]:
    # pandas 3 removed this option. The same contracts still run there; pandas
    # 2 additionally exercises the real legacy setting, without skipping tests.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="use_inf_as_na option is deprecated")
        try:
            previous = pd.get_option("mode.use_inf_as_na")
        except pd.errors.OptionError:
            yield
            return
        with pd.option_context("mode.use_inf_as_na", enabled):
            yield
            assert pd.get_option("mode.use_inf_as_na") is enabled
        assert pd.get_option("mode.use_inf_as_na") is previous


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("dtype", ["float64", "float32", "Float64", "object", "category"])
@pytest.mark.parametrize("bad", [float("inf"), -float("inf")])
def test_infinity_cannot_disappear_before_validation(
    enabled: bool, dtype: str, bad: float, monkeypatch: pytest.MonkeyPatch,
) -> None:
    series = pd.Series([0.0, bad, 0.0], dtype=dtype, index=[5, 5, 1], name="returns")
    before = series.copy(deep=True)
    calls: list[bool] = []

    def quantile(*args: Any, **kwargs: Any) -> float:
        calls.append(True)
        return 0.0

    monkeypatch.setattr(pd.Series, "quantile", quantile)
    with missing_option(enabled):
        with pytest.raises(ValidationError, match=OBSERVATION_ERROR):
            value_at_risk(series)
    assert calls == []
    pd.testing.assert_series_equal(series, before)


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("dtype", ["float64", "Float64", "object", "category"])
def test_real_missing_points_remain_excluded(enabled: bool, dtype: str) -> None:
    series = pd.Series([-0.1, None, 0.2], dtype=dtype, index=[8, 8, 3], name="returns")
    before = series.copy(deep=True)
    with missing_option(enabled):
        assert value_at_risk(series, 0.5) == pytest.approx(0.05)
    pd.testing.assert_series_equal(series, before)


@pytest.mark.parametrize("missing", [None, pd.NA, pd.NaT, np.nan, np.float32("nan")])
def test_explicit_missing_sentinels_are_not_observations(missing: Any) -> None:
    assert value_at_risk(pd.Series([1.0, missing, 3.0], dtype="object"), 0.5) == 2.0
    with pytest.raises(ValidationError, match="no observed returns"):
        value_at_risk(pd.Series([missing], dtype="object"))


@pytest.mark.parametrize("value", [
    Decimal("NaN"), complex(float("nan"), 0.0), np.datetime64("NaT"), "NaN",
])
def test_unsupported_values_are_not_silently_reclassified_as_missing(value: Any) -> None:
    with pytest.raises(ValidationError, match=OBSERVATION_ERROR):
        value_at_risk(pd.Series([0.0, value, 0.0], dtype="object"))


@pytest.mark.parametrize("bad", [float("inf"), -float("inf")])
def test_validation_does_not_delegate_missing_policy_to_dropna(
    bad: float, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[bool] = []

    def discard_everything(self: pd.Series, *args: Any, **kwargs: Any) -> pd.Series:
        calls.append(True)
        return self.iloc[0:0]

    monkeypatch.setattr(pd.Series, "dropna", discard_everything)
    with pytest.raises(ValidationError, match=OBSERVATION_ERROR):
        value_at_risk(pd.Series([0.0, bad, 0.0]))
    assert calls == []
