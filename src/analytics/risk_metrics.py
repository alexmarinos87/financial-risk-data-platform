from __future__ import annotations

import math
from numbers import Real

import numpy as np
import pandas as pd

from ..common.exceptions import ValidationError


def value_at_risk(returns: pd.Series, confidence: float = 0.95) -> float:
    """Return a finite signed lower-tail quantile, not a nonnegative loss.

    Missing observations are excluded, but an all-missing nonempty history is
    invalid. The historical empty-Series 0.0 sentinel is retained for callers;
    it does not establish adequate observations or a healthy risk state.
    """
    if isinstance(confidence, bool) or not isinstance(confidence, Real):
        raise ValidationError("Risk confidence must be a finite number between zero and one")
    try:
        selected_confidence = float(confidence)
    except (ValueError, OverflowError):
        raise ValidationError("Risk confidence must be a finite number between zero and one") from None
    if not math.isfinite(selected_confidence) or not 0 < selected_confidence < 1:
        raise ValidationError("Risk confidence must be a finite number between zero and one")
    if not isinstance(returns, pd.Series):
        raise ValidationError("Risk observations must be a pandas Series")
    if returns.empty:
        return 0.0
    # Inspect raw scalars: pandas 2 can classify infinity as missing under a
    # process-wide option. Filtering first could turn invalid history into zero.
    observations: list[float] = []
    for value in returns:
        if value is None or value is pd.NA or value is pd.NaT:
            continue
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValidationError("Risk observations must be finite real numbers")
        try:
            numeric_value = float(value)
        except (TypeError, ValueError, OverflowError):
            raise ValidationError("Risk observations must be finite real numbers") from None
        if math.isnan(numeric_value):
            continue
        if not math.isfinite(numeric_value):
            raise ValidationError("Risk observations must be finite real numbers")
        observations.append(numeric_value)
    if not observations:
        raise ValidationError("Risk history contains no observed returns")
    try:
        # Convert before interpolation so signed/unsigned integer subtraction
        # cannot wrap. Normal pipeline float64 observations remain unchanged.
        numeric = pd.Series(observations, dtype="float64")
        with np.errstate(over="ignore", invalid="ignore"):
            result = float(numeric.quantile(1 - selected_confidence))
    except (TypeError, ValueError, OverflowError, FloatingPointError):
        raise ValidationError("Risk quantile could not be calculated as a finite number") from None
    if not math.isfinite(result):
        raise ValidationError("Risk quantile could not be calculated as a finite number")
    return result
