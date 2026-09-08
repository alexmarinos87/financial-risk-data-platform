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
    observations = returns.dropna()
    if observations.empty:
        raise ValidationError("Risk history contains no observed returns")
    for value in observations:
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValidationError("Risk observations must be finite real numbers")
        try:
            finite = math.isfinite(float(value))
        except (ValueError, OverflowError):
            finite = False
        if not finite:
            raise ValidationError("Risk observations must be finite real numbers")
    try:
        # Convert before interpolation so signed/unsigned integer subtraction
        # cannot wrap. Normal pipeline float64 observations remain unchanged.
        numeric = observations.astype("float64")
        with np.errstate(over="ignore", invalid="ignore"):
            result = float(numeric.quantile(1 - selected_confidence))
    except (TypeError, ValueError, OverflowError, FloatingPointError):
        raise ValidationError("Risk quantile could not be calculated as a finite number") from None
    if not math.isfinite(result):
        raise ValidationError("Risk quantile could not be calculated as a finite number")
    return result
